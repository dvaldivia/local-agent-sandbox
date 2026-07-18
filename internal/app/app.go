// Copyright 2026 Daniel Valdivia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package app assembles the lasd server: object store, kube-facade,
// reconcilers driving Docker, and (from phase 4) the data-plane router.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	corev1 "k8s.io/api/core/v1"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
	"github.com/dvaldivia/local-agent-sandbox/internal/config"
	"github.com/dvaldivia/local-agent-sandbox/internal/driver"
	"github.com/dvaldivia/local-agent-sandbox/internal/kubeconfig"
	"github.com/dvaldivia/local-agent-sandbox/internal/kubefacade"
	"github.com/dvaldivia/local-agent-sandbox/internal/portforward"
	"github.com/dvaldivia/local-agent-sandbox/internal/reconciler"
	"github.com/dvaldivia/local-agent-sandbox/internal/router"
	"github.com/dvaldivia/local-agent-sandbox/internal/store"
	"github.com/dvaldivia/local-agent-sandbox/internal/vrouter"
)

// App is the assembled server.
type App struct {
	cfg       config.Config
	log       *slog.Logger
	store     *store.Store
	persister *store.BoltPersister
	facade    *kubefacade.Handler
	driver    driver.Driver
	recon     *reconciler.Reconcilers
	mgr       *reconciler.Manager
}

// New constructs the App: opens persistence, wires the facade, and (if Docker
// is reachable) the reconcilers.
func New(cfg config.Config, log *slog.Logger) (*App, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("app: create data dir: %w", err)
	}
	persister, err := store.OpenBolt(cfg.DBPath())
	if err != nil {
		return nil, fmt.Errorf("app: open state db: %w", err)
	}
	snap, rv, err := persister.Load()
	if err != nil {
		persister.Close()
		return nil, fmt.Errorf("app: load state: %w", err)
	}
	st := store.New(store.Options{Persister: persister, StartRV: rv})
	reg := apis.NewDefaultRegistry()
	// Port-forward server bridges the SDK tunnel to the local router listener.
	pf := portforward.New(net.JoinHostPort("127.0.0.1", itoa(cfg.RouterPort)), log)
	facade := kubefacade.New(kubefacade.Options{Store: st, Registry: reg, Logger: log, PortForward: pf})
	if err := st.LoadFrom(snap); err != nil {
		persister.Close()
		return nil, fmt.Errorf("app: restore objects: %w", err)
	}

	a := &App{cfg: cfg, log: log, store: st, persister: persister, facade: facade}

	// Best-effort Docker driver. Without it, the control plane still serves
	// (useful for kubectl inspection), but sandboxes cannot be materialized.
	if d, derr := driver.NewDockerDriver(); derr == nil {
		pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		perr := d.Ping(pingCtx)
		cancel()
		if perr == nil {
			a.driver = d
			a.recon = &reconciler.Reconcilers{
				Store: st, Driver: d, Log: log,
				DefaultServerPort:   cfg.ServerPort,
				RuntimeImage:        cfg.RuntimeImage,
				RuntimeBuildContext: cfg.RuntimeBuildContext,
				ClusterDomain:       cfg.ClusterDomain,
			}
			st.SetDeleteHook(a.recon.OnDelete)
			a.mgr = reconciler.NewManager(st, log)
			a.recon.Wire(a.mgr)
		} else {
			log.Warn("docker daemon unreachable; running control-plane only", "err", perr)
			_ = d.Close()
		}
	} else {
		log.Warn("docker client unavailable; running control-plane only", "err", derr)
	}

	return a, nil
}

// Store exposes the object store (used by tests and later phases).
func (a *App) Store() *store.Store { return a.store }

// Driver exposes the container driver (may be nil).
func (a *App) Driver() driver.Driver { return a.driver }

// Run starts the API server and reconcilers, blocking until ctx is cancelled.
func (a *App) Run(ctx context.Context) error {
	if err := kubeconfig.Write(a.cfg.APIServerURL(), a.cfg.KubeconfigPath()); err != nil {
		a.log.Warn("could not write kubeconfig", "err", err)
	}

	apiAddr := net.JoinHostPort(a.cfg.Bind, itoa(a.cfg.APIPort))
	ln, err := net.Listen("tcp", apiAddr)
	if err != nil {
		return fmt.Errorf("app: listen %s: %w", apiAddr, err)
	}
	srv := &http.Server{Handler: a.facade}
	errCh := make(chan error, 1)
	go func() {
		a.log.Info("kube-facade listening", "addr", apiAddr, "kubeconfig", a.cfg.KubeconfigPath())
		if serveErr := srv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
		}
	}()

	// Data-plane router.
	routerAddr := net.JoinHostPort(a.cfg.Bind, itoa(a.cfg.RouterPort))
	rln, err := net.Listen("tcp", routerAddr)
	if err != nil {
		a.close(context.Background())
		return fmt.Errorf("app: listen router %s: %w", routerAddr, err)
	}
	rh := router.New(&router.StoreResolver{Store: a.store, Driver: a.driver}, a.log)
	routerSrv := &http.Server{Handler: rh}
	go func() {
		a.log.Info("router listening", "addr", routerAddr)
		if serveErr := routerSrv.Serve(rln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
		}
	}()

	if a.mgr != nil {
		go func() { _ = a.mgr.Start(ctx) }()
		a.recon.StartDriverEvents(ctx)
		a.bootAdoption(ctx)
	}
	if a.cfg.Bootstrap {
		a.bootstrapObjects()
	}

	// Materialize the virtual router objects (Pod/Service/EndpointSlice) that
	// the SDK tunnel and kubectl port-forward discover, and keep them present
	// in every namespace that gets used.
	a.startVirtualRouter(ctx)

	a.printGettingStarted()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		a.close(context.Background())
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	_ = routerSrv.Shutdown(shutdownCtx)
	a.close(shutdownCtx)
	return nil
}

// startVirtualRouter materializes the router Pod/Service/EndpointSlice in the
// default and system namespaces, then watches for new namespaces and
// materializes them there too (the Go SDK lists EndpointSlices in the
// sandbox's own namespace).
func (a *App) startVirtualRouter(ctx context.Context) {
	vrouter.EnsureNamespace(a.store, "default")
	vrouter.EnsureNamespace(a.store, vrouter.SystemNamespace)
	go func() {
		ch, err := a.store.Watch(ctx, apis.NamespaceGVR, store.WatchOptions{})
		if err != nil {
			return
		}
		for ev := range ch {
			if ev.Object != nil {
				vrouter.EnsureNamespace(a.store, ev.Object.GetName())
			}
		}
	}()
}

// bootAdoption reconciles surviving containers against the store: containers we
// manage that have no corresponding Sandbox CR are orphans and get removed.
func (a *App) bootAdoption(ctx context.Context) {
	containers, err := a.driver.ListManaged(ctx)
	if err != nil {
		a.log.Warn("boot adoption: list managed containers", "err", err)
		return
	}
	for _, c := range containers {
		ns, name := c.Namespace(), c.Sandbox()
		if ns == "" || name == "" {
			continue
		}
		if _, err := a.store.Get(apis.SandboxGVR, ns, name); err != nil {
			a.log.Info("boot adoption: removing orphaned container", "namespace", ns, "sandbox", name)
			_ = a.driver.RemoveContainer(ctx, c.ID)
		}
	}
}

// bootstrapObjects creates the default SandboxTemplate + SandboxWarmPool if
// absent, so the zero-config quickstart works.
func (a *App) bootstrapObjects() {
	if _, err := a.store.Get(apis.TemplateGVR, "default", "default"); err != nil {
		tmpl := &extv1beta1.SandboxTemplate{}
		tmpl.APIVersion = apis.ExtensionsGV()
		tmpl.Kind = "SandboxTemplate"
		tmpl.Namespace = "default"
		tmpl.Name = "default"
		tmpl.Spec.PodTemplate.Spec.Containers = []corev1.Container{{
			Name:  "runtime",
			Image: a.cfg.RuntimeImage,
			Ports: []corev1.ContainerPort{{ContainerPort: int32(a.cfg.ServerPort)}},
		}}
		tmpl.Spec.EnvVarsInjectionPolicy = extv1beta1.EnvVarsInjectionPolicyAllowed
		tmpl.Spec.VolumeClaimTemplatesPolicy = extv1beta1.VolumeClaimTemplatesPolicyAllowed
		if u, err := toUnstructured(tmpl); err == nil {
			if _, err := a.store.Create(apis.TemplateGVR, u); err != nil {
				a.log.Warn("bootstrap template", "err", err)
			}
		}
	}
	if _, err := a.store.Get(apis.WarmPoolGVR, "default", "default"); err != nil {
		var zero int32
		pool := &extv1beta1.SandboxWarmPool{}
		pool.APIVersion = apis.ExtensionsGV()
		pool.Kind = "SandboxWarmPool"
		pool.Namespace = "default"
		pool.Name = "default"
		pool.Spec.Replicas = &zero
		pool.Spec.TemplateRef = extv1beta1.SandboxTemplateRef{Name: "default"}
		if u, err := toUnstructured(pool); err == nil {
			if _, err := a.store.Create(apis.WarmPoolGVR, u); err != nil {
				a.log.Warn("bootstrap warmpool", "err", err)
			}
		}
	}
}

func (a *App) close(ctx context.Context) {
	if a.cfg.EphemeralOnShutdown && a.driver != nil {
		a.teardownAll(ctx)
	}
	if a.driver != nil {
		_ = a.driver.Close()
	}
	if a.persister != nil {
		_ = a.persister.Close()
	}
}

func (a *App) teardownAll(ctx context.Context) {
	if cs, err := a.driver.ListManaged(ctx); err == nil {
		for _, c := range cs {
			_ = a.driver.RemoveContainer(ctx, c.ID)
		}
	}
	if vs, err := a.driver.ListManagedVolumes(ctx); err == nil {
		for _, v := range vs {
			_ = a.driver.RemoveVolume(ctx, v.Name)
		}
	}
}

func (a *App) printGettingStarted() {
	fmt.Fprintf(os.Stderr, `
local-agent-sandbox is running.

  export KUBECONFIG=%s

  Go SDK:      sandbox.Options{APIURL: %q, WarmPoolName: "default"}
  Python SDK:  SandboxDirectConnectionConfig(api_url=%q)

`, a.cfg.KubeconfigPath(), a.cfg.RouterURL(), a.cfg.RouterURL())
}

func toUnstructured(obj any) (*unstructured.Unstructured, error) {
	return reconciler.ToUnstructured(obj)
}

func itoa(i int) string { return fmt.Sprintf("%d", i) }
