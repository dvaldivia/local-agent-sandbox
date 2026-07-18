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

// Package e2e runs the unmodified upstream agent-sandbox SDKs against the full
// local stack (facade + reconcilers + router + Docker). These tests are opt-in
// (LASD_DOCKER_TESTS=1) and skip when Docker is unavailable.
package e2e

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
	"github.com/dvaldivia/local-agent-sandbox/internal/driver"
	"github.com/dvaldivia/local-agent-sandbox/internal/kubefacade"
	"github.com/dvaldivia/local-agent-sandbox/internal/portforward"
	"github.com/dvaldivia/local-agent-sandbox/internal/reconciler"
	"github.com/dvaldivia/local-agent-sandbox/internal/router"
	"github.com/dvaldivia/local-agent-sandbox/internal/store"
	"github.com/dvaldivia/local-agent-sandbox/internal/vrouter"
)

const e2eImage = "lasd/sandbox-runtime:e2e"

type stack struct {
	Store     *store.Store
	Driver    *driver.DockerDriver
	FacadeURL string
	RouterURL string
}

// newStack assembles the full local stack backed by a real Docker driver, or
// skips the test if Docker/opt-in is unavailable.
func newStack(t *testing.T) *stack {
	t.Helper()
	if os.Getenv("LASD_DOCKER_TESTS") == "" {
		t.Skip("set LASD_DOCKER_TESTS=1 to run E2E tests")
	}
	d, err := driver.NewDockerDriver()
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	pingCtx, pcancel := context.WithTimeout(ctx, 5*time.Second)
	if err := d.Ping(pingCtx); err != nil {
		pcancel()
		t.Skipf("docker daemon unreachable: %v", err)
	}
	pcancel()

	bctx, bcancel := context.WithTimeout(ctx, 5*time.Minute)
	if err := d.BuildRuntimeImage(bctx, e2eImage, runtimeDir(t)); err != nil {
		bcancel()
		t.Fatalf("build runtime image: %v", err)
	}
	bcancel()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	st := store.New(store.Options{})
	reg := apis.NewDefaultRegistry()

	recon := &reconciler.Reconcilers{Store: st, Driver: d, Log: log, RuntimeImage: e2eImage, ClusterDomain: "cluster.local"}
	st.SetDeleteHook(recon.OnDelete)
	mgr := reconciler.NewManager(st, log)
	recon.Wire(mgr)
	go func() { _ = mgr.Start(ctx) }()
	recon.StartDriverEvents(ctx)

	// Router first, so the port-forward server can dial its address.
	rs := httptest.NewServer(router.New(&router.StoreResolver{Store: st, Driver: d}, log))
	t.Cleanup(rs.Close)
	routerHost := mustHost(t, rs.URL)

	pf := portforward.New(routerHost, log)
	facade := kubefacade.New(kubefacade.Options{Store: st, Registry: reg, Logger: log, PortForward: pf})
	fs := httptest.NewServer(facade)
	t.Cleanup(fs.Close)

	bootstrap(t, st)
	// Virtual router objects the SDK tunnel discovers.
	vrouter.EnsureNamespace(st, "default")
	vrouter.EnsureNamespace(st, vrouter.SystemNamespace)

	t.Cleanup(func() {
		cs, _ := d.ListManaged(context.Background())
		for _, c := range cs {
			_ = d.RemoveContainer(context.Background(), c.ID)
		}
		_ = d.Close()
	})

	return &stack{Store: st, Driver: d, FacadeURL: fs.URL, RouterURL: rs.URL}
}

func bootstrap(t *testing.T, st *store.Store) {
	t.Helper()
	tmpl := &extv1beta1.SandboxTemplate{}
	tmpl.APIVersion = apis.ExtensionsGV()
	tmpl.Kind = "SandboxTemplate"
	tmpl.Namespace = "default"
	tmpl.Name = "default"
	tmpl.Spec.PodTemplate.Spec.Containers = []corev1.Container{{
		Name: "runtime", Image: e2eImage,
		Ports: []corev1.ContainerPort{{ContainerPort: 8888}},
	}}
	tmpl.Spec.EnvVarsInjectionPolicy = extv1beta1.EnvVarsInjectionPolicyAllowed
	tmpl.Spec.VolumeClaimTemplatesPolicy = extv1beta1.VolumeClaimTemplatesPolicyAllowed
	tu, _ := reconciler.ToUnstructured(tmpl)
	if _, err := st.Create(apis.TemplateGVR, tu); err != nil {
		t.Fatal(err)
	}

	var zero int32
	pool := &extv1beta1.SandboxWarmPool{}
	pool.APIVersion = apis.ExtensionsGV()
	pool.Kind = "SandboxWarmPool"
	pool.Namespace = "default"
	pool.Name = "default"
	pool.Spec.Replicas = &zero
	pool.Spec.TemplateRef = extv1beta1.SandboxTemplateRef{Name: "default"}
	pu, _ := reconciler.ToUnstructured(pool)
	if _, err := st.Create(apis.WarmPoolGVR, pu); err != nil {
		t.Fatal(err)
	}
}

func runtimeDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(thisFile), "..", "..", "runtime")
	if _, err := os.Stat(filepath.Join(dir, "Dockerfile")); err != nil {
		t.Fatalf("runtime dir not found: %v", err)
	}
	return dir
}

func mustHost(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u.Host
}
