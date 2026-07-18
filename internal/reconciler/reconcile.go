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

package reconciler

import (
	"context"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
	"github.com/dvaldivia/local-agent-sandbox/internal/driver"
	"github.com/dvaldivia/local-agent-sandbox/internal/store"
)

// Reconcilers holds shared dependencies for the sandbox/claim/warmpool/template
// reconcile loops.
type Reconcilers struct {
	Store  *store.Store
	Driver driver.Driver
	Log    *slog.Logger

	// DefaultServerPort is the runtime port assumed when a pod declares none.
	DefaultServerPort int
	// RuntimeImage is the bundled runtime image tag (for the default template).
	RuntimeImage string
	// RuntimeBuildContext, if set, is the dir to build RuntimeImage from when
	// it is missing (dev/CI). Requires a driver that can build.
	RuntimeBuildContext string
	// ClusterDomain is used to synthesize status.serviceFQDN (default cluster.local).
	ClusterDomain string
	// NodeName is reported in sandbox status (default "local-docker").
	NodeName string
	// Clock is injectable for tests (default time.Now).
	Clock func() time.Time

	mgr *Manager
}

func (r *Reconcilers) now() time.Time {
	if r.Clock != nil {
		return r.Clock()
	}
	return time.Now()
}

func (r *Reconcilers) clusterDomain() string {
	if r.ClusterDomain != "" {
		return r.ClusterDomain
	}
	return "cluster.local"
}

func (r *Reconcilers) nodeName() string {
	if r.NodeName != "" {
		return r.NodeName
	}
	return "local-docker"
}

func (r *Reconcilers) serverPort() int {
	if r.DefaultServerPort != 0 {
		return r.DefaultServerPort
	}
	return 8888
}

// Wire registers all four controllers with their secondary sources.
func (r *Reconcilers) Wire(m *Manager) {
	r.mgr = m
	m.Register("sandbox", apis.SandboxGVR, r.reconcileSandbox, 4)
	m.Register("sandboxclaim", apis.SandboxClaimGVR, r.reconcileClaim, 4,
		Source(apis.SandboxGVR, ownerMap("SandboxClaim")))
	m.Register("sandboxwarmpool", apis.WarmPoolGVR, r.reconcileWarmPool, 2,
		Source(apis.SandboxGVR, ownerMap("SandboxWarmPool")))
	m.Register("sandboxtemplate", apis.TemplateGVR, r.reconcileTemplate, 1)
	m.Register("persistentvolumeclaim", apis.PVCGVR, r.reconcilePVC, 2)
}

// StartDriverEvents bridges Docker lifecycle events into sandbox reconciles so
// a crashed/stopped container promptly updates conditions.
func (r *Reconcilers) StartDriverEvents(ctx context.Context) {
	ch, err := r.Driver.Events(ctx)
	if err != nil {
		r.Log.Warn("driver events unavailable", "err", err)
		return
	}
	go func() {
		for ev := range ch {
			if ev.Sandbox != "" {
				r.mgr.Enqueue("sandbox", ev.Namespace, ev.Sandbox)
			}
		}
	}()
}

// OnDelete is the facade delete hook: it cascades cleanup of Docker resources
// and owned CRs (mirroring Kubernetes garbage collection via ownerReferences).
func (r *Reconcilers) OnDelete(gvr apis.GVR, obj *unstructured.Unstructured) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch gvr {
	case apis.SandboxGVR:
		r.cleanupSandboxResources(ctx, obj)
	case apis.SandboxClaimGVR:
		r.cascadeClaimDelete(ctx, obj)
	case apis.WarmPoolGVR:
		r.cascadeWarmPoolDelete(ctx, obj)
	case apis.PVCGVR:
		r.cleanupPVCVolume(ctx, obj)
	}
}

// cleanupSandboxResources removes the container and VCT-derived PVCs for a
// deleted Sandbox. Pre-existing (user-created) PVCs referenced by the pod are
// deliberately left alone — they outlive the sandbox, like in Kubernetes.
func (r *Reconcilers) cleanupSandboxResources(ctx context.Context, obj *unstructured.Unstructured) {
	ns, name := obj.GetNamespace(), obj.GetName()
	if info, err := r.Driver.InspectSandbox(ctx, ns, name); err == nil {
		_ = r.Driver.RemoveContainer(ctx, info.ID)
	}
	var sb sandboxv1beta1.Sandbox
	if err := fromUnstructured(obj, &sb); err == nil {
		for _, vct := range sb.Spec.VolumeClaimTemplates {
			pvcName := vct.Name + "-" + name
			// Delete the sandbox-owned PVC object (its delete hook removes the
			// docker volume); guard on ownership so a same-named user PVC is
			// never destroyed.
			if pu, gerr := r.Store.Get(apis.PVCGVR, ns, pvcName); gerr == nil {
				if controllerOwnerUID(pu) == string(obj.GetUID()) {
					_, _ = r.Store.Delete(apis.PVCGVR, ns, pvcName)
				}
				continue
			}
			// No PVC object (pre-PVC-facade state): remove the volume directly.
			_ = r.Driver.RemoveVolume(ctx, driver.VolumeName(ns, pvcName))
		}
	}
}

// cleanupPVCVolume removes the docker volume backing a deleted PVC. Best
// effort: docker refuses to remove a volume still mounted by a container; the
// leftover is labeled las.managed and reclaimable via `lasd purge`.
func (r *Reconcilers) cleanupPVCVolume(ctx context.Context, obj *unstructured.Unstructured) {
	vol := driver.VolumeName(obj.GetNamespace(), obj.GetName())
	if err := r.Driver.RemoveVolume(ctx, vol); err != nil {
		r.Log.Warn("pvc delete: could not remove docker volume (possibly in use)", "volume", vol, "err", err)
	}
}

// cascadeClaimDelete deletes the Sandbox owned by a deleted claim (ownership
// verified by controller ownerReference UID so a foreign same-named sandbox is
// never destroyed).
func (r *Reconcilers) cascadeClaimDelete(ctx context.Context, obj *unstructured.Unstructured) {
	sbName, _, _ := unstructured.NestedString(obj.Object, "status", "sandbox", "name")
	if sbName == "" {
		sbName = obj.GetName() // cold-start default
	}
	sbU, err := r.Store.Get(apis.SandboxGVR, obj.GetNamespace(), sbName)
	if err != nil {
		return
	}
	if controllerOwnerUID(sbU) != string(obj.GetUID()) {
		r.Log.Warn("cascade claim delete: sandbox not owned by this claim; skipping", "sandbox", sbName)
		return
	}
	if _, err := r.Store.Delete(apis.SandboxGVR, obj.GetNamespace(), sbName); err != nil && !apierrors.IsNotFound(err) {
		r.Log.Warn("cascade claim delete: sandbox", "err", err)
	}
}

// cascadeWarmPoolDelete deletes warm Sandboxes owned by a deleted pool.
func (r *Reconcilers) cascadeWarmPoolDelete(ctx context.Context, obj *unstructured.Unstructured) {
	list, err := r.Store.List(apis.SandboxGVR, obj.GetNamespace(), store.Selectors{})
	if err != nil {
		return
	}
	for _, sb := range list.Items {
		if ownedBy(sb, "SandboxWarmPool", obj.GetName()) {
			_, _ = r.Store.Delete(apis.SandboxGVR, sb.GetNamespace(), sb.GetName())
		}
	}
}

// ownerMap returns a MapFunc that enqueues the owner (of the given kind) of an
// object as a key on the owning controller's queue.
func ownerMap(kind string) MapFunc {
	return func(obj *unstructured.Unstructured) []string {
		for _, ref := range obj.GetOwnerReferences() {
			if ref.Kind == kind {
				return []string{obj.GetNamespace() + "/" + ref.Name}
			}
		}
		return nil
	}
}

// ownedBy reports whether obj has a controller ownerReference of kind/name.
func ownedBy(obj *unstructured.Unstructured, kind, name string) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Kind == kind && ref.Name == name {
			return true
		}
	}
	return false
}

// controllerOwnerUID returns the UID (as a string) of obj's controller
// ownerReference, or "" if none. Used to distinguish an object owned by *this*
// incarnation of a CR from one owned by a same-named but deleted/recreated CR.
func controllerOwnerUID(obj *unstructured.Unstructured) string {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Controller != nil && *ref.Controller {
			return string(ref.UID)
		}
	}
	return ""
}
