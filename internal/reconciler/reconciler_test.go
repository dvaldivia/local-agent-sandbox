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
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
	"github.com/dvaldivia/local-agent-sandbox/internal/driver"
	"github.com/dvaldivia/local-agent-sandbox/internal/store"
)

func newHarness(t *testing.T) (*store.Store, *driver.FakeDriver, *Reconcilers) {
	t.Helper()
	st := store.New(store.Options{})
	reg := apis.NewDefaultRegistry()
	for _, res := range reg.All() {
		st.Register(res.GVR, res.Namespaced)
	}
	fd := driver.NewFakeDriver()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := &Reconcilers{Store: st, Driver: fd, Log: log, RuntimeImage: "img"}
	st.SetDeleteHook(r.OnDelete)
	m := NewManager(st, log)
	r.Wire(m)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = m.Start(ctx) }()
	r.StartDriverEvents(ctx)
	return st, fd, r
}

func mkTemplate(t *testing.T, st *store.Store, ns, name string) {
	t.Helper()
	tmpl := &extv1beta1.SandboxTemplate{}
	tmpl.APIVersion = apis.ExtensionsGV()
	tmpl.Kind = "SandboxTemplate"
	tmpl.Namespace = ns
	tmpl.Name = name
	tmpl.Spec.PodTemplate.Spec.Containers = []corev1.Container{{
		Name: "runtime", Image: "img",
		Ports: []corev1.ContainerPort{{ContainerPort: 8888}},
	}}
	tmpl.Spec.EnvVarsInjectionPolicy = extv1beta1.EnvVarsInjectionPolicyAllowed
	tmpl.Spec.VolumeClaimTemplatesPolicy = extv1beta1.VolumeClaimTemplatesPolicyAllowed
	u, err := toUnstructured(tmpl)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(apis.TemplateGVR, u); err != nil {
		t.Fatal(err)
	}
}

// mkTemplatePVCRef creates a template whose pod references a pre-existing PVC
// by claimName and mounts it at /shared.
func mkTemplatePVCRef(t *testing.T, st *store.Store, ns, name, claimName string) {
	t.Helper()
	tmpl := &extv1beta1.SandboxTemplate{}
	tmpl.APIVersion = apis.ExtensionsGV()
	tmpl.Kind = "SandboxTemplate"
	tmpl.Namespace = ns
	tmpl.Name = name
	tmpl.Spec.PodTemplate.Spec.Volumes = []corev1.Volume{{
		Name: "shared",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimName},
		},
	}}
	tmpl.Spec.PodTemplate.Spec.Containers = []corev1.Container{{
		Name: "runtime", Image: "img",
		Ports:        []corev1.ContainerPort{{ContainerPort: 8888}},
		VolumeMounts: []corev1.VolumeMount{{Name: "shared", MountPath: "/shared"}},
	}}
	u, err := toUnstructured(tmpl)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(apis.TemplateGVR, u); err != nil {
		t.Fatal(err)
	}
}

func mkPVC(t *testing.T, st *store.Store, ns, name string) {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{}
	pvc.APIVersion = "v1"
	pvc.Kind = "PersistentVolumeClaim"
	pvc.Namespace = ns
	pvc.Name = name
	pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	u, err := toUnstructured(pvc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(apis.PVCGVR, u); err != nil {
		t.Fatal(err)
	}
}

func unstructuredNestedString(u *unstructured.Unstructured, fields ...string) (string, bool, error) {
	return unstructured.NestedString(u.Object, fields...)
}

func findClaimReady(c *extv1beta1.SandboxClaim) *metav1.Condition {
	return meta.FindStatusCondition(c.Status.Conditions, "Ready")
}

func hasVolume(t *testing.T, fd *driver.FakeDriver, name string) bool {
	t.Helper()
	vols, err := fd.ListManagedVolumes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vols {
		if v.Name == name {
			return true
		}
	}
	return false
}

func mkWarmPool(t *testing.T, st *store.Store, ns, name, tmplName string, replicas int32) {
	t.Helper()
	pool := &extv1beta1.SandboxWarmPool{}
	pool.APIVersion = apis.ExtensionsGV()
	pool.Kind = "SandboxWarmPool"
	pool.Namespace = ns
	pool.Name = name
	pool.Spec.Replicas = &replicas
	pool.Spec.TemplateRef = extv1beta1.SandboxTemplateRef{Name: tmplName}
	u, err := toUnstructured(pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(apis.WarmPoolGVR, u); err != nil {
		t.Fatal(err)
	}
}

func mkClaim(t *testing.T, st *store.Store, ns, name, poolName string, mutate func(*extv1beta1.SandboxClaim)) {
	t.Helper()
	claim := &extv1beta1.SandboxClaim{}
	claim.APIVersion = apis.ExtensionsGV()
	claim.Kind = "SandboxClaim"
	claim.Namespace = ns
	claim.Name = name
	claim.Spec.WarmPoolRef = extv1beta1.SandboxWarmPoolRef{Name: poolName}
	if mutate != nil {
		mutate(claim)
	}
	u, err := toUnstructured(claim)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(apis.SandboxClaimGVR, u); err != nil {
		t.Fatal(err)
	}
}

func waitClaim(t *testing.T, st *store.Store, ns, name string, pred func(*extv1beta1.SandboxClaim) bool, timeout time.Duration) *extv1beta1.SandboxClaim {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last *extv1beta1.SandboxClaim
	for time.Now().Before(deadline) {
		u, err := st.Get(apis.SandboxClaimGVR, ns, name)
		if err == nil {
			var c extv1beta1.SandboxClaim
			_ = fromUnstructured(u, &c)
			last = &c
			if pred(&c) {
				return &c
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	if last != nil {
		t.Fatalf("claim %s/%s predicate not met; last conditions=%+v sandbox=%q", ns, name, last.Status.Conditions, last.Status.SandboxStatus.Name)
	}
	t.Fatalf("claim %s/%s never observed", ns, name)
	return nil
}

func claimReady(c *extv1beta1.SandboxClaim) bool {
	return meta.IsStatusConditionTrue(c.Status.Conditions, "Ready")
}

func claimReasonIs(reason string) func(*extv1beta1.SandboxClaim) bool {
	return func(c *extv1beta1.SandboxClaim) bool {
		rc := meta.FindStatusCondition(c.Status.Conditions, "Ready")
		return rc != nil && rc.Reason == reason
	}
}

func TestClaimColdStartToReady(t *testing.T) {
	st, fd, _ := newHarness(t)
	mkTemplate(t, st, "default", "default")
	mkWarmPool(t, st, "default", "default", "default", 0)
	mkClaim(t, st, "default", "sandbox-claim-abc", "default", nil)

	c := waitClaim(t, st, "default", "sandbox-claim-abc", claimReady, 5*time.Second)
	if c.Status.SandboxStatus.Name != "sandbox-claim-abc" {
		t.Errorf("cold-start sandbox name = %q, want claim name", c.Status.SandboxStatus.Name)
	}
	if len(c.Status.SandboxStatus.PodIPs) == 0 {
		t.Error("claim status missing podIPs")
	}
	// Ready reason is forwarded verbatim from the sandbox.
	rc := meta.FindStatusCondition(c.Status.Conditions, "Ready")
	if rc.Reason != sandboxv1beta1.SandboxReasonDependenciesReady {
		t.Errorf("claim Ready reason = %q, want DependenciesReady", rc.Reason)
	}
	// The container exists in the driver.
	if _, err := fd.InspectSandbox(context.Background(), "default", "sandbox-claim-abc"); err != nil {
		t.Errorf("container not created: %v", err)
	}
}

func TestClaimWarmPoolNotFound(t *testing.T) {
	st, _, _ := newHarness(t)
	mkClaim(t, st, "default", "c-nowp", "missing-pool", nil)
	waitClaim(t, st, "default", "c-nowp", claimReasonIs("WarmPoolNotFound"), 5*time.Second)
}

func TestClaimTemplateNotFound(t *testing.T) {
	st, _, _ := newHarness(t)
	mkWarmPool(t, st, "default", "pool-notmpl", "ghost-template", 0)
	mkClaim(t, st, "default", "c-notmpl", "pool-notmpl", nil)
	waitClaim(t, st, "default", "c-notmpl", claimReasonIs("TemplateNotFound"), 5*time.Second)
}

func TestClaimDeleteCascades(t *testing.T) {
	st, fd, _ := newHarness(t)
	mkTemplate(t, st, "default", "default")
	mkWarmPool(t, st, "default", "default", "default", 0)
	mkClaim(t, st, "default", "c-del", "default", nil)
	waitClaim(t, st, "default", "c-del", claimReady, 5*time.Second)

	if _, err := st.Delete(apis.SandboxClaimGVR, "default", "c-del"); err != nil {
		t.Fatal(err)
	}
	// Cascade: sandbox CR gone and container removed.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, gerr := st.Get(apis.SandboxGVR, "default", "c-del")
		_, ierr := fd.InspectSandbox(context.Background(), "default", "c-del")
		if gerr != nil && errors.Is(ierr, driver.ErrNotFound) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("claim delete did not cascade to sandbox + container")
}

func TestWarmPoolMaintainsReplicas(t *testing.T) {
	st, _, _ := newHarness(t)
	mkTemplate(t, st, "default", "tmpl")
	mkWarmPool(t, st, "default", "wp", "tmpl", 2)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		list, _ := st.List(apis.SandboxGVR, "default", store.Selectors{})
		warm := 0
		for _, sb := range list.Items {
			if ownedBy(sb, "SandboxWarmPool", "wp") {
				warm++
			}
		}
		if warm == 2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("warm pool never reached 2 replicas")
}

// TestPVCBackedByVolume: a user-created PVC gets a docker volume and Bound status.
func TestPVCBackedByVolume(t *testing.T) {
	st, fd, _ := newHarness(t)
	mkPVC(t, st, "default", "user-data")

	volName := driver.VolumeName("default", "user-data")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		u, err := st.Get(apis.PVCGVR, "default", "user-data")
		if err == nil && hasVolume(t, fd, volName) {
			if phase, _, _ := unstructuredNestedString(u, "status", "phase"); phase == "Bound" {
				return
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("PVC never became Bound with a backing docker volume")
}

// TestSandboxVCTCreatesPVCObject: a claim with volumeClaimTemplates yields a
// sandbox-owned PVC object (<tmpl>-<sandbox>) + volume; deleting the claim
// cascades to both.
func TestSandboxVCTCreatesPVCObject(t *testing.T) {
	st, fd, _ := newHarness(t)
	mkTemplate(t, st, "default", "default")
	mkWarmPool(t, st, "default", "default", "default", 0)
	mkClaim(t, st, "default", "c-vct", "default", func(c *extv1beta1.SandboxClaim) {
		c.Spec.VolumeClaimTemplates = []sandboxv1beta1.PersistentVolumeClaimTemplate{{
			EmbeddedObjectMetadata: sandboxv1beta1.EmbeddedObjectMetadata{Name: "data"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			},
		}}
	})
	waitClaim(t, st, "default", "c-vct", claimReady, 5*time.Second)

	pvcName := "data-c-vct"
	volName := driver.VolumeName("default", pvcName)
	pu, err := st.Get(apis.PVCGVR, "default", pvcName)
	if err != nil {
		t.Fatalf("VCT PVC object not materialized: %v", err)
	}
	if !ownedBy(pu, "Sandbox", "c-vct") {
		t.Errorf("VCT PVC not owned by the sandbox: %+v", pu.GetOwnerReferences())
	}
	if !hasVolume(t, fd, volName) {
		t.Error("VCT docker volume missing")
	}

	if _, err := st.Delete(apis.SandboxClaimGVR, "default", "c-vct"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, perr := st.Get(apis.PVCGVR, "default", pvcName)
		if perr != nil && !hasVolume(t, fd, volName) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("claim delete did not cascade to the VCT PVC object + volume")
}

// TestSandboxPreexistingPVC: a pod referencing a user PVC by claimName stays
// NotReady until the PVC exists, then mounts its volume; deleting the claim
// leaves the user PVC and volume intact.
func TestSandboxPreexistingPVC(t *testing.T) {
	st, fd, _ := newHarness(t)
	mkTemplatePVCRef(t, st, "default", "pvc-tmpl", "shared-data")
	mkWarmPool(t, st, "default", "pvc-pool", "pvc-tmpl", 0)
	mkClaim(t, st, "default", "c-pre", "pvc-pool", nil)

	// Blocked while the PVC is missing (claim mirrors the sandbox condition).
	c := waitClaim(t, st, "default", "c-pre", claimReasonIs("DependenciesNotReady"), 5*time.Second)
	rc := findClaimReady(c)
	if rc == nil || !strings.Contains(rc.Message, "persistentvolumeclaim") {
		t.Fatalf("expected pvc-not-found message, got %+v", rc)
	}

	// Create the PVC → sandbox proceeds and mounts the derived volume.
	mkPVC(t, st, "default", "shared-data")
	waitClaim(t, st, "default", "c-pre", claimReady, 5*time.Second)
	spec, ok := fd.LastSpec("default", "c-pre")
	if !ok {
		t.Fatal("no container spec recorded")
	}
	wantVol := driver.VolumeName("default", "shared-data")
	found := false
	for _, m := range spec.Mounts {
		if m.VolumeName == wantVol && m.MountPath == "/shared" {
			found = true
		}
	}
	if !found {
		t.Fatalf("container does not mount %s: %+v", wantVol, spec.Mounts)
	}

	// Deleting the claim must NOT delete the user's PVC or its volume.
	if _, err := st.Delete(apis.SandboxClaimGVR, "default", "c-pre"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if _, err := st.Get(apis.PVCGVR, "default", "shared-data"); err != nil {
		t.Errorf("pre-existing PVC was deleted by the sandbox cascade: %v", err)
	}
	if !hasVolume(t, fd, wantVol) {
		t.Error("pre-existing PVC's docker volume was deleted by the sandbox cascade")
	}
}

// TestClaimAdoptsWarmSandbox covers the warm-pool adoption path (replicas>0):
// the claim must adopt a pre-warmed sandbox (keeping its pool-generated name)
// rather than cold-create one named after the claim.
func TestClaimAdoptsWarmSandbox(t *testing.T) {
	st, _, _ := newHarness(t)
	mkTemplate(t, st, "default", "default")
	mkWarmPool(t, st, "default", "default", "default", 1)

	// Wait for the pool to pre-provision a ready warm sandbox.
	var warmName string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && warmName == "" {
		list, _ := st.List(apis.SandboxGVR, "default", store.Selectors{})
		for _, sb := range list.Items {
			if ownedBy(sb, "SandboxWarmPool", "default") && isSandboxReady(sb) {
				warmName = sb.GetName()
			}
		}
		if warmName == "" {
			time.Sleep(15 * time.Millisecond)
		}
	}
	if warmName == "" {
		t.Fatal("warm pool never produced a ready sandbox")
	}

	mkClaim(t, st, "default", "sandbox-claim-adopt", "default", nil)
	c := waitClaim(t, st, "default", "sandbox-claim-adopt", claimReady, 5*time.Second)

	if c.Status.SandboxStatus.Name == "sandbox-claim-adopt" {
		t.Fatalf("expected adoption but got cold-start (sandbox named after claim)")
	}
	if !strings.HasPrefix(c.Status.SandboxStatus.Name, "default-") {
		t.Fatalf("adopted sandbox %q is not the pool-generated name", c.Status.SandboxStatus.Name)
	}
	// The adopted sandbox must now be controller-owned by the claim.
	sbU, err := st.Get(apis.SandboxGVR, "default", c.Status.SandboxStatus.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !ownedBy(sbU, "SandboxClaim", "sandbox-claim-adopt") {
		t.Fatalf("adopted sandbox not owned by the claim: %+v", sbU.GetOwnerReferences())
	}
}

func TestSuspendResume(t *testing.T) {
	st, fd, _ := newHarness(t)
	mkTemplate(t, st, "default", "default")
	mkWarmPool(t, st, "default", "default", "default", 0)
	mkClaim(t, st, "default", "c-suspend", "default", nil)
	waitClaim(t, st, "default", "c-suspend", claimReady, 5*time.Second)

	// Suspend via a spec patch on the Sandbox.
	patch := []byte(`{"spec":{"operatingMode":"Suspended"}}`)
	if _, err := st.Patch(apis.SandboxGVR, "default", "c-suspend", "application/merge-patch+json", patch, false); err != nil {
		t.Fatal(err)
	}
	// Container should be removed and Suspended condition set.
	if !waitSandboxCond(t, st, "default", "c-suspend", "Suspended", metav1.ConditionTrue, 3*time.Second) {
		t.Fatal("sandbox never reported Suspended=True")
	}
	if _, err := fd.InspectSandbox(context.Background(), "default", "c-suspend"); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("container should be removed on suspend, got %v", err)
	}

	// Resume.
	resume := []byte(`{"spec":{"operatingMode":"Running"}}`)
	if _, err := st.Patch(apis.SandboxGVR, "default", "c-suspend", "application/merge-patch+json", resume, false); err != nil {
		t.Fatal(err)
	}
	if !waitSandboxCond(t, st, "default", "c-suspend", "Ready", metav1.ConditionTrue, 3*time.Second) {
		t.Fatal("sandbox did not become Ready again after resume")
	}
}

func TestSandboxFinishedOnDeath(t *testing.T) {
	st, fd, _ := newHarness(t)
	mkTemplate(t, st, "default", "default")
	mkWarmPool(t, st, "default", "default", "default", 0)
	mkClaim(t, st, "default", "c-die", "default", nil)
	waitClaim(t, st, "default", "c-die", claimReady, 5*time.Second)

	fd.KillContainer("default", "c-die", 1)
	if !waitSandboxCond(t, st, "default", "c-die", "Finished", metav1.ConditionTrue, 3*time.Second) {
		t.Fatal("sandbox never reported Finished after container death")
	}
	u, _ := st.Get(apis.SandboxGVR, "default", "c-die")
	var sb sandboxv1beta1.Sandbox
	_ = fromUnstructured(u, &sb)
	fc := meta.FindStatusCondition(sb.Status.Conditions, "Finished")
	if fc == nil || fc.Reason != sandboxv1beta1.SandboxReasonPodFailed {
		t.Errorf("Finished reason = %v, want PodFailed", fc)
	}
}

func waitSandboxCond(t *testing.T, st *store.Store, ns, name, condType string, status metav1.ConditionStatus, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		u, err := st.Get(apis.SandboxGVR, ns, name)
		if err == nil {
			var sb sandboxv1beta1.Sandbox
			_ = fromUnstructured(u, &sb)
			if meta.IsStatusConditionPresentAndEqual(sb.Status.Conditions, condType, status) {
				return true
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	return false
}
