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
