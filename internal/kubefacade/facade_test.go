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

package kubefacade

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	agentsversioned "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned"
	extversioned "sigs.k8s.io/agent-sandbox/clients/k8s/extensions/clientset/versioned"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
	"github.com/dvaldivia/local-agent-sandbox/internal/store"
)

func newTestServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	reg := apis.NewDefaultRegistry()
	st := store.New(store.Options{})
	h := New(Options{Store: st, Registry: reg})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, st
}

func TestDiscovery(t *testing.T) {
	srv, _ := newTestServer(t)
	c := srv.Client()

	// /apis lists our groups.
	resp, err := c.Get(srv.URL + "/apis")
	if err != nil {
		t.Fatal(err)
	}
	var gl metav1.APIGroupList
	decode(t, resp, &gl)
	foundAgents, foundExt := false, false
	for _, g := range gl.Groups {
		switch g.Name {
		case apis.GroupAgents:
			foundAgents = true
		case apis.GroupExtensions:
			foundExt = true
		}
	}
	if !foundAgents || !foundExt {
		t.Errorf("missing groups: agents=%v ext=%v", foundAgents, foundExt)
	}

	// group/version resource list contains sandboxclaims.
	resp, err = c.Get(srv.URL + "/apis/extensions.agents.x-k8s.io/v1beta1")
	if err != nil {
		t.Fatal(err)
	}
	var rl metav1.APIResourceList
	decode(t, resp, &rl)
	var names []string
	for _, r := range rl.APIResources {
		names = append(names, r.Name)
	}
	if !contains(names, "sandboxclaims") {
		t.Errorf("sandboxclaims not advertised, got %v", names)
	}
	if !contains(names, "sandboxclaims/status") {
		t.Errorf("sandboxclaims/status subresource not advertised, got %v", names)
	}
}

// TestClientsetContract drives the *upstream generated clientsets* through the
// exact call sequence the Go SDK uses (create claim with generateName, watch
// for status, list+watch sandboxes, delete). This is the definitive
// "SDK can't tell the difference" check for the control plane.
func TestClientsetContract(t *testing.T) {
	srv, st := newTestServer(t)
	cfg := &rest.Config{Host: srv.URL}
	extCS, err := extversioned.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	agentsCS, err := agentsversioned.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. Create claim with GenerateName, exactly like clients/go/sandbox/k8s.go.
	claim := &extv1beta1.SandboxClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "sandbox-claim-",
			Namespace:    "default",
			Labels:       map[string]string{sandboxv1beta1.CreatedByLabel: "contract-test"},
		},
		Spec: extv1beta1.SandboxClaimSpec{
			WarmPoolRef: extv1beta1.SandboxWarmPoolRef{Name: "default"},
		},
	}
	created, err := extCS.ExtensionsV1beta1().SandboxClaims("default").Create(ctx, claim, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create claim: %v", err)
	}
	if len(created.Name) != len("sandbox-claim-")+5 || created.Name[:14] != "sandbox-claim-" {
		t.Fatalf("generateName not honored: %q", created.Name)
	}
	claimName := created.Name

	// 2. Watch the claim, then populate status.sandbox.name (as the claim
	// controller would). resolveSandboxName loops until the name appears.
	w, err := extCS.ExtensionsV1beta1().SandboxClaims("default").Watch(ctx, metav1.ListOptions{
		FieldSelector: "metadata.name=" + claimName,
	})
	if err != nil {
		t.Fatalf("watch claim: %v", err)
	}
	defer w.Stop()

	go func() {
		time.Sleep(50 * time.Millisecond)
		u, err := st.Get(apis.SandboxClaimGVR, "default", claimName)
		if err != nil {
			return
		}
		_ = unstructured.SetNestedField(u.Object, "sb-"+claimName, "status", "sandbox", "name")
		_, _ = st.Update(apis.SandboxClaimGVR, u, true)
	}()

	resolved := ""
	deadline := time.After(10 * time.Second)
watchLoop:
	for {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				break watchLoop
			}
			c, ok := ev.Object.(*extv1beta1.SandboxClaim)
			if !ok {
				t.Fatalf("watch returned %T, want *SandboxClaim", ev.Object)
			}
			if c.Status.SandboxStatus.Name != "" {
				resolved = c.Status.SandboxStatus.Name
				break watchLoop
			}
		case <-deadline:
			t.Fatal("timed out waiting for status.sandbox.name via watch")
		}
	}
	if resolved != "sb-"+claimName {
		t.Fatalf("resolved sandbox name = %q, want %q", resolved, "sb-"+claimName)
	}

	// 3. Sandbox list -> RV -> watch(RV) -> ready, like waitForSandboxReady.
	sbName := resolved
	sb := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: "default"},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("default").Create(ctx, sb, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create sandbox: %v", err)
	}
	list, err := agentsCS.AgentsV1beta1().Sandboxes("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list sandboxes: %v", err)
	}
	sbWatch, err := agentsCS.AgentsV1beta1().Sandboxes("default").Watch(ctx, metav1.ListOptions{
		FieldSelector:   "metadata.name=" + sbName,
		ResourceVersion: list.ResourceVersion,
	})
	if err != nil {
		t.Fatalf("watch sandbox: %v", err)
	}
	defer sbWatch.Stop()

	go func() {
		time.Sleep(50 * time.Millisecond)
		u, err := st.Get(apis.SandboxGVR, "default", sbName)
		if err != nil {
			return
		}
		conds := []any{map[string]any{
			"type":               "Ready",
			"status":             "True",
			"reason":             "DependenciesReady",
			"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
			"message":            "Pod is Ready",
		}}
		_ = unstructured.SetNestedSlice(u.Object, conds, "status", "conditions")
		_ = unstructured.SetNestedStringSlice(u.Object, []string{"172.17.0.2"}, "status", "podIPs")
		_, _ = st.Update(apis.SandboxGVR, u, true)
	}()

	ready := false
	deadline = time.After(10 * time.Second)
sbLoop:
	for {
		select {
		case ev, ok := <-sbWatch.ResultChan():
			if !ok {
				break sbLoop
			}
			s, ok := ev.Object.(*sandboxv1beta1.Sandbox)
			if !ok {
				t.Fatalf("watch returned %T, want *Sandbox", ev.Object)
			}
			for _, c := range s.Status.Conditions {
				if c.Type == string(sandboxv1beta1.SandboxConditionReady) && c.Status == metav1.ConditionTrue {
					ready = true
					break sbLoop
				}
			}
		case <-deadline:
			t.Fatal("timed out waiting for sandbox Ready via watch(RV)")
		}
	}
	if !ready {
		t.Fatal("sandbox never became Ready")
	}

	// 4. Delete claim -> subsequent Get is NotFound.
	if err := extCS.ExtensionsV1beta1().SandboxClaims("default").Delete(ctx, claimName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete claim: %v", err)
	}
	_, err = extCS.ExtensionsV1beta1().SandboxClaims("default").Get(ctx, claimName, metav1.GetOptions{})
	if err == nil {
		t.Fatal("expected NotFound after delete")
	}
}

// TestRawWatchTimeoutSeconds verifies the watch stream closes when
// timeoutSeconds elapses (the Python watch relies on this).
func TestRawWatchTimeoutSeconds(t *testing.T) {
	srv, _ := newTestServer(t)
	req, _ := http.NewRequest("GET", srv.URL+"/apis/extensions.agents.x-k8s.io/v1beta1/namespaces/default/sandboxclaims?watch=true&timeoutSeconds=1", nil)
	start := time.Now()
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	// Reading to EOF should return within ~1s once the server closes the stream.
	_, _ = io.Copy(io.Discard, resp.Body)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("watch did not close on timeoutSeconds (took %s)", elapsed)
	}
}

func decode(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
