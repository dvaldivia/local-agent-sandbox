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

package store

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
)

var (
	widgetGVR  = apis.GVR{Group: "test.io", Version: "v1", Resource: "widgets"}
	clusterGVR = apis.GVR{Group: "test.io", Version: "v1", Resource: "clusterwidgets"}
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s := New(Options{})
	s.Register(widgetGVR, true)
	s.Register(clusterGVR, false)
	return s
}

func newObj(ns, name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("test.io/v1")
	u.SetKind("Widget")
	if ns != "" {
		u.SetNamespace(ns)
	}
	if name != "" {
		u.SetName(name)
	}
	_ = unstructured.SetNestedField(u.Object, "hello", "spec", "greeting")
	return u
}

func TestCreateAndGet(t *testing.T) {
	s := newTestStore(t)
	created, err := s.Create(widgetGVR, newObj("default", "w1"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.GetUID() == "" {
		t.Error("expected uid to be set")
	}
	if created.GetResourceVersion() == "" {
		t.Error("expected resourceVersion to be set")
	}
	if created.GetGeneration() != 1 {
		t.Errorf("generation = %d, want 1", created.GetGeneration())
	}
	if ct := created.GetCreationTimestamp(); ct.IsZero() {
		t.Error("expected creationTimestamp to be set")
	}
	got, err := s.Get(widgetGVR, "default", "w1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.GetUID() != created.GetUID() {
		t.Error("uid mismatch on get")
	}
}

func TestCreateDuplicate(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Create(widgetGVR, newObj("default", "w1")); err != nil {
		t.Fatal(err)
	}
	_, err := s.Create(widgetGVR, newObj("default", "w1"))
	if !apierrors.IsAlreadyExists(err) {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
}

func TestGenerateName(t *testing.T) {
	s := newTestStore(t)
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		obj := newObj("default", "")
		obj.SetGenerateName("sandbox-claim-")
		created, err := s.Create(widgetGVR, obj)
		if err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
		name := created.GetName()
		if len(name) != len("sandbox-claim-")+5 {
			t.Fatalf("unexpected generated name %q", name)
		}
		if name[:len("sandbox-claim-")] != "sandbox-claim-" {
			t.Fatalf("prefix mismatch: %q", name)
		}
		if seen[name] {
			t.Fatalf("duplicate generated name %q", name)
		}
		seen[name] = true
	}
}

func TestCreateNoNameNoGenerateName(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Create(widgetGVR, newObj("default", ""))
	if !apierrors.IsBadRequest(err) {
		t.Fatalf("expected BadRequest, got %v", err)
	}
}

func TestRVMonotonic(t *testing.T) {
	s := newTestStore(t)
	var last uint64
	check := func(obj *unstructured.Unstructured) {
		rv := parseRV(obj.GetResourceVersion())
		if rv <= last {
			t.Fatalf("rv not increasing: got %d, last %d", rv, last)
		}
		last = rv
	}
	c, _ := s.Create(widgetGVR, newObj("default", "w1"))
	check(c)
	u, _ := s.Update(widgetGVR, c, false)
	check(u)
	d, _ := s.Delete(widgetGVR, "default", "w1")
	check(d)
	c2, _ := s.Create(widgetGVR, newObj("default", "w2"))
	check(c2)
}

func TestGetNotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Get(widgetGVR, "default", "nope")
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestUpdateGenerationAndStatus(t *testing.T) {
	s := newTestStore(t)
	c, _ := s.Create(widgetGVR, newObj("default", "w1"))

	// Set a status via the status subresource.
	withStatus := c.DeepCopy()
	_ = unstructured.SetNestedField(withStatus.Object, "Ready", "status", "phase")
	su, err := s.Update(widgetGVR, withStatus, true)
	if err != nil {
		t.Fatal(err)
	}
	if su.GetGeneration() != 1 {
		t.Errorf("status update should not bump generation, got %d", su.GetGeneration())
	}
	phase, _, _ := unstructured.NestedString(su.Object, "status", "phase")
	if phase != "Ready" {
		t.Errorf("status.phase = %q, want Ready", phase)
	}

	// A spec change via a full update bumps generation and preserves status.
	specChange := su.DeepCopy()
	_ = unstructured.SetNestedField(specChange.Object, "changed", "spec", "greeting")
	// Attempt to also mutate status on the non-status path; it must be ignored.
	_ = unstructured.SetNestedField(specChange.Object, "Bogus", "status", "phase")
	uu, err := s.Update(widgetGVR, specChange, false)
	if err != nil {
		t.Fatal(err)
	}
	if uu.GetGeneration() != 2 {
		t.Errorf("spec change should bump generation to 2, got %d", uu.GetGeneration())
	}
	phase, _, _ = unstructured.NestedString(uu.Object, "status", "phase")
	if phase != "Ready" {
		t.Errorf("status must be preserved on non-status update, got %q", phase)
	}
	greeting, _, _ := unstructured.NestedString(uu.Object, "spec", "greeting")
	if greeting != "changed" {
		t.Errorf("spec.greeting = %q, want changed", greeting)
	}
}

func TestPatchMergeAndJSON(t *testing.T) {
	s := newTestStore(t)
	s.Create(widgetGVR, newObj("default", "w1"))

	// Merge patch on spec.
	merged, err := s.Patch(widgetGVR, "default", "w1", types.MergePatchType, []byte(`{"spec":{"greeting":"merged"}}`), false)
	if err != nil {
		t.Fatal(err)
	}
	g, _, _ := unstructured.NestedString(merged.Object, "spec", "greeting")
	if g != "merged" {
		t.Errorf("merge patch: greeting = %q", g)
	}

	// JSON patch adding a field.
	jp, err := s.Patch(widgetGVR, "default", "w1", types.JSONPatchType, []byte(`[{"op":"add","path":"/spec/count","value":3}]`), false)
	if err != nil {
		t.Fatal(err)
	}
	cnt, _, _ := unstructured.NestedInt64(jp.Object, "spec", "count")
	if cnt != 3 {
		t.Errorf("json patch: count = %d", cnt)
	}
}

func TestPatchStatusSubresource(t *testing.T) {
	s := newTestStore(t)
	s.Create(widgetGVR, newObj("default", "w1"))
	p, err := s.Patch(widgetGVR, "default", "w1", types.MergePatchType, []byte(`{"status":{"phase":"Running"}}`), true)
	if err != nil {
		t.Fatal(err)
	}
	phase, _, _ := unstructured.NestedString(p.Object, "status", "phase")
	if phase != "Running" {
		t.Errorf("status patch: phase = %q", phase)
	}
	if p.GetGeneration() != 1 {
		t.Errorf("status patch bumped generation: %d", p.GetGeneration())
	}
}

func TestListSelectors(t *testing.T) {
	s := newTestStore(t)
	a := newObj("default", "a")
	a.SetLabels(map[string]string{"team": "blue"})
	b := newObj("default", "b")
	b.SetLabels(map[string]string{"team": "red"})
	c := newObj("other", "c")
	c.SetLabels(map[string]string{"team": "blue"})
	for _, o := range []*unstructured.Unstructured{a, b, c} {
		if _, err := s.Create(widgetGVR, o); err != nil {
			t.Fatal(err)
		}
	}

	// Namespace scoping.
	res, _ := s.List(widgetGVR, "default", Selectors{})
	if len(res.Items) != 2 {
		t.Errorf("ns=default list = %d, want 2", len(res.Items))
	}
	// All namespaces.
	res, _ = s.List(widgetGVR, "", Selectors{})
	if len(res.Items) != 3 {
		t.Errorf("all-ns list = %d, want 3", len(res.Items))
	}
	// Label selector.
	sel, _ := ParseSelectors("team=blue", "")
	res, _ = s.List(widgetGVR, "", sel)
	if len(res.Items) != 2 {
		t.Errorf("team=blue list = %d, want 2", len(res.Items))
	}
	// Field selector metadata.name.
	sel, _ = ParseSelectors("", "metadata.name=b")
	res, _ = s.List(widgetGVR, "default", sel)
	if len(res.Items) != 1 || res.Items[0].GetName() != "b" {
		t.Errorf("metadata.name=b list = %+v", res.Items)
	}
}

func TestClusterScoped(t *testing.T) {
	s := newTestStore(t)
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("test.io/v1")
	obj.SetKind("ClusterWidget")
	obj.SetName("global")
	obj.SetNamespace("ignored")
	created, err := s.Create(clusterGVR, obj)
	if err != nil {
		t.Fatal(err)
	}
	if created.GetNamespace() != "" {
		t.Errorf("cluster-scoped object should have empty namespace, got %q", created.GetNamespace())
	}
	if _, err := s.Get(clusterGVR, "", "global"); err != nil {
		t.Fatalf("get cluster-scoped: %v", err)
	}
}

// collect reads up to n events from ch, or returns early on timeout.
func collect(t *testing.T, ch <-chan Event, n int, timeout time.Duration) []Event {
	t.Helper()
	var out []Event
	deadline := time.After(timeout)
	for len(out) < n {
		select {
		case ev, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, ev)
		case <-deadline:
			return out
		}
	}
	return out
}

func TestWatchSnapshotThenLive(t *testing.T) {
	s := newTestStore(t)
	s.Create(widgetGVR, newObj("default", "w1"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := s.Watch(ctx, widgetGVR, WatchOptions{Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	// Initial snapshot ADDED.
	evs := collect(t, ch, 1, time.Second)
	if len(evs) != 1 || evs[0].Type != Added || evs[0].Object.GetName() != "w1" {
		t.Fatalf("expected initial ADDED for w1, got %+v", evs)
	}
	// Live MODIFIED.
	c, _ := s.Get(widgetGVR, "default", "w1")
	s.Update(widgetGVR, c, false)
	evs = collect(t, ch, 1, time.Second)
	if len(evs) != 1 || evs[0].Type != Modified {
		t.Fatalf("expected MODIFIED, got %+v", evs)
	}
	// Live DELETED carries the object.
	s.Delete(widgetGVR, "default", "w1")
	evs = collect(t, ch, 1, time.Second)
	if len(evs) != 1 || evs[0].Type != Deleted || evs[0].Object.GetName() != "w1" {
		t.Fatalf("expected DELETED with object, got %+v", evs)
	}
}

func TestWatchFromRVNoStaleReplay(t *testing.T) {
	s := newTestStore(t)
	s.Create(widgetGVR, newObj("default", "w1"))
	// List to get the collection RV.
	res, _ := s.List(widgetGVR, "default", Selectors{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := s.Watch(ctx, widgetGVR, WatchOptions{Namespace: "default", StartRV: res.RV, HasStartRV: true})
	if err != nil {
		t.Fatal(err)
	}
	// No stale replay: nothing yet.
	if evs := collect(t, ch, 1, 200*time.Millisecond); len(evs) != 0 {
		t.Fatalf("expected no replay for already-seen object, got %+v", evs)
	}
	// New change after the list RV is delivered.
	c, _ := s.Get(widgetGVR, "default", "w1")
	s.Update(widgetGVR, c, false)
	if evs := collect(t, ch, 1, time.Second); len(evs) != 1 || evs[0].Type != Modified {
		t.Fatalf("expected MODIFIED after list RV, got %+v", evs)
	}
}

func TestWatchFieldSelector(t *testing.T) {
	s := newTestStore(t)
	s.Create(widgetGVR, newObj("default", "target"))
	s.Create(widgetGVR, newObj("default", "other"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sel, _ := ParseSelectors("", "metadata.name=target")
	ch, _ := s.Watch(ctx, widgetGVR, WatchOptions{Namespace: "default", Selectors: sel})
	evs := collect(t, ch, 2, 300*time.Millisecond)
	if len(evs) != 1 || evs[0].Object.GetName() != "target" {
		t.Fatalf("field-selected watch should only see 'target', got %+v", evs)
	}
}

func TestWatchConcurrentIdenticalSequence(t *testing.T) {
	s := newTestStore(t)
	const nWatchers = 20
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var chans []<-chan Event
	for i := 0; i < nWatchers; i++ {
		ch, err := s.Watch(ctx, widgetGVR, WatchOptions{Namespace: "default"})
		if err != nil {
			t.Fatal(err)
		}
		chans = append(chans, ch)
	}

	// Produce a deterministic sequence of writes.
	s.Create(widgetGVR, newObj("default", "w1"))
	c, _ := s.Get(widgetGVR, "default", "w1")
	s.Update(widgetGVR, c, false)
	s.Delete(widgetGVR, "default", "w1")

	var wg sync.WaitGroup
	results := make([][]Event, nWatchers)
	for i, ch := range chans {
		wg.Add(1)
		go func(i int, ch <-chan Event) {
			defer wg.Done()
			results[i] = collect(t, ch, 3, 2*time.Second)
		}(i, ch)
	}
	wg.Wait()

	for i, evs := range results {
		if len(evs) != 3 {
			t.Fatalf("watcher %d saw %d events, want 3: %+v", i, len(evs), evs)
		}
		want := []EventType{Added, Modified, Deleted}
		for j, ev := range evs {
			if ev.Type != want[j] {
				t.Fatalf("watcher %d event %d = %s, want %s", i, j, ev.Type, want[j])
			}
		}
	}
}

func TestUpdateResourceVersionConflict(t *testing.T) {
	s := newTestStore(t)
	c, _ := s.Create(widgetGVR, newObj("default", "w1"))
	// Bump the object so the original RV is now stale.
	if _, err := s.Update(widgetGVR, c.DeepCopy(), false); err != nil {
		t.Fatal(err)
	}
	// Updating with the now-stale RV must 409.
	stale := c.DeepCopy() // still carries the create-time RV
	if _, err := s.Update(widgetGVR, stale, false); !apierrors.IsConflict(err) {
		t.Fatalf("stale RV update: want Conflict, got %v", err)
	}
	// An empty RV means "no precondition" and must succeed.
	noRV := c.DeepCopy()
	noRV.SetResourceVersion("")
	if _, err := s.Update(widgetGVR, noRV, false); err != nil {
		t.Fatalf("empty-RV update should succeed, got %v", err)
	}
}

// TestUpdatePreservesTopLevelFields guards against the mergeForUpdate bug where
// a full update dropped non-spec top-level fields (e.g. EndpointSlice
// endpoints) and injected a null spec on schemaless resources.
func TestUpdatePreservesTopLevelFields(t *testing.T) {
	sliceGVR := apis.GVR{Group: "discovery.k8s.io", Version: "v1", Resource: "endpointslices"}
	s := New(Options{})
	s.Register(sliceGVR, true)

	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("discovery.k8s.io/v1")
	obj.SetKind("EndpointSlice")
	obj.SetNamespace("default")
	obj.SetName("s1")
	obj.Object["addressType"] = "IPv4"
	_ = unstructured.SetNestedSlice(obj.Object, []any{
		map[string]any{"addresses": []any{"10.0.0.1"}},
	}, "endpoints")
	created, err := s.Create(sliceGVR, obj)
	if err != nil {
		t.Fatal(err)
	}

	upd := created.DeepCopy()
	_ = unstructured.SetNestedSlice(upd.Object, []any{
		map[string]any{"addresses": []any{"10.0.0.2"}},
	}, "endpoints")
	res, err := s.Update(sliceGVR, upd, false)
	if err != nil {
		t.Fatal(err)
	}
	eps, _, _ := unstructured.NestedSlice(res.Object, "endpoints")
	if len(eps) != 1 {
		t.Fatalf("endpoints = %v", eps)
	}
	addrs, _, _ := unstructured.NestedStringSlice(eps[0].(map[string]any), "addresses")
	if len(addrs) != 1 || addrs[0] != "10.0.0.2" {
		t.Fatalf("endpoints not updated: %v", eps)
	}
	if _, hasSpec := res.Object["spec"]; hasSpec {
		t.Error("mergeForUpdate injected a spec key on a schemaless resource")
	}
	if at, _, _ := unstructured.NestedString(res.Object, "addressType"); at != "IPv4" {
		t.Errorf("addressType lost: %q", at)
	}
}

func TestPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.db")

	p, err := OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	s := New(Options{Persister: p})
	s.Register(widgetGVR, true)
	s.Create(widgetGVR, newObj("default", "w1"))
	s.Create(widgetGVR, newObj("default", "w2"))
	c, _ := s.Get(widgetGVR, "default", "w1")
	s.Update(widgetGVR, c, false)
	rvBefore := s.CurrentRV()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and reload.
	p2, err := OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	snap, rv, err := p2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if rv != rvBefore {
		t.Errorf("restored RV = %d, want %d", rv, rvBefore)
	}
	s2 := New(Options{Persister: p2, StartRV: rv})
	s2.Register(widgetGVR, true)
	if err := s2.LoadFrom(snap); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Get(widgetGVR, "default", "w1"); err != nil {
		t.Errorf("w1 not restored: %v", err)
	}
	if _, err := s2.Get(widgetGVR, "default", "w2"); err != nil {
		t.Errorf("w2 not restored: %v", err)
	}
	// New writes continue above the restored high-water mark.
	c3, _ := s2.Create(widgetGVR, newObj("default", "w3"))
	if parseRV(c3.GetResourceVersion()) <= rvBefore {
		t.Errorf("post-restore RV %s not above high-water %d", c3.GetResourceVersion(), rvBefore)
	}
}
