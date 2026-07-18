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

// Package store is a generic, etcd-shaped object store for unstructured
// Kubernetes objects. It provides CRUD, list with label/field selectors, and
// watch with a globally monotonic resourceVersion — the semantics the
// agent-sandbox SDKs and kubectl depend on. Persistence is pluggable so tests
// can run fully in-memory.
package store

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
)

// EventType mirrors k8s watch.EventType values.
type EventType string

const (
	Added    EventType = "ADDED"
	Modified EventType = "MODIFIED"
	Deleted  EventType = "DELETED"
	Error    EventType = "ERROR"
)

// Event is a single watch event.
type Event struct {
	Type   EventType
	Object *unstructured.Unstructured
	RV     uint64
	GVR    apis.GVR
}

// resourceMeta records per-GVR registration facts.
type resourceMeta struct {
	namespaced bool
	groupRes   schema.GroupResource
}

// Store holds all objects in memory, fanning out watch events and optionally
// persisting to a backend.
type Store struct {
	mu sync.RWMutex

	data      map[apis.GVR]map[string]*unstructured.Unstructured
	resources map[apis.GVR]resourceMeta

	rv      uint64
	history []Event
	histCap int

	subs    map[int]*subscription
	nextSub int

	persist Persister

	// onDelete, if set, is called after each Delete (outside the lock) to
	// cascade cleanup of owned resources.
	onDelete func(gvr apis.GVR, obj *unstructured.Unstructured)

	// injectable for tests
	now    func() time.Time
	newUID func() string
}

// Options configures a Store.
type Options struct {
	Persister  Persister
	HistoryCap int
	Now        func() time.Time
	NewUID     func() string
	StartRV    uint64 // initial resourceVersion high-water mark (from persistence)
}

// New constructs an empty Store.
func New(opts Options) *Store {
	if opts.HistoryCap <= 0 {
		opts.HistoryCap = 8192
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.NewUID == nil {
		opts.NewUID = newUUID
	}
	return &Store{
		data:      make(map[apis.GVR]map[string]*unstructured.Unstructured),
		resources: make(map[apis.GVR]resourceMeta),
		rv:        opts.StartRV,
		histCap:   opts.HistoryCap,
		subs:      make(map[int]*subscription),
		persist:   opts.Persister,
		now:       opts.Now,
		newUID:    opts.NewUID,
	}
}

// Register declares a resource type. Objects can only be stored for registered
// GVRs. Namespaced controls key construction and list scoping.
func (s *Store) Register(gvr apis.GVR, namespaced bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resources[gvr] = resourceMeta{
		namespaced: namespaced,
		groupRes:   schema.GroupResource{Group: gvr.Group, Resource: gvr.Resource},
	}
	if s.data[gvr] == nil {
		s.data[gvr] = make(map[string]*unstructured.Unstructured)
	}
}

// key builds the map key for an object.
func key(namespaced bool, namespace, name string) string {
	if namespaced {
		return namespace + "/" + name
	}
	return "/" + name
}

// CurrentRV returns the current global resourceVersion.
func (s *Store) CurrentRV() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rv
}

// nextRV increments and returns the global RV. Caller must hold s.mu. The RV
// is persisted together with the object write (SaveObject/RemoveObject), so no
// separate persistence happens here.
func (s *Store) nextRV() uint64 {
	s.rv++
	return s.rv
}

// conflictErr builds a 409 with the apiserver's canonical message, which
// client-go surfaces via errors.IsConflict for RetryOnConflict.
func conflictErr(gr schema.GroupResource, name string) error {
	return apierrors.NewConflict(gr, name, fmt.Errorf(
		"Operation cannot be fulfilled on %s %q: the object has been modified; please apply your changes to the latest version and try again",
		gr.Resource, name))
}

func (s *Store) meta(gvr apis.GVR) (resourceMeta, error) {
	rm, ok := s.resources[gvr]
	if !ok {
		return resourceMeta{}, apierrors.NewInternalError(fmt.Errorf("store: GVR %s not registered", gvr))
	}
	return rm, nil
}

// Create inserts a new object, honoring metadata.generateName. It returns the
// stored copy with system metadata populated.
func (s *Store) Create(gvr apis.GVR, obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rm, err := s.meta(gvr)
	if err != nil {
		return nil, err
	}
	obj = obj.DeepCopy()

	ns := obj.GetNamespace()
	if !rm.namespaced {
		ns = ""
		obj.SetNamespace("")
	}

	name := obj.GetName()
	if name == "" {
		if gn := obj.GetGenerateName(); gn != "" {
			name = s.generateUniqueName(gvr, rm, ns, gn)
			obj.SetName(name)
		} else {
			return nil, apierrors.NewBadRequest("store: object has neither metadata.name nor metadata.generateName")
		}
	}

	k := key(rm.namespaced, ns, name)
	if _, exists := s.data[gvr][k]; exists {
		return nil, apierrors.NewAlreadyExists(rm.groupRes, name)
	}

	rv := s.nextRV()
	if obj.GetUID() == "" {
		obj.SetUID(types.UID(s.newUID()))
	}
	obj.SetResourceVersion(strconv.FormatUint(rv, 10))
	obj.SetGeneration(1)
	if ct := obj.GetCreationTimestamp(); ct.IsZero() {
		obj.SetCreationTimestamp(metav1.NewTime(s.now()))
	}

	s.putLocked(gvr, k, obj, rv)
	s.emitLocked(Event{Type: Added, Object: obj.DeepCopy(), RV: rv, GVR: gvr})
	return obj.DeepCopy(), nil
}

// generateUniqueName produces a name from a generateName prefix that does not
// collide with an existing object. Caller must hold s.mu.
func (s *Store) generateUniqueName(gvr apis.GVR, rm resourceMeta, ns, prefix string) string {
	for {
		candidate := prefix + randSuffix(5)
		if _, exists := s.data[gvr][key(rm.namespaced, ns, candidate)]; !exists {
			return candidate
		}
	}
}

// Get returns a deep copy of the stored object.
func (s *Store) Get(gvr apis.GVR, namespace, name string) (*unstructured.Unstructured, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rm, err := s.meta(gvr)
	if err != nil {
		return nil, err
	}
	obj, ok := s.data[gvr][key(rm.namespaced, namespace, name)]
	if !ok {
		return nil, apierrors.NewNotFound(rm.groupRes, name)
	}
	return obj.DeepCopy(), nil
}

// Update replaces an object. When isStatus is true it behaves as the /status
// subresource (only status is written, generation is unchanged); otherwise it
// preserves the existing status and bumps generation on spec change.
func (s *Store) Update(gvr apis.GVR, obj *unstructured.Unstructured, isStatus bool) (*unstructured.Unstructured, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rm, err := s.meta(gvr)
	if err != nil {
		return nil, err
	}
	obj = obj.DeepCopy()
	ns := obj.GetNamespace()
	if !rm.namespaced {
		ns = ""
	}
	name := obj.GetName()
	k := key(rm.namespaced, ns, name)
	old, ok := s.data[gvr][k]
	if !ok {
		return nil, apierrors.NewNotFound(rm.groupRes, name)
	}
	// Optimistic concurrency: honor an explicit resourceVersion precondition
	// (empty means "no precondition", matching the apiserver). client-go's
	// RetryOnConflict depends on the 409.
	if rv := obj.GetResourceVersion(); rv != "" && rv != old.GetResourceVersion() {
		return nil, conflictErr(rm.groupRes, name)
	}

	merged := s.mergeForUpdate(old, obj, isStatus)
	rv := s.nextRV()
	merged.SetResourceVersion(strconv.FormatUint(rv, 10))
	s.putLocked(gvr, k, merged, rv)
	s.emitLocked(Event{Type: Modified, Object: merged.DeepCopy(), RV: rv, GVR: gvr})
	return merged.DeepCopy(), nil
}

// mergeForUpdate applies update semantics. Caller must hold s.mu.
//
// For a status update only the status subresource is written. For a full
// update the result IS the incoming (desired) object — so all top-level fields
// are honored, including schemaless kinds like EndpointSlice that have no
// "spec" — with immutable identity (uid, creationTimestamp) and the existing
// status preserved (status only changes via /status). Generation bumps on a
// spec change.
func (s *Store) mergeForUpdate(old, incoming *unstructured.Unstructured, isStatus bool) *unstructured.Unstructured {
	if isStatus {
		result := old.DeepCopy()
		if st, found, _ := unstructured.NestedFieldCopy(incoming.Object, "status"); found {
			_ = unstructured.SetNestedField(result.Object, st, "status")
		} else {
			unstructured.RemoveNestedField(result.Object, "status")
		}
		return result
	}

	result := incoming.DeepCopy()
	// Preserve server-owned immutable identity.
	result.SetUID(old.GetUID())
	result.SetCreationTimestamp(old.GetCreationTimestamp())
	// Preserve existing status; a non-status update must not change it.
	if st, found, _ := unstructured.NestedFieldCopy(old.Object, "status"); found {
		_ = unstructured.SetNestedField(result.Object, st, "status")
	} else {
		unstructured.RemoveNestedField(result.Object, "status")
	}
	// Generation: bump only on spec change; carry it over otherwise.
	if reflect.DeepEqual(old.Object["spec"], result.Object["spec"]) {
		result.SetGeneration(old.GetGeneration())
	} else {
		result.SetGeneration(old.GetGeneration() + 1)
	}
	return result
}

// Delete removes an object and returns the deleted copy. The delete hook (if
// set) is invoked AFTER the store lock is released, so a hook may itself call
// Delete (recursive cascade) without deadlocking.
func (s *Store) Delete(gvr apis.GVR, namespace, name string) (*unstructured.Unstructured, error) {
	s.mu.Lock()
	rm, err := s.meta(gvr)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if !rm.namespaced {
		namespace = ""
	}
	k := key(rm.namespaced, namespace, name)
	old, ok := s.data[gvr][k]
	if !ok {
		s.mu.Unlock()
		return nil, apierrors.NewNotFound(rm.groupRes, name)
	}
	delete(s.data[gvr], k)
	rv := s.nextRV()
	if s.persist != nil {
		_ = s.persist.RemoveObject(gvr, k, rv)
	}
	deleted := old.DeepCopy()
	deleted.SetResourceVersion(strconv.FormatUint(rv, 10))
	s.emitLocked(Event{Type: Deleted, Object: deleted.DeepCopy(), RV: rv, GVR: gvr})
	hook := s.onDelete
	s.mu.Unlock()

	if hook != nil {
		hook(gvr, deleted.DeepCopy())
	}
	return deleted, nil
}

// SetDeleteHook registers a callback invoked after each successful Delete
// (outside the store lock). Used to cascade cleanup of owned resources.
func (s *Store) SetDeleteHook(fn func(gvr apis.GVR, obj *unstructured.Unstructured)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onDelete = fn
}

// ListResult is the outcome of a List call.
type ListResult struct {
	Items []*unstructured.Unstructured
	RV    uint64
}

// List returns objects matching the namespace and selectors. An empty
// namespace lists across all namespaces (for namespaced resources).
func (s *Store) List(gvr apis.GVR, namespace string, sel Selectors) (ListResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rm, err := s.meta(gvr)
	if err != nil {
		return ListResult{}, err
	}
	var items []*unstructured.Unstructured
	for _, obj := range s.data[gvr] {
		if rm.namespaced && namespace != "" && obj.GetNamespace() != namespace {
			continue
		}
		if !sel.Matches(obj) {
			continue
		}
		items = append(items, obj.DeepCopy())
	}
	return ListResult{Items: items, RV: s.rv}, nil
}

// putLocked stores an object and persists it (object + RV in one txn). Caller
// must hold s.mu.
func (s *Store) putLocked(gvr apis.GVR, k string, obj *unstructured.Unstructured, rv uint64) {
	s.data[gvr][k] = obj
	if s.persist != nil {
		if data, err := obj.MarshalJSON(); err == nil {
			_ = s.persist.SaveObject(gvr, k, data, rv)
		}
	}
}

// LoadFrom restores objects from a persister snapshot. Only registered GVRs are
// loaded. Intended to be called once before serving.
func (s *Store) LoadFrom(snap map[apis.GVR]map[string][]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for gvr, objs := range snap {
		if _, ok := s.resources[gvr]; !ok {
			continue
		}
		for k, data := range objs {
			u := &unstructured.Unstructured{}
			if err := u.UnmarshalJSON(data); err != nil {
				return fmt.Errorf("store: load %s/%s: %w", gvr, k, err)
			}
			s.data[gvr][k] = u
		}
	}
	return nil
}
