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
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
	"github.com/dvaldivia/local-agent-sandbox/internal/store"
)

// maxBodyBytes caps request bodies (objects, patches) to a sane local limit.
const maxBodyBytes = 64 << 20 // 64 MB

// handleCollection dispatches list/watch (GET) and create (POST) on a
// collection endpoint.
func (h *Handler) handleCollection(namespaced bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		res, ok := h.resolveByPath(r)
		if !ok {
			writeErr(w, apierrors.NewNotFound(schema.GroupResource{Group: r.PathValue("group"), Resource: r.PathValue("resource")}, ""))
			return
		}
		namespace := ""
		if namespaced {
			namespace = r.PathValue("namespace")
		}
		switch r.Method {
		case http.MethodGet:
			if isWatch(r) {
				h.watch(w, r, res, namespace)
			} else {
				h.list(w, r, res, namespace)
			}
		case http.MethodPost:
			h.create(w, r, res, namespace)
		default:
			writeErr(w, apierrors.NewMethodNotSupported(res.GVR.GroupResource(), r.Method))
		}
	}
}

// handleItem dispatches get/update/patch/delete on a single object, optionally
// on a subresource (status/scale).
func (h *Handler) handleItem(namespaced, sub bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		res, ok := h.resolveByPath(r)
		if !ok {
			writeErr(w, apierrors.NewNotFound(schema.GroupResource{Group: r.PathValue("group"), Resource: r.PathValue("resource")}, r.PathValue("name")))
			return
		}
		namespace := ""
		if namespaced {
			namespace = r.PathValue("namespace")
		}
		name := r.PathValue("name")
		subresource := ""
		if sub {
			subresource = r.PathValue("subresource")
		}
		if subresource == "scale" {
			h.handleScale(w, r, res, namespace, name)
			return
		}
		isStatus := subresource == "status"

		switch r.Method {
		case http.MethodGet:
			h.get(w, res, namespace, name)
		case http.MethodPut:
			h.update(w, r, res, namespace, name, isStatus)
		case http.MethodPatch:
			h.patch(w, r, res, namespace, name, isStatus)
		case http.MethodDelete:
			h.delete(w, res, namespace, name)
		default:
			writeErr(w, apierrors.NewMethodNotSupported(res.GVR.GroupResource(), r.Method))
		}
	}
}

// handleScale implements the /scale subresource: GET synthesizes an
// autoscaling/v1 Scale from the object's spec.replicas; PUT/PATCH set
// spec.replicas on the object (a spec write, not a status write).
func (h *Handler) handleScale(w http.ResponseWriter, r *http.Request, res apis.Resource, namespace, name string) {
	obj, err := h.store.Get(res.GVR, namespace, name)
	if err != nil {
		writeErr(w, err)
		return
	}
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, scaleObject(obj))
		return
	}
	if r.Method != http.MethodPut && r.Method != http.MethodPatch {
		writeErr(w, apierrors.NewMethodNotSupported(res.GVR.GroupResource(), r.Method))
		return
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, apierrors.NewBadRequest(err.Error()))
		return
	}
	replicas, ok := extractReplicas(data)
	if !ok {
		writeErr(w, apierrors.NewBadRequest("scale: could not read spec.replicas from request body"))
		return
	}
	_ = unstructured.SetNestedField(obj.Object, replicas, "spec", "replicas")
	updated, err := h.store.Update(res.GVR, obj, false)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, scaleObject(updated))
}

// scaleObject builds an autoscaling/v1 Scale view of a resource.
func scaleObject(obj *unstructured.Unstructured) map[string]any {
	specReplicas, _, _ := unstructured.NestedInt64(obj.Object, "spec", "replicas")
	statusReplicas, found, _ := unstructured.NestedInt64(obj.Object, "status", "replicas")
	if !found {
		statusReplicas = specReplicas
	}
	return map[string]any{
		"apiVersion": "autoscaling/v1",
		"kind":       "Scale",
		"metadata": map[string]any{
			"name":              obj.GetName(),
			"namespace":         obj.GetNamespace(),
			"resourceVersion":   obj.GetResourceVersion(),
			"creationTimestamp": obj.GetCreationTimestamp(),
		},
		"spec":   map[string]any{"replicas": specReplicas},
		"status": map[string]any{"replicas": statusReplicas},
	}
}

// extractReplicas reads spec.replicas from a Scale object (PUT) or a merge
// patch body (PATCH); both carry {"spec":{"replicas":N}}.
func extractReplicas(data []byte) (int64, bool) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, false
	}
	u := unstructured.Unstructured{Object: m}
	v, found, err := unstructured.NestedInt64(u.Object, "spec", "replicas")
	return v, found && err == nil
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request, res apis.Resource, namespace string) {
	sel, err := store.ParseSelectors(r.URL.Query().Get("labelSelector"), r.URL.Query().Get("fieldSelector"))
	if err != nil {
		writeErr(w, apierrors.NewBadRequest(err.Error()))
		return
	}
	result, err := h.store.List(res.GVR, namespace, sel)
	if err != nil {
		writeErr(w, err)
		return
	}
	items := make([]any, 0, len(result.Items))
	for _, it := range result.Items {
		items = append(items, it.Object)
	}
	list := map[string]any{
		"apiVersion": res.GVR.GroupVersion(),
		"kind":       res.ListKind,
		"metadata": map[string]any{
			"resourceVersion": strconv.FormatUint(result.RV, 10),
		},
		"items": items,
	}
	writeJSON(w, http.StatusOK, list)
}

func (h *Handler) get(w http.ResponseWriter, res apis.Resource, namespace, name string) {
	obj, err := h.store.Get(res.GVR, namespace, name)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, obj.Object)
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request, res apis.Resource, namespace string) {
	obj, err := decodeObject(r)
	if err != nil {
		writeErr(w, apierrors.NewBadRequest(err.Error()))
		return
	}
	if res.Namespaced {
		if obj.GetNamespace() == "" {
			obj.SetNamespace(namespace)
		} else if namespace != "" && obj.GetNamespace() != namespace {
			writeErr(w, apierrors.NewBadRequest("namespace in body does not match namespace in URL"))
			return
		}
		if obj.GetNamespace() == "" {
			writeErr(w, apierrors.NewBadRequest("the namespace of the object ("+obj.GetName()+") must be specified"))
			return
		}
	}
	if obj.GetKind() == "" {
		obj.SetKind(res.Kind)
	}
	if obj.GetAPIVersion() == "" {
		obj.SetAPIVersion(res.GVR.GroupVersion())
	}
	// The apiserver strips a client-supplied status on create for resources
	// with a /status subresource; status is owned by the status endpoint.
	if res.HasStatus {
		unstructured.RemoveNestedField(obj.Object, "status")
	}
	created, err := h.store.Create(res.GVR, obj)
	if err != nil {
		writeErr(w, err)
		return
	}
	h.ensureNamespace(created.GetNamespace())
	writeJSON(w, http.StatusCreated, created.Object)
}

func (h *Handler) update(w http.ResponseWriter, r *http.Request, res apis.Resource, namespace, name string, isStatus bool) {
	obj, err := decodeObject(r)
	if err != nil {
		writeErr(w, apierrors.NewBadRequest(err.Error()))
		return
	}
	obj.SetName(name)
	if res.Namespaced {
		obj.SetNamespace(namespace)
	}
	updated, err := h.store.Update(res.GVR, obj, isStatus)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, updated.Object)
}

func (h *Handler) patch(w http.ResponseWriter, r *http.Request, res apis.Resource, namespace, name string, isStatus bool) {
	data, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, apierrors.NewBadRequest(err.Error()))
		return
	}
	patchType := types.PatchType(r.Header.Get("Content-Type"))
	patched, err := h.store.Patch(res.GVR, namespace, name, patchType, data, isStatus)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, patched.Object)
}

func (h *Handler) delete(w http.ResponseWriter, res apis.Resource, namespace, name string) {
	deleted, err := h.store.Delete(res.GVR, namespace, name)
	if err != nil {
		writeErr(w, err)
		return
	}
	// Cascade cleanup of owned resources is handled by the store's delete hook.
	writeJSON(w, http.StatusOK, deleted.Object)
}

// watch streams events as newline-delimited watch.Event JSON objects.
func (h *Handler) watch(w http.ResponseWriter, r *http.Request, res apis.Resource, namespace string) {
	sel, err := store.ParseSelectors(r.URL.Query().Get("labelSelector"), r.URL.Query().Get("fieldSelector"))
	if err != nil {
		writeErr(w, apierrors.NewBadRequest(err.Error()))
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, apierrors.NewInternalError(errNoFlush))
		return
	}

	opts := store.WatchOptions{Namespace: namespace, Selectors: sel}
	if rv := r.URL.Query().Get("resourceVersion"); rv != "" {
		if n, perr := strconv.ParseUint(rv, 10, 64); perr == nil {
			opts.StartRV = n
			opts.HasStartRV = true
		}
	}

	ctx := r.Context()
	if ts := r.URL.Query().Get("timeoutSeconds"); ts != "" {
		if secs, perr := strconv.Atoi(ts); perr == nil && secs > 0 {
			var cancel func()
			ctx, cancel = withTimeout(ctx, time.Duration(secs)*time.Second)
			defer cancel()
		}
	}

	ch, err := h.store.Watch(ctx, res.GVR, opts)
	if err != nil {
		writeErr(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// A write deadline per event ensures a stalled/slow client (applying TCP
	// backpressure without disconnecting) eventually errors so this handler
	// returns, cancels the request context, and lets the store watch goroutine
	// be reclaimed. Idle watches never write, so they are unaffected.
	rc := http.NewResponseController(w)
	enc := json.NewEncoder(w)
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			raw, merr := ev.Object.MarshalJSON()
			if merr != nil {
				continue
			}
			we := metav1.WatchEvent{Type: string(ev.Type), Object: rawExt(raw)}
			_ = rc.SetWriteDeadline(time.Now().Add(watchWriteTimeout))
			if err := enc.Encode(&we); err != nil {
				return
			}
			if err := rc.Flush(); err != nil {
				return
			}
		}
	}
}

// watchWriteTimeout bounds a single watch-event write. Generous so slow but
// live clients are not killed, but finite so a wedged client is reclaimed.
const watchWriteTimeout = 30 * time.Second

// ensureNamespace lazily materializes a Namespace object so `kubectl get ns`
// reflects namespaces created implicitly by writing namespaced objects.
func (h *Handler) ensureNamespace(name string) {
	if name == "" {
		return
	}
	if _, err := h.store.Get(apis.NamespaceGVR, "", name); err == nil {
		return
	}
	ns := &unstructured.Unstructured{}
	ns.SetAPIVersion("v1")
	ns.SetKind("Namespace")
	ns.SetName(name)
	_ = unstructured.SetNestedField(ns.Object, "Active", "status", "phase")
	_, _ = h.store.Create(apis.NamespaceGVR, ns)
}

func decodeObject(r *http.Request) (*unstructured.Unstructured, error) {
	data, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		return nil, err
	}
	u := &unstructured.Unstructured{}
	if err := u.UnmarshalJSON(data); err != nil {
		return nil, err
	}
	return u, nil
}

func isWatch(r *http.Request) bool {
	v := r.URL.Query().Get("watch")
	return v == "true" || v == "1"
}
