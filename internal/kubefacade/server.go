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

// Package kubefacade serves the subset of the Kubernetes REST API that the
// agent-sandbox SDKs and kubectl require: discovery documents plus CRUD/list/
// watch for the registered CRDs (and core pods/services/namespaces/
// endpointslices used by later phases). It is a thin HTTP shell over the
// object store.
package kubefacade

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
	"github.com/dvaldivia/local-agent-sandbox/internal/store"
)

var errNoFlush = errors.New("kubefacade: ResponseWriter does not support flushing (required for watch)")

// Handler is the HTTP handler for the control-plane facade.
type Handler struct {
	store       *store.Store
	reg         *apis.Registry
	log         *slog.Logger
	portForward http.Handler
	mux         *http.ServeMux
}

// Options configures a Handler.
type Options struct {
	Store    *store.Store
	Registry *apis.Registry
	Logger   *slog.Logger
	// PortForward, if set, handles the pods/{name}/portforward subresource
	// (SPDY). Wired in phase 5 to bridge the SDK tunnel to the router.
	PortForward http.Handler
}

// New builds a Handler and registers all routes. Every registered resource is
// also registered with the store.
func New(opts Options) *Handler {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	h := &Handler{
		store:       opts.Store,
		reg:         opts.Registry,
		log:         opts.Logger,
		portForward: opts.PortForward,
	}
	for _, res := range h.reg.All() {
		h.store.Register(res.GVR, res.Namespaced)
	}
	h.mux = h.buildMux()
	return h
}

func (h *Handler) buildMux() *http.ServeMux {
	mux := http.NewServeMux()

	// Discovery.
	mux.HandleFunc("GET /version", h.handleVersion)
	mux.HandleFunc("GET /api", h.handleAPIVersions)
	mux.HandleFunc("GET /api/v1", h.handleCoreResourceList)
	mux.HandleFunc("GET /apis", h.handleAPIGroupList)
	mux.HandleFunc("GET /apis/{group}/{version}", h.handleGroupResourceList)

	// OpenAPI v3 (kubectl client-side validation). v2 is intentionally not
	// served: it is protobuf-only, and a valid v3 document prevents kubectl
	// from falling back to it.
	mux.HandleFunc("GET /openapi/v3", h.handleOpenAPIV3Root)
	mux.HandleFunc("GET /openapi/v3/api/v1", h.handleOpenAPIV3Core)
	mux.HandleFunc("GET /openapi/v3/apis/{group}/{version}", h.handleOpenAPIV3Group)

	// Core group (/api/v1). group path value is absent ⇒ "".
	coreColl := h.handleCollection(true)
	coreItem := h.handleItem(true, false)
	coreSub := h.handleItem(true, true)
	clusterColl := h.handleCollection(false)
	clusterItem := h.handleItem(false, false)

	mux.HandleFunc("/api/v1/namespaces/{namespace}/{resource}", coreColl)
	mux.HandleFunc("/api/v1/namespaces/{namespace}/{resource}/{name}", coreItem)
	mux.HandleFunc("/api/v1/namespaces/{namespace}/{resource}/{name}/{subresource}", coreSub)
	// Cluster-scoped core resources (namespaces) + all-namespace list.
	mux.HandleFunc("/api/v1/{resource}", clusterColl)
	mux.HandleFunc("/api/v1/{resource}/{name}", clusterItem)

	// Named groups (/apis/{group}/{version}).
	groupColl := h.handleCollection(true)
	groupItem := h.handleItem(true, false)
	groupSub := h.handleItem(true, true)
	groupClusterColl := h.handleCollection(false)
	groupClusterItem := h.handleItem(false, false)

	mux.HandleFunc("/apis/{group}/{version}/namespaces/{namespace}/{resource}", groupColl)
	mux.HandleFunc("/apis/{group}/{version}/namespaces/{namespace}/{resource}/{name}", groupItem)
	mux.HandleFunc("/apis/{group}/{version}/namespaces/{namespace}/{resource}/{name}/{subresource}", groupSub)
	mux.HandleFunc("/apis/{group}/{version}/{resource}", groupClusterColl)
	mux.HandleFunc("/apis/{group}/{version}/{resource}/{name}", groupClusterItem)

	// Pod port-forward subresource (SPDY), if wired.
	if h.portForward != nil {
		mux.Handle("/api/v1/namespaces/{namespace}/pods/{name}/portforward", h.portForward)
	}

	// Health.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })

	return mux
}

// ServeHTTP implements http.Handler with request logging.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
	h.mux.ServeHTTP(sw, r)
	h.log.Debug("request",
		"method", r.Method,
		"path", r.URL.Path,
		"query", r.URL.RawQuery,
		"status", sw.code,
		"dur", time.Since(start).String(),
	)
}

// statusWriter captures the response status code for logging while preserving
// the Flusher interface required by watch streams.
type statusWriter struct {
	http.ResponseWriter
	code        int
	wroteHeader bool
}

func (s *statusWriter) WriteHeader(code int) {
	if !s.wroteHeader {
		s.code = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if !s.wroteHeader {
		s.wroteHeader = true
	}
	return s.ResponseWriter.Write(b)
}

func (s *statusWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack delegates to the underlying ResponseWriter so the port-forward
// subresource can perform the SPDY connection upgrade.
func (s *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := s.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, fmt.Errorf("kubefacade: underlying ResponseWriter is not a Hijacker")
}

func withTimeout(ctx context.Context, d time.Duration) (context.Context, func()) {
	return context.WithTimeout(ctx, d)
}

func rawExt(raw []byte) runtime.RawExtension {
	return runtime.RawExtension{Raw: raw}
}
