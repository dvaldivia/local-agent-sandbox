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

// Package router is the local data-plane reverse proxy, mirroring the
// in-cluster sandbox-router. It reads the SDK's X-Sandbox-* headers, resolves
// (namespace, sandbox, port) to a localhost-published container port, and
// proxies the request through — so the unmodified SDKs work in Direct/APIURL
// mode (and, via the port-forward emulation in phase 5, in tunnel mode too).
package router

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
	"github.com/dvaldivia/local-agent-sandbox/internal/driver"
	"github.com/dvaldivia/local-agent-sandbox/internal/store"
)

// ResolveState is the outcome of resolving a sandbox target.
type ResolveState int

const (
	StateOK ResolveState = iota
	StateSandboxNotFound
	StateNotReady
	StateNoPort
)

// Resolver maps (namespace, sandbox id, port) to an upstream URL.
type Resolver interface {
	Resolve(ctx context.Context, namespace, id string, port int) (target string, state ResolveState)
}

// StoreResolver resolves via the object store (readiness) and the driver
// (published host port). Container IPs are never used — only loopback-published
// ports — so it works on macOS.
type StoreResolver struct {
	Store  *store.Store
	Driver driver.Driver
}

// Resolve implements Resolver.
func (s *StoreResolver) Resolve(ctx context.Context, namespace, id string, port int) (string, ResolveState) {
	u, err := s.Store.Get(apis.SandboxGVR, namespace, id)
	if err != nil {
		return "", StateSandboxNotFound
	}
	if !sandboxReady(u) {
		return "", StateNotReady
	}
	if s.Driver == nil {
		return "", StateNotReady
	}
	info, err := s.Driver.InspectSandbox(ctx, namespace, id)
	if err != nil || info.State != "running" {
		return "", StateNotReady
	}
	hostPort := info.PortMap[port]
	if hostPort == 0 {
		return "", StateNoPort
	}
	return fmt.Sprintf("http://127.0.0.1:%d", hostPort), StateOK
}

// Handler is the router HTTP handler.
type Handler struct {
	resolver Resolver
	log      *slog.Logger
	proxy    *httputil.ReverseProxy
}

type ctxKey int

const targetKey ctxKey = iota

// Header names (canonicalized form; http.Header.Get is case-insensitive).
const (
	headerID        = "X-Sandbox-Id"
	headerNamespace = "X-Sandbox-Namespace"
	headerPort      = "X-Sandbox-Port"
	headerTimeout   = "X-Sandbox-Timeout"
	headerRequestID = "X-Request-Id"
	headerPodIP     = "X-Sandbox-Pod-Ip"
)

// New builds a router Handler.
func New(resolver Resolver, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	h := &Handler{resolver: resolver, log: log}
	h.proxy = &httputil.ReverseProxy{
		// FlushInterval -1 streams responses immediately (exec output, large
		// downloads, websockets).
		FlushInterval: -1,
		Rewrite: func(pr *httputil.ProxyRequest) {
			target := pr.In.Context().Value(targetKey).(*url.URL)
			pr.Out.URL.Scheme = target.Scheme
			pr.Out.URL.Host = target.Host
			pr.Out.Host = target.Host
			// Preserve the exact (possibly percent-encoded) path and query.
			pr.Out.URL.Path = pr.In.URL.Path
			pr.Out.URL.RawPath = pr.In.URL.RawPath
			pr.Out.URL.RawQuery = pr.In.URL.RawQuery
			// Match the in-cluster router: forward the X-Sandbox-* headers to the
			// runtime, but strip Authorization/Origin and manage X-Forwarded-*.
			pr.Out.Header.Del("Authorization")
			pr.Out.Header.Del("Origin")
			pr.SetXForwarded()
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			h.log.Warn("router upstream error", "err", err, "path", r.URL.Path)
			writeProxyError(w, http.StatusBadGateway, "upstream request failed")
		},
	}
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
		_, _ = w.Write([]byte("ok"))
		return
	}

	id := r.Header.Get(headerID)
	if !validDNSLabel(id) {
		writeProxyError(w, http.StatusBadRequest, "X-Sandbox-ID header is required and must be a DNS label")
		return
	}
	namespace := r.Header.Get(headerNamespace)
	if namespace == "" {
		namespace = "default"
	}
	port := 8888
	if p := r.Header.Get(headerPort); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 && n <= 65535 {
			port = n
		}
	}
	reqID := r.Header.Get(headerRequestID)

	ctx := r.Context()
	if to := r.Header.Get(headerTimeout); to != "" {
		if secs, err := strconv.ParseFloat(to, 64); err == nil && secs > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(secs*float64(time.Second)))
			defer cancel()
		}
	}

	target, state := h.resolver.Resolve(ctx, namespace, id, port)
	if state != StateOK {
		code, msg := statusFor(state, id, port)
		h.log.Info("router resolve miss", "sandbox", namespace+"/"+id, "state", int(state), "reqID", reqID, "status", code)
		writeProxyError(w, code, msg)
		return
	}

	targetURL, _ := url.Parse(target)
	pr := r.WithContext(context.WithValue(ctx, targetKey, targetURL))
	sw := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
	h.proxy.ServeHTTP(sw, pr)

	h.log.Debug("router proxied",
		"method", r.Method, "path", r.URL.Path, "sandbox", namespace+"/"+id,
		"upstream", target, "status", sw.code, "reqID", reqID, "dur", time.Since(start).String())
}

func statusFor(state ResolveState, id string, port int) (int, string) {
	switch state {
	case StateSandboxNotFound:
		return http.StatusBadGateway, fmt.Sprintf("sandbox %q not found", id)
	case StateNotReady:
		return http.StatusServiceUnavailable, fmt.Sprintf("sandbox %q is not ready", id)
	case StateNoPort:
		return http.StatusBadGateway, fmt.Sprintf("sandbox %q does not publish port %d", id, port)
	default:
		return http.StatusBadGateway, "unresolved"
	}
}

// sandboxReady reports whether an unstructured Sandbox has Ready=True.
func sandboxReady(u *unstructured.Unstructured) bool {
	conds, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	if !found {
		return false
	}
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if m["type"] == "Ready" && m["status"] == "True" {
			return true
		}
	}
	return false
}

// validDNSLabel checks RFC 1123 label rules (lowercase alnum + '-', ≤63).
func validDNSLabel(s string) bool {
	if len(s) == 0 || len(s) > 63 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-':
			if i == 0 || i == len(s)-1 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func writeProxyError(w http.ResponseWriter, code int, msg string) {
	// The in-cluster sandbox-router returns {"detail": ...}; match it.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = fmt.Fprintf(w, `{"detail":%q}`, msg)
}

type statusRecorder struct {
	http.ResponseWriter
	code        int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wroteHeader {
		s.code = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if !s.wroteHeader {
		s.wroteHeader = true
	}
	return s.ResponseWriter.Write(b)
}

// Flush and Hijack passthrough for streaming and websocket upgrades.
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack delegates to the underlying ResponseWriter so httputil.ReverseProxy
// can upgrade connections (WebSocket) through the router.
func (s *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := s.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, fmt.Errorf("router: underlying ResponseWriter is not a Hijacker")
}
