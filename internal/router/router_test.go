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

package router

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeResolver returns a fixed target/state.
type fakeResolver struct {
	target string
	state  ResolveState
	// captured
	gotNS, gotID string
	gotPort      int
}

func (f *fakeResolver) Resolve(_ context.Context, ns, id string, port int) (string, ResolveState) {
	f.gotNS, f.gotID, f.gotPort = ns, id, port
	return f.target, f.state
}

func newRouter(res Resolver) *Handler {
	return New(res, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestRouterProxiesAndForwardsHeaders(t *testing.T) {
	var gotPath, gotSandboxHeader, gotContentType, gotAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		gotSandboxHeader = r.Header.Get("X-Sandbox-Id")
		gotContentType = r.Header.Get("Content-Type")
		gotAuth = r.Header.Get("Authorization")
		_, _ = io.WriteString(w, "upstream-ok")
	}))
	defer upstream.Close()

	res := &fakeResolver{target: upstream.URL, state: StateOK}
	h := newRouter(res)
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Percent-encoded path segment must reach the upstream verbatim.
	req, _ := http.NewRequest("GET", srv.URL+"/download/a%2Fb%20c", nil)
	req.Header.Set("X-Sandbox-ID", "mybox")
	req.Header.Set("X-Sandbox-Namespace", "team-a")
	req.Header.Set("X-Sandbox-Port", "9000")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer secret")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if string(body) != "upstream-ok" {
		t.Fatalf("body = %q", body)
	}
	if gotPath != "/download/a%2Fb%20c" {
		t.Errorf("path fidelity broken: upstream saw %q", gotPath)
	}
	// Match the in-cluster router: X-Sandbox-* are forwarded to the runtime,
	// Authorization is stripped, other headers pass through.
	if gotSandboxHeader != "mybox" {
		t.Errorf("X-Sandbox-* headers should be forwarded, upstream saw ID=%q", gotSandboxHeader)
	}
	if gotAuth != "" {
		t.Errorf("Authorization must be stripped, upstream saw %q", gotAuth)
	}
	if gotContentType != "application/json" {
		t.Errorf("non-sandbox headers must pass through, content-type=%q", gotContentType)
	}
	if res.gotNS != "team-a" || res.gotID != "mybox" || res.gotPort != 9000 {
		t.Errorf("resolver got ns=%q id=%q port=%d", res.gotNS, res.gotID, res.gotPort)
	}
}

func TestRouterHeaderValidation(t *testing.T) {
	h := newRouter(&fakeResolver{state: StateOK, target: "http://127.0.0.1:1"})
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Missing ID → 400.
	resp, _ := srv.Client().Get(srv.URL + "/execute")
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("missing ID: status = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()

	// Invalid ID (uppercase/space) → 400.
	req, _ := http.NewRequest("GET", srv.URL+"/", nil)
	req.Header.Set("X-Sandbox-ID", "Bad Id")
	resp, _ = srv.Client().Do(req)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("invalid ID: status = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestRouterResolveStateMapping(t *testing.T) {
	cases := []struct {
		state ResolveState
		want  int
	}{
		{StateSandboxNotFound, http.StatusBadGateway},
		{StateNotReady, http.StatusServiceUnavailable},
		{StateNoPort, http.StatusBadGateway},
	}
	for _, c := range cases {
		h := newRouter(&fakeResolver{state: c.state})
		srv := httptest.NewServer(h)
		req, _ := http.NewRequest("GET", srv.URL+"/execute", nil)
		req.Header.Set("X-Sandbox-ID", "mybox")
		resp, _ := srv.Client().Do(req)
		if resp.StatusCode != c.want {
			t.Errorf("state %d: status = %d, want %d", c.state, resp.StatusCode, c.want)
		}
		resp.Body.Close()
		srv.Close()
	}
}

func TestRouterDefaultsPortAndNamespace(t *testing.T) {
	res := &fakeResolver{state: StateNotReady}
	h := newRouter(res)
	srv := httptest.NewServer(h)
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL+"/execute", nil)
	req.Header.Set("X-Sandbox-ID", "mybox")
	resp, _ := srv.Client().Do(req)
	resp.Body.Close()
	if res.gotNS != "default" {
		t.Errorf("default namespace = %q, want default", res.gotNS)
	}
	if res.gotPort != 8888 {
		t.Errorf("default port = %d, want 8888", res.gotPort)
	}
}

func TestRouterStreamsBody(t *testing.T) {
	// A large upstream body must stream through intact.
	const size = 4 << 20
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(strings.Repeat("x", size)))
	}))
	defer upstream.Close()
	h := newRouter(&fakeResolver{target: upstream.URL, state: StateOK})
	srv := httptest.NewServer(h)
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL+"/download/big", nil)
	req.Header.Set("X-Sandbox-ID", "mybox")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	n, _ := io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if n != size {
		t.Errorf("streamed %d bytes, want %d", n, size)
	}
}
