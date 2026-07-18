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

package portforward

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

// TestPortForwardLoopback drives the SPDY port-forward server with the exact
// client-go machinery the SDK's tunnel strategy uses (spdy.RoundTripperFor +
// portforward.NewForStreaming), proving an HTTP request tunnels through to the
// bridged "router".
func TestPortForwardLoopback(t *testing.T) {
	// Stub router: a plain HTTP server the port-forward server bridges TCP to.
	router := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "router-saw:%s", r.URL.Path)
	}))
	defer router.Close()
	routerHost := mustHost(t, router.URL)

	// Facade exposing the portforward subresource.
	pf := New(routerHost, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux := http.NewServeMux()
	mux.Handle("/api/v1/namespaces/{namespace}/pods/{name}/portforward", pf)
	facade := httptest.NewServer(mux)
	defer facade.Close()

	// Client-go port-forward over SPDY (mirrors clients/go/sandbox/tunnel.go).
	cfg := &rest.Config{Host: facade.URL}
	transport, upgrader, err := spdy.RoundTripperFor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	reqURL, _ := url.Parse(facade.URL + "/api/v1/namespaces/default/pods/" + RouterPodName + "/portforward")
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, reqURL)

	stopCh := make(chan struct{})
	readyCh := make(chan struct{})
	defer close(stopCh)
	fw, err := portforward.New(dialer, []string{"0:8080"}, stopCh, readyCh, io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- fw.ForwardPorts() }()

	select {
	case <-readyCh:
	case err := <-errCh:
		t.Fatalf("ForwardPorts: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("port-forward not ready")
	}

	ports, err := fw.GetPorts()
	if err != nil || len(ports) == 0 {
		t.Fatalf("GetPorts: %v", err)
	}
	local := ports[0].Local

	// Two sequential requests over the same SPDY session (connection reuse).
	for i := 0; i < 2; i++ {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/hello%d", local, i))
		if err != nil {
			t.Fatalf("request %d through tunnel: %v", i, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		want := fmt.Sprintf("router-saw:/hello%d", i)
		if string(body) != want {
			t.Fatalf("request %d: body = %q, want %q", i, body, want)
		}
	}
}

// TestPortForwardRejectsUnknownPod verifies non-router pods are refused.
func TestPortForwardRejectsUnknownPod(t *testing.T) {
	pf := New("127.0.0.1:1", slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux := http.NewServeMux()
	mux.Handle("/api/v1/namespaces/{namespace}/pods/{name}/portforward", pf)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/api/v1/namespaces/default/pods/some-other-pod/portforward", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown pod: status = %d, want 404", resp.StatusCode)
	}
}

func mustHost(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u.Host
}
