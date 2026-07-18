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

// Package portforward implements the server side of the Kubernetes
// pod/portforward subresource over SPDY. It lets the agent-sandbox Go SDK's
// default tunnel strategy (native SPDY port-forward) and kubectl port-forward
// reach the local data-plane router without any SDK changes. Every forwarded
// connection is bridged to the router's real listener (the requested port is
// ignored — only the router is a valid target).
package portforward

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/streaming/pkg/httpstream"
	"k8s.io/streaming/pkg/httpstream/spdy"
)

const (
	portForwardProtocolV1Name = "portforward.k8s.io"
	idleTimeout               = 10 * time.Minute
)

// RouterPodName is the virtual pod that hosts the router; portforward requests
// to any other pod are rejected.
const RouterPodName = "sandbox-router-0"

// Server bridges SPDY port-forward streams to the router's TCP listener.
type Server struct {
	// RouterAddr is the real address of the local router (e.g. 127.0.0.1:8880).
	RouterAddr string
	Log        *slog.Logger
}

// New creates a port-forward server bridging to routerAddr.
func New(routerAddr string, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{RouterAddr: routerAddr, Log: log}
}

func (s *Server) dial() (net.Conn, error) {
	return net.DialTimeout("tcp", s.RouterAddr, 5*time.Second)
}

// ServeHTTP handles POST/GET /api/v1/namespaces/{ns}/pods/{name}/portforward.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if name := r.PathValue("name"); name != RouterPodName {
		http.Error(w, fmt.Sprintf("port-forward only supported for pod %q", RouterPodName), http.StatusNotFound)
		return
	}
	if _, err := httpstream.Handshake(r, w, []string{portForwardProtocolV1Name}); err != nil {
		// Handshake wrote the error response.
		return
	}

	streamChan := make(chan httpstream.Stream, 1)
	upgrader := spdy.NewResponseUpgrader()
	conn := upgrader.UpgradeResponse(w, r, func(stream httpstream.Stream, _ <-chan struct{}) error {
		streamChan <- stream
		return nil
	})
	if conn == nil {
		return // upgrade failed; response already written
	}
	defer conn.Close()
	conn.SetIdleTimeout(idleTimeout)

	h := &connHandler{server: s, conn: conn, streamChan: streamChan, pairs: map[string]*streamPair{}}
	h.run()
}

// connHandler demultiplexes streams on one SPDY connection into request pairs.
type connHandler struct {
	server     *Server
	conn       httpstream.Connection
	streamChan chan httpstream.Stream

	mu    sync.Mutex
	pairs map[string]*streamPair
}

type streamPair struct {
	requestID   string
	dataStream  httpstream.Stream
	errorStream httpstream.Stream
	created     time.Time
}

func (h *connHandler) run() {
	for {
		select {
		case <-h.conn.CloseChan():
			return
		case stream := <-h.streamChan:
			h.onStream(stream)
		}
	}
}

func (h *connHandler) onStream(stream httpstream.Stream) {
	requestID := stream.Headers().Get(corev1.PortForwardRequestIDHeader)
	if requestID == "" {
		h.server.Log.Warn("portforward: stream without requestID; resetting")
		_ = stream.Reset()
		return
	}
	streamType := stream.Headers().Get(corev1.StreamType)

	h.mu.Lock()
	p, ok := h.pairs[requestID]
	if !ok {
		p = &streamPair{requestID: requestID, created: time.Now()}
		h.pairs[requestID] = p
	}
	switch streamType {
	case corev1.StreamTypeError:
		p.errorStream = stream
	case corev1.StreamTypeData:
		p.dataStream = stream
	default:
		h.mu.Unlock()
		h.server.Log.Warn("portforward: unknown stream type", "type", streamType)
		_ = stream.Reset()
		return
	}
	complete := p.dataStream != nil && p.errorStream != nil
	if complete {
		delete(h.pairs, requestID)
	}
	h.mu.Unlock()

	if complete {
		go h.forward(p)
	}
}

// forward bridges a data stream to the router, reporting dial/copy failures on
// the error stream (matching kubelet's behavior).
func (h *connHandler) forward(p *streamPair) {
	defer p.dataStream.Close()
	defer p.errorStream.Close()

	target, err := h.server.dial()
	if err != nil {
		fmt.Fprintf(p.errorStream, "error dialing router: %v", err)
		return
	}
	defer target.Close()

	done := make(chan struct{}, 2)
	// client -> router
	go func() {
		_, _ = io.Copy(target, p.dataStream)
		if tc, ok := target.(*net.TCPConn); ok {
			_ = tc.CloseWrite() // half-close so the router sees end-of-request
		}
		done <- struct{}{}
	}()
	// router -> client
	go func() {
		_, _ = io.Copy(p.dataStream, target)
		done <- struct{}{}
	}()
	<-done
	<-done
}

var _ http.Handler = (*Server)(nil)
