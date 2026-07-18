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

package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/rest"

	"sigs.k8s.io/agent-sandbox/clients/go/sandbox"
)

// TestGoSDKTunnelMode drives the unmodified Go SDK in its DEFAULT connection
// mode (no APIURL): it discovers the router via EndpointSlices and reaches it
// through the emulated SPDY port-forward — the "zero connection config" path.
func TestGoSDKTunnelMode(t *testing.T) {
	s := newStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// No APIURL and no GatewayName => the SDK uses its tunnel (port-forward)
	// strategy against the facade referenced by RestConfig.
	client, err := sandbox.NewClient(ctx, sandbox.Options{
		RestConfig:              &rest.Config{Host: s.FacadeURL},
		WarmPoolName:            "default",
		Namespace:               "default",
		Quiet:                   true,
		SandboxReadyTimeout:     2 * time.Minute,
		PortForwardReadyTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.DeleteAll(context.Background())

	sb, err := client.CreateSandbox(ctx, "default", "default")
	if err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	t.Logf("tunnel sandbox ready: claim=%s sandbox=%s", sb.ClaimName(), sb.SandboxName())

	res, err := sb.Run(ctx, "echo tunnel-ok")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ExitCode != 0 || strings.TrimSpace(res.Stdout) != "tunnel-ok" {
		t.Fatalf("Run via tunnel: %+v", res)
	}

	if err := sb.Write(ctx, "t.txt", []byte("via-tunnel")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	data, err := sb.Read(ctx, "t.txt")
	if err != nil || string(data) != "via-tunnel" {
		t.Fatalf("Read via tunnel = %q, %v", data, err)
	}
}
