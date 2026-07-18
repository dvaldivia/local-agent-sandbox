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
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/rest"

	"sigs.k8s.io/agent-sandbox/clients/go/sandbox"

	"github.com/dvaldivia/local-agent-sandbox/internal/driver"
)

// TestGoSDKDirectMode drives the unmodified upstream Go SDK end to end in
// Direct (APIURL) mode: control plane via the facade, data plane via the
// router — the whole point of the project.
func TestGoSDKDirectMode(t *testing.T) {
	s := newStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	client, err := sandbox.NewClient(ctx, sandbox.Options{
		RestConfig:          &rest.Config{Host: s.FacadeURL},
		APIURL:              s.RouterURL,
		WarmPoolName:        "default",
		Namespace:           "default",
		Quiet:               true,
		SandboxReadyTimeout: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.DeleteAll(context.Background())

	sb, err := client.CreateSandbox(ctx, "default", "default")
	if err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	t.Logf("sandbox ready: claim=%s sandbox=%s", sb.ClaimName(), sb.SandboxName())

	// Run a command.
	res, err := sb.Run(ctx, "echo hello-sdk")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ExitCode != 0 || strings.TrimSpace(res.Stdout) != "hello-sdk" {
		t.Fatalf("Run result: %+v", res)
	}

	// Write then read a file.
	if err := sb.Write(ctx, "note.txt", []byte("persisted-data")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	data, err := sb.Read(ctx, "note.txt")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(data) != "persisted-data" {
		t.Fatalf("Read = %q", data)
	}

	// Exists + List.
	ok, err := sb.Exists(ctx, "note.txt")
	if err != nil || !ok {
		t.Fatalf("Exists = %v, %v", ok, err)
	}
	entries, err := sb.List(ctx, ".")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	found := false
	for _, e := range entries {
		if e.Name == "note.txt" {
			found = true
		}
	}
	if !found {
		t.Fatalf("List did not include note.txt: %+v", entries)
	}

	// Delete via the SDK; the container should be cleaned up.
	claim := sb.ClaimName()
	if err := client.DeleteSandbox(ctx, claim, "default"); err != nil {
		t.Fatalf("DeleteSandbox: %v", err)
	}
	sbName := sb.SandboxName()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, ierr := s.Driver.InspectSandbox(context.Background(), "default", sbName); errors.Is(ierr, driver.ErrNotFound) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("container not cleaned up after SDK DeleteSandbox")
}
