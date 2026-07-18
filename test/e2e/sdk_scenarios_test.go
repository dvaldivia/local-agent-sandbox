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
	"testing"
	"time"

	"k8s.io/client-go/rest"

	"sigs.k8s.io/agent-sandbox/clients/go/sandbox"
)

func directClient(t *testing.T, s *stack) *sandbox.Client {
	t.Helper()
	c, err := sandbox.NewClient(context.Background(), sandbox.Options{
		RestConfig:          &rest.Config{Host: s.FacadeURL},
		APIURL:              s.RouterURL,
		WarmPoolName:        "default",
		Namespace:           "default",
		Quiet:               true,
		SandboxReadyTimeout: 2 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestTwoSandboxesIsolated verifies filesystem isolation between sandboxes.
func TestTwoSandboxesIsolated(t *testing.T) {
	s := newStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	client := directClient(t, s)
	defer client.DeleteAll(context.Background())

	a, err := client.CreateSandbox(ctx, "default", "default")
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	b, err := client.CreateSandbox(ctx, "default", "default")
	if err != nil {
		t.Fatalf("create B: %v", err)
	}
	if a.SandboxName() == b.SandboxName() {
		t.Fatal("two sandboxes share a name")
	}

	if err := a.Write(ctx, "only-in-a.txt", []byte("x")); err != nil {
		t.Fatalf("write A: %v", err)
	}
	inA, err := a.Exists(ctx, "only-in-a.txt")
	if err != nil || !inA {
		t.Fatalf("file should exist in A: %v %v", inA, err)
	}
	inB, err := b.Exists(ctx, "only-in-a.txt")
	if err != nil {
		t.Fatalf("exists B: %v", err)
	}
	if inB {
		t.Fatal("filesystem isolation broken: A's file visible in B")
	}
}

// TestGetSandboxReattach verifies a second client can re-attach to an existing
// sandbox by claim name and operate on it.
func TestGetSandboxReattach(t *testing.T) {
	s := newStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	client := directClient(t, s)
	defer client.DeleteAll(context.Background())
	sb, err := client.CreateSandbox(ctx, "default", "default")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	claim := sb.ClaimName()
	if err := sb.Write(ctx, "shared.txt", []byte("hi")); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Fresh client re-attaches by claim name.
	client2 := directClient(t, s)
	sb2, err := client2.GetSandbox(ctx, claim, "default")
	if err != nil {
		t.Fatalf("GetSandbox: %v", err)
	}
	data, err := sb2.Read(ctx, "shared.txt")
	if err != nil || string(data) != "hi" {
		t.Fatalf("re-attached read = %q, %v", data, err)
	}
}
