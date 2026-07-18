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

package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

const testImageTag = "lasd/sandbox-runtime:test"

var (
	buildOnce sync.Once
	buildErr  error
)

// driverOrSkip returns a live Docker driver or skips the test. Integration
// tests are opt-in (LASD_DOCKER_TESTS=1) and additionally skip if Docker is
// unreachable.
func driverOrSkip(t *testing.T) *DockerDriver {
	t.Helper()
	if os.Getenv("LASD_DOCKER_TESTS") == "" {
		t.Skip("set LASD_DOCKER_TESTS=1 to run Docker integration tests")
	}
	d, err := NewDockerDriver()
	if err != nil {
		t.Skipf("docker client unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.Ping(ctx); err != nil {
		t.Skipf("docker daemon unreachable: %v", err)
	}
	return d
}

func runtimeContextDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(thisFile), "..", "..", "runtime")
	if _, err := os.Stat(filepath.Join(dir, "Dockerfile")); err != nil {
		t.Fatalf("runtime context dir not found at %s: %v", dir, err)
	}
	return dir
}

func ensureTestImage(t *testing.T, d *DockerDriver) {
	t.Helper()
	buildOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		buildErr = d.BuildRuntimeImage(ctx, testImageTag, runtimeContextDir(t))
	})
	if buildErr != nil {
		t.Fatalf("build runtime image: %v", buildErr)
	}
}

func TestDockerLifecycle(t *testing.T) {
	d := driverOrSkip(t)
	defer d.Close()
	ensureTestImage(t, d)
	ctx := context.Background()

	if err := d.EnsureImage(ctx, testImageTag, PullNever); err != nil {
		t.Fatalf("EnsureImage(Never) with present image: %v", err)
	}

	spec := SandboxContainerSpec{
		Namespace: "default", SandboxName: "lifecycle", UID: "uid-lifecycle",
		Image: testImageTag, ServerPort: 8888, RuntimePorts: []int{8888},
	}
	info, err := d.CreateSandboxContainer(ctx, spec)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = d.RemoveContainer(context.Background(), info.ID) })

	hostPort := info.PortMap[8888]
	if hostPort == 0 {
		t.Fatalf("no host port mapped for 8888: %+v", info.PortMap)
	}

	// Probe until ready.
	if err := probeUntilReady(ctx, d, hostPort, 30*time.Second); err != nil {
		t.Fatalf("runtime never ready: %v", err)
	}

	// Exercise the runtime API through the localhost-published port.
	out := execViaRuntime(t, hostPort, "echo integration-ok")
	if out.ExitCode != 0 || out.Stdout == "" {
		t.Fatalf("execute via runtime: %+v", out)
	}

	// Inspect reports running + a bridge IP.
	got, err := d.InspectSandbox(ctx, "default", "lifecycle")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if got.State != "running" {
		t.Errorf("state = %q, want running", got.State)
	}

	if err := d.StopContainer(ctx, info.ID, 5*time.Second); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := d.RemoveContainer(ctx, info.ID); err != nil {
		t.Fatalf("remove: %v", err)
	}
}

func TestDockerVolumePersistence(t *testing.T) {
	d := driverOrSkip(t)
	defer d.Close()
	ensureTestImage(t, d)
	ctx := context.Background()

	volName := VolumeName("default", "data-persist")
	if _, err := d.EnsureVolume(ctx, VolumeSpec{Name: volName, Namespace: "default", PVCName: "data-persist"}); err != nil {
		t.Fatalf("ensure volume: %v", err)
	}
	t.Cleanup(func() { _ = d.RemoveVolume(context.Background(), volName) })

	// First container: write a file into the mounted volume via the runtime.
	spec := SandboxContainerSpec{
		Namespace: "default", SandboxName: "persist1", UID: "uid-p1",
		Image: testImageTag, ServerPort: 8888, RuntimePorts: []int{8888},
		Mounts: []Mount{{VolumeName: volName, MountPath: "/data"}},
	}
	c1, err := d.CreateSandboxContainer(ctx, spec)
	if err != nil {
		t.Fatalf("create c1: %v", err)
	}
	t.Cleanup(func() { _ = d.RemoveContainer(context.Background(), c1.ID) })
	p1 := c1.PortMap[8888]
	if err := probeUntilReady(ctx, d, p1, 30*time.Second); err != nil {
		t.Fatalf("c1 not ready: %v", err)
	}
	if r := execViaRuntime(t, p1, "sh -c 'echo persisted > /data/marker.txt'"); r.ExitCode != 0 {
		t.Fatalf("write marker: %+v", r)
	}
	if err := d.RemoveContainer(ctx, c1.ID); err != nil {
		t.Fatalf("remove c1: %v", err)
	}

	// Second container mounting the same volume sees the file.
	spec2 := spec
	spec2.SandboxName = "persist2"
	spec2.UID = "uid-p2"
	c2, err := d.CreateSandboxContainer(ctx, spec2)
	if err != nil {
		t.Fatalf("create c2: %v", err)
	}
	t.Cleanup(func() { _ = d.RemoveContainer(context.Background(), c2.ID) })
	p2 := c2.PortMap[8888]
	if err := probeUntilReady(ctx, d, p2, 30*time.Second); err != nil {
		t.Fatalf("c2 not ready: %v", err)
	}
	r := execViaRuntime(t, p2, "cat /data/marker.txt")
	if r.ExitCode != 0 || !bytes.Contains([]byte(r.Stdout), []byte("persisted")) {
		t.Fatalf("volume did not persist across containers: %+v", r)
	}
}

func TestDockerListManagedSurvivesReconnect(t *testing.T) {
	d := driverOrSkip(t)
	defer d.Close()
	ensureTestImage(t, d)
	ctx := context.Background()

	spec := SandboxContainerSpec{
		Namespace: "adopt", SandboxName: "survivor", UID: "uid-surv",
		Image: testImageTag, ServerPort: 8888, RuntimePorts: []int{8888},
	}
	info, err := d.CreateSandboxContainer(ctx, spec)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = d.RemoveContainer(context.Background(), info.ID) })

	// A fresh driver instance (simulating a lasd restart) rediscovers the
	// container purely from labels.
	d2, err := NewDockerDriver()
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	managed, err := d2.ListManaged(ctx)
	if err != nil {
		t.Fatalf("list managed: %v", err)
	}
	found := false
	for _, m := range managed {
		if m.Namespace() == "adopt" && m.Sandbox() == "survivor" {
			found = true
		}
	}
	if !found {
		t.Fatal("restarted driver did not rediscover the container from labels")
	}
}

func probeUntilReady(ctx context.Context, d *DockerDriver, hostPort int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		if err := d.ProbeRuntime(ctx, hostPort); err == nil {
			return nil
		} else {
			last = err
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("timeout: %w", last)
}

type execResult struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
}

func execViaRuntime(t *testing.T, hostPort int, command string) execResult {
	t.Helper()
	payload, _ := json.Marshal(map[string]string{"command": command})
	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/execute", hostPort), "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("post execute: %v", err)
	}
	defer resp.Body.Close()
	var r execResult
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatalf("decode execute: %v", err)
	}
	return r
}
