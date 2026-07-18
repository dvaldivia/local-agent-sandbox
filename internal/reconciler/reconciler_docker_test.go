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

package reconciler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
	"github.com/dvaldivia/local-agent-sandbox/internal/driver"
	"github.com/dvaldivia/local-agent-sandbox/internal/store"
)

const dockerTestImage = "lasd/sandbox-runtime:rectest"

// TestDockerFullStack exercises the real reconcilers against real Docker: a
// claim cold-starts a container from the bundled runtime image, becomes Ready,
// serves the runtime API through its published port, and is torn down on
// claim deletion.
func TestDockerFullStack(t *testing.T) {
	if os.Getenv("LASD_DOCKER_TESTS") == "" {
		t.Skip("set LASD_DOCKER_TESTS=1 to run Docker integration tests")
	}
	d, err := driver.NewDockerDriver()
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	pingCtx, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	if err := d.Ping(pingCtx); err != nil {
		cancelPing()
		t.Skipf("docker daemon unreachable: %v", err)
	}
	cancelPing()
	defer d.Close()

	buildCtx := runtimeDir(t)
	bctx, cancelBuild := context.WithTimeout(context.Background(), 5*time.Minute)
	if err := d.BuildRuntimeImage(bctx, dockerTestImage, buildCtx); err != nil {
		cancelBuild()
		t.Fatalf("build runtime image: %v", err)
	}
	cancelBuild()

	// Harness with the real driver.
	st := store.New(store.Options{})
	reg := apis.NewDefaultRegistry()
	for _, res := range reg.All() {
		st.Register(res.GVR, res.Namespaced)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := &Reconcilers{Store: st, Driver: d, Log: log, RuntimeImage: dockerTestImage, ClusterDomain: "cluster.local"}
	st.SetDeleteHook(r.OnDelete)
	m := NewManager(st, log)
	r.Wire(m)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = m.Start(ctx) }()
	r.StartDriverEvents(ctx)

	// Ensure clean state and clean up after.
	t.Cleanup(func() {
		cs, _ := d.ListManaged(context.Background())
		for _, c := range cs {
			if c.Namespace() == "default" {
				_ = d.RemoveContainer(context.Background(), c.ID)
			}
		}
	})

	// Template uses the just-built image.
	mkTemplateImage(t, st, "default", "default", dockerTestImage)
	mkWarmPool(t, st, "default", "default", "default", 0)
	mkClaim(t, st, "default", "sandbox-claim-dock", "default", nil)

	c := waitClaim(t, st, "default", "sandbox-claim-dock", claimReady, 90*time.Second)
	if c.Status.SandboxStatus.Name != "sandbox-claim-dock" {
		t.Fatalf("sandbox name = %q", c.Status.SandboxStatus.Name)
	}

	// Reach the runtime API through the published localhost port.
	info, err := d.InspectSandbox(context.Background(), "default", "sandbox-claim-dock")
	if err != nil {
		t.Fatal(err)
	}
	hostPort := info.PortMap[8888]
	if hostPort == 0 {
		t.Fatalf("no host port for 8888: %+v", info.PortMap)
	}
	payload, _ := json.Marshal(map[string]string{"command": "echo full-stack-ok"})
	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/execute", hostPort), "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Stdout   string `json:"stdout"`
		ExitCode int    `json:"exit_code"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.ExitCode != 0 {
		t.Fatalf("runtime execute failed: %+v", out)
	}

	// Delete the claim; cascade must remove the container.
	if _, err := st.Delete(apis.SandboxClaimGVR, "default", "sandbox-claim-dock"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, ierr := d.InspectSandbox(context.Background(), "default", "sandbox-claim-dock"); errors.Is(ierr, driver.ErrNotFound) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("container not removed after claim deletion")
}

func mkTemplateImage(t *testing.T, st *store.Store, ns, name, image string) {
	t.Helper()
	mkTemplate(t, st, ns, name)
	// Patch the image to the requested one.
	patch := []byte(fmt.Sprintf(`{"spec":{"podTemplate":{"spec":{"containers":[{"name":"runtime","image":%q,"ports":[{"containerPort":8888}]}]}}}}`, image))
	if _, err := st.Patch(apis.TemplateGVR, ns, name, "application/merge-patch+json", patch, false); err != nil {
		t.Fatal(err)
	}
}

func runtimeDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(thisFile), "..", "..", "runtime")
	if _, err := os.Stat(filepath.Join(dir, "Dockerfile")); err != nil {
		t.Fatalf("runtime context dir not found: %v", err)
	}
	return dir
}
