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

package kubefacade

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dvaldivia/local-agent-sandbox/internal/kubeconfig"
)

// TestKubectlWorkflow drives real kubectl (apply with validation, get, delete)
// against the facade. It is the durable regression guard for the OpenAPI v3
// contract that lets `kubectl apply` succeed. Skipped when kubectl is absent.
func TestKubectlWorkflow(t *testing.T) {
	kubectl, err := exec.LookPath("kubectl")
	if err != nil {
		t.Skip("kubectl not installed")
	}
	srv, _ := newTestServer(t)
	dir := t.TempDir()
	kcPath := filepath.Join(dir, "kubeconfig")
	if err := kubeconfig.Write(srv.URL, kcPath); err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(dir, "cache")

	run := func(stdin string, args ...string) (string, error) {
		full := append([]string{"--kubeconfig", kcPath, "--cache-dir", cache}, args...)
		cmd := exec.Command(kubectl, full...)
		if stdin != "" {
			cmd.Stdin = strings.NewReader(stdin)
		}
		out, err := cmd.CombinedOutput()
		return string(out), err
	}

	const tmpl = `apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxTemplate
metadata: {name: default, namespace: default}
spec: {podTemplate: {spec: {containers: [{name: runtime, image: "lasd/sandbox-runtime:dev"}]}}}
`
	// apply WITH validation (the OpenAPI-dependent path).
	if out, err := run(tmpl, "apply", "-f", "-"); err != nil {
		t.Fatalf("kubectl apply failed (openapi validation regression?): %v\n%s", err, out)
	}
	// generateName create.
	const claim = `apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxClaim
metadata: {generateName: sandbox-claim-, namespace: default}
spec: {warmPoolRef: {name: default}}
`
	if out, err := run(claim, "create", "-f", "-"); err != nil {
		t.Fatalf("kubectl create failed: %v\n%s", err, out)
	}
	// get.
	if out, err := run("", "get", "sandboxtemplates,sandboxclaims"); err != nil {
		t.Fatalf("kubectl get failed: %v\n%s", err, out)
	} else if !strings.Contains(out, "default") {
		t.Fatalf("kubectl get missing template: %s", out)
	}
	// delete.
	if out, err := run("", "delete", "sandboxtemplate", "default"); err != nil {
		t.Fatalf("kubectl delete failed: %v\n%s", err, out)
	}
}
