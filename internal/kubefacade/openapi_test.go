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
	"testing"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
	"github.com/dvaldivia/local-agent-sandbox/internal/store"
)

// TestOpenAPIV3DocStructure locks in the structure kubectl's query-param
// verifier requires: a path per resource whose mutating operation carries the
// operation-level x-kubernetes-group-version-kind extension and a
// fieldValidation query parameter. A regression here silently breaks
// `kubectl apply`.
func TestOpenAPIV3DocStructure(t *testing.T) {
	h := New(Options{Store: store.New(store.Options{}), Registry: apis.NewDefaultRegistry()})
	doc := h.openAPIV3Doc("extensions.agents.x-k8s.io", "v1beta1")

	collPath := "/apis/extensions.agents.x-k8s.io/v1beta1/namespaces/{namespace}/sandboxtemplates"
	p, ok := doc.Paths.Paths[collPath]
	if !ok {
		t.Fatalf("missing collection path %q; have %v", collPath, keysOf(doc.Paths.Paths))
	}
	if p.Post == nil {
		t.Fatal("collection path has no POST operation")
	}
	ext := p.Post.Extensions["x-kubernetes-group-version-kind"]
	if ext == nil {
		t.Fatal("POST operation missing x-kubernetes-group-version-kind extension")
	}
	var hasFieldValidation bool
	for _, param := range p.Post.Parameters {
		if param.Name == "fieldValidation" && param.In == "query" {
			hasFieldValidation = true
		}
	}
	if !hasFieldValidation {
		t.Fatal("POST operation missing fieldValidation query parameter")
	}

	// Schema component tagged with the GVK must exist.
	if _, ok := doc.Components.Schemas["extensions.agents.x-k8s.io.v1beta1.SandboxTemplate"]; !ok {
		t.Fatalf("missing schema component; have %v", keysOf(doc.Components.Schemas))
	}
}

func keysOf[V any](m map[string]V) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}
