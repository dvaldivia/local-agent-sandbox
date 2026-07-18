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
	"net/http"

	"k8s.io/kube-openapi/pkg/spec3"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// OpenAPI v3 support. kubectl downloads these documents to validate objects
// client-side before apply/create; a missing or malformed doc makes it refuse
// with "failed to download openapi" (and fall back to the protobuf-only v2
// endpoint). We build documents with kube-openapi's own spec3 types so
// kubectl's parser accepts them, and use permissive schemas
// (preserve-unknown-fields) so any well-formed object for a known GVK
// validates — matching how kubectl treats CRDs without structural schemas.

// handleOpenAPIV3Root serves GET /openapi/v3 — the index of per-group-version
// schema documents.
func (h *Handler) handleOpenAPIV3Root(w http.ResponseWriter, _ *http.Request) {
	paths := map[string]any{}
	paths["api/v1"] = map[string]any{"serverRelativeURL": "/openapi/v3/api/v1?hash=lasd"}
	seen := map[string]bool{}
	for _, res := range h.reg.All() {
		if res.GVR.Group == "" {
			continue
		}
		gv := res.GVR.Group + "/" + res.GVR.Version
		if seen[gv] {
			continue
		}
		seen[gv] = true
		paths["apis/"+gv] = map[string]any{"serverRelativeURL": "/openapi/v3/apis/" + gv + "?hash=lasd"}
	}
	writeJSON(w, http.StatusOK, map[string]any{"paths": paths})
}

// handleOpenAPIV3Core serves GET /openapi/v3/api/v1.
func (h *Handler) handleOpenAPIV3Core(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.openAPIV3Doc("", "v1"))
}

// handleOpenAPIV3Group serves GET /openapi/v3/apis/{group}/{version}.
func (h *Handler) handleOpenAPIV3Group(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.openAPIV3Doc(r.PathValue("group"), r.PathValue("version")))
}

// openAPIV3Doc builds a valid, permissive OpenAPI v3 document for a
// group/version. Besides a GVK-tagged permissive schema per kind, it emits the
// resource paths (collection POST, item PUT/PATCH) whose requestBody references
// that schema and which declare the `fieldValidation` query parameter. kubectl's
// query-param verifier looks up the GVK via these paths; without them it deems
// the document unusable and falls back to the protobuf-only v2 endpoint.
func (h *Handler) openAPIV3Doc(group, version string) *spec3.OpenAPI {
	schemas := map[string]*spec.Schema{}
	paths := map[string]*spec3.Path{}

	prefix := "/apis/" + group + "/" + version
	if group == "" {
		prefix = "/api/" + version
	}

	for _, res := range h.reg.ResourcesForGroupVersion(group, version) {
		schemaName := group + "." + version + "." + res.Kind
		if group == "" {
			schemaName = "io.lasd." + version + "." + res.Kind
		}
		s := &spec.Schema{}
		s.Type = spec.StringOrArray{"object"}
		s.AddExtension("x-kubernetes-preserve-unknown-fields", true)
		s.AddExtension("x-kubernetes-group-version-kind", []any{
			map[string]any{"group": group, "version": version, "kind": res.Kind},
		})
		schemas[schemaName] = s

		collPath := prefix + "/" + res.GVR.Resource
		itemPath := collPath + "/{name}"
		if res.Namespaced {
			collPath = prefix + "/namespaces/{namespace}/" + res.GVR.Resource
			itemPath = collPath + "/{name}"
		}
		gvk := map[string]any{"group": group, "version": version, "kind": res.Kind}
		paths[collPath] = &spec3.Path{PathProps: spec3.PathProps{
			Post: mutatingOp(schemaName, gvk),
		}}
		paths[itemPath] = &spec3.Path{PathProps: spec3.PathProps{
			Put:   mutatingOp(schemaName, gvk),
			Patch: mutatingOp(schemaName, gvk),
		}}
	}

	return &spec3.OpenAPI{
		Version: "3.0.0",
		Info: &spec.Info{
			InfoProps: spec.InfoProps{Title: "lasd", Version: GitVersion},
		},
		Paths:      &spec3.Paths{Paths: paths},
		Components: &spec3.Components{Schemas: schemas},
	}
}

// mutatingOp builds an operation whose JSON requestBody references the named
// schema, which advertises the fieldValidation query parameter, and which
// carries the operation-level x-kubernetes-group-version-kind extension that
// kubectl's query-param verifier uses to locate the path for a GVK.
func mutatingOp(schemaName string, gvk map[string]any) *spec3.Operation {
	op := &spec3.Operation{OperationProps: spec3.OperationProps{
		Parameters: []*spec3.Parameter{{
			ParameterProps: spec3.ParameterProps{
				Name:   "fieldValidation",
				In:     "query",
				Schema: &spec.Schema{SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
			},
		}},
		RequestBody: &spec3.RequestBody{RequestBodyProps: spec3.RequestBodyProps{
			Content: map[string]*spec3.MediaType{
				"application/json": {MediaTypeProps: spec3.MediaTypeProps{
					Schema: &spec.Schema{SchemaProps: spec.SchemaProps{
						Ref: spec.MustCreateRef("#/components/schemas/" + schemaName),
					}},
				}},
			},
		}},
	}}
	op.AddExtension("x-kubernetes-group-version-kind", gvk)
	return op
}
