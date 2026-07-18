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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/version"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
)

// GitVersion is advertised on /version; the -lasd suffix flags the emulator.
var GitVersion = "v1.33.0-lasd"

func (h *Handler) handleVersion(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, &version.Info{
		Major:        "1",
		Minor:        "33",
		GitVersion:   GitVersion,
		GitTreeState: "clean",
		Platform:     "local-docker",
	})
}

// handleAPIVersions serves GET /api (the core group's available versions).
func (h *Handler) handleAPIVersions(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, &metav1.APIVersions{
		TypeMeta: metav1.TypeMeta{Kind: "APIVersions"},
		Versions: []string{"v1"},
	})
}

// handleAPIGroupList serves GET /apis.
func (h *Handler) handleAPIGroupList(w http.ResponseWriter, _ *http.Request) {
	var groups []metav1.APIGroup
	for _, g := range h.reg.Groups() {
		// Collect distinct versions for this group.
		seen := map[string]bool{}
		var gvs []metav1.GroupVersionForDiscovery
		for _, res := range h.reg.All() {
			if res.GVR.Group != g || seen[res.GVR.Version] {
				continue
			}
			seen[res.GVR.Version] = true
			gvs = append(gvs, metav1.GroupVersionForDiscovery{
				GroupVersion: g + "/" + res.GVR.Version,
				Version:      res.GVR.Version,
			})
		}
		if len(gvs) == 0 {
			continue
		}
		groups = append(groups, metav1.APIGroup{
			Name:             g,
			Versions:         gvs,
			PreferredVersion: gvs[0],
		})
	}
	writeJSON(w, http.StatusOK, &metav1.APIGroupList{
		TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"},
		Groups:   groups,
	})
}

// handleCoreResourceList serves GET /api/v1.
func (h *Handler) handleCoreResourceList(w http.ResponseWriter, r *http.Request) {
	h.writeResourceList(w, "", "v1")
}

// handleGroupResourceList serves GET /apis/{group}/{version}.
func (h *Handler) handleGroupResourceList(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")
	version := r.PathValue("version")
	h.writeResourceList(w, group, version)
}

func (h *Handler) writeResourceList(w http.ResponseWriter, group, version string) {
	{
		resources := h.reg.ResourcesForGroupVersion(group, version)
		var list []metav1.APIResource
		groupVersion := version
		if group != "" {
			groupVersion = group + "/" + version
		}
		for _, res := range resources {
			list = append(list, metav1.APIResource{
				Name:         res.GVR.Resource,
				SingularName: res.Singular,
				Namespaced:   res.Namespaced,
				Kind:         res.Kind,
				Verbs:        res.Verbs,
				ShortNames:   res.ShortNames,
			})
			if res.HasStatus {
				list = append(list, metav1.APIResource{
					Name:       res.GVR.Resource + "/status",
					Namespaced: res.Namespaced,
					Kind:       res.Kind,
					Verbs:      []string{"get", "patch", "update"},
				})
			}
			if res.HasScale {
				list = append(list, metav1.APIResource{
					Name:       res.GVR.Resource + "/scale",
					Namespaced: res.Namespaced,
					Kind:       "Scale",
					Group:      "autoscaling",
					Version:    "v1",
					Verbs:      []string{"get", "patch", "update"},
				})
			}
		}
		writeJSON(w, http.StatusOK, &metav1.APIResourceList{
			TypeMeta:     metav1.TypeMeta{Kind: "APIResourceList"},
			GroupVersion: groupVersion,
			APIResources: list,
		})
	}
}

// resolveByPath resolves the target resource from request path values.
func (h *Handler) resolveByPath(r *http.Request) (apis.Resource, bool) {
	group := r.PathValue("group") // "" for core
	version := r.PathValue("version")
	if version == "" {
		version = "v1"
	}
	resource := r.PathValue("resource")
	return h.reg.ResolveResource(group, version, resource)
}
