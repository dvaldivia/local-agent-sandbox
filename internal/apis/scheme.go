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

// Package apis holds the GroupVersionResource registry that the local
// control-plane facade serves. It is intentionally decoupled from the object
// store: the store is a generic KV+watch engine, while this package knows the
// concrete Kubernetes resource surface (kinds, plurals, scopes, subresources)
// that the agent-sandbox SDKs and kubectl expect.
package apis

import (
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GVR identifies a resource type by group, version, and plural resource name.
type GVR struct {
	Group    string
	Version  string
	Resource string
}

func (g GVR) String() string { return g.Group + "/" + g.Version + "/" + g.Resource }

// GroupResource returns the schema.GroupResource for this GVR.
func (g GVR) GroupResource() schema.GroupResource {
	return schema.GroupResource{Group: g.Group, Resource: g.Resource}
}

// GroupVersion returns the "group/version" string ("v1" for the core group).
func (g GVR) GroupVersion() string {
	if g.Group == "" {
		return g.Version
	}
	return g.Group + "/" + g.Version
}

// Resource describes a single served resource type.
type Resource struct {
	GVR        GVR
	Kind       string
	ListKind   string
	Singular   string
	ShortNames []string
	Namespaced bool
	// HasStatus enables the /status subresource endpoint.
	HasStatus bool
	// HasScale enables the /scale subresource endpoint.
	HasScale bool
	// Verbs advertised in discovery.
	Verbs []string
}

var defaultVerbs = []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"}

// Group/version constants for the agent-sandbox CRDs.
const (
	GroupAgents     = "agents.x-k8s.io"
	GroupExtensions = "extensions.agents.x-k8s.io"
	GroupDiscovery  = "discovery.k8s.io"
	GroupGateway    = "gateway.networking.k8s.io"
	VersionV1Beta1  = "v1beta1"
)

// ExtensionsGV returns the "group/version" for the extensions CRDs.
func ExtensionsGV() string { return GroupExtensions + "/" + VersionV1Beta1 }

// AgentsGV returns the "group/version" for the agents CRDs.
func AgentsGV() string { return GroupAgents + "/" + VersionV1Beta1 }

// Convenience GVRs referenced across the codebase.
var (
	SandboxGVR       = GVR{GroupAgents, VersionV1Beta1, "sandboxes"}
	SandboxClaimGVR  = GVR{GroupExtensions, VersionV1Beta1, "sandboxclaims"}
	TemplateGVR      = GVR{GroupExtensions, VersionV1Beta1, "sandboxtemplates"}
	WarmPoolGVR      = GVR{GroupExtensions, VersionV1Beta1, "sandboxwarmpools"}
	NamespaceGVR     = GVR{"", "v1", "namespaces"}
	PodGVR           = GVR{"", "v1", "pods"}
	ServiceGVR       = GVR{"", "v1", "services"}
	PVCGVR           = GVR{"", "v1", "persistentvolumeclaims"}
	EndpointSliceGVR = GVR{GroupDiscovery, "v1", "endpointslices"}
	GatewayGVR       = GVR{GroupGateway, "v1", "gateways"}
)

// Registry is an immutable table of served resources with lookup indexes.
type Registry struct {
	byGVR      map[GVR]Resource
	byGVKind   map[string]Resource // "group/version/Kind"
	byResource map[string]Resource // "group/version/plural" and shortnames/singular
	list       []Resource
}

// NewDefaultRegistry returns the registry with every resource the local
// control plane serves.
func NewDefaultRegistry() *Registry {
	rs := []Resource{
		{GVR: SandboxGVR, Kind: "Sandbox", ListKind: "SandboxList", Singular: "sandbox", ShortNames: []string{"sandbox", "sb"}, Namespaced: true, HasStatus: true},
		{GVR: SandboxClaimGVR, Kind: "SandboxClaim", ListKind: "SandboxClaimList", Singular: "sandboxclaim", ShortNames: []string{"sbc"}, Namespaced: true, HasStatus: true},
		{GVR: TemplateGVR, Kind: "SandboxTemplate", ListKind: "SandboxTemplateList", Singular: "sandboxtemplate", ShortNames: []string{"sbt"}, Namespaced: true},
		{GVR: WarmPoolGVR, Kind: "SandboxWarmPool", ListKind: "SandboxWarmPoolList", Singular: "sandboxwarmpool", ShortNames: []string{"sbwp"}, Namespaced: true, HasStatus: true, HasScale: true},
		{GVR: NamespaceGVR, Kind: "Namespace", ListKind: "NamespaceList", Singular: "namespace", ShortNames: []string{"ns"}, Namespaced: false, HasStatus: true},
		{GVR: PodGVR, Kind: "Pod", ListKind: "PodList", Singular: "pod", ShortNames: []string{"po"}, Namespaced: true, HasStatus: true},
		{GVR: ServiceGVR, Kind: "Service", ListKind: "ServiceList", Singular: "service", ShortNames: []string{"svc"}, Namespaced: true, HasStatus: true},
		{GVR: PVCGVR, Kind: "PersistentVolumeClaim", ListKind: "PersistentVolumeClaimList", Singular: "persistentvolumeclaim", ShortNames: []string{"pvc"}, Namespaced: true, HasStatus: true},
		{GVR: EndpointSliceGVR, Kind: "EndpointSlice", ListKind: "EndpointSliceList", Singular: "endpointslice", ShortNames: []string{"eps"}, Namespaced: true},
		{GVR: GatewayGVR, Kind: "Gateway", ListKind: "GatewayList", Singular: "gateway", ShortNames: []string{"gtw"}, Namespaced: true, HasStatus: true},
	}
	reg := &Registry{
		byGVR:      make(map[GVR]Resource),
		byGVKind:   make(map[string]Resource),
		byResource: make(map[string]Resource),
	}
	for _, r := range rs {
		if len(r.Verbs) == 0 {
			r.Verbs = defaultVerbs
		}
		reg.byGVR[r.GVR] = r
		reg.byGVKind[r.GVR.GroupVersion()+"/"+r.Kind] = r
		gv := r.GVR.GroupVersion()
		reg.byResource[gv+"/"+r.GVR.Resource] = r
		reg.byResource[gv+"/"+r.Singular] = r
		for _, sn := range r.ShortNames {
			reg.byResource[gv+"/"+sn] = r
		}
		reg.list = append(reg.list, r)
	}
	return reg
}

// Get returns the resource for a GVR.
func (r *Registry) Get(gvr GVR) (Resource, bool) {
	res, ok := r.byGVR[gvr]
	return res, ok
}

// ResolveResource looks up a resource by group, version, and plural/singular/shortname.
func (r *Registry) ResolveResource(group, version, resource string) (Resource, bool) {
	gv := version
	if group != "" {
		gv = group + "/" + version
	}
	res, ok := r.byResource[gv+"/"+strings.ToLower(resource)]
	return res, ok
}

// All returns every registered resource, sorted by GVR string for stable output.
func (r *Registry) All() []Resource {
	out := make([]Resource, len(r.list))
	copy(out, r.list)
	sort.Slice(out, func(i, j int) bool { return out[i].GVR.String() < out[j].GVR.String() })
	return out
}

// Groups returns the distinct api groups (excluding the core group "") in a
// stable order.
func (r *Registry) Groups() []string {
	seen := map[string]bool{}
	var groups []string
	for _, res := range r.list {
		if res.GVR.Group == "" || seen[res.GVR.Group] {
			continue
		}
		seen[res.GVR.Group] = true
		groups = append(groups, res.GVR.Group)
	}
	sort.Strings(groups)
	return groups
}

// ResourcesForGroupVersion returns resources for a given group/version.
func (r *Registry) ResourcesForGroupVersion(group, version string) []Resource {
	var out []Resource
	for _, res := range r.list {
		if res.GVR.Group == group && res.GVR.Version == version {
			out = append(out, res)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GVR.Resource < out[j].GVR.Resource })
	return out
}
