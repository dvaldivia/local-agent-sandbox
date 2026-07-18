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

package store

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
)

// Selectors bundles the label and field selectors used to filter list/watch
// results. A nil/empty Selectors matches everything.
type Selectors struct {
	Label labels.Selector
	Field fields.Selector
}

// ParseSelectors builds Selectors from the raw query strings. Empty strings
// yield "match everything" selectors. Only metadata.name and
// metadata.namespace are supported for field selectors (all the SDKs use).
func ParseSelectors(labelSelector, fieldSelector string) (Selectors, error) {
	ls := labels.Everything()
	if labelSelector != "" {
		var err error
		ls, err = labels.Parse(labelSelector)
		if err != nil {
			return Selectors{}, err
		}
	}
	fs := fields.Everything()
	if fieldSelector != "" {
		var err error
		fs, err = fields.ParseSelector(fieldSelector)
		if err != nil {
			return Selectors{}, err
		}
		// Only metadata.name/metadata.namespace are indexed. Reject anything
		// else so callers get an error rather than a silent empty result
		// (matching the apiserver, which rejects unindexed field selectors).
		for _, req := range fs.Requirements() {
			if req.Field != "metadata.name" && req.Field != "metadata.namespace" {
				return Selectors{}, fmt.Errorf("field label %q not supported (only metadata.name and metadata.namespace)", req.Field)
			}
		}
	}
	return Selectors{Label: ls, Field: fs}, nil
}

// Matches reports whether obj satisfies both selectors.
func (s Selectors) Matches(obj *unstructured.Unstructured) bool {
	if s.Label != nil && !s.Label.Empty() {
		if !s.Label.Matches(labels.Set(obj.GetLabels())) {
			return false
		}
	}
	if s.Field != nil && !s.Field.Empty() {
		set := fields.Set{
			"metadata.name":      obj.GetName(),
			"metadata.namespace": obj.GetNamespace(),
		}
		if !s.Field.Matches(set) {
			return false
		}
	}
	return true
}
