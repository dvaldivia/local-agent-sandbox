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
	"strconv"

	jsonpatch "github.com/evanphx/json-patch/v5"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
)

// Patch applies a patch of the given type to an existing object. It supports
// JSON merge patch (RFC 7386), JSON patch (RFC 6902), strategic-merge (treated
// as merge patch for unstructured/CRDs, matching real apiserver behavior), and
// server-side apply (treated as a merge patch onto the existing object — field
// management is not emulated). When isStatus is true only the status
// subresource is affected.
func (s *Store) Patch(gvr apis.GVR, namespace, name string, patchType types.PatchType, data []byte, isStatus bool) (*unstructured.Unstructured, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rm, err := s.meta(gvr)
	if err != nil {
		return nil, err
	}
	if !rm.namespaced {
		namespace = ""
	}
	k := key(rm.namespaced, namespace, name)
	old, ok := s.data[gvr][k]
	if !ok {
		return nil, apierrors.NewNotFound(rm.groupRes, name)
	}

	oldJSON, err := old.MarshalJSON()
	if err != nil {
		return nil, apierrors.NewInternalError(err)
	}

	var patchedJSON []byte
	switch patchType {
	case types.JSONPatchType:
		p, err := jsonpatch.DecodePatch(data)
		if err != nil {
			return nil, apierrors.NewBadRequest(fmt.Sprintf("store: invalid json patch: %v", err))
		}
		patchedJSON, err = p.Apply(oldJSON)
		if err != nil {
			return nil, apierrors.NewBadRequest(fmt.Sprintf("store: json patch apply failed: %v", err))
		}
	case types.MergePatchType, types.StrategicMergePatchType:
		patchedJSON, err = jsonpatch.MergePatch(oldJSON, data)
		if err != nil {
			return nil, apierrors.NewBadRequest(fmt.Sprintf("store: merge patch failed: %v", err))
		}
	case types.ApplyPatchType:
		// kubectl apply (server-side apply). Convert YAML->JSON and merge onto
		// the existing object. Field management/ownership is intentionally not
		// tracked for local dev.
		jsonData, err := sigsyaml.YAMLToJSON(data)
		if err != nil {
			return nil, apierrors.NewBadRequest(fmt.Sprintf("store: apply patch yaml decode failed: %v", err))
		}
		patchedJSON, err = jsonpatch.MergePatch(oldJSON, jsonData)
		if err != nil {
			return nil, apierrors.NewBadRequest(fmt.Sprintf("store: apply patch merge failed: %v", err))
		}
	default:
		return nil, apierrors.NewBadRequest(fmt.Sprintf("store: unsupported patch type %q", patchType))
	}

	patched := &unstructured.Unstructured{}
	if err := patched.UnmarshalJSON(patchedJSON); err != nil {
		return nil, apierrors.NewInternalError(err)
	}

	// Optimistic concurrency: a patch body (e.g. controller-runtime
	// MergeFromWithOptimisticLock) may embed a resourceVersion precondition.
	if prv := patched.GetResourceVersion(); prv != "" && prv != old.GetResourceVersion() {
		return nil, conflictErr(rm.groupRes, name)
	}
	merged := s.mergeForUpdate(old, patched, isStatus)
	rv := s.nextRV()
	merged.SetResourceVersion(strconv.FormatUint(rv, 10))
	s.putLocked(gvr, k, merged, rv)
	s.emitLocked(Event{Type: Modified, Object: merged.DeepCopy(), RV: rv, GVR: gvr})
	return merged.DeepCopy(), nil
}
