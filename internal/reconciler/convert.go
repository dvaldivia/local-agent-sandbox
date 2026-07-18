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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// fromUnstructured decodes an unstructured object into a typed struct.
func fromUnstructured[T any](u *unstructured.Unstructured, out *T) error {
	return runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, out)
}

// toUnstructured encodes a typed object into an *unstructured.Unstructured.
func toUnstructured(obj any) (*unstructured.Unstructured, error) {
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: m}, nil
}

// ToUnstructured is the exported form used by the app to build bootstrap CRs.
func ToUnstructured(obj any) (*unstructured.Unstructured, error) { return toUnstructured(obj) }

// setStatus builds an unstructured copy of obj with its status replaced by the
// unstructured form of statusObj (a typed status struct). Used to write status
// via the store's /status path.
//
//nolint:unused // reserved for future condition helpers
func setStatus(obj *unstructured.Unstructured, statusObj any) (*unstructured.Unstructured, error) {
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(statusObj)
	if err != nil {
		return nil, err
	}
	out := obj.DeepCopy()
	// Drop zero-value fields the converter emits (creationTimestamp: null etc.
	// don't appear on plain status structs, but be defensive).
	if err := unstructured.SetNestedMap(out.Object, m, "status"); err != nil {
		return nil, err
	}
	return out, nil
}
