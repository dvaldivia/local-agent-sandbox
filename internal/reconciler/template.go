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
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
)

// reconcileTemplate validates a SandboxTemplate. The type has no status
// subresource, so this only logs actionable warnings (single-container
// restriction, missing pod spec).
func (r *Reconcilers) reconcileTemplate(ctx context.Context, ns, name string) (Result, error) {
	u, err := r.Store.Get(apis.TemplateGVR, ns, name)
	if apierrors.IsNotFound(err) {
		return Result{}, nil
	}
	if err != nil {
		return Result{}, err
	}
	var tmpl extv1beta1.SandboxTemplate
	if err := fromUnstructured(u, &tmpl); err != nil {
		return Result{}, err
	}
	if len(tmpl.Spec.PodTemplate.Spec.Containers) == 0 {
		r.Log.Warn("SandboxTemplate has no containers", "template", ns+"/"+name)
	} else if len(tmpl.Spec.PodTemplate.Spec.Containers) > 1 {
		r.Log.Warn("SandboxTemplate has multiple containers; only the first is run locally", "template", ns+"/"+name)
	}
	return Result{}, nil
}
