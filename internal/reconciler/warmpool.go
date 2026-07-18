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
	"sort"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
	"github.com/dvaldivia/local-agent-sandbox/internal/store"
)

// reconcileWarmPool maintains spec.replicas pre-warmed Sandboxes owned by the
// pool. Adopted sandboxes lose the pool ownerRef (the claim takes over) and so
// drop out of the pool's set, triggering replenishment.
func (r *Reconcilers) reconcileWarmPool(ctx context.Context, ns, name string) (Result, error) {
	u, err := r.Store.Get(apis.WarmPoolGVR, ns, name)
	if apierrors.IsNotFound(err) {
		return Result{}, nil
	}
	if err != nil {
		return Result{}, err
	}
	var pool extv1beta1.SandboxWarmPool
	if err := fromUnstructured(u, &pool); err != nil {
		return Result{}, err
	}

	desired := 1 // CRD default
	if pool.Spec.Replicas != nil {
		desired = int(*pool.Spec.Replicas)
	}

	// Resolve the template; absence just means we can't provision yet.
	tmplName := pool.Spec.TemplateRef.Name
	var tmpl extv1beta1.SandboxTemplate
	haveTmpl := false
	if tu, terr := r.Store.Get(apis.TemplateGVR, ns, tmplName); terr == nil {
		if err := fromUnstructured(tu, &tmpl); err == nil {
			haveTmpl = true
		}
	}

	// Current pool members.
	list, err := r.Store.List(apis.SandboxGVR, ns, store.Selectors{})
	if err != nil {
		return Result{}, err
	}
	var mine []*unstructured.Unstructured
	ready := 0
	for _, sb := range list.Items {
		if !ownedBy(sb, "SandboxWarmPool", name) {
			continue
		}
		mine = append(mine, sb)
		if isSandboxReady(sb) {
			ready++
		}
	}

	if haveTmpl && len(mine) < desired {
		labels := map[string]string{
			sandboxv1beta1.SandboxWarmPoolLabel:        nameHash(name),
			sandboxv1beta1.SandboxTemplateRefHashLabel: nameHash(tmplName),
			sandboxv1beta1.SandboxLaunchTypeLabel:      sandboxv1beta1.SandboxLaunchTypeWarm,
			sandboxv1beta1.CreatedByLabel:              "controller",
		}
		for i := 0; i < desired-len(mine); i++ {
			sb, berr := r.buildSandbox(&tmpl, ns, "", name+"-", u, "SandboxWarmPool", labels, nil, nil, nil)
			if berr != nil {
				r.Log.Warn("warmpool: build sandbox", "pool", ns+"/"+name, "err", berr)
				break
			}
			if _, cerr := r.Store.Create(apis.SandboxGVR, sb); cerr != nil {
				return Result{}, cerr
			}
		}
	} else if len(mine) > desired {
		// Delete excess: unready first, then newest.
		sort.Slice(mine, func(i, j int) bool {
			ri, rj := isSandboxReady(mine[i]), isSandboxReady(mine[j])
			if ri != rj {
				return !ri // unready first
			}
			return mine[i].GetCreationTimestamp().After(mine[j].GetCreationTimestamp().Time)
		})
		for i := 0; i < len(mine)-desired; i++ {
			_, _ = r.Store.Delete(apis.SandboxGVR, ns, mine[i].GetName())
		}
	}

	st := extv1beta1.SandboxWarmPoolStatus{
		Replicas:      int32(len(mine)),
		ReadyReplicas: int32(ready),
		Selector:      sandboxv1beta1.SandboxWarmPoolLabel + "=" + nameHash(name),
	}
	if err := r.writeWarmPoolStatus(u, st); err != nil {
		return Result{}, err
	}
	return Result{}, nil
}

func (r *Reconcilers) writeWarmPoolStatus(u *unstructured.Unstructured, st extv1beta1.SandboxWarmPoolStatus) error {
	n, err := setStatus(u, &st)
	if err != nil {
		return err
	}
	n.SetResourceVersion("")
	_, err = r.Store.Update(apis.WarmPoolGVR, n, true)
	if apierrors.IsConflict(err) {
		return nil
	}
	return err
}

// isSandboxReady reports whether an unstructured Sandbox has Ready=True.
func isSandboxReady(u *unstructured.Unstructured) bool {
	var sb sandboxv1beta1.Sandbox
	if err := fromUnstructured(u, &sb); err != nil {
		return false
	}
	return meta.IsStatusConditionTrue(sb.Status.Conditions, string(sandboxv1beta1.SandboxConditionReady))
}

var _ = metav1.Now
