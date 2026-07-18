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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
	"github.com/dvaldivia/local-agent-sandbox/internal/driver"
)

// reconcilePVC backs a PersistentVolumeClaim object with a docker named volume
// (las-<ns>-<name>) and reflects Bound status — the local equivalent of a
// storage provisioner. Works for both user-created (pre-existing) PVCs and the
// PVC objects the sandbox reconciler materializes from volumeClaimTemplates.
func (r *Reconcilers) reconcilePVC(ctx context.Context, ns, name string) (Result, error) {
	u, err := r.Store.Get(apis.PVCGVR, ns, name)
	if apierrors.IsNotFound(err) {
		return Result{}, nil // volume removal handled by the delete hook
	}
	if err != nil {
		return Result{}, err
	}

	volName := driver.VolumeName(ns, name)
	if _, err := r.Driver.EnsureVolume(ctx, driver.VolumeSpec{Name: volName, Namespace: ns, PVCName: name}); err != nil {
		_ = r.writePVCStatus(u, corev1.ClaimPending)
		return Result{}, err
	}
	return Result{}, r.writePVCStatus(u, corev1.ClaimBound)
}

// writePVCStatus sets status.phase (and mirrors accessModes/capacity from the
// spec when Bound), skipping the write when already up to date.
func (r *Reconcilers) writePVCStatus(u *unstructured.Unstructured, phase corev1.PersistentVolumeClaimPhase) error {
	current, _, _ := unstructured.NestedString(u.Object, "status", "phase")
	if current == string(phase) {
		return nil
	}
	var pvc corev1.PersistentVolumeClaim
	if err := fromUnstructured(u, &pvc); err != nil {
		return err
	}
	st := corev1.PersistentVolumeClaimStatus{Phase: phase}
	if phase == corev1.ClaimBound {
		st.AccessModes = pvc.Spec.AccessModes
		if req, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
			st.Capacity = corev1.ResourceList{corev1.ResourceStorage: req}
		}
	}
	n, err := setStatus(u, &st)
	if err != nil {
		return err
	}
	n.SetResourceVersion("")
	_, err = r.Store.Update(apis.PVCGVR, n, true)
	if apierrors.IsConflict(err) {
		return nil
	}
	return err
}

// ensureVCTPVCObject materializes the PersistentVolumeClaim object for a
// volumeClaimTemplate, controller-owned by the sandbox so claim/sandbox
// deletion cascades to it (upstream naming: <templateName>-<sandboxName>, with
// the sandbox name-hash label). Create-only; AlreadyExists is fine.
func (r *Reconcilers) ensureVCTPVCObject(sbU *unstructured.Unstructured, vct sandboxv1beta1.PersistentVolumeClaimTemplate, pvcName string) {
	labels := map[string]string{}
	for k, v := range vct.Labels {
		labels[k] = v
	}
	labels[sandboxNameHashLabel] = nameHash(sbU.GetName())

	pvc := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		ObjectMeta: metav1.ObjectMeta{
			Name:            pvcName,
			Namespace:       sbU.GetNamespace(),
			Labels:          labels,
			Annotations:     vct.Annotations,
			OwnerReferences: []metav1.OwnerReference{controllerRef(sbU, "agents.x-k8s.io/v1beta1", "Sandbox")},
		},
		Spec: vct.Spec,
	}
	pu, err := toUnstructured(pvc)
	if err != nil {
		r.Log.Warn("build VCT pvc object", "pvc", pvcName, "err", err)
		return
	}
	if _, err := r.Store.Create(apis.PVCGVR, pu); err != nil && !apierrors.IsAlreadyExists(err) {
		r.Log.Warn("create VCT pvc object", "pvc", pvcName, "err", err)
	}
}
