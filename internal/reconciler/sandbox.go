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
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
	"github.com/dvaldivia/local-agent-sandbox/internal/driver"
)

const readyRequeue = 500 * time.Millisecond

// reconcileSandbox drives a Sandbox CR to a running, health-checked Docker
// container and reflects the observable status/conditions the SDKs consume.
func (r *Reconcilers) reconcileSandbox(ctx context.Context, ns, name string) (Result, error) {
	u, err := r.Store.Get(apis.SandboxGVR, ns, name)
	if apierrors.IsNotFound(err) {
		return Result{}, nil // deletion handled by the OnDelete cascade
	}
	if err != nil {
		return Result{}, err
	}
	var sb sandboxv1beta1.Sandbox
	if err := fromUnstructured(u, &sb); err != nil {
		return Result{}, err
	}

	// Orphan garbage collection: if this sandbox is controller-owned by a
	// SandboxClaim/SandboxWarmPool that no longer exists, delete it (and its
	// container). This mirrors Kubernetes GC and closes the race where a claim
	// reconcile in-flight at delete time resurrects the sandbox after the
	// delete cascade already ran.
	if r.orphaned(u) {
		if _, err := r.Store.Delete(apis.SandboxGVR, ns, name); err != nil && !apierrors.IsNotFound(err) {
			return Result{}, err
		}
		return Result{}, nil
	}

	// Lifecycle expiry.
	if exp := sb.Spec.ShutdownTime; exp != nil && !r.now().Before(exp.Time) {
		return r.handleSandboxExpiry(ctx, u, &sb)
	}

	if sb.Spec.OperatingMode == sandboxv1beta1.SandboxOperatingModeSuspended {
		return r.reconcileSuspended(ctx, u, &sb)
	}
	return r.reconcileRunning(ctx, u, &sb)
}

func (r *Reconcilers) reconcileSuspended(ctx context.Context, u *unstructured.Unstructured, sb *sandboxv1beta1.Sandbox) (Result, error) {
	ns, name := sb.Namespace, sb.Name
	podGone := true
	if info, err := r.Driver.InspectSandbox(ctx, ns, name); err == nil {
		_ = r.Driver.StopContainer(ctx, info.ID, 5*time.Second)
		_ = r.Driver.RemoveContainer(ctx, info.ID)
	} else if !errors.Is(err, driver.ErrNotFound) {
		podGone = false
	}

	conds := sb.Status.Conditions
	if podGone {
		setCond(&conds, sandboxv1beta1.SandboxConditionSuspended, metav1.ConditionTrue, sandboxv1beta1.SandboxReasonSuspendedPodTerminated, "Pod has been terminated. Sandbox is not operational.", sb.Generation, r.now())
		setCond(&conds, sandboxv1beta1.SandboxConditionReady, metav1.ConditionFalse, sandboxv1beta1.SandboxReasonSuspended, "Sandbox is suspended", sb.Generation, r.now())
	} else {
		setCond(&conds, sandboxv1beta1.SandboxConditionSuspended, metav1.ConditionFalse, sandboxv1beta1.SandboxReasonSuspendedPodNotTerminated, "Pod has not been terminated. Sandbox is operational.", sb.Generation, r.now())
		setCond(&conds, sandboxv1beta1.SandboxConditionReady, metav1.ConditionFalse, sandboxv1beta1.SandboxReasonSuspended, "Sandbox is suspending", sb.Generation, r.now())
	}
	meta.RemoveStatusCondition(&conds, string(sandboxv1beta1.SandboxConditionFinished))

	st := sandboxv1beta1.SandboxStatus{Conditions: conds}
	if err := r.writeSandboxStatus(u, st); err != nil {
		return Result{}, err
	}
	return r.expiryRequeue(sb), nil
}

func (r *Reconcilers) reconcileRunning(ctx context.Context, u *unstructured.Unstructured, sb *sandboxv1beta1.Sandbox) (Result, error) {
	ns, name := sb.Namespace, sb.Name
	podSpec := *sb.Spec.PodTemplate.Spec.DeepCopy()
	if len(podSpec.Containers) == 0 {
		return r.failReady(u, sb, "Pod spec has no containers"), nil
	}

	// Ensure PVC-equivalent volumes and inject the pod volume entries.
	volNames := map[string]string{}
	for _, vct := range sb.Spec.VolumeClaimTemplates {
		pvc := vct.Name + "-" + name
		dv := driver.VolumeName(ns, pvc)
		if _, err := r.Driver.EnsureVolume(ctx, driver.VolumeSpec{Name: dv, Namespace: ns, PVCName: pvc}); err != nil {
			return Result{}, err
		}
		volNames[vct.Name] = dv
		podSpec = injectPodVolume(podSpec, vct.Name, pvc)
	}

	image := podSpec.Containers[0].Image
	if err := r.ensureImage(ctx, image); err != nil {
		return r.failReady(u, sb, "image unavailable: "+err.Error()), nil
	}

	info, err := r.Driver.InspectSandbox(ctx, ns, name)
	if errors.Is(err, driver.ErrNotFound) {
		spec, warnings, mErr := driver.MapPodSpec(podSpec, driver.MappingMeta{
			Namespace: ns, SandboxName: name, UID: string(sb.UID),
			ServerPort: podServerPort(podSpec, r.serverPort()), VolumeNames: volNames,
		}, r.serverPort())
		if mErr != nil {
			return r.failReady(u, sb, mErr.Error()), nil // fatal mapping error: no retry storm
		}
		for _, wmsg := range warnings {
			r.Log.Warn("pod mapping", "sandbox", ns+"/"+name, "warning", wmsg)
		}
		info, err = r.Driver.CreateSandboxContainer(ctx, spec)
		if err != nil {
			return Result{}, err
		}
	} else if err != nil {
		return Result{}, err
	}

	r.ensurePodNameAnnotation(u, name)

	serverPort := podServerPort(podSpec, r.serverPort())
	conds := sb.Status.Conditions
	meta.RemoveStatusCondition(&conds, string(sandboxv1beta1.SandboxConditionSuspended))
	st := sandboxv1beta1.SandboxStatus{}

	switch info.State {
	case "exited", "dead":
		reason := sandboxv1beta1.SandboxReasonPodSucceeded
		msg := "Pod completed successfully"
		if info.ExitCode != 0 {
			reason = sandboxv1beta1.SandboxReasonPodFailed
			msg = fmt.Sprintf("Pod failed (exit %d)", info.ExitCode)
		}
		setCond(&conds, sandboxv1beta1.SandboxConditionFinished, metav1.ConditionTrue, reason, msg, sb.Generation, r.now())
		setCond(&conds, sandboxv1beta1.SandboxConditionReady, metav1.ConditionFalse, reason, msg, sb.Generation, r.now())
	case "running":
		meta.RemoveStatusCondition(&conds, string(sandboxv1beta1.SandboxConditionFinished))
		st.NodeName = r.nodeName()
		st.LabelSelector = nameHashSelector(name)
		hostPort := info.PortMap[serverPort]
		ipOK := info.IPAddress != ""
		if ipOK {
			st.PodIPs = []string{info.IPAddress}
		}
		probeErr := error(nil)
		if hostPort != 0 {
			probeErr = r.Driver.ProbeRuntime(ctx, hostPort)
		} else {
			probeErr = fmt.Errorf("no host port for server port %d", serverPort)
		}
		if probeErr == nil && ipOK {
			setCond(&conds, sandboxv1beta1.SandboxConditionReady, metav1.ConditionTrue, sandboxv1beta1.SandboxReasonDependenciesReady, "Pod is Ready", sb.Generation, r.now())
		} else {
			setCond(&conds, sandboxv1beta1.SandboxConditionReady, metav1.ConditionFalse, sandboxv1beta1.SandboxReasonDependenciesNotReady, "Pod is Running but not Ready", sb.Generation, r.now())
		}
	default:
		setCond(&conds, sandboxv1beta1.SandboxConditionReady, metav1.ConditionFalse, sandboxv1beta1.SandboxReasonDependenciesNotReady, "Pod exists with phase: "+info.State, sb.Generation, r.now())
	}

	if sb.Spec.Service != nil && *sb.Spec.Service {
		st.Service = name
		st.ServiceFQDN = name + "." + ns + ".svc." + r.clusterDomain()
	}
	st.Conditions = conds

	if err := r.writeSandboxStatus(u, st); err != nil {
		return Result{}, err
	}

	res := r.expiryRequeue(sb)
	if !meta.IsStatusConditionTrue(conds, string(sandboxv1beta1.SandboxConditionReady)) &&
		!meta.IsStatusConditionPresentAndEqual(conds, string(sandboxv1beta1.SandboxConditionFinished), metav1.ConditionTrue) {
		if res.RequeueAfter == 0 || readyRequeue < res.RequeueAfter {
			res.RequeueAfter = readyRequeue
		}
	}
	return res, nil
}

// handleSandboxExpiry removes runtime resources on expiry, keeping volumes, and
// either deletes the CR (Delete policy) or retains it (Retain, status reset to
// conditions with Ready=False/SandboxExpired).
func (r *Reconcilers) handleSandboxExpiry(ctx context.Context, u *unstructured.Unstructured, sb *sandboxv1beta1.Sandbox) (Result, error) {
	ns, name := sb.Namespace, sb.Name
	if info, err := r.Driver.InspectSandbox(ctx, ns, name); err == nil {
		_ = r.Driver.RemoveContainer(ctx, info.ID)
	}
	policy := sandboxv1beta1.ShutdownPolicyRetain
	if sb.Spec.ShutdownPolicy != nil {
		policy = *sb.Spec.ShutdownPolicy
	}
	if policy == sandboxv1beta1.ShutdownPolicyDelete {
		_, err := r.Store.Delete(apis.SandboxGVR, ns, name)
		if err != nil && !apierrors.IsNotFound(err) {
			return Result{}, err
		}
		return Result{}, nil
	}
	// Retain: reset status to conditions-only with SandboxExpired.
	conds := sb.Status.Conditions
	setCond(&conds, sandboxv1beta1.SandboxConditionReady, metav1.ConditionFalse, sandboxv1beta1.SandboxReasonExpired, "Sandbox has expired", sb.Generation, r.now())
	st := sandboxv1beta1.SandboxStatus{Conditions: conds}
	if err := r.writeSandboxStatus(u, st); err != nil {
		return Result{}, err
	}
	return Result{}, nil
}

// orphaned reports whether the sandbox is controller-owned by a CR that no
// longer exists in the store.
func (r *Reconcilers) orphaned(u *unstructured.Unstructured) bool {
	for _, ref := range u.GetOwnerReferences() {
		if ref.Controller == nil || !*ref.Controller {
			continue
		}
		var ownerGVR apis.GVR
		switch ref.Kind {
		case "SandboxClaim":
			ownerGVR = apis.SandboxClaimGVR
		case "SandboxWarmPool":
			ownerGVR = apis.WarmPoolGVR
		default:
			continue
		}
		owner, err := r.Store.Get(ownerGVR, u.GetNamespace(), ref.Name)
		if apierrors.IsNotFound(err) {
			return true
		}
		// Owner exists but is a different incarnation (name reused after
		// delete): the current sandbox is an orphan of the old one.
		if err == nil && ref.UID != "" && string(owner.GetUID()) != string(ref.UID) {
			return true
		}
	}
	return false
}

func (r *Reconcilers) failReady(u *unstructured.Unstructured, sb *sandboxv1beta1.Sandbox, msg string) Result {
	conds := sb.Status.Conditions
	setCond(&conds, sandboxv1beta1.SandboxConditionReady, metav1.ConditionFalse, "ReconcilerError", "Error seen: "+msg, sb.Generation, r.now())
	st := sandboxv1beta1.SandboxStatus{Conditions: conds}
	_ = r.writeSandboxStatus(u, st)
	return Result{RequeueAfter: 5 * time.Second}
}

func (r *Reconcilers) expiryRequeue(sb *sandboxv1beta1.Sandbox) Result {
	if exp := sb.Spec.ShutdownTime; exp != nil {
		d := time.Until(exp.Time)
		if d < 0 {
			d = 0
		}
		return Result{RequeueAfter: maxDur(d, time.Second)}
	}
	return Result{}
}

// writeSandboxStatus writes the status subresource. It clears the
// resourceVersion so the write carries no optimistic-concurrency precondition:
// the reconciler is the sole status writer (the workqueue serializes reconciles
// per key), and a preceding metadata patch (e.g. the pod-name annotation) would
// otherwise make the read-time RV stale and 409 the status write.
func (r *Reconcilers) writeSandboxStatus(u *unstructured.Unstructured, st sandboxv1beta1.SandboxStatus) error {
	n, err := setStatus(u, &st)
	if err != nil {
		return err
	}
	n.SetResourceVersion("")
	_, err = r.Store.Update(apis.SandboxGVR, n, true)
	if apierrors.IsConflict(err) {
		return nil
	}
	return err
}

// ensurePodNameAnnotation stamps agents.x-k8s.io/pod-name if absent.
func (r *Reconcilers) ensurePodNameAnnotation(u *unstructured.Unstructured, podName string) {
	if u.GetAnnotations()[sandboxv1beta1.SandboxPodNameAnnotation] == podName {
		return
	}
	patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, sandboxv1beta1.SandboxPodNameAnnotation, podName))
	_, err := r.Store.Patch(apis.SandboxGVR, u.GetNamespace(), u.GetName(), types.MergePatchType, patch, false)
	if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		r.Log.Warn("stamp pod-name annotation", "err", err)
	}
}

func (r *Reconcilers) ensureImage(ctx context.Context, image string) error {
	err := r.Driver.EnsureImage(ctx, image, driver.PullIfNotPresent)
	if err == nil {
		return nil
	}
	if image == r.RuntimeImage && r.RuntimeBuildContext != "" {
		if b, ok := r.Driver.(interface {
			BuildRuntimeImage(context.Context, string, string) error
		}); ok {
			if berr := b.BuildRuntimeImage(ctx, image, r.RuntimeBuildContext); berr == nil {
				return nil
			}
		}
	}
	return err
}

// --- helpers ---

func setCond(conds *[]metav1.Condition, t sandboxv1beta1.ConditionType, status metav1.ConditionStatus, reason, msg string, gen int64, now time.Time) {
	meta.SetStatusCondition(conds, metav1.Condition{
		Type:               string(t),
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: gen,
		LastTransitionTime: metav1.NewTime(now),
	})
}

// nameHash is FNV-1a 32-bit as 8 lowercase hex digits (matches upstream).
func nameHash(name string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return fmt.Sprintf("%08x", h.Sum32())
}

func nameHashSelector(name string) string {
	return "agents.x-k8s.io/sandbox-name-hash=" + nameHash(name)
}

func podServerPort(podSpec corev1.PodSpec, def int) int {
	if len(podSpec.Containers) > 0 {
		for _, p := range podSpec.Containers[0].Ports {
			if p.ContainerPort > 0 {
				return int(p.ContainerPort)
			}
		}
	}
	return def
}

// injectPodVolume adds a PVC-backed pod volume named volName (claimName pvc) if
// not already present (StatefulSet-style volume injection).
func injectPodVolume(podSpec corev1.PodSpec, volName, pvc string) corev1.PodSpec {
	for i := range podSpec.Volumes {
		if podSpec.Volumes[i].Name == volName {
			podSpec.Volumes[i].VolumeSource = corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc},
			}
			return podSpec
		}
	}
	podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
		Name:         volName,
		VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc}},
	})
	return podSpec
}

func maxDur(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
