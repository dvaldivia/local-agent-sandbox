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
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
	"github.com/dvaldivia/local-agent-sandbox/internal/store"
)

const (
	claimIdentityLabel = "agents.x-k8s.io/claim-uid"
	dependencyRequeue  = time.Minute
)

// reconcileClaim resolves a SandboxClaim to a backing Sandbox (adopting a warm
// one or cold-creating from the template), records status.sandbox.name, and
// mirrors the Sandbox's Ready/Finished conditions.
func (r *Reconcilers) reconcileClaim(ctx context.Context, ns, name string) (Result, error) {
	u, err := r.Store.Get(apis.SandboxClaimGVR, ns, name)
	if apierrors.IsNotFound(err) {
		return Result{}, nil
	}
	if err != nil {
		return Result{}, err
	}
	var claim extv1beta1.SandboxClaim
	if err := fromUnstructured(u, &claim); err != nil {
		return Result{}, err
	}

	if expired, _ := r.claimExpired(&claim); expired {
		return r.handleClaimExpiry(ctx, u, &claim)
	}

	// Resolve warm pool → template (the SDKs string-match these reasons).
	poolName := claim.Spec.WarmPoolRef.Name
	pu, perr := r.Store.Get(apis.WarmPoolGVR, ns, poolName)
	if apierrors.IsNotFound(perr) {
		return r.claimNotReady(u, &claim, "WarmPoolNotFound", fmt.Sprintf("SandboxWarmPool %q not found", poolName)), nil
	}
	if perr != nil {
		return Result{}, perr
	}
	var pool extv1beta1.SandboxWarmPool
	if err := fromUnstructured(pu, &pool); err != nil {
		return Result{}, err
	}
	tmplName := pool.Spec.TemplateRef.Name
	tu, terr := r.Store.Get(apis.TemplateGVR, ns, tmplName)
	if apierrors.IsNotFound(terr) {
		return r.claimNotReady(u, &claim, "TemplateNotFound", fmt.Sprintf("SandboxTemplate %q not found", tmplName)), nil
	}
	if terr != nil {
		return Result{}, terr
	}
	var tmpl extv1beta1.SandboxTemplate
	if err := fromUnstructured(tu, &tmpl); err != nil {
		return Result{}, err
	}

	// Find or create the backing Sandbox.
	sbU, adopted, err := r.getOrCreateSandbox(ctx, u, &claim, &tmpl, poolName)
	if err != nil {
		return r.claimNotReady(u, &claim, buildErrorReason(err), "Error seen: "+err.Error()), nil
	}
	if sbU == nil {
		return r.claimNotReady(u, &claim, "SandboxMissing", "waiting for a sandbox to become available"), nil
	}
	sbName := sbU.GetName()

	if adopted {
		r.stampClaimSandboxAnnotation(u, sbName)
	}

	// Mirror conditions from the Sandbox.
	var sb sandboxv1beta1.Sandbox
	_ = fromUnstructured(sbU, &sb)
	conds := claim.Status.Conditions
	if rc := meta.FindStatusCondition(sb.Status.Conditions, string(sandboxv1beta1.SandboxConditionReady)); rc != nil {
		reason := rc.Reason
		if rc.Status != metav1.ConditionTrue && reason == "" {
			reason = "SandboxNotReady"
		}
		setCondRaw(&conds, "Ready", rc.Status, reason, rc.Message, claim.Generation, r.now())
	} else {
		setCondRaw(&conds, "Ready", metav1.ConditionFalse, "SandboxNotReady", "Sandbox is not ready", claim.Generation, r.now())
	}
	if fc := meta.FindStatusCondition(sb.Status.Conditions, string(sandboxv1beta1.SandboxConditionFinished)); fc != nil {
		setCondRaw(&conds, "Finished", fc.Status, fc.Reason, fc.Message, claim.Generation, r.now())
	} else {
		meta.RemoveStatusCondition(&conds, "Finished")
	}

	status := extv1beta1.SandboxClaimStatus{
		Conditions: conds,
		SandboxStatus: extv1beta1.SandboxStatus{
			Name:   sbName,
			PodIPs: sb.Status.PodIPs,
		},
	}
	if err := r.writeClaimStatus(u, status); err != nil {
		return Result{}, err
	}

	res := Result{}
	if !meta.IsStatusConditionTrue(conds, "Ready") {
		res.RequeueAfter = readyRequeue
	}
	if rq := r.claimLifecycleRequeue(&claim); rq > 0 && (res.RequeueAfter == 0 || rq < res.RequeueAfter) {
		res.RequeueAfter = rq
	}
	return res, nil
}

// getOrCreateSandbox returns the backing Sandbox, creating or adopting one as
// needed. The bool reports whether the sandbox was adopted from the warm pool.
func (r *Reconcilers) getOrCreateSandbox(ctx context.Context, u *unstructured.Unstructured, claim *extv1beta1.SandboxClaim, tmpl *extv1beta1.SandboxTemplate, poolName string) (*unstructured.Unstructured, bool, error) {
	ns := claim.Namespace
	// Existing binding — only reuse if the sandbox is controlled by THIS claim
	// incarnation (guards against a same-named but recreated claim).
	if existing := claim.Status.SandboxStatus.Name; existing != "" {
		if sbU, err := r.Store.Get(apis.SandboxGVR, ns, existing); err == nil {
			if controllerOwnerUID(sbU) == string(claim.UID) {
				return sbU, false, nil
			}
		}
	}

	forceCold := len(claim.Spec.Env) > 0 || len(claim.Spec.VolumeClaimTemplates) > 0
	if !forceCold {
		if sbU := r.adoptWarmSandbox(ctx, u, claim, poolName); sbU != nil {
			return sbU, true, nil
		}
	}

	// Cold-create a Sandbox named exactly claim.Name.
	labels := map[string]string{
		sandboxv1beta1.SandboxLaunchTypeLabel: sandboxv1beta1.SandboxLaunchTypeCold,
		sandboxv1beta1.CreatedByLabel:         "controller",
		claimIdentityLabel:                    string(claim.UID),
	}
	sbU, err := r.buildSandbox(tmpl, ns, claim.Name, "", u, "SandboxClaim", labels,
		claim.Spec.Env, claim.Spec.VolumeClaimTemplates, &claim.Spec.AdditionalPodMetadata)
	if err != nil {
		return nil, false, err
	}
	created, err := r.Store.Create(apis.SandboxGVR, sbU)
	if apierrors.IsAlreadyExists(err) {
		got, gerr := r.Store.Get(apis.SandboxGVR, ns, claim.Name)
		if gerr != nil {
			return nil, false, gerr
		}
		// Only bind to a pre-existing same-named sandbox if it is ours.
		if controllerOwnerUID(got) != string(claim.UID) {
			return nil, false, fmt.Errorf("sandbox %q exists but is not controlled by this claim", claim.Name)
		}
		return got, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return created, false, nil
}

// adoptWarmSandbox binds an idle warm Sandbox to the claim, rewriting its
// ownerReference and labels. Returns nil if none available.
func (r *Reconcilers) adoptWarmSandbox(_ context.Context, u *unstructured.Unstructured, claim *extv1beta1.SandboxClaim, poolName string) *unstructured.Unstructured {
	list, err := r.Store.List(apis.SandboxGVR, claim.Namespace, store.Selectors{})
	if err != nil {
		return nil
	}
	var candidates []*unstructured.Unstructured
	for _, sb := range list.Items {
		if ownedBy(sb, "SandboxWarmPool", poolName) && sb.GetDeletionTimestamp() == nil {
			candidates = append(candidates, sb)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		return isSandboxReady(candidates[i]) && !isSandboxReady(candidates[j])
	})
	name := candidates[0].GetName()

	// Rewrite ownership to the claim, retrying on conflict with a fresh read.
	// The ownership Update races with the sandbox reconciler's concurrent
	// status writes on the same object; without the retry a 409 would drop the
	// adoption and the claim would cold-create a second sandbox, leaking the
	// warm one.
	for attempt := 0; attempt < 5; attempt++ {
		pick, err := r.Store.Get(apis.SandboxGVR, claim.Namespace, name)
		if err != nil {
			return nil
		}
		if !ownedBy(pick, "SandboxWarmPool", poolName) {
			return nil // adopted by someone else in the meantime
		}
		pick.SetOwnerReferences([]metav1.OwnerReference{controllerRef(u, apis.ExtensionsGV(), "SandboxClaim")})
		labels := pick.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		delete(labels, sandboxv1beta1.SandboxWarmPoolLabel)
		labels[sandboxv1beta1.SandboxLaunchTypeLabel] = sandboxv1beta1.SandboxLaunchTypeWarm
		labels[claimIdentityLabel] = string(claim.UID)
		pick.SetLabels(labels)

		updated, err := r.Store.Update(apis.SandboxGVR, pick, false)
		if err == nil {
			return updated
		}
		if !apierrors.IsConflict(err) {
			return nil
		}
	}
	return nil
}

func (r *Reconcilers) claimNotReady(u *unstructured.Unstructured, claim *extv1beta1.SandboxClaim, reason, msg string) Result {
	conds := claim.Status.Conditions
	setCondRaw(&conds, "Ready", metav1.ConditionFalse, reason, msg, claim.Generation, r.now())
	status := claim.Status
	status.Conditions = conds
	_ = r.writeClaimStatus(u, status)
	return Result{RequeueAfter: dependencyRequeue}
}

// claimExpired reports whether the claim has passed its shutdownTime or its
// ttlSecondsAfterFinished (measured from the mirrored Finished transition).
func (r *Reconcilers) claimExpired(claim *extv1beta1.SandboxClaim) (bool, time.Time) {
	lc := claim.Spec.Lifecycle
	if lc == nil {
		return false, time.Time{}
	}
	now := r.now()
	if lc.ShutdownTime != nil && !now.Before(lc.ShutdownTime.Time) {
		return true, lc.ShutdownTime.Time
	}
	if lc.TTLSecondsAfterFinished != nil {
		if fc := meta.FindStatusCondition(claim.Status.Conditions, "Finished"); fc != nil && fc.Status == metav1.ConditionTrue {
			deadline := fc.LastTransitionTime.Add(time.Duration(*lc.TTLSecondsAfterFinished) * time.Second)
			if !now.Before(deadline) {
				return true, deadline
			}
		}
	}
	return false, time.Time{}
}

func (r *Reconcilers) claimLifecycleRequeue(claim *extv1beta1.SandboxClaim) time.Duration {
	lc := claim.Spec.Lifecycle
	if lc == nil || lc.ShutdownTime == nil {
		return 0
	}
	d := time.Until(lc.ShutdownTime.Time)
	if d < 0 {
		return time.Second
	}
	return maxDur(d, time.Second)
}

func (r *Reconcilers) handleClaimExpiry(_ context.Context, u *unstructured.Unstructured, claim *extv1beta1.SandboxClaim) (Result, error) {
	policy := extv1beta1.ShutdownPolicyRetain
	if claim.Spec.Lifecycle != nil && claim.Spec.Lifecycle.ShutdownPolicy != "" {
		policy = claim.Spec.Lifecycle.ShutdownPolicy
	}
	switch policy {
	case extv1beta1.ShutdownPolicyDelete, extv1beta1.ShutdownPolicyDeleteForeground:
		// Delete the claim; OnDelete cascades to the Sandbox + container.
		if _, err := r.Store.Delete(apis.SandboxClaimGVR, claim.Namespace, claim.Name); err != nil && !apierrors.IsNotFound(err) {
			return Result{}, err
		}
		return Result{}, nil
	default: // Retain
		// Delete the owned Sandbox (only if controlled by this claim), keep the
		// claim marked expired.
		if sbName := claim.Status.SandboxStatus.Name; sbName != "" {
			if sbU, err := r.Store.Get(apis.SandboxGVR, claim.Namespace, sbName); err == nil && controllerOwnerUID(sbU) == string(claim.UID) {
				_, _ = r.Store.Delete(apis.SandboxGVR, claim.Namespace, sbName)
			}
		}
		conds := claim.Status.Conditions
		setCondRaw(&conds, "Ready", metav1.ConditionFalse, extv1beta1.ClaimExpiredReason, "Sandbox cleanup initiated.", claim.Generation, r.now())
		status := claim.Status
		status.Conditions = conds
		if err := r.writeClaimStatus(u, status); err != nil {
			return Result{}, err
		}
		return Result{}, nil
	}
}

func (r *Reconcilers) writeClaimStatus(u *unstructured.Unstructured, st extv1beta1.SandboxClaimStatus) error {
	n, err := setStatus(u, &st)
	if err != nil {
		return err
	}
	// Sole-writer status update with no RV precondition; see writeSandboxStatus.
	n.SetResourceVersion("")
	_, err = r.Store.Update(apis.SandboxClaimGVR, n, true)
	if apierrors.IsConflict(err) {
		return nil
	}
	return err
}

func (r *Reconcilers) stampClaimSandboxAnnotation(u *unstructured.Unstructured, sbName string) {
	patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, extv1beta1.AssignedSandboxNameAnnotation, sbName))
	_, err := r.Store.Patch(apis.SandboxClaimGVR, u.GetNamespace(), u.GetName(), "application/merge-patch+json", patch, false)
	if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		r.Log.Warn("stamp claim sandbox-name annotation", "err", err)
	}
}

// buildErrorReason maps a getOrCreateSandbox error to the claim Ready reason
// the upstream controller would use.
func buildErrorReason(err error) string {
	switch {
	case errors.Is(err, ErrEnvInjectionRejected):
		return "EnvVarsInjectionRejected"
	case errors.Is(err, ErrVCTRejected):
		return "VolumeClaimTemplatesError"
	default:
		return "ReconcilerError"
	}
}

// setCondRaw sets a condition by raw type string.
func setCondRaw(conds *[]metav1.Condition, t string, status metav1.ConditionStatus, reason, msg string, gen int64, now time.Time) {
	if reason == "" {
		reason = "Unknown"
	}
	meta.SetStatusCondition(conds, metav1.Condition{
		Type:               t,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: gen,
		LastTransitionTime: metav1.NewTime(now),
	})
}
