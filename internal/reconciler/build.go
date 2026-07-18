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
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
)

// controllerRef builds a controller ownerReference to owner (an unstructured CR).
func controllerRef(owner *unstructured.Unstructured, apiVersion, kind string) metav1.OwnerReference {
	t := true
	return metav1.OwnerReference{
		APIVersion:         apiVersion,
		Kind:               kind,
		Name:               owner.GetName(),
		UID:                owner.GetUID(),
		Controller:         &t,
		BlockOwnerDeletion: &t,
	}
}

// applySecureDefaults mirrors the extensions controller's ApplySandboxSecureDefaults:
// default automountServiceAccountToken to false, and in managed (secure-by-
// default) mode force DNS to public resolvers. networkPolicyManagement defaults
// to Managed.
func applySecureDefaults(podSpec *corev1.PodSpec, npManagement extv1beta1.NetworkPolicyManagement, hasCustomPolicy bool) {
	if podSpec.AutomountServiceAccountToken == nil {
		f := false
		podSpec.AutomountServiceAccountToken = &f
	}
	managed := npManagement == "" || npManagement == extv1beta1.NetworkPolicyManagementManaged
	if managed && !hasCustomPolicy {
		podSpec.DNSPolicy = corev1.DNSNone
		if podSpec.DNSConfig == nil {
			podSpec.DNSConfig = &corev1.PodDNSConfig{}
		}
		if len(podSpec.DNSConfig.Nameservers) == 0 {
			podSpec.DNSConfig.Nameservers = []string{"8.8.8.8", "1.1.1.1"}
		}
	}
}

// Rejection sentinels let the claim reconciler map failures to the same
// condition reasons the upstream controller emits.
var (
	ErrEnvInjectionRejected = errors.New("environment variable injection rejected by template policy")
	ErrVCTRejected          = errors.New("volume claim template injection rejected by template policy")
)

// injectEnv applies claim env vars into the pod spec per the template's
// injection policy. Returns ErrEnvInjectionRejected when the policy forbids it.
func injectEnv(podSpec *corev1.PodSpec, env []extv1beta1.EnvVar, policy extv1beta1.EnvVarsInjectionPolicy) error {
	if len(env) == 0 {
		return nil
	}
	if policy != extv1beta1.EnvVarsInjectionPolicyAllowed && policy != extv1beta1.EnvVarsInjectionPolicyOverrides {
		return fmt.Errorf("%w (%q)", ErrEnvInjectionRejected, policy)
	}
	for _, e := range env {
		idx := 0
		if e.ContainerName != "" {
			found := -1
			for i := range podSpec.Containers {
				if podSpec.Containers[i].Name == e.ContainerName {
					found = i
					break
				}
			}
			if found < 0 {
				return fmt.Errorf("env targets container %q which does not exist", e.ContainerName)
			}
			idx = found
		}
		c := &podSpec.Containers[idx]
		existing := -1
		for i := range c.Env {
			if c.Env[i].Name == e.Name {
				existing = i
				break
			}
		}
		switch {
		case existing >= 0 && policy == extv1beta1.EnvVarsInjectionPolicyOverrides:
			c.Env[existing].Value = e.Value
			c.Env[existing].ValueFrom = nil
		case existing >= 0:
			return fmt.Errorf("env %q already set on container %q and policy is not Overrides", e.Name, c.Name)
		default:
			c.Env = append(c.Env, corev1.EnvVar{Name: e.Name, Value: e.Value})
		}
	}
	return nil
}

// mergeVCT merges claim-supplied volumeClaimTemplates into the template's per
// the policy: Disallowed (default) rejects any; Allowed appends new names but
// rejects name collisions; Overrides replaces same-named entries and appends
// the rest.
func mergeVCT(base, extra []sandboxv1beta1.PersistentVolumeClaimTemplate, policy extv1beta1.VolumeClaimTemplatesPolicy) ([]sandboxv1beta1.PersistentVolumeClaimTemplate, error) {
	if policy != extv1beta1.VolumeClaimTemplatesPolicyAllowed && policy != extv1beta1.VolumeClaimTemplatesPolicyOverrides {
		return nil, fmt.Errorf("%w (%q)", ErrVCTRejected, policy)
	}
	idx := map[string]int{}
	out := append([]sandboxv1beta1.PersistentVolumeClaimTemplate(nil), base...)
	for i := range out {
		idx[out[i].Name] = i
	}
	for _, v := range extra {
		if at, ok := idx[v.Name]; ok {
			if policy != extv1beta1.VolumeClaimTemplatesPolicyOverrides {
				return nil, fmt.Errorf("%w: %q already defined and policy is not Overrides", ErrVCTRejected, v.Name)
			}
			out[at] = v
			continue
		}
		idx[v.Name] = len(out)
		out = append(out, v)
	}
	return out, nil
}

// buildSandbox constructs a Sandbox unstructured object from a template
// blueprint. Exactly one of name (cold-create) or generateName (warm pool)
// should be set. owner (+ownerKind/apiVersion) becomes the controller ref.
func (r *Reconcilers) buildSandbox(
	tmpl *extv1beta1.SandboxTemplate,
	ns, name, generateName string,
	owner *unstructured.Unstructured, ownerKind string,
	labels map[string]string,
	env []extv1beta1.EnvVar,
	extraVCT []sandboxv1beta1.PersistentVolumeClaimTemplate,
	podMeta *sandboxv1beta1.PodMetadata,
) (*unstructured.Unstructured, error) {
	blueprint := tmpl.Spec.SandboxBlueprint.DeepCopy()
	podSpec := &blueprint.PodTemplate.Spec

	if err := injectEnv(podSpec, env, tmpl.Spec.EnvVarsInjectionPolicy); err != nil {
		return nil, err
	}
	hasCustomPolicy := tmpl.Spec.NetworkPolicy != nil
	applySecureDefaults(podSpec, tmpl.Spec.NetworkPolicyManagement, hasCustomPolicy)

	// Merge additionalPodMetadata onto the pod template metadata.
	if podMeta != nil {
		if blueprint.PodTemplate.ObjectMeta.Labels == nil {
			blueprint.PodTemplate.ObjectMeta.Labels = map[string]string{}
		}
		for k, v := range podMeta.Labels {
			blueprint.PodTemplate.ObjectMeta.Labels[k] = v
		}
		if blueprint.PodTemplate.ObjectMeta.Annotations == nil {
			blueprint.PodTemplate.ObjectMeta.Annotations = map[string]string{}
		}
		for k, v := range podMeta.Annotations {
			blueprint.PodTemplate.ObjectMeta.Annotations[k] = v
		}
	}

	if len(extraVCT) > 0 {
		merged, err := mergeVCT(blueprint.VolumeClaimTemplates, extraVCT, tmpl.Spec.VolumeClaimTemplatesPolicy)
		if err != nil {
			return nil, err
		}
		blueprint.VolumeClaimTemplates = merged
	}

	sb := &sandboxv1beta1.Sandbox{
		Spec: sandboxv1beta1.SandboxSpec{
			SandboxBlueprint: *blueprint,
			OperatingMode:    sandboxv1beta1.SandboxOperatingModeRunning,
		},
	}
	sb.APIVersion = apis.SandboxGVR.GroupVersion()
	sb.Kind = "Sandbox"
	sb.Namespace = ns
	if name != "" {
		sb.Name = name
	}
	if generateName != "" {
		sb.GenerateName = generateName
	}
	sb.Labels = labels
	if owner != nil {
		sb.OwnerReferences = []metav1.OwnerReference{controllerRef(owner, apis.ExtensionsGV(), ownerKind)}
	}

	m, err := toUnstructured(sb)
	if err != nil {
		return nil, err
	}
	return m, nil
}
