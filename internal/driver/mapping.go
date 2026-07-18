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

package driver

import (
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
)

// MappingMeta carries the sandbox identity and resolved volume names needed to
// map a pod spec into a container spec.
type MappingMeta struct {
	Namespace   string
	SandboxName string
	UID         string
	ServerPort  int
	// VolumeNames maps a pod volume name (PVC template name) to the docker
	// volume name the reconciler created for it.
	VolumeNames map[string]string
	// ExtraLabels are merged onto the container (e.g. propagated pod labels).
	ExtraLabels   map[string]string
	InjectRuntime bool
}

// MapError is a fatal mapping problem (the sandbox cannot be represented).
type MapError struct{ msg string }

func (e *MapError) Error() string { return e.msg }

func mapErrf(f string, a ...any) *MapError { return &MapError{msg: fmt.Sprintf(f, a...)} }

// MapPodSpec converts a Kubernetes PodSpec into a SandboxContainerSpec. It maps
// the first container (multi-container pods log a warning and use container 0).
// Unsupported fields are returned as warnings rather than failing, except
// privileged which is refused outright. defaultServerPort is used when no
// container port is declared.
func MapPodSpec(podSpec corev1.PodSpec, meta MappingMeta, defaultServerPort int) (SandboxContainerSpec, []string, error) {
	var warnings []string
	if len(podSpec.Containers) == 0 {
		return SandboxContainerSpec{}, nil, mapErrf("driver: pod spec has no containers")
	}
	if len(podSpec.Containers) > 1 {
		warnings = append(warnings, fmt.Sprintf("multi-container pod: only the first container (%q) is run; %d others ignored", podSpec.Containers[0].Name, len(podSpec.Containers)-1))
	}
	c := podSpec.Containers[0]

	serverPort := meta.ServerPort
	if serverPort == 0 {
		serverPort = defaultServerPort
	}

	spec := SandboxContainerSpec{
		Namespace:     meta.Namespace,
		SandboxName:   meta.SandboxName,
		UID:           meta.UID,
		Image:         c.Image,
		Command:       append([]string(nil), c.Command...),
		Args:          append([]string(nil), c.Args...),
		Env:           map[string]string{},
		WorkingDir:    c.WorkingDir,
		ServerPort:    serverPort,
		Labels:        map[string]string{},
		InjectRuntime: meta.InjectRuntime,
	}
	for k, v := range meta.ExtraLabels {
		spec.Labels[k] = v
	}

	// Image pull policy.
	switch c.ImagePullPolicy {
	case corev1.PullAlways:
		spec.PullPolicy = PullAlways
	case corev1.PullNever:
		spec.PullPolicy = PullNever
	default:
		spec.PullPolicy = PullIfNotPresent
	}

	// Env (plain values only; valueFrom is unsupported locally).
	for _, e := range c.Env {
		if e.ValueFrom != nil {
			warnings = append(warnings, fmt.Sprintf("env %q uses valueFrom which is unsupported; skipped", e.Name))
			continue
		}
		spec.Env[e.Name] = e.Value
	}

	// Ports: publish every declared containerPort plus the server port.
	portSet := map[int]struct{}{serverPort: {}}
	for _, p := range c.Ports {
		if p.ContainerPort > 0 {
			portSet[int(p.ContainerPort)] = struct{}{}
		}
	}
	for p := range portSet {
		spec.RuntimePorts = append(spec.RuntimePorts, p)
	}
	sort.Ints(spec.RuntimePorts)

	// Volumes: match volumeMounts to pod volumes → docker volumes / tmpfs.
	volByName := map[string]corev1.Volume{}
	for _, v := range podSpec.Volumes {
		volByName[v.Name] = v
	}
	for _, vm := range c.VolumeMounts {
		vol, ok := volByName[vm.Name]
		if !ok {
			warnings = append(warnings, fmt.Sprintf("volumeMount %q references unknown volume; skipped", vm.Name))
			continue
		}
		if vm.SubPath != "" {
			warnings = append(warnings, fmt.Sprintf("volumeMount %q uses subPath which is unsupported; mounting whole volume", vm.Name))
		}
		switch {
		case vol.PersistentVolumeClaim != nil:
			dockerVol := meta.VolumeNames[vm.Name]
			if dockerVol == "" {
				warnings = append(warnings, fmt.Sprintf("volume %q has no resolved docker volume; skipped", vm.Name))
				continue
			}
			spec.Mounts = append(spec.Mounts, Mount{VolumeName: dockerVol, MountPath: vm.MountPath, ReadOnly: vm.ReadOnly})
		case vol.EmptyDir != nil:
			m := Mount{MountPath: vm.MountPath, ReadOnly: vm.ReadOnly}
			if vol.EmptyDir.Medium == corev1.StorageMediumMemory {
				m.Tmpfs = true
				if vol.EmptyDir.SizeLimit != nil {
					m.TmpfsSizeBytes = vol.EmptyDir.SizeLimit.Value()
				}
			} else {
				// Disk emptyDir → anonymous named volume via the reconciler;
				// if none resolved, fall back to tmpfs semantics unbounded.
				dockerVol := meta.VolumeNames[vm.Name]
				if dockerVol != "" {
					m.VolumeName = dockerVol
				} else {
					m.Tmpfs = true
				}
			}
			spec.Mounts = append(spec.Mounts, m)
		default:
			warnings = append(warnings, fmt.Sprintf("volume %q type is unsupported (only PVC and emptyDir); skipped", vm.Name))
		}
	}

	// Security context.
	if sc := c.SecurityContext; sc != nil {
		if sc.Privileged != nil && *sc.Privileged {
			return SandboxContainerSpec{}, warnings, mapErrf("driver: privileged containers are refused")
		}
		if sc.RunAsUser != nil {
			spec.User = fmt.Sprintf("%d", *sc.RunAsUser)
			if sc.RunAsGroup != nil {
				spec.User = fmt.Sprintf("%d:%d", *sc.RunAsUser, *sc.RunAsGroup)
			}
		}
		if sc.ReadOnlyRootFilesystem != nil {
			spec.ReadOnlyRoot = *sc.ReadOnlyRootFilesystem
		}
	}
	if psc := podSpec.SecurityContext; psc != nil && spec.User == "" && psc.RunAsUser != nil {
		spec.User = fmt.Sprintf("%d", *psc.RunAsUser)
		if psc.RunAsGroup != nil {
			spec.User = fmt.Sprintf("%d:%d", *psc.RunAsUser, *psc.RunAsGroup)
		}
	}

	// Resource limits.
	if lim := c.Resources.Limits; lim != nil {
		if cpu, ok := lim[corev1.ResourceCPU]; ok {
			// milliCPU → NanoCPUs.
			spec.Resources.NanoCPUs = cpu.MilliValue() * 1_000_000
		}
		if mem, ok := lim[corev1.ResourceMemory]; ok {
			spec.Resources.MemoryBytes = mem.Value()
		}
	}

	// DNS (from the extensions secure-default path).
	if podSpec.DNSConfig != nil {
		spec.DNSServers = append([]string(nil), podSpec.DNSConfig.Nameservers...)
	}

	return spec, warnings, nil
}
