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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func ptrBool(b bool) *bool  { return &b }
func ptrI64(i int64) *int64 { return &i }

func basePod() corev1.PodSpec {
	return corev1.PodSpec{
		Containers: []corev1.Container{{
			Name:  "runtime",
			Image: "lasd/sandbox-runtime:dev",
			Ports: []corev1.ContainerPort{{ContainerPort: 8888}},
		}},
	}
}

func TestMapPodSpecBasic(t *testing.T) {
	spec, warnings, err := MapPodSpec(basePod(), MappingMeta{
		Namespace: "default", SandboxName: "sb1", UID: "uid1", ServerPort: 8888,
	}, 8888)
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 0 {
		t.Errorf("unexpected warnings: %v", warnings)
	}
	if spec.Image != "lasd/sandbox-runtime:dev" {
		t.Errorf("image = %q", spec.Image)
	}
	if len(spec.RuntimePorts) != 1 || spec.RuntimePorts[0] != 8888 {
		t.Errorf("ports = %v", spec.RuntimePorts)
	}
}

func TestMapPortsDedupAndServerPort(t *testing.T) {
	pod := basePod()
	pod.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 9000}, {ContainerPort: 8888}}
	spec, _, err := MapPodSpec(pod, MappingMeta{Namespace: "n", SandboxName: "s", ServerPort: 8888}, 8888)
	if err != nil {
		t.Fatal(err)
	}
	// 8888 (server) + 9000, deduped and sorted.
	if len(spec.RuntimePorts) != 2 || spec.RuntimePorts[0] != 8888 || spec.RuntimePorts[1] != 9000 {
		t.Errorf("ports = %v, want [8888 9000]", spec.RuntimePorts)
	}
}

func TestMapEnvSkipsValueFrom(t *testing.T) {
	pod := basePod()
	pod.Containers[0].Env = []corev1.EnvVar{
		{Name: "FOO", Value: "bar"},
		{Name: "SECRET", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{}}},
	}
	spec, warnings, err := MapPodSpec(pod, MappingMeta{Namespace: "n", SandboxName: "s"}, 8888)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Env["FOO"] != "bar" {
		t.Errorf("env FOO = %q", spec.Env["FOO"])
	}
	if _, ok := spec.Env["SECRET"]; ok {
		t.Error("valueFrom env should be skipped")
	}
	if len(warnings) == 0 {
		t.Error("expected a warning for valueFrom env")
	}
}

func TestMapVolumesPVCAndTmpfs(t *testing.T) {
	pod := basePod()
	pod.Volumes = []corev1.Volume{
		{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-sb"}}},
		{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}}},
	}
	pod.Containers[0].VolumeMounts = []corev1.VolumeMount{
		{Name: "data", MountPath: "/data"},
		{Name: "scratch", MountPath: "/scratch"},
	}
	spec, _, err := MapPodSpec(pod, MappingMeta{Namespace: "n", SandboxName: "s"}, 8888)
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Mounts) != 2 {
		t.Fatalf("mounts = %d, want 2: %+v", len(spec.Mounts), spec.Mounts)
	}
	var pvc, tmpfs *Mount
	for i := range spec.Mounts {
		switch spec.Mounts[i].MountPath {
		case "/data":
			pvc = &spec.Mounts[i]
		case "/scratch":
			tmpfs = &spec.Mounts[i]
		}
	}
	if pvc == nil || pvc.VolumeName != "las-n-data-sb" {
		t.Errorf("pvc mount = %+v", pvc)
	}
	if tmpfs == nil || !tmpfs.Tmpfs {
		t.Errorf("tmpfs mount = %+v", tmpfs)
	}
}

// TestMapPreexistingPVCByClaimName verifies a pod volume referencing a
// user-created PVC (not from a volumeClaimTemplate) resolves to the docker
// volume derived from its claimName.
func TestMapPreexistingPVCByClaimName(t *testing.T) {
	pod := basePod()
	pod.Volumes = []corev1.Volume{
		{Name: "shared", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "shared-data", ReadOnly: true}}},
	}
	pod.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "shared", MountPath: "/shared"}}
	spec, warnings, err := MapPodSpec(pod, MappingMeta{Namespace: "team-a", SandboxName: "s"}, 8888)
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 0 {
		t.Errorf("unexpected warnings: %v", warnings)
	}
	if len(spec.Mounts) != 1 {
		t.Fatalf("mounts = %+v", spec.Mounts)
	}
	m := spec.Mounts[0]
	if m.VolumeName != VolumeName("team-a", "shared-data") {
		t.Errorf("volume = %q, want %q", m.VolumeName, VolumeName("team-a", "shared-data"))
	}
	if !m.ReadOnly {
		t.Error("persistentVolumeClaim.readOnly should propagate to the mount")
	}
}

func TestMapPrivilegedRefused(t *testing.T) {
	pod := basePod()
	pod.Containers[0].SecurityContext = &corev1.SecurityContext{Privileged: ptrBool(true)}
	_, _, err := MapPodSpec(pod, MappingMeta{Namespace: "n", SandboxName: "s"}, 8888)
	if err == nil {
		t.Fatal("privileged container must be refused")
	}
}

func TestMapSecurityAndResources(t *testing.T) {
	pod := basePod()
	pod.Containers[0].SecurityContext = &corev1.SecurityContext{
		RunAsUser:              ptrI64(1000),
		RunAsGroup:             ptrI64(2000),
		ReadOnlyRootFilesystem: ptrBool(true),
	}
	pod.Containers[0].Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
	spec, _, err := MapPodSpec(pod, MappingMeta{Namespace: "n", SandboxName: "s"}, 8888)
	if err != nil {
		t.Fatal(err)
	}
	if spec.User != "1000:2000" {
		t.Errorf("user = %q", spec.User)
	}
	if !spec.ReadOnlyRoot {
		t.Error("readOnlyRoot should be true")
	}
	if spec.Resources.NanoCPUs != 500*1_000_000 {
		t.Errorf("nanocpus = %d, want %d", spec.Resources.NanoCPUs, 500*1_000_000)
	}
	if spec.Resources.MemoryBytes != 256*1024*1024 {
		t.Errorf("memory = %d", spec.Resources.MemoryBytes)
	}
}

func TestMapMultiContainerWarns(t *testing.T) {
	pod := basePod()
	pod.Containers = append(pod.Containers, corev1.Container{Name: "sidecar", Image: "busybox"})
	spec, warnings, err := MapPodSpec(pod, MappingMeta{Namespace: "n", SandboxName: "s"}, 8888)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Image != "lasd/sandbox-runtime:dev" {
		t.Errorf("should use first container, got %q", spec.Image)
	}
	if len(warnings) == 0 {
		t.Error("expected multi-container warning")
	}
}

func TestMapNoContainersError(t *testing.T) {
	_, _, err := MapPodSpec(corev1.PodSpec{}, MappingMeta{Namespace: "n", SandboxName: "s"}, 8888)
	if err == nil {
		t.Fatal("empty pod spec must error")
	}
}

// keep metav1 import used (for potential future metadata fields).
var _ = metav1.Now
