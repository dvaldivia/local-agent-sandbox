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

// Package driver turns resolved sandbox pod specs into running Docker
// containers with localhost-published ports and named volumes, discoverable
// after a restart via container labels. Reconcilers depend on the Driver
// interface, never on the Docker client directly, so they can be unit-tested
// against a fake.
package driver

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrNotFound is returned when a requested sandbox container does not exist.
var ErrNotFound = errors.New("driver: sandbox container not found")

// VolumeName derives the docker volume name for a PVC-equivalent, matching the
// container naming scheme: las-<ns>-<pvcName>.
func VolumeName(namespace, pvcName string) string {
	return fmt.Sprintf("las-%s-%s", namespace, pvcName)
}

// Managed-object label keys. Container/volume identity lives in labels (not
// names) because Docker labels are immutable and survive restarts — the boot
// adoption scan rebuilds state from them.
const (
	LabelManaged    = "las.managed"     // "true" on every object we own
	LabelNamespace  = "las.namespace"   // sandbox namespace
	LabelSandbox    = "las.sandbox"     // sandbox name
	LabelUID        = "las.uid"         // sandbox UID
	LabelServerPort = "las.server-port" // runtime port inside the container
	LabelVolume     = "las.volume"      // PVC-equivalent name (volumes only)
)

// PullPolicy mirrors the subset of Kubernetes imagePullPolicy we honor.
type PullPolicy string

const (
	PullIfNotPresent PullPolicy = "IfNotPresent"
	PullAlways       PullPolicy = "Always"
	PullNever        PullPolicy = "Never"
)

// Mount is a volume or tmpfs mount inside the container.
type Mount struct {
	// VolumeName is the docker named volume; empty for a tmpfs mount.
	VolumeName string
	// Tmpfs requests an in-memory mount (emptyDir{medium: Memory}).
	Tmpfs bool
	// TmpfsSizeBytes optionally bounds a tmpfs mount (0 = unbounded).
	TmpfsSizeBytes int64
	MountPath      string
	ReadOnly       bool
}

// Resources caps container CPU/memory (from resources.limits).
type Resources struct {
	NanoCPUs    int64 // 1 CPU = 1e9
	MemoryBytes int64
}

// SandboxContainerSpec is the driver-level description of a sandbox container.
type SandboxContainerSpec struct {
	Namespace   string
	SandboxName string
	UID         string

	Image      string
	PullPolicy PullPolicy
	Command    []string // overrides ENTRYPOINT
	Args       []string // overrides CMD
	Env        map[string]string
	WorkingDir string
	// RuntimePorts are container ports to publish to 127.0.0.1 (deduped;
	// always includes ServerPort).
	RuntimePorts []int
	ServerPort   int
	Mounts       []Mount
	User         string // "uid[:gid]"
	ReadOnlyRoot bool
	Resources    Resources
	DNSServers   []string
	// Labels are extra labels merged with the mandatory las.* identity labels.
	Labels map[string]string
	// InjectRuntime copies the bundled runtime binary into the container and
	// runs it as the entrypoint (opt-in; local-only convenience).
	InjectRuntime bool
}

// ContainerInfo is the observed state of a managed container.
type ContainerInfo struct {
	ID         string
	Name       string
	State      string // running | exited | paused | created | dead
	ExitCode   int
	IPAddress  string      // bridge IP (informational; unreachable on macOS)
	PortMap    map[int]int // containerPort -> 127.0.0.1 host port
	Labels     map[string]string
	StartedAt  time.Time
	FinishedAt time.Time
}

// Namespace/Sandbox accessors from labels.
func (c ContainerInfo) Namespace() string { return c.Labels[LabelNamespace] }
func (c ContainerInfo) Sandbox() string   { return c.Labels[LabelSandbox] }
func (c ContainerInfo) UID() string       { return c.Labels[LabelUID] }

// VolumeSpec describes a named docker volume (PVC-equivalent).
type VolumeSpec struct {
	Name      string // docker volume name (already namespaced/derived)
	Namespace string
	PVCName   string
	Labels    map[string]string
}

// Event is a container lifecycle event streamed from the driver.
type Event struct {
	Type      string // start | die | stop | oom | destroy
	ID        string
	Namespace string
	Sandbox   string
	ExitCode  int
}

// Driver is the container backend abstraction.
type Driver interface {
	// Ping verifies connectivity to the container runtime.
	Ping(ctx context.Context) error
	// EnsureImage makes image available per policy.
	EnsureImage(ctx context.Context, image string, policy PullPolicy) error
	// CreateSandboxContainer creates and starts a container for the spec.
	CreateSandboxContainer(ctx context.Context, spec SandboxContainerSpec) (ContainerInfo, error)
	// InspectSandbox returns the container for a sandbox (by labels).
	InspectSandbox(ctx context.Context, namespace, name string) (ContainerInfo, error)
	// ListManaged returns all containers we own.
	ListManaged(ctx context.Context) ([]ContainerInfo, error)
	// StopContainer stops a container, waiting up to grace before killing.
	StopContainer(ctx context.Context, id string, grace time.Duration) error
	// RemoveContainer removes a container (force).
	RemoveContainer(ctx context.Context, id string) error
	// EnsureVolume creates the named volume if absent, returning its name.
	EnsureVolume(ctx context.Context, spec VolumeSpec) (string, error)
	// ListManagedVolumes returns names of volumes we own.
	ListManagedVolumes(ctx context.Context) ([]VolumeSpec, error)
	// RemoveVolume removes a named volume.
	RemoveVolume(ctx context.Context, name string) error
	// ProbeRuntime performs a health GET against the runtime on a host port.
	ProbeRuntime(ctx context.Context, hostPort int) error
	// Events streams container lifecycle events for managed containers until
	// ctx is cancelled.
	Events(ctx context.Context) (<-chan Event, error)
	// Close releases the underlying client.
	Close() error
}
