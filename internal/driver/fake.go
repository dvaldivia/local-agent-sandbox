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
	"context"
	"fmt"
	"sync"
	"time"
)

// FakeDriver is an in-memory Driver for unit tests. By default images are
// always present, created containers become "running" immediately, and
// ProbeRuntime succeeds. Hooks let tests simulate failures, non-readiness, and
// container death.
type FakeDriver struct {
	mu         sync.Mutex
	containers map[string]*ContainerInfo // key: ns/name
	volumes    map[string]VolumeSpec
	nextPort   int
	events     chan Event

	// Ready controls whether ProbeRuntime succeeds (default true).
	Ready bool
	// CreateErr, if set, makes CreateSandboxContainer fail.
	CreateErr error
	// PingErr, if set, makes Ping fail.
	PingErr error
}

// NewFakeDriver returns a ready-to-use fake driver.
func NewFakeDriver() *FakeDriver {
	return &FakeDriver{
		containers: map[string]*ContainerInfo{},
		volumes:    map[string]VolumeSpec{},
		nextPort:   40000,
		events:     make(chan Event, 128),
		Ready:      true,
	}
}

var _ Driver = (*FakeDriver)(nil)

func fkey(ns, name string) string { return ns + "/" + name }

func (f *FakeDriver) Ping(context.Context) error { return f.PingErr }
func (f *FakeDriver) Close() error               { return nil }

func (f *FakeDriver) EnsureImage(context.Context, string, PullPolicy) error { return nil }

func (f *FakeDriver) CreateSandboxContainer(_ context.Context, spec SandboxContainerSpec) (ContainerInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.CreateErr != nil {
		return ContainerInfo{}, f.CreateErr
	}
	port := f.nextPort
	f.nextPort++
	info := &ContainerInfo{
		ID:        "fake-" + spec.Namespace + "-" + spec.SandboxName,
		Name:      containerName(spec.Namespace, spec.SandboxName, spec.UID),
		State:     "running",
		IPAddress: fmt.Sprintf("10.88.0.%d", (port%250)+2),
		PortMap:   map[int]int{spec.ServerPort: port},
		Labels: map[string]string{
			LabelManaged: "true", LabelNamespace: spec.Namespace,
			LabelSandbox: spec.SandboxName, LabelUID: spec.UID,
		},
		StartedAt: time.Unix(0, 0),
	}
	for _, p := range spec.RuntimePorts {
		if _, ok := info.PortMap[p]; !ok {
			info.PortMap[p] = f.nextPort
			f.nextPort++
		}
	}
	f.containers[fkey(spec.Namespace, spec.SandboxName)] = info
	return *info, nil
}

func (f *FakeDriver) InspectSandbox(_ context.Context, ns, name string) (ContainerInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.containers[fkey(ns, name)]
	if !ok {
		return ContainerInfo{}, ErrNotFound
	}
	return *c, nil
}

func (f *FakeDriver) ListManaged(context.Context) ([]ContainerInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]ContainerInfo, 0, len(f.containers))
	for _, c := range f.containers {
		out = append(out, *c)
	}
	return out, nil
}

func (f *FakeDriver) StopContainer(_ context.Context, id string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.containers {
		if c.ID == id {
			c.State = "exited"
		}
	}
	return nil
}

func (f *FakeDriver) RemoveContainer(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, c := range f.containers {
		if c.ID == id {
			delete(f.containers, k)
		}
	}
	return nil
}

func (f *FakeDriver) EnsureVolume(_ context.Context, spec VolumeSpec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.volumes[spec.Name] = spec
	return spec.Name, nil
}

func (f *FakeDriver) ListManagedVolumes(context.Context) ([]VolumeSpec, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]VolumeSpec, 0, len(f.volumes))
	for _, v := range f.volumes {
		out = append(out, v)
	}
	return out, nil
}

func (f *FakeDriver) RemoveVolume(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.volumes, name)
	return nil
}

func (f *FakeDriver) ProbeRuntime(context.Context, int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.Ready {
		return fmt.Errorf("fake: runtime not ready")
	}
	return nil
}

func (f *FakeDriver) Events(ctx context.Context) (<-chan Event, error) {
	return f.events, nil
}

// EmitEvent pushes a lifecycle event (test helper).
func (f *FakeDriver) EmitEvent(ev Event) { f.events <- ev }

// KillContainer marks a container exited with a code and emits a die event.
func (f *FakeDriver) KillContainer(ns, name string, exitCode int) {
	f.mu.Lock()
	c, ok := f.containers[fkey(ns, name)]
	if ok {
		c.State = "exited"
		c.ExitCode = exitCode
	}
	f.mu.Unlock()
	if ok {
		f.events <- Event{Type: "die", ID: c.ID, Namespace: ns, Sandbox: name, ExitCode: exitCode}
	}
}
