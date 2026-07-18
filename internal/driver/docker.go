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
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

// DockerDriver implements Driver against the Docker Engine API.
type DockerDriver struct {
	cli  *client.Client
	http *http.Client
}

// NewDockerDriver constructs a driver from the environment (DOCKER_HOST, docker
// context, platform defaults) with API-version negotiation.
func NewDockerDriver() (*DockerDriver, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("driver: docker client: %w", err)
	}
	return &DockerDriver{
		cli:  cli,
		http: &http.Client{Timeout: 5 * time.Second},
	}, nil
}

func (d *DockerDriver) Ping(ctx context.Context) error {
	_, err := d.cli.Ping(ctx)
	return err
}

func (d *DockerDriver) Close() error { return d.cli.Close() }

func managedFilter() filters.Args {
	f := filters.NewArgs()
	f.Add("label", LabelManaged+"=true")
	return f
}

func (d *DockerDriver) EnsureImage(ctx context.Context, ref string, policy PullPolicy) error {
	present := d.imagePresent(ctx, ref)
	switch policy {
	case PullNever:
		if !present {
			return fmt.Errorf("driver: image %q not present and pull policy is Never", ref)
		}
		return nil
	case PullIfNotPresent:
		if present {
			return nil
		}
	}
	// PullAlways, or IfNotPresent with the image absent.
	rc, err := d.cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		if present {
			return nil // registry unreachable but we have it locally (dev images)
		}
		return fmt.Errorf("driver: pull %q: %w", ref, err)
	}
	defer rc.Close()
	_, _ = io.Copy(io.Discard, rc)
	return nil
}

func (d *DockerDriver) imagePresent(ctx context.Context, ref string) bool {
	_, _, err := d.cli.ImageInspectWithRaw(ctx, ref)
	return err == nil
}

// imageUser returns the image's configured USER (may be "uid", "uid:gid",
// "root", or ""), used to emulate fsGroup when the pod spec omits runAsUser.
func (d *DockerDriver) imageUser(ctx context.Context, ref string) string {
	insp, _, err := d.cli.ImageInspectWithRaw(ctx, ref)
	if err != nil || insp.Config == nil {
		return ""
	}
	return insp.Config.User
}

func (d *DockerDriver) CreateSandboxContainer(ctx context.Context, spec SandboxContainerSpec) (ContainerInfo, error) {
	labels := map[string]string{
		LabelManaged:    "true",
		LabelNamespace:  spec.Namespace,
		LabelSandbox:    spec.SandboxName,
		LabelUID:        spec.UID,
		LabelServerPort: strconv.Itoa(spec.ServerPort),
	}
	for k, v := range spec.Labels {
		labels[k] = v
	}

	exposed := nat.PortSet{}
	bindings := nat.PortMap{}
	for _, p := range spec.RuntimePorts {
		port, err := nat.NewPort("tcp", strconv.Itoa(p))
		if err != nil {
			return ContainerInfo{}, fmt.Errorf("driver: bad port %d: %w", p, err)
		}
		exposed[port] = struct{}{}
		// HostPort "" => Docker auto-assigns a free port, bound to loopback so
		// the mapping is reachable on macOS (container IPs are not).
		bindings[port] = []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: ""}}
	}

	var envs []string
	for k, v := range spec.Env {
		envs = append(envs, k+"="+v)
	}

	cfg := &container.Config{
		Image:        spec.Image,
		Env:          envs,
		WorkingDir:   spec.WorkingDir,
		Labels:       labels,
		ExposedPorts: exposed,
		User:         spec.User,
	}
	if len(spec.Command) > 0 {
		cfg.Entrypoint = spec.Command
	}
	if len(spec.Args) > 0 {
		cfg.Cmd = spec.Args
	}

	var mounts []mount.Mount
	for _, m := range spec.Mounts {
		switch {
		case m.Tmpfs:
			tm := mount.Mount{Type: mount.TypeTmpfs, Target: m.MountPath}
			if m.TmpfsSizeBytes > 0 {
				tm.TmpfsOptions = &mount.TmpfsOptions{SizeBytes: m.TmpfsSizeBytes}
			}
			mounts = append(mounts, tm)
		case m.VolumeName != "":
			mounts = append(mounts, mount.Mount{Type: mount.TypeVolume, Source: m.VolumeName, Target: m.MountPath, ReadOnly: m.ReadOnly})
		}
	}

	hostCfg := &container.HostConfig{
		PortBindings:   bindings,
		Mounts:         mounts,
		RestartPolicy:  container.RestartPolicy{Name: container.RestartPolicyDisabled},
		ReadonlyRootfs: spec.ReadOnlyRoot,
		DNS:            spec.DNSServers,
		Resources: container.Resources{
			NanoCPUs: spec.Resources.NanoCPUs,
			Memory:   spec.Resources.MemoryBytes,
		},
	}

	// Emulate Kubernetes fsGroup: a fresh docker named volume is root-owned, so
	// a non-root container cannot write to it. Chown the writable volume mounts
	// to the container user via a one-shot root container from the same image
	// (best-effort; requires sh+chown, which the bundled alpine runtime has).
	d.prepareVolumePermissions(ctx, spec, mounts)

	name := containerName(spec.Namespace, spec.SandboxName, spec.UID)
	created, err := d.cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, name)
	if err != nil {
		return ContainerInfo{}, fmt.Errorf("driver: create container: %w", err)
	}
	if err := d.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		_ = d.cli.ContainerRemove(ctx, created.ID, container.RemoveOptions{Force: true})
		return ContainerInfo{}, fmt.Errorf("driver: start container: %w", err)
	}
	return d.inspect(ctx, created.ID)
}

func (d *DockerDriver) inspect(ctx context.Context, id string) (ContainerInfo, error) {
	j, err := d.cli.ContainerInspect(ctx, id)
	if err != nil {
		return ContainerInfo{}, fmt.Errorf("driver: inspect: %w", err)
	}
	info := ContainerInfo{
		ID:      j.ID,
		Name:    strings.TrimPrefix(j.Name, "/"),
		Labels:  j.Config.Labels,
		PortMap: map[int]int{},
	}
	if j.State != nil {
		info.State = j.State.Status
		info.ExitCode = j.State.ExitCode
		info.StartedAt, _ = time.Parse(time.RFC3339Nano, j.State.StartedAt)
		info.FinishedAt, _ = time.Parse(time.RFC3339Nano, j.State.FinishedAt)
	}
	if j.NetworkSettings != nil {
		for _, n := range j.NetworkSettings.Networks {
			if n.IPAddress != "" {
				info.IPAddress = n.IPAddress
				break
			}
		}
		for p, binds := range j.NetworkSettings.Ports {
			for _, b := range binds {
				if b.HostPort == "" {
					continue
				}
				hp, _ := strconv.Atoi(b.HostPort)
				// Prefer loopback bindings.
				if b.HostIP == "127.0.0.1" || info.PortMap[p.Int()] == 0 {
					info.PortMap[p.Int()] = hp
				}
			}
		}
	}
	return info, nil
}

func (d *DockerDriver) InspectSandbox(ctx context.Context, namespace, name string) (ContainerInfo, error) {
	f := managedFilter()
	f.Add("label", LabelNamespace+"="+namespace)
	f.Add("label", LabelSandbox+"="+name)
	list, err := d.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		return ContainerInfo{}, err
	}
	if len(list) == 0 {
		return ContainerInfo{}, ErrNotFound
	}
	return d.inspect(ctx, list[0].ID)
}

func (d *DockerDriver) ListManaged(ctx context.Context) ([]ContainerInfo, error) {
	list, err := d.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: managedFilter()})
	if err != nil {
		return nil, err
	}
	var out []ContainerInfo
	for _, c := range list {
		info, err := d.inspect(ctx, c.ID)
		if err != nil {
			continue
		}
		out = append(out, info)
	}
	return out, nil
}

func (d *DockerDriver) StopContainer(ctx context.Context, id string, grace time.Duration) error {
	secs := int(grace.Seconds())
	err := d.cli.ContainerStop(ctx, id, container.StopOptions{Timeout: &secs})
	if err != nil && !client.IsErrNotFound(err) {
		return err
	}
	return nil
}

func (d *DockerDriver) RemoveContainer(ctx context.Context, id string) error {
	err := d.cli.ContainerRemove(ctx, id, container.RemoveOptions{Force: true})
	if err != nil && !client.IsErrNotFound(err) {
		return err
	}
	return nil
}

func (d *DockerDriver) EnsureVolume(ctx context.Context, spec VolumeSpec) (string, error) {
	labels := map[string]string{
		LabelManaged:   "true",
		LabelNamespace: spec.Namespace,
		LabelVolume:    spec.PVCName,
	}
	for k, v := range spec.Labels {
		labels[k] = v
	}
	v, err := d.cli.VolumeCreate(ctx, volume.CreateOptions{Name: spec.Name, Labels: labels})
	if err != nil {
		return "", fmt.Errorf("driver: create volume %q: %w", spec.Name, err)
	}
	return v.Name, nil
}

func (d *DockerDriver) ListManagedVolumes(ctx context.Context) ([]VolumeSpec, error) {
	resp, err := d.cli.VolumeList(ctx, volume.ListOptions{Filters: managedFilter()})
	if err != nil {
		return nil, err
	}
	var out []VolumeSpec
	for _, v := range resp.Volumes {
		out = append(out, VolumeSpec{
			Name:      v.Name,
			Namespace: v.Labels[LabelNamespace],
			PVCName:   v.Labels[LabelVolume],
			Labels:    v.Labels,
		})
	}
	return out, nil
}

func (d *DockerDriver) RemoveVolume(ctx context.Context, name string) error {
	err := d.cli.VolumeRemove(ctx, name, true)
	if err != nil && !client.IsErrNotFound(err) {
		return err
	}
	return nil
}

func (d *DockerDriver) ProbeRuntime(ctx context.Context, hostPort int) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/", hostPort)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := d.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 500 {
		return fmt.Errorf("driver: runtime probe status %d", resp.StatusCode)
	}
	return nil
}

func (d *DockerDriver) Events(ctx context.Context) (<-chan Event, error) {
	f := managedFilter()
	f.Add("type", "container")
	msgs, errs := d.cli.Events(ctx, events.ListOptions{Filters: f})
	out := make(chan Event, 64)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errs:
				if err != nil {
					return
				}
			case m := <-msgs:
				attrs := m.Actor.Attributes
				ev := Event{
					Type:      string(m.Action),
					ID:        m.Actor.ID,
					Namespace: attrs[LabelNamespace],
					Sandbox:   attrs[LabelSandbox],
				}
				if code, ok := attrs["exitCode"]; ok {
					ev.ExitCode, _ = strconv.Atoi(code)
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// prepareVolumePermissions chowns writable named-volume mounts to the
// container's user, emulating fsGroup. No-op for root or when there are no
// writable volume mounts. Failures are logged into the returned error path by
// the caller's best-effort contract: we swallow errors here so an image
// without sh/chown still yields a running sandbox (the mount may just be
// read-only-effective for that user).
func (d *DockerDriver) prepareVolumePermissions(ctx context.Context, spec SandboxContainerSpec, mounts []mount.Mount) {
	// Effective user: the spec's runAsUser if set, otherwise the image's baked
	// USER (an image can run non-root without the pod spec saying so).
	owner := spec.User
	if owner == "" {
		owner = d.imageUser(ctx, spec.Image)
	}
	if owner == "" || owner == "0" || owner == "root" || strings.HasPrefix(owner, "0:") || strings.HasPrefix(owner, "root:") {
		return
	}
	var paths []string
	var volMounts []mount.Mount
	for _, m := range mounts {
		if m.Type == mount.TypeVolume && !m.ReadOnly {
			paths = append(paths, m.Target)
			volMounts = append(volMounts, m)
		}
	}
	if len(paths) == 0 {
		return
	}
	initCfg := &container.Config{
		Image:      spec.Image,
		User:       "0",
		Entrypoint: []string{"sh", "-c"},
		Cmd:        []string{fmt.Sprintf("chown -R %s %s", owner, strings.Join(paths, " "))},
		Labels:     map[string]string{LabelManaged: "true", LabelNamespace: spec.Namespace, LabelSandbox: spec.SandboxName},
	}
	initHost := &container.HostConfig{Mounts: volMounts, RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled}}
	created, err := d.cli.ContainerCreate(ctx, initCfg, initHost, nil, nil, containerName(spec.Namespace, spec.SandboxName, spec.UID)+"-init")
	if err != nil {
		return
	}
	defer d.cli.ContainerRemove(context.Background(), created.ID, container.RemoveOptions{Force: true})
	if err := d.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return
	}
	waitCh, errCh := d.cli.ContainerWait(ctx, created.ID, container.WaitConditionNotRunning)
	select {
	case <-waitCh:
	case <-errCh:
	case <-ctx.Done():
	}
}

func containerName(namespace, sandbox, uid string) string {
	short := uid
	if len(short) > 8 {
		short = short[:8]
	}
	return fmt.Sprintf("las-%s-%s-%s", namespace, sandbox, short)
}
