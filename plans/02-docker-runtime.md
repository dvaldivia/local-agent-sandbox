# Phase 2 — Docker runtime driver + bundled runtime image

**Outcome**: an internal `driver` package that turns a resolved pod template into a running,
health-checked Docker container with volumes and localhost port mappings, discoverable after
restart via labels — on macOS and Linux. Plus `sandbox-runtime` — our bundled image serving the
data-plane API.

## internal/driver — interface first

Reconcilers (phase 3) depend on this interface, never on the Docker client directly (enables a
fake for unit tests):

```go
type Driver interface {
    EnsureImage(ctx, image string, pull PullPolicy) error
    CreateSandboxContainer(ctx, SandboxContainerSpec) (ContainerInfo, error) // create+start
    InspectSandbox(ctx, ns, name string) (ContainerInfo, error)             // by labels
    ListManaged(ctx) ([]ContainerInfo, error)                               // label las.managed=true
    StopContainer(ctx, id string, grace time.Duration) error
    RemoveContainer(ctx, id string, keepVolumes bool) error
    EnsureVolume(ctx, VolumeSpec) (string, error)                           // named docker volume
    RemoveVolume(ctx, name string) error
    ProbeRuntime(ctx, hostPort int) error                                   // GET / on runtime
}

type SandboxContainerSpec struct {
    Namespace, SandboxName, UID string
    Image        string
    Command, Args []string
    Env          map[string]string
    WorkingDir   string
    RuntimePorts []int              // containerPorts + ServerPort default 8888, deduped
    Mounts       []Mount            // named volume or tmpfs(emptyDir{medium:Memory})
    User         string             // securityContext.runAsUser/runAsGroup
    Resources    Resources          // cpu (NanoCPUs), memory limits from resources.limits
    Labels       map[string]string  // las.* + propagated pod labels/annotations (prefixed)
}
type ContainerInfo struct {
    ID, Name  string
    State     string          // running/exited/paused…
    ExitCode  int
    IPAddress string          // bridge IP (reported in status.podIPs; informational on macOS)
    PortMap   map[int]int     // containerPort → 127.0.0.1 host port
    Labels    map[string]string
    StartedAt time.Time
}
```

Implementation: `github.com/docker/docker/client` with `client.FromEnv` +
`client.WithAPIVersionNegotiation()`. Socket discovery order: `DOCKER_HOST` → docker context
(`~/.docker/config.json` current context) → platform defaults (`/var/run/docker.sock`,
`~/.colima/docker.sock`, `~/.orbstack/run/docker.sock`, Podman compat socket). Emit one clear
startup log line saying which endpoint was chosen; `lasd doctor` (small subcommand) prints
diagnosis when none found.

### Pod-template → Docker mapping rules

- **Container naming**: `las-<ns>-<sandbox>` (+ short uid suffix to survive rapid
  delete/recreate); identity comes from labels, not the name:
  `las.managed=true`, `las.namespace`, `las.sandbox`, `las.uid`, `las.claim` (empty until bound —
  binding lives in the store because Docker labels are immutable; label set is chosen so adoption
  never requires relabeling).
- **Primary container** = `podTemplate.spec.containers[0]` (v1). Additional containers: create in
  the same network namespace K8s-style via a tiny pause container +
  `--network container:<pause>` — deferred to a stretch phase; log-and-ignore in v1.
- **Ports**: for each `RuntimePorts` entry, publish `127.0.0.1:0 → port/tcp` (Docker picks a free
  host port; read it back from inspect). We never rely on container IPs (unroutable on macOS).
- **Env**: template env + claim-injected env (phase 3 merges before calling the driver).
- **Volumes**:
  - PVC-backed pod volume → named docker volume (name from phase-3 PVC naming rule, labeled
    `las.*`), mounted at each referencing `volumeMount.mountPath`.
  - `emptyDir` → anonymous volume (`medium: Memory` → tmpfs mount; tmpfs size from sizeLimit).
  - configMap/secret/downwardAPI/projected → unsupported in v1: log warning, skip mount.
  - `subPath`: docker can't express it directly → unsupported warning in v1.
- **Resources**: `limits.cpu` → NanoCPUs, `limits.memory` → Memory. Requests ignored.
- **Security**: `runAsUser`/`runAsGroup` → `User: "uid:gid"`; `readOnlyRootFilesystem` →
  `ReadonlyRootfs`. Never pass through `privileged: true` (refuse with a clear error) — this tool
  runs on dev laptops.
- **Restart policy**: `no` (the reconciler owns lifecycle; a crashed runtime should surface as
  NotReady/Finished, phase 3 maps exit → conditions via docker events/inspect).
- **ImagePullPolicy**: honor `IfNotPresent`/`Always`/`Never` semantics
  (`EnsureImage`). Local-tag images (like `sandbox-runtime:latest`) resolve without registry.

### Readiness probing

`ProbeRuntime` = `GET http://127.0.0.1:<hostPort>/` expecting HTTP 200 (matches the reference
runtime health endpoint). Poll with backoff after start; phase 3 flips the Sandbox Ready condition
only after (container running) && (probe OK) — this is what makes SDK `Open()` reliable, since it
connects immediately after Ready. If the pod template declares its own readinessProbe
(httpGet), prefer its path/port.

### Event stream

Subscribe to Docker events (`container die/stop/oom`) filtered by `las.managed=true`; forward to
phase-3 reconcilers so container death promptly updates conditions (SDK reconnect logic depends on
`verifySandboxAlive` seeing Ready=False or the claim/sandbox disappearing).

## sandbox-runtime image (bundled data-plane server)

`runtime/` in our repo: a small pure-Go HTTP server byte-compatible with
`examples/python-runtime-sandbox/main.py`:

- Same routes/status codes/JSON shapes as the table in `00-overview.md` (including `{"message":…}`
  error bodies, `exists` returning `{"path","exists"}`, list entry `mod_time` as float seconds).
- `/execute`: shlex-style splitting (use `github.com/google/shlex`), **no shell**, cwd `/app`,
  inherit container env; command failure → HTTP 200 with `exit_code`; spawn failure → 200 with
  `exit_code:1` + stderr message (mirror main.py's behavior exactly).
- Root dir `/app`, path-escape guard via `filepath.EvalSymlinks`-hardened prefix check (same
  policy as `get_safe_path`, including rejecting `..` after decoding).
- Static binary (CGO_DISABLED, linux/amd64+arm64), `FROM scratch`-ish image (need a writable
  `/app`: use `FROM alpine` + `RUN mkdir -p /app && chown 1000:1000 /app`, `USER 1000`,
  `EXPOSE 8888`). Multi-arch matters: hosts are amd64 Linux CI + arm64 macs.
- Build path: `lasd` embeds the Dockerfile+source (go:embed) and does `docker build` on first use
  (tag `lasd/sandbox-runtime:<version>`); also `make runtime-image` for dev. If we later publish
  to GHCR, `EnsureImage` prefers pull.
- Also provide `Dockerfile.python` documentation pointing at the upstream example image for users
  who want the exact reference runtime.

### Optional: runtime *injection* mode (OpenSandbox's execd trick)

OpenSandbox never requires user images to bundle its agent: at create time the server
`docker cp`s a static agent binary into the container and overrides the entrypoint
(`reference/OpenSandbox/server/opensandbox_server/services/docker/runtime.py`). We adopt this as
an **opt-in** convenience (template annotation `las.dev/inject-runtime: "true"` or bootstrap
sugar): copy our static runtime binary into any image (e.g. plain `python:3.12`) via
`CopyToContainer` before start and run it as PID 1 alongside… nothing else (entrypoint replaced),
serving `ServerPort`. Clearly documented as a **local-only divergence** — on a real cluster that
template would not serve the runtime API — and therefore off by default; the faithful path
remains "the image runs its own server" (agent-sandbox's contract).

### DNS fidelity note

Templates provisioned in upstream "secure by default" mode get `dnsPolicy: None` + nameservers
`8.8.8.8`/`1.1.1.1` injected (see phase 3). The driver maps `PodSpec.dnsConfig.nameservers` →
container DNS settings so this is observable locally too.

## Tests

- **Unit (no Docker)**: pod-template mapping (given PodSpec fixture → expected
  container/host config, port specs, mounts, warnings for unsupported fields); label round-trip;
  socket discovery ordering (env-injected fakes).
- **Runtime server unit tests** (pure Go, no Docker): httptest against the runtime handlers —
  execute stdout/stderr/exit codes, upload/download/list/exists round-trip, percent-encoded paths
  (spaces, UTF-8, `%2E%2E` traversal rejected with 403), multipart field name `file`, large file
  (64MB) streaming.
- **Parity test vs reference** (optional, guarded): run the same test table against the FastAPI
  `main.py` in a venv and diff responses — proves byte-compat.
- **Driver integration** (`//go:build integration` or `LASD_DOCKER_TESTS=1`; auto-skip when the
  Docker socket probe fails): EnsureImage builds embedded runtime; create→probe→stop→remove;
  volume persists data across container recreate; ListManaged finds container after simulated
  restart (new driver instance); port published on 127.0.0.1 only; docker-events death
  notification.
- CI: Linux always runs integration; macOS lane best-effort via Colima (same tests, `-short`
  subset if slow).

## Exit criteria

`go test ./internal/driver/... ./runtime/...` green with and without Docker present (skips clean);
manual: `lasd debug run-sandbox --image lasd/sandbox-runtime:dev` starts a container, `curl
127.0.0.1:<port>/execute` echoes.
