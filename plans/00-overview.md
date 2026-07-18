# local-agent-sandbox — Plan Overview

## Goal

A single Go binary (working name: `lasd`) that emulates the control plane of
[kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox) on a developer
machine, backing sandboxes with **Docker containers** instead of Kubernetes pods.

The unmodified upstream SDKs (Go: `sigs.k8s.io/agent-sandbox/clients/go/sandbox`, Python:
`k8s_agent_sandbox`) must work against it exactly as they do against a real cluster — the same way
OpenSandbox lets you develop locally against its server without the SDK knowing the difference.
The only thing a user changes is `KUBECONFIG` (pointing at our local server) and, optionally,
`APIURL`/`api_url` (pointing at our local router).

Must run identically on macOS and Linux.

## Why this works: what the SDKs actually talk to

Recon of `reference/agent-sandbox` shows the SDKs speak to **two planes**, neither of which is a
bespoke controller API:

1. **Control plane = the Kubernetes API server itself.** Both SDKs do CRUD + WATCH on two CRD
   groups (storage/client version `v1beta1`):
   - `extensions.agents.x-k8s.io/v1beta1` → `sandboxclaims` (create/get/list/watch/delete/patch)
   - `agents.x-k8s.io/v1beta1` → `sandboxes` (get/list/watch)
   - Go client: generated clientsets (`clients/go/sandbox/k8s.go`). Python: `CustomObjectsApi`
     (`k8s_agent_sandbox/k8s_helper.py`). Same REST paths either way.
2. **Data plane = plain HTTP to a runtime server inside the pod**, fronted by the
   **sandbox-router** reverse proxy. The SDK reaches the router via one of three strategies
   (`clients/go/sandbox/strategy.go`, `connector.go`):
   - **Direct** (`Options.APIURL` / `SandboxDirectConnectionConfig.api_url`) — SDK hits the URL
     directly, still sending all `X-Sandbox-*` routing headers.
   - **Tunnel** (default) — Kubernetes **port-forward** to the router pod
     (Go: native SPDY against `POST /api/v1/namespaces/{ns}/pods/{pod}/portforward`, pod found via
     EndpointSlices for `sandbox-router-svc`; Python: shells out to
     `kubectl port-forward svc/sandbox-router-svc <local>:8080 -n agent-sandbox-system`).
   - **Gateway** (Gateway API resource watch) — production mode, out of local scope (stub later).

So the emulator is: **a partial fake kube-apiserver + reconcilers that drive Docker + a local
sandbox-router**. No SDK changes, no Kubernetes.

## Wire contract (authoritative reference for all phases)

### Control-plane REST surface we must serve

| Call | Used by | Notes |
|---|---|---|
| `POST /apis/extensions.agents.x-k8s.io/v1beta1/namespaces/{ns}/sandboxclaims` | Go + Py | Go uses `metadata.generateName: sandbox-claim-` — we must implement generateName suffixing. Py sends explicit `metadata.name`. |
| `GET …/sandboxclaims/{name}` | Go + Py | 404 semantics matter (`IsNotFound` drives SDK state machine). |
| `DELETE …/sandboxclaims/{name}` | Go + Py | 404 tolerated by both SDKs. |
| `GET …/sandboxclaims?watch=true&fieldSelector=metadata.name={n}&timeoutSeconds=N` | Go + Py | Watch stream of JSON event lines `{"type":"ADDED\|MODIFIED\|DELETED","object":{…}}`. `timeoutSeconds` must close the stream. |
| `GET …/sandboxclaims[?labelSelector=…]` | Py list / Go `ListAllSandboxes` | |
| `PATCH …/sandboxclaims/{name}` | Py `patch_sandbox_claim` | merge-patch content type. |
| `GET/LIST/WATCH /apis/agents.x-k8s.io/v1beta1/namespaces/{ns}/sandboxes` | Go + Py | Watch with `fieldSelector=metadata.name=…`; Go re-watches with `resourceVersion` from prior LIST — RV must be monotonic and never go backwards. |
| `GET /apis/discovery.k8s.io/v1/namespaces/{ns}/endpointslices?labelSelector=kubernetes.io/service-name=sandbox-router-svc` | Go tunnel | Must return a slice whose ready endpoint has `targetRef.name` = our virtual router pod. **The Go SDK looks in the sandbox's namespace**, Python's kubectl looks in `agent-sandbox-system` — materialize virtual router objects in any namespace on demand. |
| `POST /api/v1/namespaces/{ns}/pods/{pod}/portforward` | Go tunnel + kubectl | SPDY upgrade; streams pairs (`error`,`data`) keyed by `requestID` with `port` header. Go SDK forwards `0:8080`. |
| `GET /api/v1/namespaces/{ns}/services/sandbox-router-svc`, `GET/LIST pods` | kubectl (Py tunnel) | kubectl resolves service → selector → ready pod, then portforwards. |
| `/api`, `/apis`, `/version`, group discovery docs | kubectl, python client init | Minimal discovery documents; needed for kubectl UX and the `svc/` shortname path. |
| `GET/WATCH /apis/gateway.networking.k8s.io/v1/namespaces/{ns}/gateways/{name}` | Gateway mode only | Optional/stub. |

Auth: none. Serve plain HTTP on `127.0.0.1`; generate a kubeconfig whose `cluster.server` is
`http://127.0.0.1:<api-port>`. client-go, kubectl, and the python kubernetes client all accept
http kubeconfigs. This is the entire "the SDK can't tell the difference" trick.

### SDK-observable behavioral contract (what our reconcilers must produce)

Sequenced exactly as `clients/go/sandbox/sandbox.go#Open` and `k8s_agent_sandbox` consume it:

1. Claim created → claim `status.sandbox.name` becomes non-empty (watch event). Failure reasons the
   Python SDK **string-matches** on the claim's `Ready` condition: `TemplateNotFound`,
   `WarmPoolNotFound` (`k8s_helper.py:136-150`).
2. Sandbox `{status.conditions[type=Ready].status == "True"}` (watch event) with:
   - `status.podIPs` (SDKs prefer IPv4, `ip.go#selectPodIP`)
   - `metadata.annotations["agents.x-k8s.io/pod-name"]` (else SDKs fall back to sandbox name)
3. Claim deletion ⇒ sandbox + container teardown (SDK `Close()` only deletes the claim).
4. Suspend/resume (`spec.operatingMode: Suspended|Running`) and lifecycle expiry
   (`shutdownTime` + `shutdownPolicy`) per upstream controller semantics (see phase 3).

### Data-plane contract (router + runtime)

Router request headers (`clients/go/sandbox/types.go`, `sandbox-router/proxy/headers.go`):
`X-Sandbox-ID` (sandbox name, DNS label), `X-Sandbox-Namespace`, `X-Sandbox-Port` (runtime port,
default **8888**), optional `X-Sandbox-Pod-IP`, `X-Sandbox-Timeout` (float seconds),
`X-Request-ID`, W3C `traceparent`. Router proxies the request path verbatim to the runtime,
passes WebSocket/Upgrade through, and returns 502/503 on upstream failure (SDKs retry
500/502/503/504 with backoff).

Runtime HTTP API inside the container (reference implementation
`examples/python-runtime-sandbox/main.py`, port 8888, rooted at `/app`):

| Endpoint | Request | Response |
|---|---|---|
| `GET /` | — | `{"status":"ok",…}` health |
| `POST /execute` | `{"command":"…"}` | `{"stdout","stderr","exit_code"}`; args split shlex-style, **no shell**, cwd `/app`; non-zero exit still HTTP 200 |
| `POST /upload` | multipart, field `file`, plain filename | 200 `{"message":…}`; 403 on path escape |
| `GET /download/{pct-encoded-path}` | — | raw bytes; 404 `{"message":…}`; 403 escape |
| `GET /list/{pct-encoded-path}` | — | JSON array `{name,size,type:"file"\|"directory",mod_time:float}`; 404 if not dir |
| `GET /exists/{pct-encoded-path}` | — | `{"path":…,"exists":bool}` |

Paths are percent-encoded by the SDK for **every** byte outside RFC-3986 unreserved
(`files.go#percentEncode`), decoded server-side.

## Architecture

```
                   ┌──────────────────────────── lasd (single Go binary) ───────────────────────────┐
                   │                                                                                 │
 Go/Py SDK ──HTTP──► kube-facade :6644                store (bbolt + in-mem index,                  │
 kubectl           │  /apis/... CRUD+WATCH             monotonic RV, watch fan-out)                 │
                   │  /api/v1 pods/services/eps           │                                         │
                   │  portforward (SPDY) ─────────┐       │ events                                  │
                   │                              │       ▼                                         │
                   │                              │   reconcilers: claim ⇄ sandbox ⇄ docker         │
                   │                              │       │                                         │
 Go/Py SDK ──HTTP──► router :8880 ◄───pipe────────┘       ▼                                         │
 (APIURL mode)     │  X-Sandbox-* → registry ──► DockerDriver (docker API client)                   │
                   └──────────────────────────────────────┼─────────────────────────────────────────┘
                                                          ▼
                                        containers (runtime :8888) + named volumes
```

Key decisions:

- **Language/runtime**: Go ≥1.24 single binary. Reuse upstream types by importing
  `sigs.k8s.io/agent-sandbox` (`api/v1beta1`, `extensions/api/v1beta1`) so serialization is
  definitionally correct.
- **Docker access**: official `github.com/docker/docker/client` (works with Docker Desktop,
  OrbStack, Colima, rootless Docker, Podman's docker-compat socket; honors `DOCKER_HOST`).
- **Networking (macOS-proof)**: never dial container IPs. Every sandbox container publishes its
  runtime port(s) to `127.0.0.1:<random>`; the router resolves `(namespace, sandbox-id, port)` →
  host-mapped port. Container bridge IPs are still *reported* in `status.podIPs` for fidelity, and
  our router deliberately ignores `X-Sandbox-Pod-IP`. (Python's in-cluster strategy is
  cluster-only; documented as unsupported.)
- **State**: Docker labels (`las.managed=true`, `las.namespace`, `las.sandbox`, `las.uid`, …) are
  the source of truth for containers/volumes; CRs live in a bbolt file
  (`~/.local/share/local-agent-sandbox/state.db`). On startup, reconcile store ↔ `docker ps -a`
  (re-adopt survivors, GC orphans). Container labels are chosen to be **adoption-invariant** (they
  never need mutation — Docker labels are immutable).
- **Zero-config UX** (the OpenSandbox lesson): on first run, auto-create a `default`
  SandboxTemplate + SandboxWarmPool pointing at a bundled runtime image, so
  `client.CreateSandbox(ctx, "default", "default")` works with no YAML. Users can `kubectl apply`
  their own templates/pools at any time.
- **Runtime image**: ship our own tiny static-Go runtime server implementing the table above
  (byte-compatible with `main.py`), built on demand (`docker build` from embedded context) or
  pulled if published. Any user image that serves the same API on `ServerPort` also works.
- **v1beta1 only** for both groups (what both SDKs and current CRDs use). No conversion webhooks.

## Design lessons borrowed from OpenSandbox (recon of `reference/OpenSandbox`)

Their local mode is `[runtime] type="docker"` behind a `SandboxService` ABC
(`server/opensandbox_server/services/sandbox_service.py`, factory in `services/factory.py`) —
validating our Driver-interface + factory shape. Specific patterns we adopt:

1. **Labels as the source of truth, side-store for the rest** — OpenSandbox keeps *all* sandbox
   state on Docker labels (`opensandbox.io/id`, `/expires-at`, port map…), rebuilds everything
   from `docker ps` on startup, and uses a small JSON side-store only because running containers
   can't be relabeled. Our labels+bbolt split mirrors this; startup adoption/GC scan is the same
   move (`_restore_existing_sandboxes`).
2. **In-process TTL timers rebuilt from state on boot** (their `threading.Timer` per sandbox) —
   our reaper recomputes deadlines from CRs at startup rather than persisting timers.
3. **Injected static agent + entrypoint override** (`services/docker/runtime.py`): user images
   need no bundled server. We offer this as opt-in only (agent-sandbox's contract is that the
   image serves the runtime API itself) — see phase 2.
4. **Never dial container IPs** — they publish only the agent port to random host ports
   (40000–60000) and reach every other in-container port through the agent's `/proxy/{port}`
   route. We get the same macOS-safety by publishing declared ports to `127.0.0.1:0`; the
   `X-Sandbox-Port` header plays the role their `/proxy/{port}` plays.
5. **What we deliberately do differently**: OpenSandbox exposes its own bespoke REST API, so its
   SDKs are captive; agent-sandbox SDKs speak Kubernetes, so our compatibility surface is the
   K8s API itself — bigger up-front cost (watch semantics, port-forward) but zero SDK divergence
   forever after.

## Phases

| Phase | File | Deliverable |
|---|---|---|
| 1 | `01-control-plane.md` | Object store + watch + kube-facade HTTP API + kubeconfig; SDK clientsets pass against it |
| 2 | `02-docker-runtime.md` | DockerDriver + runtime image + volume/port handling on macOS & Linux |
| 3 | `03-reconcilers.md` | Claim/Sandbox/Template/WarmPool reconcilers with upstream-faithful conditions & lifecycle |
| 4 | `04-router-data-plane.md` | Local sandbox-router; Direct (`APIURL`) mode end-to-end |
| 5 | `05-tunnel-portforward.md` | Virtual router pod/service/EndpointSlice + SPDY port-forward; default SDK mode + kubectl |
| 6 | `06-e2e-cli-packaging.md` | CLI UX, real-SDK conformance/E2E suites (Go+Python), CI matrix, docs |

Each phase ends green and independently demo-able; 1+2+3+4 is the minimum "SDK works in Direct
mode" milestone; phase 5 removes the last config knob (pure default SDK settings).

## Test strategy (cross-cutting)

- **Unit**: store semantics (RV monotonicity, generateName, field/label selectors, watch
  timeoutSeconds/DELETED), reconciler state machines against a fake driver, router header/proxy
  logic, runtime filesystem guards. Pure Go, no Docker required.
- **Contract tests**: run the *upstream generated clientsets* (Go) and the *python kubernetes
  client* against our facade; assert the exact call sequences from `k8s.go`/`k8s_helper.py`
  succeed (create→resolve name→watch ready→delete).
- **Integration (Docker required, auto-skip otherwise)**: driver ops, full claim→container→exec
  flows, suspend/resume, expiry reaper, volume persistence.
- **Conformance/E2E**: run the **unmodified Go SDK** (and its upstream integration test suite via
  `-api-url` + `INTEGRATION_TEST=1`) and the Python SDK examples against `lasd` + Docker.
- **CI**: GitHub Actions — `ubuntu-latest` runs everything; `macos-latest` runs unit/contract
  always and integration via Colima (nightly/manual lane if runner Docker proves flaky). All paths
  use `path/filepath`, no GNU-only tooling, to keep macOS/Linux parity.

## Risks / open questions

1. **SPDY port-forward server** is the trickiest piece (phase 5). Mitigation: Direct/`APIURL` mode
   ships first (phase 4) and is fully supported by both SDKs including router headers; SPDY is
   additive. Implementation reuses `k8s.io/apimachinery` httpstream/spdy server upgrader (same
   code kubelet uses); newer kubectl falls back from WebSocket to SPDY automatically.
2. **kubectl niceties** (Table output for `kubectl get`, OpenAPI schemas) — not needed by SDKs;
   implement minimal discovery first, add Table conversion later if we want pretty `kubectl get`.
3. **Pod-spec coverage**: full PodSpec cannot map to Docker. We map a documented subset (image,
   command/args, env, workdir, ports, PVC/emptyDir volume mounts, runAsUser, resource limits) and
   surface unsupported fields as events/log warnings rather than hard failures.
4. **Upstream drift**: agent-sandbox is pre-1.0. Pin the reference SHA in `go.mod`; the
   conformance suite (phase 6) is the drift alarm.
5. **Podman/rootless quirks** (port publish binding, cgroup limits): best-effort; CI covers Docker
   Engine + Docker Desktop-equivalent (Colima) only.
