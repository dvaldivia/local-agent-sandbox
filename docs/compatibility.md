# Compatibility & wire contract

What `lasd` emulates, and where it deviates. This is the contract the test suite gates against.

## Control plane (Kubernetes API surface)

Served by the kube-facade. The SDKs and kubectl treat `lasd` as a Kubernetes API server.

| Area | Emulated | Notes |
|---|---|---|
| CRDs `sandboxes` (`agents.x-k8s.io/v1beta1`) | ✅ | get/list/watch/create/update/patch/delete + `/status` |
| CRDs `sandboxclaims`/`sandboxtemplates`/`sandboxwarmpools` (`extensions.agents.x-k8s.io/v1beta1`) | ✅ | warmpool has `/scale` |
| `metadata.generateName` | ✅ | Go SDK creates claims this way |
| resourceVersion (monotonic), List→Watch(RV) handoff | ✅ | global counter; watch replays history or snapshots |
| Watch: `fieldSelector=metadata.name`, `labelSelector`, `timeoutSeconds`, `resourceVersion` | ✅ | newline-delimited `watch.Event` JSON |
| 409 Conflict on stale `resourceVersion` | ✅ | supports client-go RetryOnConflict |
| Discovery (`/api`, `/apis`, group/version resource lists, `/version`) | ✅ | |
| OpenAPI v3 (`/openapi/v3`) | ✅ | permissive schemas + per-GVK path with `fieldValidation` so `kubectl apply` validates |
| OpenAPI v2 (`/openapi/v2`) | ❌ | not served (protobuf-only); a valid v3 doc prevents kubectl fallback |
| core `pods`/`services`/`namespaces`, `discovery.k8s.io/v1 endpointslices` | ✅ (subset) | used by tunnel discovery + virtual router objects |
| `pods/{name}/portforward` | ✅ | SPDY, only for the virtual `sandbox-router-0` pod |
| `gateways` (`gateway.networking.k8s.io/v1`) | ⚠️ stub | GVR served so watches don't 404; gateway mode not emulated |
| Strategic-merge patch for core types | ⚠️ | treated as JSON merge patch (fine for CRDs; approximate for pods/services) |
| Authn/authz, admission webhooks, most core resources | ❌ | plain HTTP on 127.0.0.1, no auth |

## Data plane (router + runtime)

| Area | Emulated | Notes |
|---|---|---|
| Router `X-Sandbox-ID/-Namespace/-Port/-Timeout/-Request-ID` | ✅ | resolves to a localhost-published container port |
| `X-Sandbox-Pod-IP` | accepted, ignored | container IPs are unreachable on macOS; the registry is authoritative |
| Path fidelity (percent-encoded segments) | ✅ | forwarded verbatim (RawPath preserved) |
| Streaming + WebSocket/Upgrade passthrough | ✅ | `FlushInterval:-1`, Hijack delegation |
| Miss → status codes | ✅ | unknown sandbox/port → 502; not-ready → 503 (SDKs retry) |
| Runtime API `/execute`,`/upload`,`/download`,`/list`,`/exists` | ✅ | bundled Go runtime, byte-compatible with the reference Python runtime |

## Reconciler semantics (vs upstream controllers)

Verified against the upstream agent-sandbox controllers (see `plans/03-reconcilers.md` for line refs).

| Behavior | Emulated |
|---|---|
| Cold-start Sandbox named `claim.Name`; warm-adopted keeps pool name | ✅ |
| `status.sandbox.name` recorded; adoption annotation `agents.x-k8s.io/sandbox-name` | ✅ |
| Claim Ready mirrors the Sandbox Ready condition verbatim (reason `DependenciesReady`) | ✅ |
| Failure reasons `WarmPoolNotFound` / `TemplateNotFound` (Python SDK string-matches) | ✅ |
| Sandbox conditions Ready/Suspended/Finished with upstream reasons | ✅ |
| `agents.x-k8s.io/pod-name` annotation; `status.selector` = name-hash label | ✅ |
| VolumeClaimTemplates → PVC `<tmpl>-<sandbox>` → docker volume; fsGroup-style chown | ✅ |
| OperatingMode Suspend (remove container, keep volumes) / Resume | ✅ |
| Lifecycle `shutdownTime` + `shutdownPolicy` (Delete/Retain), claim `ttlSecondsAfterFinished` | ✅ |
| Secure defaults: `automountServiceAccountToken=false`, managed DNS (8.8.8.8/1.1.1.1) | ✅ |
| Env injection policy (`Disallowed`/`Allowed`/`Overrides`) | ✅ |
| Deletion cascade claim→sandbox→container/volumes; orphan GC | ✅ (store delete hook + reconcile GC) |

### Documented deviations

- Adoption happens in one reconcile pass; the SDK-visible `AdoptionPending` window may be absent.
- NetworkPolicy specs are accepted but only the secure-default DNS effect is applied.
- Warm pool `updateStrategy` staleness recycling is simplified (replicas maintained; drift
  recycling is best-effort).
- Single-container pods only; multi-container is logged and the first container is run.
- `status.selector` is not cleared on pod loss (matches an upstream quirk); wiped on expiry.

## Upstream pin

`lasd` depends on `sigs.k8s.io/agent-sandbox` as an ordinary Go module, pinned in `go.mod`/`go.sum`
and fetched from kubernetes-sigs/agent-sandbox — no local clone. Bump with
`go get sigs.k8s.io/agent-sandbox@<tag-or-commit> && go mod tidy`. The E2E/conformance suite is the
drift alarm when upstream moves.
