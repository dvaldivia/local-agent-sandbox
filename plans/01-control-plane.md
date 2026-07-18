# Phase 1 — Control plane: object store + kube-facade API + kubeconfig

**Outcome**: `lasd serve` exposes an HTTP kube-API facade on `127.0.0.1:6644`; the upstream
generated Go clientsets and the python `kubernetes` client can create/get/list/watch/delete
SandboxClaims/Sandboxes/SandboxTemplates/SandboxWarmPools against it. No Docker yet — objects just
sit in the store (a temporary `--static-ready` dev flag can mark sandboxes Ready for manual SDK
smoke tests).

## Repo scaffolding

```
go.mod                      module github.com/dvaldivia/local-agent-sandbox   (go ≥ 1.24)
cmd/lasd/main.go            cobra CLI: serve, kubeconfig, version
internal/store/             object store + watch hub
internal/kubefacade/        HTTP handlers for the k8s REST surface
internal/apis/              scheme registration, GVR/GVK tables, codecs
internal/config/            server config (ports, data dir, bootstrap objects)
Makefile                    build, test, lint (golangci-lint), integration targets
```

Dependencies: `k8s.io/apimachinery`, `k8s.io/api`, `sigs.k8s.io/agent-sandbox` (pinned SHA — gives
us `api/v1beta1.Sandbox` and `extensions/api/v1beta1.SandboxClaim/Template/WarmPool` structs +
deepcopy), `github.com/spf13/cobra`, `go.etcd.io/bbolt`.

Types served (all namespaced, plural → kind):

| GVR | Kind |
|---|---|
| `agents.x-k8s.io/v1beta1/sandboxes` | Sandbox |
| `extensions.agents.x-k8s.io/v1beta1/sandboxclaims` | SandboxClaim |
| `extensions.agents.x-k8s.io/v1beta1/sandboxtemplates` | SandboxTemplate |
| `extensions.agents.x-k8s.io/v1beta1/sandboxwarmpools` | SandboxWarmPool |
| core `v1` `namespaces`, `pods`, `services`; `discovery.k8s.io/v1` `endpointslices` | phase 5 fills these; the store must handle arbitrary registered GVRs from day 1 |

## internal/store design

A generic ordered object store, deliberately etcd-shaped so watch semantics come out right:

```go
type Store struct {
    db      *bbolt.DB               // persistence: bucket per GVR, key ns/name, value JSON
    mu      sync.RWMutex
    objects map[GVR]map[nsName]*unstructured.Unstructured // working set (decoded once)
    rv      atomic.Uint64           // single global, monotonic resourceVersion counter
    hub     *watchHub               // per-GVR subscriber lists
}
```

- **resourceVersion**: one global counter, incremented on every write; stamped on the object and
  on List responses (`metadata.resourceVersion`). Never reused, never decreases, survives restart
  (persist high-water mark). This satisfies the Go SDK's List→Watch(RV) handoff
  (`k8s.go#waitForSandboxReady`).
- **Create**: reject duplicate names (409, k8s-shaped Status body); implement
  **`metadata.generateName`** (append 5-char lowercase random suffix, retry on collision) — the Go
  SDK depends on it. Default `metadata.uid` (uuid), `creationTimestamp`, `generation=1`.
- **Update/Patch**: support `application/merge-patch+json` (python `patch_namespaced_custom_object`
  default) and `application/json` PUT (our own reconcilers can use store API directly). Bump RV,
  bump `generation` on spec change. Status subresource endpoints (`…/{name}/status`) should exist
  and simply write the status field — kubectl and future controllers use them; our reconcilers go
  through the same path to get watch events for free.
- **Delete**: emit DELETED event carrying the final object. Support `DeleteOptions` body but ignore
  propagation policies (our reconcilers cascade explicitly). 404 → k8s Status NotFound (both SDKs
  branch on it).
- **List**: filter by namespace, `labelSelector` (use `k8s.io/apimachinery/pkg/labels.Parse`),
  `fieldSelector` limited to `metadata.name=X` / `metadata.namespace=X` (all either SDK uses).
- **Watch**: `?watch=true` → `Transfer-Encoding: chunked`, one JSON watch event per line, flushed
  per event. Honor `timeoutSeconds` (close stream when elapsed — python watch relies on this),
  honor `resourceVersion`: replay ADDED for matching objects with RV > requested (RV absent/0 ⇒
  replay all as ADDED), then stream live events. Duplicate ADDED events are safe for both SDK
  state machines (they re-inspect conditions); missing events are not — bias accordingly.
  Backpressure: per-watcher buffered channel; on overflow, drop the watcher (clients re-list).
- Namespaces are implicit: any write into a namespace materializes it (kubectl `get ns` should
  show it).

## internal/kubefacade HTTP surface (this phase)

- `GET /version` → static `version.Info` JSON (give a recognizable `gitVersion`, e.g.
  `v1.33.0-lasd`).
- `GET /api` → `{"kinds":…}` core group versions; `GET /api/v1` → core v1 resource list
  (namespaces, pods, services — even before phase 5 fills their behavior).
- `GET /apis` → APIGroupList with our two agent groups (+ `discovery.k8s.io`,
  `gateway.networking.k8s.io` stub); `GET /apis/{group}/{version}` → APIResourceList (correct
  `namespaced: true`, kind, verbs). kubectl also probes the aggregated discovery endpoint
  (`Accept: application/json;g=apidiscovery.k8s.io;v=v2;as=APIDiscoveryList`) — serve it or let
  kubectl fall back to legacy discovery (verify at implementation; legacy fallback is acceptable).
- Resource routes (`/apis/{g}/{v}/namespaces/{ns}/{resource}[/{name}[/status]]`) wired to the
  store with the query params above. Return objects with correct `apiVersion`/`kind` stamped
  (unstructured passthrough keeps this trivial).
- Error bodies must be `metav1.Status` JSON with proper `reason`/`code` — client-go's
  `errors.IsNotFound` etc. parse these.
- Log every request at debug level (method, path, latency) — this doubles as the trace tooling
  for SDK compatibility work.

## kubeconfig generation

`lasd kubeconfig [--path ~/.lasd/kubeconfig] [--print-export]`:

```yaml
apiVersion: v1
clusters: [{name: lasd, cluster: {server: "http://127.0.0.1:6644"}}]
contexts: [{name: lasd, context: {cluster: lasd, namespace: default}}]
current-context: lasd
```

`lasd serve` also writes it to the data dir on boot and prints
`export KUBECONFIG=…` guidance. No TLS/auth — bind `127.0.0.1` only (flag `--bind` to override,
with a loud warning).

## Tests

- `internal/store` unit tests: create/generateName/duplicate-409; RV strictly increases across
  ops; merge-patch semantics; label & field selector filtering; watch: (a) replay-then-live,
  (b) RV filtering, (c) DELETED payload, (d) `timeoutSeconds` closes stream, (e) 100 concurrent
  watchers see identical sequences; restart test: reopen bbolt, RV high-water preserved.
- **Clientset contract test** (the important one): spin the facade on `httptest`, build a
  `rest.Config{Host: srv.URL}`, and drive the *upstream* clientsets from
  `sigs.k8s.io/agent-sandbox/clients/k8s`:
  1. Create SandboxClaim with `GenerateName` → name returned with suffix.
  2. `Watch(fieldSelector=metadata.name=X)` → receive ADDED; update claim status via store →
     receive MODIFIED.
  3. List sandboxes, then Watch with returned RV → no stale replay, live event on update.
  4. Delete → IsNotFound on re-Get; watch shows DELETED.
  Mirrors `k8s.go#createClaim/resolveSandboxName/waitForSandboxReady` exactly.
- **kubectl smoke** (optional, guarded by `kubectl` in PATH): `kubectl --kubeconfig … get
  sandboxclaims -A`, `apply -f` a SandboxTemplate, `delete`. Plain-JSON fallback output is fine.
- **Python client contract test** (guarded by `python3` + `kubernetes` package, or a pinned uv
  venv under `tests/python/`): script does create/watch/delete via `CustomObjectsApi` exactly like
  `k8s_helper.py`; run in CI Linux lane.

## Exit criteria

Upstream Go clientset + python kubernetes client pass the contract tests; `kubectl get/apply/
delete` works on all four CRs; state survives `lasd` restart.
