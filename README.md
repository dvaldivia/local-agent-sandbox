# local-agent-sandbox (`lasd`)

Run [kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox)
sandboxes **locally on Docker**, with **no Kubernetes cluster** — and without changing a line of
the agent-sandbox SDKs.

`lasd` is a single Go binary that emulates the agent-sandbox control plane (a partial Kubernetes
API server) and data plane (the `sandbox-router`), backing each sandbox with a Docker container.
The unmodified upstream Go and Python SDKs connect to it exactly as they would to a real cluster —
the same trick OpenSandbox uses for local development. You point `KUBECONFIG` at `lasd` and
everything else just works.

## Why

Developing against agent-sandbox normally means a Kubernetes cluster (kind/GKE), the controller,
the router, warm pools, and container images — a lot to stand up just to iterate on agent code.
`lasd` collapses that to `lasd serve` + Docker Desktop/Colima/OrbStack. Same SDK, same objects,
same semantics; sandboxes are local containers.

## Install

Homebrew (macOS/Linux):

```bash
brew install dvaldivia/tap/lasd
```

Or download a prebuilt binary for your OS/arch from the
[Releases](https://github.com/dvaldivia/local-agent-sandbox/releases) page, or build from source
(`make build`). Requires a running Docker daemon (Docker Desktop, Colima, or OrbStack).

## Quickstart

```bash
# Run the server (control plane :6644, router :8880).
lasd serve
#    In another terminal:
export KUBECONFIG=~/.local/share/local-agent-sandbox/kubeconfig
lasd doctor    # sanity-check Docker/kubectl/ports
```

Go SDK (unmodified):

```go
client, _ := sandbox.NewClient(ctx, sandbox.Options{
    WarmPoolName: "default",         // a default template + pool are auto-created
    // No APIURL/Gateway → default tunnel mode works via emulated port-forward.
})
sb, _ := client.CreateSandbox(ctx, "default", "default")
res, _ := sb.Run(ctx, "echo hello")   // runs in a real Docker container
sb.Write(ctx, "note.txt", []byte("hi"))
data, _ := sb.Read(ctx, "note.txt")
client.DeleteSandbox(ctx, sb.ClaimName(), "default")
```

Python SDK (unmodified): set `KUBECONFIG`, then use `SandboxClient` with the default
`SandboxLocalTunnelConnectionConfig()` (kubectl port-forward), or
`SandboxDirectConnectionConfig(api_url="http://127.0.0.1:8880")`.

`kubectl` works too:

```bash
kubectl get sandboxtemplates,sandboxwarmpools,sandboxclaims,sandboxes
kubectl apply -f my-template.yaml
```

## How it works

```
Go/Py SDK, kubectl ──HTTP──► lasd
                              ├── kube-facade :6644   fake Kubernetes API (CRDs, watch, discovery,
                              │                       OpenAPI, pods/services/endpointslices,
                              │                       pod/portforward over SPDY)
                              ├── reconcilers          SandboxClaim → Sandbox → Docker container
                              │                       (warm pools, lifecycle, conditions)
                              └── router :8880         X-Sandbox-* headers → localhost-published
                                                       container port (Direct + tunnel modes)
```

- **Control plane** is the Kubernetes API itself: the SDKs do CRUD+watch on `sandboxclaims`
  (`extensions.agents.x-k8s.io/v1beta1`) and `sandboxes` (`agents.x-k8s.io/v1beta1`). `lasd`
  serves those against an in-memory + bbolt-backed object store.
- **Reconcilers** turn a claim into a Sandbox and a Sandbox into a Docker container, mirroring the
  upstream controllers' observable conditions/status.
- **Data plane**: container runtime ports are published to `127.0.0.1:<random>` (never dialed by
  container IP — so it works on macOS). The router maps `(namespace, sandbox, port)` to the host
  port. Tunnel mode is served by an emulated SPDY `pod/portforward` against a virtual
  `sandbox-router` pod/service/EndpointSlice.

See [docs/compatibility.md](docs/compatibility.md) for the exact wire contract and supported
feature matrix, and [plans/](plans/) for the phased design.

## CLI

```
lasd serve        # start control plane + router + reconcilers
lasd kubeconfig   # print/write a kubeconfig pointing at lasd
lasd status       # server health + resource counts
lasd ls           # list sandboxes and their containers
lasd gc           # remove containers with no backing Sandbox CR
lasd purge        # remove ALL managed containers/volumes (--state also deletes the db)
lasd doctor       # diagnose Docker/kubectl/ports
lasd version
```

## Building

```bash
make build
```

`lasd` depends on the upstream agent-sandbox Go types and clientsets as an ordinary Go module
(`sigs.k8s.io/agent-sandbox`, pinned in `go.mod`/`go.sum`), fetched by `go` from
kubernetes-sigs/agent-sandbox — no local clone required. To move to a different upstream revision:

```bash
go get sigs.k8s.io/agent-sandbox@<tag-or-commit> && go mod tidy
```

Requirements: Go ≥ 1.24, Docker (Desktop, Colima, OrbStack, or Engine). macOS and Linux.

## Testing

```bash
make test          # unit + contract tests (no Docker required; Docker tests auto-skip)
make integration   # Docker-backed driver/reconciler/E2E tests (LASD_DOCKER_TESTS=1)
make race          # unit tests under the race detector
```

The E2E suite runs the **unmodified upstream Go SDK** end to end in both Direct and tunnel modes
against real containers — the strongest "the SDK can't tell the difference" signal.

## Limitations

- Single-container pods (extra containers are logged and ignored).
- Volume sources: PVC (volumeClaimTemplates and pre-existing PVCs by claimName, each backed by a
  Docker named volume) and emptyDir; configMap/secret/downwardAPI unsupported.
- Gateway connection mode is not emulated (Direct and tunnel modes are supported).
- The Python SDK's in-cluster connection strategy is cluster-only and not applicable locally.
- Not a security boundary — sandboxes are ordinary local containers.
