# Phase 5 — Tunnel mode: virtual router pod + SPDY port-forward (default SDK settings work)

**Outcome**: both SDKs work with **zero connection configuration** — the Go SDK's default
`tunnelStrategy` and the Python SDK's default `SandboxLocalTunnelConnectionConfig` (which shells
out to `kubectl port-forward`) reach our router through an emulated Kubernetes port-forward. After
this phase the only user-visible setup is `KUBECONFIG`.

## What the clients actually do (from recon — implement exactly this)

- **Go SDK** (`clients/go/sandbox/tunnel.go`):
  1. `LIST /apis/discovery.k8s.io/v1/namespaces/{sandbox-ns}/endpointslices?labelSelector=kubernetes.io/service-name=sandbox-router-svc`
     → picks first ready endpoint's `targetRef.name` as the router pod. **Namespace = the
     sandbox's namespace** (not a fixed system ns!).
  2. `POST /api/v1/namespaces/{ns}/pods/{pod}/portforward` with SPDY upgrade
     (`spdy.NewDialerForStreaming`), forwarding `0:8080`.
- **Python SDK** (`connector.py#LocalTunnelConnectionStrategy`): runs
  `kubectl port-forward svc/sandbox-router-svc <free-port>:8080 -n agent-sandbox-system`
  (default `router_namespace="agent-sandbox-system"`). kubectl resolves: GET service → selector →
  LIST pods → pick ready pod → POST `pods/{pod}/portforward` (SPDY; newer kubectl tries a
  WebSocket tunnel first and **falls back to SPDY** when the server rejects the upgrade).

## Virtual objects (served by the phase-1 store, materialized lazily)

For any namespace that hosts sandboxes **plus** `agent-sandbox-system`:

- Pod `sandbox-router-0` — labels `app=sandbox-router`; status `Running`, ready condition True
  (kubectl checks pod phase/readiness before forwarding).
- Service `sandbox-router-svc` — `spec.selector: {app: sandbox-router}`, port 8080→8080.
- EndpointSlice `sandbox-router-svc-1` — label `kubernetes.io/service-name=sandbox-router-svc`,
  one ready endpoint with `targetRef: {kind: Pod, name: sandbox-router-0}`.

Materialize on namespace first-use (cheap store writes); they're inert records — the only
behavior behind them is the portforward subresource.

## SPDY port-forward server

Endpoint: `POST/GET /api/v1/namespaces/{ns}/pods/sandbox-router-0/portforward`.

- Upgrade via `k8s.io/apimachinery/pkg/util/httpstream/spdy.NewResponseUpgrader()` negotiating
  `portforward.k8s.io/v1` (constant `PortForwardProtocolV1Name`) — the same server machinery
  kubelet uses (`k8s.io/kubelet/pkg/cri/streaming/portforward` is importable; prefer reusing its
  `httpStreamHandler` if the dependency is tolerable, else port that file — it's ~200 lines).
- Stream protocol: client opens streams in pairs with headers `streamType: error|data`,
  `port: 8080`, `requestID: n`. For each pair: dial our own router listener
  (`127.0.0.1:<router-port>`), bidirectional copy data-stream ↔ TCP conn, write dial failures to
  the error stream, half-close correctly (SDK/kubectl reuse one SPDY session for many
  connections).
- Validate requested port == 8080 (router); anything else → error-stream message mirroring
  kubelet ("unable to forward port").
- Requests for portforward on non-virtual pods → 404 (no real pods exist).
- Optional hardening for future kubectl defaults: WebSocket tunnel support
  (`SPDY/3.1+portforward.k8s.io` subprotocol — SPDY framing over a WS stream). Not required
  while kubectl's fallback works; keep as a tracked follow-up with a conformance test that runs
  `kubectl port-forward` and asserts fallback engaged.

## Gateway mode (documented stub)

`Options.GatewayName` / `SandboxGatewayConnectionConfig` watch
`gateway.networking.k8s.io/v1` Gateways for `status.addresses[0].value` and then hit
`http://<addr>` (port 80 implied) — awkward locally (privileged port). Serve the GVR (so watches
don't 404) but document Direct/Tunnel as the supported local modes. Revisit only if users ask.

## Tests

- **Unit**: virtual-object materialization (correct labels/targetRef; per-namespace); portforward
  port validation; stream-pair bookkeeping (out-of-order stream arrival, missing error stream).
- **client-go loopback test** (no kubectl needed): use
  `k8s.io/client-go/tools/portforward.NewForStreaming` + `spdy.NewDialerForStreaming` — the exact
  code path from `tunnel.go` — against the facade; forward `0:8080`; assert an HTTP GET through
  the tunnel reaches a stub router and returns; multiple concurrent connections over one session;
  session teardown mid-transfer surfaces on the error channel.
- **kubectl integration** (guarded by kubectl in PATH): `kubectl port-forward
  svc/sandbox-router-svc 0:8080 -n agent-sandbox-system` → curl through it; run with
  `KUBECTL_PORT_FORWARD_WEBSOCKETS=true` and `=false` to prove the fallback path.
- **Go SDK default-mode E2E** (Docker): `sandbox.NewClient` with **no** APIURL/Gateway →
  CreateSandbox("default") → Run/Write/Read → Close. This exercises EndpointSlice discovery +
  SPDY + router + reconcilers end to end.
- **Python SDK default-mode E2E** (Docker + kubectl, optional lane): `SandboxClient` with default
  `SandboxLocalTunnelConnectionConfig()`.

## Exit criteria

`clients/go/examples/basic` runs **unmodified except WarmPoolName** (no APIURL) against `lasd`;
Python quickstart runs with only `KUBECONFIG` set. Port-forward survives 100 sequential SDK
requests on one session (connection-reuse soak).
