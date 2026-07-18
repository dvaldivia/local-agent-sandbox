# Phase 4 — Local sandbox-router (data plane), Direct/APIURL mode end-to-end

**Outcome**: `lasd` serves a router on `127.0.0.1:8880` that behaves like the in-cluster
`sandbox-router`: SDK `Commands`/`Files` calls work with `Options.APIURL="http://127.0.0.1:8880"`
(Go) / `SandboxDirectConnectionConfig(api_url=…)` (Python). This is the first full-SDK milestone
(phases 1+2+3 provide the sandboxes it routes to).

## Behavior (mirror `sandbox-router/proxy`)

- **Request contract**: read `X-Sandbox-ID` (required; validate DNS label ≤63 chars → else 400
  with a JSON error body shaped like the router's), `X-Sandbox-Namespace` (default `default`),
  `X-Sandbox-Port` (default 8888), `X-Sandbox-Timeout` (float seconds → per-request upstream
  timeout), `X-Request-ID` (propagate into logs + response), `X-Sandbox-Pod-IP` (**accepted but
  ignored** — container IPs aren't reachable on macOS; our registry is authoritative; log at
  debug when we override).
- **Resolution**: `(namespace, sandbox-id)` → store lookup (sandbox exists? Ready?) → container
  `PortMap[X-Sandbox-Port]` → target `http://127.0.0.1:<hostPort>`. Misses map to the router's
  observable errors:
  - unknown sandbox / no container → 502 (SDK retries then surfaces; matches router dial-fail
    class) with JSON detail;
  - sandbox exists but not Ready / container not running → 503 (retryable — SDK backoff covers
    the resume-in-progress window);
  - declared port not published → 502.
- **Proxy**: `httputil.ReverseProxy` with `Rewrite` (preserve path+query verbatim — SDK sends
  `/execute`, `/download/<pct-encoded>`, etc.; do NOT re-encode: use `RawPath`), strip the
  `X-Sandbox-*` headers before forwarding, pass `traceparent` through, stream bodies both ways
  (no buffering limits of our own — SDK enforces its own 256MB caps), honor Upgrade/WebSocket
  passthrough (`isUpgradeRequest` logic) for user runtimes that speak WS.
- **Retry-once on dial failure** with registry refresh (mirrors the router's ErrorHandler +
  cache invalidation): container may have just restarted with a new host port.
- `/healthz`, `/readyz` on a **separate localhost port** (or the API port) to avoid shadowing
  proxied paths — the real router serves probes on its own listener; verify exact layout at
  implementation and copy it.
- Access log per request: method, path, sandbox, namespace, upstream, status, duration, reqID —
  this is the primary debugging surface for SDK users.

## Wiring

- Router shares the process: reads the store + driver registry directly (no polling). A small
  `resolver` interface keeps it testable:
  `Resolve(ns, id string, port int) (target string, state ResolveState)`.
- `lasd serve` prints copy-paste snippets on boot:
  Go: `sandbox.Options{APIURL: "http://127.0.0.1:8880", WarmPoolName: "default"}`;
  Python: `SandboxDirectConnectionConfig(api_url="http://127.0.0.1:8880")`.

## Tests

- **Unit**: header parsing/validation matrix (missing ID → 400; bad namespace → 400; port
  fallback 8888); resolver state → status-code mapping (502/503 table above); header stripping;
  path fidelity for percent-encoded segments (`/download/a%2Fb%20c` must reach upstream
  unmodified — regression-prone!); timeout header honored (upstream hangs → 504-class result
  within budget).
- **Proxy integration (httptest upstream, no Docker)**: byte-identical body pass-through both
  directions incl. 100MB stream; multipart upload; WebSocket echo through the proxy; upstream
  500 passes through (SDK retries are client-side).
- **SDK-level test (with Docker, builds on phase 3)**: real Go SDK `New(...APIURL...)` →
  `Open→Run→Write→Read→List→Exists→Close` full pass; assert the runtime effects (file exists in
  volume, command ran). Python equivalent in the optional pytest lane.
- **Fault injection**: kill the container mid-`Run` → SDK gets retryable error, sandbox flips
  NotReady; recreate → next call succeeds after `Open()` reconnect path.

## Exit criteria

Upstream Go SDK example (`clients/go/examples/basic`) modified only by
`APIURL`+`WarmPoolName` runs green against `lasd` on macOS and Linux.
