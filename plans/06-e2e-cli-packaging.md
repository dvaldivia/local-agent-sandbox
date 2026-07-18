# Phase 6 — CLI polish, conformance/E2E suites, CI, packaging, docs

**Outcome**: `lasd` is a tool someone else can install and trust: one-command startup, real-SDK
conformance gates in CI on Linux + macOS, versioned releases.

## CLI (cobra)

```
lasd serve        # start facade + router + reconcilers (foreground; --daemon later if wanted)
                  #   --api-port 6644 --router-port 8880 --bind 127.0.0.1
                  #   --data-dir ~/.local/share/local-agent-sandbox
                  #   --config ~/.config/local-agent-sandbox/config.yaml
lasd kubeconfig   # print/write kubeconfig (--print-export for eval)
lasd status       # server health, docker endpoint, counts (pools/claims/sandboxes/containers)
lasd ls           # table of sandboxes: NS NAME CLAIM READY CONTAINER HOSTPORT AGE
lasd gc           # remove orphaned containers/volumes (las.managed label, no matching CR)
lasd purge        # stop server-owned resources: all managed containers+volumes (+--state)
lasd doctor       # docker socket discovery, kubectl presence, port availability
lasd version
```

Config file (`config.yaml`): ports, defaults, and **bootstrap objects** — inline
SandboxTemplates/WarmPools created at startup (zero-config default pool included unless
`bootstrap.default: false`):

```yaml
bootstrap:
  templates:
    - name: default
      image: lasd/sandbox-runtime:latest   # sugar → expanded into full SandboxTemplate
  warmPools:
    - name: default
      templateRef: default
      replicas: 0        # 0 = cold-start on claim; >0 = real pre-warmed containers
```

Quality-of-life: `lasd serve` prints the three-line getting-started block (export KUBECONFIG, Go
snippet, Python snippet). SIGINT/SIGTERM → graceful: stop reconcilers, leave containers running
(they're re-adopted on next boot) unless `--ephemeral` (then full teardown) — document both.

## Conformance & E2E suites (`tests/e2e`, `//go:build e2e`)

1. **Go SDK lifecycle matrix** (real `sigs.k8s.io/agent-sandbox/clients/go/sandbox` dependency):
   - modes: Direct(APIURL) × Tunnel(default) ;
   - flows: create→run→files→close; GetSandbox re-attach; Disconnect/Open reconnect;
     DeleteSandbox by claim; ListAllSandboxes; two concurrent sandboxes isolated (file written in
     A absent in B); claim with `lifecycle.shutdownTime=+5s` reaps container; suspend→resume via
     kubectl patch mid-session (SDK reconnects, per its `reconnect()` path).
   - failure UX: nonexistent warm pool → SDK error contains claim-failure; sandbox deleted behind
     the SDK's back → `ErrSandboxDeleted` path.
2. **Upstream integration suite as conformance**: run
   `go test sigs.k8s.io/agent-sandbox/clients/go/sandbox -run TestIntegration` with
   `INTEGRATION_TEST=1 KUBECONFIG=<lasd> -api-url=http://127.0.0.1:8880
   -warmpool-name=default` (flags per `integration_test.go`). Upstream's own tests passing
   against us is the strongest "SDK can't tell" signal — wire as a nightly/manual CI job so
   upstream churn doesn't block PRs; failures file drift issues.
3. **Python SDK suite** (`tests/python`, pytest + uv, pinned `k8s_agent_sandbox` from the
   reference checkout or PyPI when published): direct-mode create/execute/upload/download/delete;
   tunnel-mode variant when kubectl present.
4. **Soak/robustness**: 20 sandboxes churn loop (create/exec/delete ×3) asserting no leaked
   containers/volumes (`lasd gc --dry-run` reports zero); `lasd` kill -9 mid-flight → restart →
   re-adoption invariant (containers re-associated, claims still Ready).

## CI (GitHub Actions)

- `lint` — golangci-lint, gofmt, go vet (ubuntu).
- `unit` — matrix `{ubuntu-latest, macos-latest}`; pure unit + contract tests (no Docker).
- `integration-linux` — ubuntu (Docker preinstalled): driver + reconciler + router + portforward
  integration, Go SDK E2E both modes, python lane.
- `integration-macos` — macos runner + Colima (`colima start --vm-type vz`): same suite,
  `continue-on-error: true` initially; promote to required once stable. (macOS runners have no
  Docker by default — Colima is the standard workaround; document local `make e2e` as the
  authoritative macOS check.)
- `conformance-upstream` — nightly: upstream SDK integration tests vs `lasd@main` + pinned SDK,
  plus a second leg against SDK@main to catch upstream drift early.

## Packaging & docs

- goreleaser: darwin/linux × amd64/arm64 binaries, Homebrew tap formula, `install.sh`. Version
  embedded via ldflags (`lasd version`, `/version` gitVersion suffix `-lasd`).
- Runtime image: build+push multi-arch `ghcr.io/dvaldivia/lasd-sandbox-runtime` on release;
  `EnsureImage` prefers pull, falls back to local embedded build.
- README: what/why (OpenSandbox-style local dev for agent-sandbox), quickstart (brew install →
  `lasd serve` → Go/Python snippet), architecture diagram, compatibility matrix (SDK feature ×
  supported), limitations (no gateway mode, single-container pods, no configMap volumes, python
  in-cluster strategy N/A), troubleshooting (doctor, access logs).
- `docs/compatibility.md`: the wire-contract tables from `00-overview.md` kept current — this is
  the contract we test against and the first thing to update when upstream moves.

## Exit criteria

Fresh macOS and Linux machines: `brew install`/`curl|sh` → `lasd serve` → paste README Go snippet
→ sandbox runs, files round-trip, `lasd purge` leaves docker clean. CI green including Linux E2E;
nightly conformance job running.
