# Phase 3 — Reconcilers: claim ⇄ sandbox ⇄ Docker, warm pools, lifecycle

**Outcome**: creating a SandboxClaim (via either SDK or kubectl) produces a Ready sandbox backed
by a Docker container; deletion/suspension/expiry behave like the upstream controllers
(`reference/agent-sandbox/controllers`, `…/extensions/controllers`). This phase makes the SDK
state machines (`sandbox.go#Open`, `k8s_helper.py`) actually progress.

## Engine

A small controller runtime over the phase-1 store (no controller-runtime dependency): each
reconciler consumes store watch events + Docker driver events through a workqueue with
rate-limited retries; reconciles are idempotent, level-triggered, keyed by `ns/name`. Status
writes go through the store's status path so watchers see every transition.

Reconcilers: `sandboxtemplate` (validation only), `sandboxwarmpool`, `sandboxclaim`, `sandbox`
(the only one that touches Docker), plus a `reaper` ticker for time-based lifecycle and an
`adoption` scan at boot (re-link store ⇄ surviving containers, GC orphans).

## Sandbox reconciler (Sandbox CR → container)

Semantics below are **verified against `controllers/sandbox_controller.go`** (line refs are to
that file at the pinned SHA); encode each as a table-driven test.

- **Identity**: cold-create "pod" name == sandbox name (`:960`); annotation
  `agents.x-k8s.io/pod-name` stamped back on the Sandbox (`ensurePodNameAnnotation`, `:801`) and
  it **wins over the default on lookup** (`resolvePodName`, `:89`) — warm adoption relies on
  this. `nameHash = fnv1a-32(name)` printed `%08x` (`:466`); label
  `agents.x-k8s.io/sandbox-name-hash=<hash>` goes on pod-equivalents/volumes and
  `status.selector` is exactly that `key=value` string (python `get_sandbox_name_hash` parses
  it). Note: upstream does **not** clear `status.selector` when the pod disappears — only expiry
  wipes it.
- **Pod-spec handling**: the core controller applies **no defaulting** — the template spec is
  passed through verbatim except PVC volume injection. (`AutomountServiceAccountToken=false` +
  DNS defaults happen only on the extensions/template provisioning path,
  `extensions/controllers/utils.go#ApplySandboxSecureDefaults` — our claim reconciler applies
  them there, not here.) CRD-schema defaults we must emulate on write: `operatingMode: Running`,
  `shutdownPolicy: Retain`. Pod labels = user template labels minus reserved prefixes
  (`agents.x-k8s.io/`, `extensions.agents.x-k8s.io/`) + name-hash label; tracking annotations
  `agents.x-k8s.io/propagated-labels|propagated-annotations` (CSV of propagated keys).
- **VolumeClaimTemplates** (`:947-955`, `:1153`): PVC name = `<templateName>-<sandboxName>`;
  injected pod volume has `name: <templateName>` (template name, not PVC name!) with
  `persistentVolumeClaim.claimName: <pvcName>`, replacing any same-named template volume
  (StatefulSet semantics). Docker volume name: `las-<ns>-<templateName>-<sandboxName>` (i.e.
  `las-<ns>-<pvcName>`, matching the container naming scheme).
  **PVCs are create-only and never deleted by the controller** — not on suspend, not on expiry;
  they go away only when the Sandbox object is deleted (K8s GC via ownerRef → our explicit
  cascade).
- **Service** (`reconcileService`, `:552`): gated by `spec.service *bool` — `nil`: create
  nothing, clear status fields; `true`: headless Service named `<sandbox>` (selector = name-hash
  label), `status.service = <name>`, `status.serviceFQDN = <name>.<ns>.svc.cluster.local`;
  `false`: delete-if-owned, clear status. Locally the Service is a store-only record (nothing to
  materialize), but Ready gating must respect it.
- **Status**: `podIPs` ← container bridge IP (SDKs prefer IPv4 — `ip.go#selectPodIP`); `nodeName`
  ← `"local-docker"` (upstream: pod's node); both cleared when no container.
- **Conditions** (`computeConditions`, `:242-273`; constants `sandbox_types.go:28-55`) — exact
  presence rules matter:
  - `Ready` (always present): True/`DependenciesReady` **only when** container Running + runtime
    probe passing + podIPs set **and** the Service requirement is satisfied (required iff
    `spec.service==true` or a service record exists). False reasons:
    `ReconcilerError` (reconcile error, message `Error seen: …`), `SandboxSuspended` (message
    "Sandbox is suspending" while container still exists, else "Sandbox is suspended"),
    `PodSucceeded`/`PodFailed` (terminal container), `SandboxExpired` (expiry),
    `DependenciesNotReady` (default; descriptive message e.g. "Pod does not exist").
  - `Suspended`: **present only when** `operatingMode==Suspended` — True/`PodTerminated` once the
    container is gone, False/`PodNotTerminated` while it lingers; **removed** entirely when
    Running.
  - `Finished`: **present only when** the container reached a terminal state —
    True/`PodSucceeded` (exit 0) or `PodFailed` (nonzero, from driver death events + inspect);
    **removed** while no container or non-terminal.
- **OperatingMode**: `Suspended` → delete the container, **keep volumes and Service**, clear the
  `pod-name` annotation (`:794`); `Running` again → recreate container from spec (new IP/host
  port — SDKs re-resolve via `get_pod_ip`/reconnect). PVC/Service reconciliation keeps running in
  both modes.
- **Lifecycle expiry** (`checkSandboxExpiry` `:1306`, `handleSandboxExpiry` `:1225`) — two-phase,
  driven by requeue at `max(shutdownTime-now, 2s)`:
  1. First pass after `now >= shutdownTime`: only set Ready=False/`SandboxExpired` ("Sandbox has
     expired") and requeue immediately.
  2. Second pass: delete container + Service record (**not** volumes). Then `shutdownPolicy`:
     `Delete` → delete the Sandbox CR (cascade removes volumes); `Retain` (default) → keep the
     CR but reset status to **conditions only** (service/serviceFQDN/podIPs/nodeName/selector
     wiped; Finished condition preserved).
- **Deletion**: upstream uses no finalizers — GC via ownerReferences does everything. Our
  single-process equivalent: claim/sandbox delete paths explicitly cascade (container → volumes →
  CR removal), and the boot adoption scan re-drives any crash-interrupted teardown
  (`las.managed` containers with no CR ⇒ GC).

## SandboxClaim reconciler (claim → sandbox binding)

Semantics **verified against `extensions/controllers/sandboxclaim_controller.go`** (line refs to
that file) + `sandboxclaim_types.go`:

- **Template resolution is indirect**: claim → `spec.warmPoolRef.name` → WarmPool →
  `spec.sandboxTemplateRef.name` → SandboxTemplate (`getTemplate:1711`). Missing pool → claim
  `Ready=False` reason **`WarmPoolNotFound`** (message `SandboxWarmPool "x" not found`); missing
  template → **`TemplateNotFound`** — exact strings, the Python SDK raises typed exceptions off
  them. Both cases requeue every ~1 min and the claim just sits NotReady until the dependency
  appears (`:339-352`).
- **Get-or-create order** (`getOrCreateSandbox:1520`): existing binding via
  `status.sandbox.name` → adoption annotation `agents.x-k8s.io/sandbox-name` → Sandbox literally
  named `claim.Name` (foreign-owned ⇒ hard error) → **cold-start bypass** when `spec.env` or
  `spec.volumeClaimTemplates` non-empty (never consults the pool) → adopt a warm candidate →
  cold-create.
- **Naming**: cold-created Sandbox is named **exactly `claim.Name`**; an adopted one keeps the
  pool-generated name `<poolName>-xxxxx` — this is why `resolveSandboxName` exists in the SDKs.
  Binding recorded in `status.sandbox.name` (+ `status.sandbox.podIPs` mirror) always, and in the
  claim annotation `agents.x-k8s.io/sandbox-name` **only on adoption**.
- **Env injection** (`injectEnvs:1236`): gated by the template's `envVarsInjectionPolicy`
  (default **Disallowed** ⇒ claims with env are *rejected*, reason `EnvVarsInjectionRejected`;
  `Allowed` appends—conflict with existing var errors; `Overrides` replaces). Empty
  `containerName` targets the first container; unknown container ⇒ error. Same policy pattern
  for `volumeClaimTemplatesPolicy` (`VolumeClaimTemplatesError`). Our bootstrap default template
  sets both policies to `Allowed` so SDK conveniences work out of the box.
- **Secure defaults on provisioned pod specs** (`extensions/controllers/utils.go:24`):
  `automountServiceAccountToken=false` if unset (no-op in Docker, keep for spec fidelity);
  secure-by-default mode (networkPolicyManagement `Managed` + no custom policy) forces
  `dnsPolicy: None` with nameservers `8.8.8.8`/`1.1.1.1` → map to the container's DNS config in
  the driver.
- **additionalPodMetadata**: validated (label keys must be under an allowlisted domain, default
  `sandbox.users.io`; annotation keys blocklisted for `kubernetes.io`/`k8s.io`/
  `agents.x-k8s.io`) → `InvalidMetadata` on violation; merged no-override into pod-template
  metadata.
- **Claim conditions** — only `Ready` and `Finished` are set:
  - Ready=True **forwards the Sandbox's Ready condition verbatim** (so reason
    `DependenciesReady`, message `Pod is Ready`); not-ready mirrors similarly
    (`DependenciesNotReady`, fallback `SandboxNotReady`).
  - Error reasons (exact): `TemplateNotFound`, `WarmPoolNotFound`, `AdoptionPending`,
    `InvalidMetadata`, `EnvVarsInjectionRejected`, `VolumeClaimTemplatesError`, `ClaimExpired`,
    `SandboxMissing`, `SandboxExpired`, `ReconcilerError` (`Error seen: <err>`).
  - `Finished` copied verbatim from the Sandbox when present, else removed — it drives the TTL
    clock.
- **Ownership/cascade**: the claim controller-owns its Sandbox (adoption nulls the pool's
  ownerRef and installs the claim's; identity label `agents.x-k8s.io/claim-uid` applied). Claim
  deletion ⇒ Sandbox GC ⇒ container/volumes — in our store this is the claim delete path's
  explicit cascade (SDK `Close()` only deletes the claim).
- **Lifecycle** (`internal/lifecycle/expiry.go`, `checkExpiration:410`): expiry =
  `min(shutdownTime, FinishedAt + ttlSecondsAfterFinished)`; **shutdownTime is not propagated to
  the Sandbox** — the claim reaper enforces it. On expiry by `shutdownPolicy` (default Retain):
  `Retain` → delete the owned Sandbox (ownership verified first), claim stays with
  Ready=False/`ClaimExpired`; `Delete` → delete claim (cascade); `DeleteForeground` → foreground
  delete — the claim object must remain visible (deletionTimestamp set) until the Sandbox and
  container are actually gone, *then* disappear. Python's `shutdown_after_seconds` lands here.

## WarmPool & Template reconcilers

Verified against `sandboxwarmpool_controller.go` / `sandboxtemplate_types.go`:

- **SandboxTemplate**: `spec` = core `SandboxBlueprint` (podTemplate, volumeClaimTemplates,
  service) + `networkPolicy`/`networkPolicyManagement` (Managed default; local: no-op beyond the
  DNS mapping above) + the two injection policies (defaults `Disallowed`). No status. Validate on
  write; single-container restriction surfaced as a warning event in v1.
- **SandboxWarmPool**: `spec.replicas` (**default 1**, min 0), `spec.sandboxTemplateRef.name`
  (exact JSON key), `spec.updateStrategy.type: Recreate|OnReplenish` (default OnReplenish);
  `status.{replicas,readyReplicas,selector}`. Members are full Sandboxes with `generateName:
  <pool>-`, controller-owned by the pool, labeled `agents.x-k8s.io/warm-pool-sandbox =
  NameHash(poolName)` (**fnv hash, not the raw name**) + `sandbox-template-ref-hash` +
  `launch-type: warm` + `created-by: controller`, annotation `sandbox-template-ref`. Maintain
  replicas (slow-start batching upstream; sequential is fine locally), recycle stuck-unready
  members (>5 min) and stale ones per updateStrategy (template-hash drift). Adoption rewrite on
  bind (`completeAdoption:997`): drop pool labels, keep `launch-type: warm`, set claim ownerRef +
  `claim-uid` label, set `agents.x-k8s.io/pod-name` annotation, re-derive pod metadata from
  template + claim's additionalPodMetadata. Upstream needs a two-write handshake and may
  transiently show `AdoptionPending`; we adopt in one pass (SDKs tolerate both).
- **Bootstrap** (from `config.yaml`, phase 6): default template (+ `Allowed` injection policies)
  and pool created at startup so `CreateSandbox(ctx, "default", …)` works instantly. Our
  `replicas: 0` bootstrap default is a deliberate deviation from the CRD default of 1 (avoid a
  container idling on laptops); warm replicas are the "instant Open()" knob.

## Documented deviations (encode in docs/compatibility.md + tests)

1. Adoption happens in one reconcile pass — SDK-visible `AdoptionPending` window may be absent.
2. NetworkPolicy specs are accepted but only the secure-default DNS part has an effect.
3. Warm pool `scale` subresource (HPA) — optional; implement GET/PUT scale only if kubectl users
   ask.
4. `status.selector` quirks copied from upstream (not cleared on pod loss; wiped on expiry).
5. Claims/Sandboxes created by kubectl directly (no SDK) follow the same paths — supported.

## Tests

- **Unit (fake driver)**, table-driven per reconciler:
  - claim happy-path: create → sandbox created → driver "running+probed" → sandbox Ready →
    claim Ready + `status.sandbox.name`; watch-event ordering matches what
    `resolveSandboxName`/`waitForSandboxReady` need (name before Ready).
  - `WarmPoolNotFound`/`TemplateNotFound` reasons byte-exact (incl. the 1-min requeue "sits
    NotReady" behavior the Python SDK turns into typed exceptions).
  - env-injection: policy `Disallowed` (CRD default) → `EnvVarsInjectionRejected`; `Allowed`
    appends/conflict-errors; `Overrides` replaces; forces cold start even with warm sandboxes
    idle; VCT claims produce volumes (name `<tmpl>-<sandbox>`) and also force cold start.
  - claim Ready mirrors the sandbox's Ready condition verbatim (reason `DependenciesReady`,
    message `Pod is Ready`).
  - suspend → container removed, volumes kept, Suspended=True/`PodTerminated`; resume →
    recreated, new "IP", Ready again.
  - shutdownTime expiry × {Retain, Delete, DeleteForeground} × {claim, sandbox};
    ttlSecondsAfterFinished GC.
  - container dies (driver event, exit 0 / exit 1) → Finished=True `PodSucceeded|PodFailed`,
    Ready=False; claim mirrors.
  - reconciler crash-safety: replay same event twice → no duplicate containers (idempotence).
- **SDK state-machine contract test (fake driver, no Docker)**: run the real Go SDK
  `Client.CreateSandbox` against facade+reconcilers with the driver faked to "instant ready" —
  proves the full watch choreography without Docker; also `GetSandbox` re-attach and
  `DeleteSandbox`.
- **Integration (Docker)**: cold-start claim → real container Ready; two claims → two isolated
  containers; volume data survives suspend/resume; expiry reaps the container; boot-adoption:
  restart `lasd` mid-life, container re-adopted, claim still Ready, orphaned container (label but
  no CR) GC'd by `lasd gc`.

## Exit criteria

Go SDK `CreateSandbox→IsReady` and Python `create_sandbox` reach Ready against real Docker;
kubectl shows claim/sandbox with sensible conditions; suspend/resume/expiry behave per upstream;
fidelity checklist items each have a passing test or a documented deviation in
`docs/compatibility.md`.
