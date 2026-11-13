# Changelog

All notable changes to Brie are recorded here. Stable releases use semantic
versioning. Dated nightly prereleases are development builds, not stable releases.

## [Unreleased]

### Added

- A bounded Kubernetes Events recorder (`internal/events`) for admission
  refusals, image pull failures, barrier entry and exit, blocked cleanup, and
  probe failures, with local deduplication, an agent-wide rate limit, and
  client-go correlation. It needs no RBAC beyond the Node authorizer.
- An agent Prometheus registry (`internal/metrics`) for admission decisions,
  reservations against the budget, reconciliation pass age, barrier cause,
  pending stage age, CRI latency and errors by method, image pulls, journal
  bounds, Lease renewal failures, probes, log rotation, and Events, with
  bounded label values and no tenant data in labels. CRI clients accept a call
  observer. Only CRI metrics can be fed without further executor hooks.
- A kubelet-compatible HTTPS serving package (`internal/serving`) with TLS
  1.2+, client-certificate and TokenReview authentication, SubjectAccessReview
  authorization using kubelet's node subresource mapping, `/healthz`,
  `/metrics`, bounded `/containerLogs`, and `/exec`, `/attach`, and
  `/portForward` proxied to CRI streaming URLs. Review errors refuse requests,
  and a sandbox in a lifecycle-extension state other than running is refused
  without being resumed. CRI `Exec`, `Attach`, `PortForward`, `ExecSync`, and
  `ReopenContainerLog` client calls support it. Not yet wired into the agent;
  tested only against fakes. See [docs/observability.md](docs/observability.md).
- A structured JSON agent logger (`internal/agentlog`) with Pod UID, operation
  ID, and journal stage fields, key-based redaction of environment values,
  commands, arguments, request bodies, and credentials, and rate limiting of
  repeated warnings and errors. It is not yet the default agent log format.
- With `--enable-workloads`, an execution whose Pod is absent from a complete,
  quorum-consistent node-scoped Pod list is now stopped with zero grace and
  removed, and its reservation released. The list resource version is journaled
  first and authorizes cleanup across restarts. Failed or partial lists, Pod
  read errors, foreign or ambiguous runtime objects, and unresolved sandbox
  creation still preserve the execution and block admission.
- Explicit `--enable-workloads` mode for multiple restricted Pods through the
  upstream scheduler and containerd CRI, with configured capacity, RuntimeClass
  overhead, DNS, status, deletion, and durable restart reconstruction. Defaults
  remain registration-only; a limited C4A scheduler-to-Firecracker smoke run is
  documented with a terminal-deletion cleanup limitation, not production or
  conformance qualification.
- A fail-closed initial one-container Linux Pod profile validator and bounded
  read-only CRI sandbox/container list and status calls. These do not enable
  Pod execution, cleanup, scheduling, or readiness.
- Optional `--runtime-socket` startup version and health observation over CRI.
  This does not enable workloads or change Node readiness.
- A read-only local CRI v1 client for runtime version and health, with bounded
  calls and fake-socket tests; it is not yet wired into the node agent.
- Optional owner-managed vsock configuration for guest control.
- An experimental standalone runtime owner with a private Unix-socket API.
- A Linux Firecracker launcher for disposable experiments, with startup checks
  and process cleanup on failure.
- The runtime owner now journals lifecycle state and prevents concurrent owners
  for the same execution on one host.
- The owner can configure a Firecracker machine and request instance start.
  This does not launch a VM.
- A disposable kind registration test and CI job covering Node and Lease
  lifecycle, heartbeat loss, restart, and node-scoped permission denials.
- A Go module and version-only `brie-node` command.
- An opt-in `brie-node --run --node-name=NAME` scaffold with validated host
  identity and clean SIGINT/SIGTERM shutdown. It does not register a node,
  connect to Kubernetes, or invoke Firecracker.
- Optional explicit `--kubeconfig=/absolute/path` host registration, using
  Kubernetes v1.37 clients, NotReady status, zero advertised resources, and
  Node Lease heartbeats. Registered Nodes remain cordoned and tainted; no
  workloads are executed. New HTTP/2 dependencies include security fixes.
- A runtime-internal Firecracker Unix-socket API client for instance metadata
  and minimal pre-boot CPU/memory configuration, with bounded requests and fake
  socket regression/fuzz tests. No real VM execution is implemented.
- Fake-socket-tested Firecracker boot-source, virtio-blk root-drive, and
  instance-start API methods, with artifact-path validation and no automatic
  mutation retries. No VM launcher or guest images are included.
- CLI subprocess e2e tests and a pinned kind/Sonobuoy Kubernetes baseline
  workflow with PR smoke tests, weekly/manual full conformance, and retained
  diagnostics. These do not qualify Brie for Node Conformance.
- Race-enabled tests, CLI fuzzing, golangci-lint, and govulncheck.
- GitHub Actions for quality checks, CodeQL, and OpenSSF Scorecard.
- Linux amd64 and arm64 release archives, SHA-256 checksums, and signed build
  provenance on GitHub Releases.
- Dated nightly prereleases.
- Apache-2.0 licensing and contributor guidance.

### Changed

- Execution readiness uses status-only updates and gated allocatable resources
  instead of modifying startup taints forbidden by NodeRestriction.
- Registration heartbeats preserve externally managed Node conditions and status.
- Retry bounded Node/Lease resource-version conflicts during registration
  heartbeats, revalidating Node identity and scheduling guards on each attempt.
- Reduced the README to an abstract ten-word sentence.

[Unreleased]: https://github.com/ameukam/brie/commits/main
