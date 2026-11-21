# Changelog

All notable changes to Brie are recorded here. Stable releases use semantic
versioning. Dated nightly prereleases are development builds, not stable releases.

## [Unreleased]

### Added

- Opt-in `--enable-probes` (requires `--enable-workloads`) accepts the
  `ProfileProbes` increment in execution mode. The agent probes a container it
  observes running, publishes `Ready` and `ContainersReady` for a Pod with a
  readiness probe only after that probe succeeds, and after a liveness failure
  stops the container through CRI with the Pod's grace period and publishes it
  `Failed` with reason `LivenessProbeFailed`, never restarting it. Probes stop
  when the Pod is deleted, becomes terminal, or exits. Tested only against a
  fake CRI runtime and a fake exec prober.
- Executor hooks now feed the opt-in metrics and Events: admission decisions by
  reason with a Pod Event per refusal; reservations, journal records and bytes,
  and pending-stage age after each journal save; `ImagePullFailed` and
  `ImagePullBackOff` Events carrying the gRPC code, not the image reference;
  an Event when the `ExecutionBlocked` condition is published; `CleanupBlocked`
  Events when cleanup waits for deletion or runtime evidence; probe results;
  and a `LivenessProbeFailed` Event. Journal records gain optional
  `PendingStage` and `PendingSince` fields; a pending record from an older
  journal reports its age as `NaN` instead of a guessed time.

- A versioned cooperative guest supervisor protocol (`brie.guestsupervisor/v1`)
  with prepare, resume, and abort messages, length-limited canonical JSON, and
  a `brie-guestctl` relay. The supervisor listens only on a Unix socket inside
  the workload container; the host reaches it through CRI ExecSync, so Kata
  stays the only guest-control channel. Nothing in the node agent calls it, and
  it is tested only against fakes and local sockets, not on Kata. After an
  invalid safe point whose cleanup abort failed, the supervisor refuses other
  requests until an abort of that operation succeeds. A resume retry is refused
  once a later epoch has been seen. The supervisor holds an exclusive lock on
  `<socket>.lock` while it listens and replaces a leftover socket only when
  connecting to it is refused.
- A credential-free sample cooperative workload, `brie-counter`, with an
  unbuilt `scratch` image Dockerfile in `examples/counter`. It fsyncs every
  acknowledged increment to a state file, stops admitting work at prepare,
  reports its count as the safe point and acknowledged-request watermark, and
  on resume verifies the state file, rebinds identity, and refreshes entropy
  before reopening. It does not hibernate anything by itself.
- A versioned checkpoint manifest schema (`brie.checkpoint/v1alpha1`) with
  strict canonical JSON decoding and validation of identity, epoch, operation,
  generation, VM-state, memory, and disk artifacts by object generation, size,
  SHA-256, and CRC32C, the restore compatibility tuple (including the guest
  agent version and security profile), device layout, resume
  protocol, safe point, watermark, creation time, and encryption, access, and
  retention references. No checkpoint is produced yet.
- A standard-library Cloud Storage JSON API checkpoint store with immutable
  `ifGenerationMatch=0` uploads, CRC32C/MD5, size, and generation read-back
  verification, artifact verification that downloads each object generation
  and recomputes its size, CRC32C, and SHA-256, manifest publication and
  reads only after every artifact verifies,
  strict manifest reads, errors that name only the HTTP status and a known
  storage reason, a redacting GCE metadata-server token source, refused
  redirects, and bounded retries with per-attempt deadlines. It is tested only
  against an in-process fake server; no real bucket has been used.
- A conditional checkpoint-reference commit helper. It updates a Sandbox's
  committed-checkpoint reference only with UID and resource-version
  preconditions, rejects stale epochs, withdrawn suspensions, deletion, and
  older generations, and is idempotent after a lost reply only while the
  Sandbox is not being deleted, is still at the reference's placement epoch,
  and still requests suspension by its operation. It is an interface
  tested against a fake; the Sandbox controller implementation is separate.
- Opt-in `brie-node` flags, all off by default: `--log-format=json`;
  `--enable-serving` with `--serving-address`, `--tls-cert-file`,
  `--tls-private-key-file`, and `--client-ca-file`; `--enable-events`; and
  `--container-log-max-size` with `--container-log-max-files`. Serving, Events,
  and rotation require `--enable-workloads`. The executor now exposes
  observation hooks for CRI calls, Lease renewals, reconciliation passes,
  barrier inputs, and its verified execution inventory. Serving and log
  rotation act only on runtime objects in that inventory, never on a sandbox
  merely because it carries an owner label, and refuse a Pod name with several
  verified executions; see [docs/observability.md](docs/observability.md). Tested
  only against fakes.
- Size-based container log rotation (`internal/logrotation`) through CRI
  `ReopenContainerLog`, with bounded max size and max files, restoring the log
  name after a reopen error without ever replacing a current log the runtime
  may already have created, and removal of Pod log directories
  only for journal-finished Pods with no remaining sandbox. Symlinks and paths
  outside the Pod log directory are refused.
- A probe manager (`internal/probe`) for readiness and liveness probes: exec
  through CRI `ExecSync`, and httpGet and tcpSocket against the Pod IP, keyed by
  Pod UID and suspendable for a recorded extension state. Suspension cancels
  queued and in-flight probes and discards their results, and concurrent
  `Add` calls for one Pod never leave untracked probe workers. `ValidatePodProfile`
  accepts bounded probes and declared TCP ports only with the new
  `ProfileProbes` feature, which execution mode passes only with
  `--enable-probes`.
- A bounded Kubernetes Events recorder (`internal/events`) for admission
  refusals, image pull failures, barrier entry and exit, blocked cleanup, and
  probe failures, with local deduplication, an agent-wide rate limit, and
  client-go correlation. It needs no RBAC beyond the Node authorizer.
- An agent Prometheus registry (`internal/metrics`) for admission decisions,
  reservations against the budget, reconciliation pass age, barrier cause,
  pending stage age, CRI latency and errors by method, image pulls, journal
  bounds, Lease renewal failures, probes, log rotation, and Events, with
  bounded label values and no tenant data in labels. CRI clients accept a call
  observer. A gauge that was never set is not exported, so unreported state
  is never exported as zero.
- A kubelet-compatible HTTPS serving package (`internal/serving`) with TLS
  1.2+, client-certificate and TokenReview authentication, SubjectAccessReview
  authorization using kubelet's node subresource mapping, `/healthz`,
  `/metrics`, bounded `/containerLogs`, and `/exec`, `/attach`, and
  `/portForward` proxied to CRI streaming URLs. `/exec` and `/attach` take
  kubelet's `input`, `output`, `error`, `tty`, and `command` parameters.
  A `tailLines` request whose start lies beyond the 64 MiB scan bound is
  refused with HTTP 422 before any output, not answered with an empty 200.
  Failed requests are logged with an operation name and a bounded error code,
  never the text of runtime, streaming proxy, or review errors. Review errors refuse requests,
  and a sandbox in a lifecycle-extension state other than running is refused
  without being resumed. CRI `Exec`, `Attach`, `PortForward`, `ExecSync`, and
  `ReopenContainerLog` client calls support it. Not yet wired into the agent;
  tested only against fakes. See [docs/observability.md](docs/observability.md).
- A structured JSON agent logger (`internal/agentlog`) with Pod UID, operation
  ID, and journal stage fields, key-based redaction of environment values,
  commands, arguments, request bodies, and credentials, and rate limiting of
  repeated warnings and errors. It is not yet the default agent log format.
- `tests/hw/node-conformance.sh` runs the pinned upstream 1.37
  `[NodeConformance]` tests against a lab cluster without skip filters and
  writes every failure to `failures.txt`. It refuses to run if another node
  could accept test Pods. Only its input checks and report summary are tested.
- `tests/hw/m1-harness.sh`, a reproducible M1 sequence for one Brie host:
  Pod startup, Pod networking, agent restart comparing VM PIDs, container IDs,
  and guest boot IDs, and cleanup, with credential-free evidence and SHA-256
  sums. Steps needing features Brie lacks are reported as
  `SKIPPED-UNSUPPORTED`, never PASS. Tested only against mock `gcloud` and
  `kubectl`.
- `tests/hw` install scripts for the provisional D4 tuple and a kubeadm 1.37
  control plane with the Node authorizer, NodeRestriction, bridge CNI, and
  kube-proxy in iptables mode; `kubeadm init` withholds the bootstrap token
  from its output. Downloads are pinned by URL and SHA-256 from an
  operator-filled checksums file that refuses placeholders, and the host
  install verifies Kata, Firecracker, and jailer binaries against a node-local
  digest manifest (D9) that must list each required binary exactly once. Only the input validation is tested; neither script has
  run on a host.
- `tests/hw/fence.sh`, a provisional lab fence (D5) that stops only a host
  labelled for the run, waits for the same incarnation to report TERMINATED,
  detaches its data disk, and prints JSON evidence; it exits nonzero unless
  every step is observed. Tested only against a mock `gcloud`.
- `tests/hw/provision.sh` for a disposable C4A lab of at most four bare-metal
  and two standard hosts, with a dedicated VPC, NAT, and 7-day evidence bucket.
  It refuses plans that an operator-supplied price table projects over the
  spend ceiling, falls back to a second zone on stockout, and tears down after
  45 minutes of stockout or any failure. Tested only against a mock `gcloud`.
- A label-guarded hardware lab teardown in `tests/hw`: it deletes only
  resources carrying the run's label or description marker, refuses others,
  and verifies that nothing remains. It requires `APPROVE_COSTS=yes`, an
  explicit project, zones, run id, and spend ceiling, and never reads gcloud
  configuration. Tested only against a mock `gcloud`; it has not run against
  Google Cloud.
- With `--enable-workloads`, assigned Pods are read through a node-scoped watch
  with bookmarks instead of a two-second API list. An expired resource version
  or a watch error triggers a full relist with bounded backoff, and a full
  relist runs every five minutes. A watch `DELETED` event for an execution the
  agent created is journaled as deletion evidence before cleanup. Passes still
  read the complete CRI inventory at least every two seconds.
- With `--enable-workloads`, finished execution journal records are compacted
  once `RemovePodSandbox` succeeded, the Pod is gone from the complete
  assigned-Pod view, and a complete CRI inventory shows none of the record's
  objects. The journal schema moves to version 2; version 1 journals are
  migrated on start, and older agents refuse version 2 journals.
- With `--enable-workloads`, a sandbox or container create that the runtime
  definitely rejected (`InvalidArgument`, `NotFound`, `FailedPrecondition`, or
  `OutOfRange`), and that a later complete CRI inventory shows created
  nothing, now fails its Pod with `SandboxCreateRejected` or
  `ContainerCreateRejected` and releases the reservation instead of blocking
  admission. Lost or interrupted replies, and `PermissionDenied`, which
  grpc-go also reports for transport failures, still block admission.
- With `--enable-workloads`, the new `--image-gc-threshold` flag enables image
  collection through CRI (disabled by default). While the reported image
  filesystem usage exceeds the threshold, images that no container, image pull,
  or retained execution references, that the runtime does not pin, and that
  were unreferenced for five minutes are removed, largest first. A failed or
  partial list, a running pull, a container whose image is not identified, or
  any existing sandbox, whose image CRI does not name, removes nothing.
- With `--enable-workloads`, image pulls run on two bounded pull workers,
  shared by every Pod that needs the same image reference, with a ten-minute
  deadline and per-image backoff with jitter. A waiting Pod reports
  `ContainerCreating`, `ErrImagePull`, or `ImagePullBackOff` in its container
  status, and a failed pull no longer withholds Node readiness or other
  admissions.
- With `--enable-workloads`, a spec change to an admitted Pod is published on
  that Pod as the `brie.dev/ExecutionBlocked` condition
  (`ExecutionConfigChanged`) and no longer withholds Node readiness or other
  admissions.
- With `--enable-workloads`, Pods are reconciled by bounded workers with at most
  one operation per Pod, and deletion, cleanup, and status work has its own
  workers and CRI client, so a slow create or pull no longer delays other Pods.
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
