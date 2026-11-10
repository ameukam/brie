# Runtime comparison: Kata or Brie guest execution

Status: option B selected by the maintainer on 2026-10-26; qualification incomplete.
Brie replacing kubelet and using the upstream Kubernetes scheduler is mandatory.
Firecracker is the only candidate VMM. B is the implementation target; C is
retained as historical comparison evidence, not a parallel runtime roadmap.

| Candidate | Execution path | Exclusive VM lifecycle owner |
| --- | --- | --- |
| B | Brie node agent -> containerd CRI -> Kata shim -> Firecracker -> Kata agent | Kata runtime/shim |
| C | Brie node agent -> Brie CRI -> Brie runtime owner -> Firecracker -> Brie guest supervisor | Brie runtime owner |

Upstream kubelet may be a reference or installation diagnostic, never evidence
that either candidate works with Brie. Runtime-only tests likewise do not prove
Brie Pod reconciliation. Brie must not mutate a Kata-owned Firecracker instance
through its existing owner or direct VMM API calls.

## Feasibility before performance

Kata source reviewed at commit
[`7b1fa8808f1d53e3ab3402bd206955e824f64d4d`](https://github.com/kata-containers/kata-containers/tree/7b1fa8808f1d53e3ab3402bd206955e824f64d4d).
This is a source reference, not an approved deployment version.

| Question | Source evidence | Experiment still required |
| --- | --- | --- |
| Does Kata provide CRI directly? | Its architecture documents containerd/CRI-O integration through shim v2. | Select and pin a containerd CRI configuration that Brie can call; implement Brie's common CRI client and Pod reconciliation. |
| Does Firecracker require special storage? | The Firecracker guide uses a rootfs image, jailer, and containerd devmapper snapshotter. | Qualify arm64 assets and disposable block storage, including thin-pool restart and exhaustion. Do not run upstream example disk setup against existing storage. |
| Is the guest wire protocol the same? | Kata agent documentation specifies ttRPC with Protobuf; Brie uses gRPC-Go with Protobuf. | Exercise equivalent operations through their native transports, not the same wire client. |
| Does container pause mean VM hibernation? | The agent API exposes container pause/resume. That alone proves neither VMM pause nor a durable memory/disk checkpoint. | Identify a supported owner-mediated lifecycle extension and verify VM state, storage consistency, and restore identity. Mark absent capabilities unsupported. |
| Does Brie already execute containers? | The existing C4A benchmark runs only synthetic GetInfo requests. | Implement real guest execution, image preparation, CRI integration, networking, and status before timing Pods. |

Source references:
[architecture](https://github.com/kata-containers/kata-containers/blob/7b1fa8808f1d53e3ab3402bd206955e824f64d4d/docs/design/architecture/README.md),
[Firecracker setup](https://github.com/kata-containers/kata-containers/blob/7b1fa8808f1d53e3ab3402bd206955e824f64d4d/docs/how-to/how-to-use-kata-containers-with-firecracker.md),
[agent transport](https://github.com/kata-containers/kata-containers/blob/7b1fa8808f1d53e3ab3402bd206955e824f64d4d/src/agent/README.md),
[agent API](https://github.com/kata-containers/kata-containers/blob/7b1fa8808f1d53e3ab3402bd206955e824f64d4d/src/libs/protocols/protos/agent.proto).

## Staged experiments

Run each stage only after its prerequisites work. A failed or unavailable stage
is recorded as failed, blocked, or unsupported, never as a successful zero-cost
operation. Begin with one workload host; cross-host recovery is a later,
separately approved two-host experiment.

| Stage | Procedure and measurements | Pass gate |
| --- | --- | --- |
| 1. Runtime lifecycle | Use CRI to create/start a single digest-pinned arm64 container, exec a command, collect stdout/stderr and exit status, stop/remove it. Record operation latency, CPU, memory, and residual resources. | Real execution inside Firecracker, correct exit/log results, complete cleanup. This is runtime evidence only. |
| 2. Brie Pod lifecycle | Submit the same networked single-container Pod without nodeName; observe scheduler binding, CRI calls, readiness probe, application response, exec/logs, and deletion. | No kubelet on the worker; correct assigned Pod and guest/container identity; truthful status and limits. |
| 3. Restart survival | Restart only Brie while the application increments a sequence counter and serves requests. Record interruption, reconciliation duration, process identity, and counter continuity. Separately kill the runtime owner and report its supported recovery behavior. | Brie restart does not recreate the VM/container or lose application state. Owner failure cannot produce a second owner or fabricate Running state. |
| 4. Cooperative warm pause | Application stops admission and acknowledges a safe point; runtime owner pauses and resumes the VM. Record preparation, pause acknowledgment, resume-to-ready, and first correct response. Test wake/delete races and dropped replies. | State survives, traffic remains gated until ready, reservations remain accounted, stale requests cannot revive deleted execution. Container-only freezing is a separate result. |
| 5. Durable recovery | Modify memory and disk with matching sequence/checksum markers; commit their checkpoint, terminate source, restore, and verify markers before traffic. Inject failure before/after each commit boundary. Later repeat across hosts with source fencing. | Only committed matching generations restore; no silent cold start or duplicate execution. Same-host success does not prove cross-host fencing. |
| 6. Capacity and compatibility | Increase active/paused population and simultaneous wakes; run pinned Node Conformance and profile-specific storage/network tests. | Bounded overload, truthful accounting, published failures/skips. Prototype results do not establish full conformance. |

The first shared workload is a credential-free HTTP service with a monotonic
counter, deterministic response checksum, and a writable block-backed state
file. Use identical application bytes, request/response sizes, limits, and
readiness criteria. Record its image digest before running. The cooperative
prepare/resume behavior must be implemented in the application, not inferred
from inactivity.

## Timing and resource definitions

| Metric | Start -> end |
| --- | --- |
| Pod startup | Client begins Pod creation -> first validated external application response; also record API acknowledgment, binding observation, container start, and readiness separately. |
| Runtime startup | CRI RunPodSandbox invocation -> successful StartContainer completion, followed by separately timed application readiness. |
| Cold/cache-hit startup | Same startup measurement with image absent or present; record guest/kernel, rootfs, snapshotter, and page-cache state independently. |
| Wake latency | Accepted wake request -> first validated application response after the restore barrier; include queueing and separately record runtime-only work. |
| Checkpoint commit | Accepted preparation/checkpoint request -> durable manifest commit covering VM state, memory, and matching disks. |
| Control operation | Client invocation -> validated result for status, exec, signal, wait, or stop; distinguish wait duration from transport overhead. |
| Codec and handler | Local monotonic duration around native encode/decode and handler boundaries, measured separately from uninstrumented RTT. |
| Memory | Idle-host baseline and total physical memory at each population, plus process PSS, cgroup memory, page cache, and guest allocation; never sum RSS as unique physical memory. |
| Cost | Host, disk, checkpoint storage/I/O, and reserved-capacity charges divided by successful workload-hours or successful wakes; report failures and excluded setup time. |

Record host-monotonic end-to-end durations. Component timestamps from different
hosts/clocks are not directly subtractable. Keep phase observations and their
clock domains; do not pretend watch arrival time is the actual scheduler event.
Codec/handler percentiles cannot be subtracted from round-trip percentiles.

## Repetition, load, and evidence

Use the same C4A machine type, pin host image and complete software tuples, and
randomize B/C order across at least three independent repetitions. Record CPU
affinity, governor, background load, guest limits, storage layout, CNI, security
settings, and all patches. Match versions where supported and disclose necessary
differences. An unjailed C prototype cannot be ranked against hardened B.

For control RPCs, retain the existing 1,000-call warmup and 10,000 measured calls
at concurrency 1 and 8 per repetition, using equivalent native operations.
The GetInfo benchmark is a transport reference, not an equivalent container
operation. For lifecycle tests, start with 100 independent cycles per repetition;
publish sample count and uncertainty, and do not promote their p99 as a stable
tail estimate. Increase sampling only after correctness and cleanup are proven.

Measure both closed-loop concurrency and open-loop offered load. Start open-loop
load at 25%, 50%, 75%, and 100% of each candidate's measured sustainable rate;
also compare both at identical absolute rates within their shared envelope.
Timestamp intended arrival, admission, dispatch, and completion. Bound queues
and driver concurrency; count every missed arrival, rejection, timeout, and
failure rather than silently reducing offered load.

Retain raw samples with run/candidate/workload IDs, operation, outcome,
deadline, monotonic duration, concurrency, offered rate, and observed identities.
For every run report attempted/succeeded/failed/timed-out/rejected counts, actual
arrival rate, successful throughput, p50/p95/p99/max, and resource use. Failures
are not successful latency samples. Publish per-run distributions as well as
pooled results, instrumentation overhead, and evidence of teardown.

## Selection and current blockers

Correctness and required lifecycle capabilities are hard gates. There is no
requirement to outperform upstream kubelet or meet an arbitrary percentage gain.
Before the comparative run, the maintainer must select the workload envelope,
startup/wake SLOs, acceptable memory/cost, and recovery objectives. Until then,
report tradeoffs or Pareto results, not a numeric winner.

The maintainer selected B after reviewing the runtime feasibility and
hibernation gaps. Qualification gates still apply; the choice is not a claim
that B implements the missing lifecycle features. Document Kata extension and
upgrade costs. Reopening C requires another explicit architecture decision,
not an automatic fallback after a failed test.

As of 2026-10-26, neither B nor C has demonstrated a scheduler-assigned Pod
executing through Brie. B's runtime-only arm64 lifecycle passed the experiment
below; its owner-mediated VM hibernation path remains unimplemented in the
tested stack. C has real synthetic vsock transport evidence but no production
guest supervisor or container execution. Brie's shared Pod/CRI path is a
prerequisite for stage 2, not an outcome of this specification.

Brie's bounded read-only CRI client now has fake-socket version/status tests;
it is not wired into the node agent or qualified against real containerd.
The next implementation gate is explicit node-agent connection configuration,
then Pod reconciliation and a scheduler-assigned Pod through Brie.
Owner-mediated Kata pause/checkpoint/restore remains separate work; container
pause is not a substitute. Provisioning, storage setup, and real VM execution
need separate explicit approval.

## Runtime-only C4A qualification, 2026-10-26

This was a feasibility experiment, not the repeated performance comparison
specified above. One disposable `c4a-highmem-96-metal` worker in `us-east4-c`
ran no kubelet, Brie agent, or Kubernetes control plane.

| Component | Tested value |
| --- | --- |
| Host | Ubuntu 24.04 image `ubuntu-2404-noble-arm64-v20261021`, kernel `7.0.0-1014-gcp` |
| Kata | Go runtime 4.3.1, source `03cf2ec06a5d302f23af811744f6bcea49988bfb`, arm64 release archive |
| Firecracker / jailer | Both 1.17.0, shipped with Kata; jailer enabled |
| Guest | Shipped Kata ext4 image and kernel `6.18.56`, configured defaults 1 vCPU / 512 MiB |
| containerd / CRI client | containerd 2.4.2, crictl 1.37.0 |
| Storage | devmapper, 16-GiB sparse loopback data file, 256-MiB metadata, 512-MB base snapshots |
| Networking | CNI plugins 1.9.1; isolated bridge, host-local IPAM, no masquerading or external route |
| Container limits | 128 MiB, one CPU quota |

All downloaded runtime/plugin archives were checked against their GitHub release
asset SHA-256 digests. Kata's shipped settings were retained except guest memory
and maximum vCPUs. In particular, the shipped `disable_guest_seccomp=true`
remained set: this was not a hardened-profile security qualification. The
development thin-pool used `skip_block_zeroing` and synthetic data only; it is
not a production storage recommendation.

Image references were resolved for arm64 before execution:

```text
docker.io/library/busybox@sha256:c542a2b3cb3c982440890284aac884e89dbf468d5ff1c206fd2ba8ea23257420
registry.k8s.io/pause@sha256:a433214620b407678934ebd69690a9ed6066247231eacfba181fab20dbc3dbf1
```

Three CRI lifecycle cycles passed RunPodSandbox, CreateContainer, StartContainer,
ExecSync, stream-labelled stdout/stderr collection, observed natural exit code
17, StopContainer, RemoveContainer, StopPodSandbox, and RemovePodSandbox. Exec
reported guest kernel `6.18.56`, distinct from the host. The running process
record showed a Kata shim and jailed Firecracker, not a host-only container.

During the first cycle, restarting containerd preserved the reported VMM PID
4519 and Running status; a subsequent exec succeeded. This tests containerd
reconnection, not Brie restart, shim crash recovery, or host failure.

The following are observed ranges across only three cached-image cycles,
including crictl process startup and wall-clock shell timing overhead. They are
not pure RPC timings, Pod readiness, or stable tail-latency estimates:

| Operation | Observed range |
| --- | --- |
| RunPodSandbox | 695-864 ms |
| CreateContainer | 109-158 ms |
| StartContainer | 113-115 ms |
| ExecSync | 59.0-59.8 ms |
| StopPodSandbox | 171-197 ms |

These cannot be compared directly with C's 60-byte GetInfo benchmark.

### Pause and checkpoint findings

Native containerd task pause/resume succeeded through Kata's shim. While the
target was PAUSED, its log counter stayed at 13 ticks across a two-second wait.
After resume it reached 18. An observer container in the same sandbox continued
executing, and guest uptime advanced from 2.54 to 4.60 seconds during the wait.
This demonstrates container freezing, not pausing the entire VM or cooperative
application preparation.

The CRI checkpoint attempt failed first at containerd's missing-host-CRIU
precheck. To avoid attributing that prerequisite failure to Kata, a separate
native containerd task checkpoint was attempted against a fresh running Kata
container. It returned:

```text
ctr: service Checkpoint: not implemented
```

The container remained executable after rejection. The exact released
[shim implementation](https://github.com/kata-containers/kata-containers/blob/03cf2ec06a5d302f23af811744f6bcea49988bfb/src/runtime/pkg/containerd-shim-v2/service.go#L940-L954)
returns NotImplemented for Checkpoint. Its
[Firecracker backend](https://github.com/kata-containers/kata-containers/blob/03cf2ec06a5d302f23af811744f6bcea49988bfb/src/runtime/virtcontainers/fc.go#L946-L956)
has PauseVM, SaveVM, and ResumeVM methods that return nil without issuing VMM
operations. Successful returns from those methods would not establish lifecycle
support. This is a limitation of the tested Kata implementation/interfaces,
not of Firecracker's underlying snapshot API or every possible Kata extension.

### Evidence and teardown

The initial loopback-only CNI setup failed before execution because it supplied
no usable sandbox network result. It was replaced with an isolated bridge
configuration; the failed attempt was retained separately.

The final CRI inventory contained zero containers and sandboxes, with no
Firecracker process remaining. The worker, automatic-delete boot disk, and
dedicated firewall, NAT/router, subnet, and network were deleted; subsequent
resource queries returned no matching resources.

Raw logs, CRI JSON, operation timings, executed scripts, pinned source excerpts,
and setup/configuration evidence are retained in the session artifact directory
`files/kata-c4a-20261026`. The evidence archive SHA-256 is
`ffa43648854699ae21cf00956e867f94f0efead5f4b452592740228cca762aec`.
No runtime selection, Brie Pod execution, durable recovery, or conformance claim
follows from this experiment.
