# Restricted Pod execution

The profile limits each Pod to one ordinary container, not the number of Pods or
nodes. Explicit `--enable-workloads` mode reconciles multiple assigned Pods using
containerd CRI and the upstream Kubernetes scheduler. Without that flag,
registration-only behavior remains cordoned, tainted, NotReady, and zero-capacity.
Fake-runtime and API transport tests cover the code path. A limited C4A hardware
smoke run exercised scheduler-to-Firecracker execution and exposed a terminal
deletion limitation, described below. Deployment qualification and Node
Conformance remain outstanding.

The caller supplies a trusted node name and RuntimeClass name. The Pod must have
a name, namespace, UID, and matching node assignment and RuntimeClass. Class-name
equality does not verify its handler: administrators must separately qualify the
RuntimeClass -> containerd -> Kata -> Firecracker configuration. Pod annotations
are not runtime configuration or proof of ownership.

## Selected contract

- Explicit Linux OS, exactly one ordinary container, `restartPolicy: Never`.
  Deleting and terminal Pods are not candidates for new execution.
- A digest-pinned `repository@sha256:<64 lowercase hex digits>` image with
  `imagePullPolicy: IfNotPresent`. Pinning does not verify image availability,
  authenticity, guest compatibility, or support for the requested UID.
- Positive, equal CPU and memory requests/limits; no additional resources,
  claims or Pod-level resources. Execution requires measured positive CPU/memory
  overhead in the approved RuntimeClass and the identical API-injected Pod
  overhead. Admission reserves both container requests and overhead.
- Pod-level `runAsNonRoot: true`, positive explicit `runAsUser` and `runAsGroup`,
  and `seccompProfile.type: RuntimeDefault`. The container must explicitly disable
  privilege escalation, drop `ALL` capabilities, and add none. Privileged mode,
  container identity/seccomp overrides, custom LSM profiles, supplemental groups,
  sysctls, host namespaces, shared process namespaces, and user namespaces are
  unsupported. An optional read-only container rootfs is allowed.
- Command, args, working directory, and literal environment values are selected
  container inputs. `automountServiceAccountToken` and `enableServiceLinks` must
  explicitly be false. ServiceAccount names may be API-defaulted but authorize no
  token or Secret delivery. No image-pull Secrets or environment references.
- Standard Pod networking and `dnsPolicy: ClusterFirst` use the CRI/CNI/Kata
  path, not a networkless/vsock substitute. The agent supplies
  Pod CIDR configuration and cluster DNS; the administrator must qualify CNI,
  guest routing, Service reachability, and DNS. Port declarations, custom
  DNS settings, host aliases, and hostname overrides remain unsupported.
- Nonnegative termination grace periods and API-defaulted termination-message
  fields are recognized; custom termination-message settings, active deadlines,
  probes, lifecycle hooks, readiness gates, streaming/TTY, restart rules, and
  resize policies are unsupported.
- No volumes, mounts, block devices, projected data, init/sidecar containers,
  ephemeral containers, CSI, or DRA. Disable token injection in the submitted
  Pod; admission-injected volumes or containers are rejected too.

The upstream scheduler retains placement ownership. Existing node selectors,
affinity, scheduler name, tolerations, priority/preemption, and topology-spread
fields are accepted as scheduling inputs, not reimplemented by Brie.
All other nonzero PodSpec, Container, and security-context fields fail closed;
API upgrades must explicitly review additions. Kubernetes API validation remains
required; this profile check does not replace it or implement Kubernetes Pod
Security Admission.

## Inventory before reconciliation

The read-only CRI client exposes unfiltered `ListPodSandbox` and `ListContainers`,
plus exact-ID, non-verbose `PodSandboxStatus` and `ContainerStatus`. Calls retain
runtime IDs, Pod namespace/name/UID, container names, attempt numbers, sandbox
associations, runtime handlers, labels, annotations, and observed state. Foreign,
unready, exited, and `CONTAINER_UNKNOWN` objects are not filtered out or claimed.
Container status has no sandbox ID: retain its list association, never join by
name alone, and recheck identity before any future action.

Each RPC shares the client's bounded call slots, timeout (including queueing and
connection setup), cancellation, and 64-KiB send/receive bounds. CRI lists have
no pagination here; an oversized response fails rather than returning a truncated
inventory. Malformed identities, missing metadata, unknown enum values, invalid
creation timestamps, duplicate list IDs, or mismatched status IDs fail the call
without partial results. A successful empty list is different from any error,
including `NotFound`, unavailable transport, cancellation, or oversized messages.

Separate lists/status calls are not an atomic snapshot. A disappearing object,
an inconsistent association, or unavailable API/runtime data requires resync,
not orphan deletion or evidence of absence.

## Enabling scheduling and execution

Use a dedicated containerd CRI endpoint configured for Kata's Firecracker
handler and a qualified block-backed image/snapshotter path. Create a
`node.k8s.io/v1` RuntimeClass with the approved `handler`, measured CPU/memory
`overhead.podFixed`, and these scheduling settings:

```yaml
scheduling:
  nodeSelector:
    brie.dev/runtime: kata-fc
  tolerations:
    - key: brie.dev/runtime
      operator: Equal
      value: kata-fc
      effect: NoSchedule
```

Replace `kata-fc` with the approved handler name. The API server injects
RuntimeClass overhead and scheduling constraints into submitted Pods.
Set the Pod's `runtimeClassName`; do not bind it with `spec.nodeName`.
Pods still need the explicit OS/security/resource/token settings above.
Pin an immutable application image that supports the selected non-root UID/GID.

Run on each Linux workload host with explicitly reserved, measured budgets:

```sh
brie-node --run --enable-workloads \
  --node-name=worker-exec-1 --node-ip=192.0.2.10 \
  --kubeconfig=/etc/brie/kubeconfig \
  --runtime-socket=/run/containerd/containerd.sock \
  --runtime-class=kata-fc --runtime-handler=kata-fc \
  --allocatable-cpu=4 --allocatable-memory=8Gi --max-pods=10 \
  --cluster-dns=10.96.0.10 --cluster-domain=cluster.local \
  --state-dir=/var/lib/brie/execution --pod-log-dir=/var/log/brie/pods
```

Addresses and budgets are illustrative, not host discovery or sizing guidance.
Reserve host services before selecting budgets; per-Pod VM overhead must come
from measurement. Never share the state directory between hosts. It must be
private, owned by the agent user, durable, and retained across restarts. State
pins node/runtime-class identity, DNS, node IP, and log location. Corrupt,
incompatible, or missing state cannot be replaced to adopt running VMs.

Execution creates a new Node or reconnects its original execution-owned Node;
it never takes over an existing registration-only or unrelated Node. Use a new
node name for migration, or arrange a separate reviewed migration. An existing
administrator cordon, unrelated taints, and externally managed conditions remain
intact. The permanent runtime `NoSchedule` taint is not removed.

The control plane must assign one Pod CIDR. The agent sets CRI network
configuration, continuously checks runtime/network health and the pinned
RuntimeClass, and reconstructs complete assigned-Pod and CRI inventories before
reporting Ready or advertising nonzero allocatable resources. Until then,
allocatable CPU, memory, and Pods remain zero. NodeRestriction remains enabled:
the agent updates Node status, not existing taints or administrator cordons.
The upstream node controller owns the standard readiness taints. Configured
budgets become schedulable only through that gated path. Kubernetes remains the
only scheduler; the agent performs local resource admission.

The explicit kubeconfig needs node-scoped Node get/create/update/status access,
Lease get/create/update, assigned-Pod list/get/status update/delete, and get access
to the approved RuntimeClass. Field selection is not authorization: use appropriate
Node authorization/RBAC and NodeRestriction, not broad administrator credentials.
Credential plugins, impersonation, insecure TLS, and ambient kubeconfigs remain
unsupported.

## Lifecycle and failure behavior

Assigned Pods are read through a node-scoped watch. A complete,
quorum-consistent list with a node field selector and bounded pagination seeds
the view; a watch with bookmarks, scoped by the same selector, then continues
from the list's resource version. A closed stream resumes from the latest event
or bookmark version. An expired resource version (HTTP 410), a watch error
event, or an event the view cannot apply triggers a full relist with bounded
exponential backoff (one to thirty seconds with jitter), and a full relist also
runs every five minutes as a safety net. While the view is not synchronized,
passes mutate nothing and the Node stays unready. WatchList streaming is not
used. A pass runs when the view changes and at least every two seconds, reading
the watched view and the complete local CRI inventory; it makes no API list
request. After each complete read, at most one operation per Pod UID runs
on bounded workers: four for deletion, cleanup, and status work, and two for
admitted creation, each pool with its own CRI client. A Pod with an operation in
flight is skipped until it ends, and a full pool defers work to the next pass
without delaying other Pods. Admission still runs in Pod creation order. Lease
renewal runs independently of the five-second health/status loop and the
workload RPC queues. Workload RPCs have a two-minute timeout; health RPCs retain
ten-second deadlines.

The agent observes/pulls immutable public images, runs a sandbox with the explicit
handler, creates the container, then starts it. Security settings, CPU/memory
limits, Pod overhead, DNS, and logs are translated into native CRI fields;
arbitrary Pod annotations are never forwarded as Kata configuration.
All VM/container/network mutations remain with containerd and Kata.

Namespace/name/UID, container identity, owner labels, execution-config hashes, and
durable intent records guard reconstruction. The private journal has a schema,
checksum, exclusive lock, and write/fsync/rename/directory-fsync commit sequence.
Each create/start intent is committed before the RPC. Lost replies are recovered
only from matching observations, including across restart. An uncertain absent
result blocks admission rather than triggering another create or start.
A sandbox or container create is proven to have created nothing only when the
runtime answered with a definite rejection (`InvalidArgument`, `NotFound`,
`FailedPrecondition`, `PermissionDenied`, or `OutOfRange`, codes grpc-go never
generates itself), the code is journaled, and a later complete sandbox and
container inventory holds no object claiming the Pod. The Pod then fails with
`SandboxCreateRejected` or `ContainerCreateRejected` and its reservation is
released through normal cleanup; the create is never retried. Unavailable,
deadline, cancellation, `Unknown`, and other codes remain unresolved.

Faults confined to one Pod's inputs do not change Node readiness or other
admissions. A spec change to an admitted Pod sets the Pod condition
`brie.dev/ExecutionBlocked` with reason `ExecutionConfigChanged` and leaves the
execution untouched until the Pod is deleted.

Image pulls run outside Pod reconciliation on two pull workers with their own CRI
client and a ten-minute deadline per pull. The deadline is fixed, not sized to
the image. Pods that need the same image reference share one pull. An admitted
Pod whose image the runtime reports absent stays `Pending` with its reservation,
and its container status reports why it is waiting: `ContainerCreating` while
the pull is queued or running, `ErrImagePull` with the bounded runtime error once
after a failure, and `ImagePullBackOff` while the next attempt waits. Failed
pulls retry per image reference with exponential backoff and jitter from ten
seconds up to five minutes. A failed or slow pull never holds a creation worker,
changes Node readiness, or blocks other admissions. Pod intent is read again
after the image wait and before the sandbox is created, so a Pod deleted during
its pull never reaches the runtime. Pull Secrets remain unsupported.

Pod status reports observed pending/running/unknown/exited state, IP, readiness,
exit code, and runtime identity; restartPolicy Never never restarts an exit.
Explicit deletion or an acknowledged terminal Pod authorizes idempotent CRI
stop/remove, after rechecking UID and current API intent. Graceful deletion uses
the current deletion deadline. The agent releases reservations after cleanup,
and finalizes deletion with UID/resource-version preconditions.

A Pod that disappears before cleanup finishes, including a terminal Pod deleted
right after its exit is reported, a force-deleted running Pod, and terminated-Pod
garbage collection, is cleaned up only on positive deletion evidence: a
successful, complete, quorum-consistent node-scoped Pod list with a resource
version that omits the Pod UID, or a watch `DELETED` event for that UID received
on a stream that continues such a list without a gap. The execution must be this agent's own: its
journal record, sandbox metadata UID, and owner labels agree. The agent journals
the `DELETED` event's resource version, or otherwise the view's resource
version, as `DeletionObserved` before the first destructive step; the durable record then authorizes the idempotent stop/remove steps across
restarts without a Pod read, which the Node authorizer rejects once the Pod is
gone. The container is stopped with zero grace. A failed, forbidden, truncated,
or version-less list, a `NotFound` or `Forbidden` Pod read, a foreign or
ambiguous runtime object, an unresolved sandbox creation, and a recorded deletion
contradicted by a listed Pod are not evidence; they preserve the execution and
block admission.

API/runtime outages, foreign or orphan executions without deletion evidence,
ambiguous associations, missing known IDs, and over-budget reconstructed
reservations preserve workloads and block admission. Shutdown closes agent
connections without killing VMs. There is no age- or inventory-based orphan GC,
cross-host recovery, probes, streaming,
hibernation, or conformance claim. A finished journal record is compacted only
when, in one pass, `RemovePodSandbox` succeeded for the sandbox it names (or it
never named one), its Pod UID is absent from the complete assigned-Pod view, and
complete sandbox and container lists hold no object that it names or that claims
its Pod UID. A terminal Pod that is still listed keeps its record, and so does a
record whose sandbox disappeared without a successful `RemovePodSandbox`, such as
one removed by an operator or finished under the version 1 journal schema. The
journal schema is now version 2; version 1 journals are read and rewritten as
version 2, and older agents refuse version 2. The journal stays bounded to
10,000 records and 4 MiB; retained records still need reviewed operator
maintenance before those limits are reached. Watch and pass intervals, startup
latency, host/guest resource enforcement, and real scheduler-to-Kata execution
still require runtime qualification before deploying this mode.

## C4A hardware smoke (2026-10-30)

A disposable `c4a-highmem-96-metal` Arm64 host in `us-east4-c`, with `/dev/kvm`
and no worker kubelet, ran containerd 2.4.2, Kata Go 4.3.1, and its Firecracker
handler. A separate upstream Kubernetes 1.37.0 control plane retained the Node
authorizer and NodeRestriction. The tested agent SHA-256 was
`b7031afd5bbe721a870c5a533c85c42d07d1cdd6e604598fc6e9bc340159940e`.
The lab used synthetic data and a disposable loopback-backed devmapper thin pool,
not production storage.

The upstream scheduler bound two restricted Pods submitted without `spec.nodeName`.
Both ran concurrently through Brie -> containerd -> Kata -> Firecracker. The
configured budget was 8 CPU, 8 GiB, and four Pods; each application requested one
CPU and 128 MiB, with an additional RuntimeClass reservation of one CPU and
768 MiB. This was not a host-capacity or performance test.

Cross-host Pod HTTP, ClusterIP Service HTTP, and in-guest cluster DNS succeeded.
Guest observations confirmed UID/GID 65534, no effective capabilities,
`NoNewPrivs=1`, seccomp filtering, and the requested CPU/memory cgroup limits.
Across an agent restart, 40 direct-Pod HTTP samples succeeded; VM PIDs, container
IDs, and guest boot IDs stayed unchanged and application counters increased.
Service continuity across that restart was not measured. An oversized Pod stayed
unscheduled, a naturally exiting container reported exit code 17 with no restart,
and deleting one running Pod preserved the other.

The run found a NodeRestriction incompatibility in startup-taint updates.
Readiness now uses status-only updates and zero allocatable resources until the
health/reconstruction gates pass, leaving readiness taints to the node controller.
Regression tests cover initializing, ready, unhealthy, and recovering states.

**Known terminal-deletion limitation:** deleting the Failed Pod immediately after
exit reporting removed its API relationship before the agent finished cleanup.
The node authorizer then rejected the exact Pod GET. Brie preserved the exited
container's sandbox and journal reservation and blocked admission, rather than
performing destructive orphan GC. Scoped operator cleanup through CRI removed the
remaining lab sandbox; the journal was not manually modified. All API test Pods,
runtime containers/sandboxes, VMM processes, and cloud experiment resources were
removed, but automatic terminal cleanup did not complete in this case. The
deletion-evidence rule above addresses this case and is covered by fake-runtime
regression tests (`TestExecutionDeletionEvidence`); it has not been rerun on
hardware.

Credential-free evidence was retained outside Git. This smoke run does not
establish production isolation, load/capacity qualification, hibernation, or
Kubernetes conformance. In particular, the stock Kata VMM retained host root
identity and broad capabilities despite jail/mount isolation and host seccomp;
guest restrictions are not production least-privilege qualification.
