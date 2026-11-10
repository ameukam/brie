# Initial Pod profile

This is the restricted contract for a future one-Pod, one-node experiment,
not an enabled execution path or a Node Conformance claim.
`nodeagent.ValidatePodProfile` is a side-effect-free check; there is no assigned-Pod
watch, admission loop, image pull, sandbox/container mutation, or PodStatus writer.
Registration stays cordoned, tainted, NotReady, and zero-capacity.

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
  claims, Pod-level resources, or RuntimeClass overhead. Capacity accounting,
  VM overhead, and host/guest enforcement must be implemented before execution.
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
- Standard Pod networking and `dnsPolicy: ClusterFirst` are the intended future
  path through CRI/CNI/Kata, not a networkless/vsock substitute. Networking and DNS
  are not implemented or qualified by this validator. Port declarations, custom
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

Each RPC shares the existing serialized slot, timeout (including queueing and
connection setup), cancellation, and 64-KiB send/receive bounds. CRI lists have
no pagination here; an oversized response fails rather than returning a truncated
inventory. Malformed identities, missing metadata, unknown enum values, invalid
creation timestamps, duplicate list IDs, or mismatched status IDs fail the call
without partial results. A successful empty list is different from any error,
including `NotFound`, unavailable transport, cancellation, or oversized messages.

Separate lists/status calls are not an atomic snapshot. A disappearing object,
an inconsistent association, or unavailable API/runtime data requires resync,
not orphan deletion or evidence of absence. This increment adds no cleanup,
ownership reconstruction, runtime health loop, or startup inventory scan.
The existing opt-in CLI still reads only Version and Status once at startup.
