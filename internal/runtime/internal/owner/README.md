# Runtime owner

One owner represents one sandbox/Pod execution attempt and accepts at most one
process launch. Start, stop, and Firecracker boot requests are serialized; the
configured deadline includes time waiting for another operation. Status reads
remain independent.

The injected launcher and process must honor operation contexts. A start context
limits launching, not process lifetime. Only the process's single `Wait` call
establishes exit; a successful stop request is not enough.

A private, local JSON journal is atomically replaced and synced before lifecycle
mutations. A Linux `flock` excludes concurrent Brie owners using the same
execution identity on this host. This is advisory host-local coordination, not
distributed fencing; other processes that bypass the owner are not controlled.
The journal is retained after `Close`, and `New` refuses to reuse an identity
with an existing record. There is no automatic restart recovery or stale-record
cleanup. An interrupted or uncertain attempt requires operator reconciliation;
do not remove its record until the associated VMM is confirmed absent.

A failed start leaves unknown state and cannot be retried on this owner. Failed
stop requests are not repeated either: subsequent calls only wait for observed
exit. Canceling a caller does not discard or implicitly terminate its process.
Unexpected exits remain visible with their error. An exited owner cannot restart.

The Linux `FirecrackerLauncher` starts a foreground VMM in a fresh private
directory, records output in `vmm.log`, and waits for matching pre-boot API
metadata. It passes no parent environment to the child. Failed startup kills
and reaps that child; cancellation after startup does not terminate it.
`Stop` kills the owned process and `Wait` observes its exit. This is not a
graceful guest shutdown. Runtime directories and logs are retained.

This launcher is for disposable host experiments, not tenant workloads: there
is no jailer, privilege separation, resource isolation, CLI wiring, or owner
crash recovery. Tests run fake VMM subprocesses without KVM. Recovery after an
owner crash is not implemented.

## Separate experimental owner process

`make build` also builds `bin/brie-owner`. It is intentionally not included in
release archives yet. Run it independently of the node agent; the node agent
does not launch or connect to it in this increment.

```sh
bin/brie-owner --sandbox-uid=experiment --pod-uid=attempt-1 \
  --directory=/private/experiment/owner \
  --firecracker=/absolute/firecracker --kernel=/absolute/vmlinux \
  --rootfs=/absolute/rootfs.ext4 \
  --boot-args='console=ttyS0 root=/dev/vda ro init=/sbin/init'
```

The operator must prepare and verify the artifacts and trusted parent directory.
The owner creates a fresh mode-0700 directory, a mode-0600 `control.sock`, and
private journal and VMM subdirectories. Any existing owner directory is refused;
no sockets, journals, or runtime artifacts are adopted or deleted.

`--guest-cid=3` optionally configures a guest-control vsock device at
`<directory>/vmm/vsock.sock`; the default, 0, leaves the device absent. Reserved
CIDs are rejected. Socket paths are limited to 96 bytes to leave room for
Firecracker's guest-initiated `_<port>` suffix. The owner journals the device
configuration before its API mutation. Failure or a lost reply prevents boot
replay and instance start. Existing boot profiles without vsock are unchanged.
This configures only the device: there is no guest listener, handshake,
authenticated execution protocol, or container execution in this increment.
Vsock does not provide Pod networking.

The version-1 HTTP API accepts no request bodies or query parameters:

| Method | Path | Effect |
| --- | --- | --- |
| GET | `/v1/status` | Observe identity, process state, boot state, and errors. |
| POST | `/v1/start` | Launch the VMM once; do not boot a guest. |
| POST | `/v1/boot` | Apply the fixed boot profile and request instance start once. |
| POST | `/v1/stop` | Request termination and wait for observed exit. |

POST requests require `X-Brie-Sandbox-UID` and `X-Brie-Pod-UID` matching the
configured attempt. These prevent accidental identity mixups, not malicious
access by the same host user or root. Filesystem permissions are the local
access boundary; there is no remote endpoint.

At most 16 connections and one mutation are accepted at a time; a busy mutation
returns 503. Invalid
operations or identity mismatches return 409 with an observation and error.
Client disconnects do not cancel accepted operations. After an uncertain reply,
read status instead of blindly repeating mutations. Process `Running` and boot
`StartAcknowledged` are not guest/container readiness.

SIGINT/SIGTERM to the owner closes the API, waits for bounded in-flight work,
and explicitly stops/reaps the VMM. Client exit does not. Do not configure a
supervisor to kill owners alongside the node agent. Owner crashes, host failure,
automatic recovery, durable operation-ID replay, and CRI integration remain
unsupported. The subprocess test uses a fake VMM without KVM and proves only
client independence, duplicate-launch rejection, and explicit shutdown cleanup.

After an injected process is owned, `Boot` sends the complete machine, kernel,
and root-drive profile in order, then an optional vsock device, followed by one
`InstanceStart` request.
Validation of the whole profile happens before the first PUT. An API error or
lost reply stops the sequence and leaves boot state unknown; that owner will
not retry it. `StartAcknowledged` records only the API response, not VMM health,
guest boot, or workload readiness.

The nested `internal` path prevents the node-agent package from importing this
process-mutation boundary directly.
