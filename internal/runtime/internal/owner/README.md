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
crash recovery. Tests run fake VMM subprocesses without KVM. Survival across
node-agent restart is not implemented.

After an injected process is owned, `Boot` sends the complete machine, kernel,
and root-drive profile in order, followed by one `InstanceStart` request.
Validation of the whole profile happens before the first PUT. An API error or
lost reply stops the sequence and leaves boot state unknown; that owner will
not retry it. `StartAcknowledged` records only the API response, not VMM health,
guest boot, or workload readiness.

The nested `internal` path prevents the node-agent package from importing this
process-mutation boundary directly.
