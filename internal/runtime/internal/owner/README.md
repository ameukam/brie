# Runtime owner foundation

One owner represents one sandbox/Pod execution attempt and accepts at most one
process launch. Start and stop requests are serialized; the configured deadline
includes time waiting for another operation. Status reads remain independent.

The injected launcher and process must honor operation contexts. A start context
limits launching, not process lifetime. Only the process's single `Wait` call
establishes exit; a successful stop request is not enough.

A failed start leaves unknown state and cannot be retried on this owner. Failed
stop requests are not repeated either: subsequent calls only wait for observed
exit. Canceling a caller does not discard or implicitly terminate its process.
Unexpected exits remain visible with their error. An exited owner cannot restart.

There is no production launcher or CLI wiring. Tests use fake processes only.
This package does not provide a journal, cross-process exclusion, identity
fencing, owner crash recovery, or survival across node-agent restart. Those are
required before wiring real Firecracker processes or accepting workloads.

The nested `internal` path prevents the node-agent package from importing this
process-mutation boundary directly.
