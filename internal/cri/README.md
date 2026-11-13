# CRI client

Reads CRI v1 `Version`, `Status`, `ListPodSandbox`, `ListContainers`,
`PodSandboxStatus`, and `ContainerStatus` over a local Unix socket.
The node agent reads only Version and Status at startup in registration-only
mode. Explicit `--enable-workloads` uses inventory and bounded image, network
configuration, sandbox, and container lifecycle calls.

Create a client with `New(socketPath, timeout)`, or `NewConcurrent` to allow a
fixed number of calls in flight, and close it when done. `New` runs calls one at
a time; the timeout includes queueing and connection setup.
Responses are limited to 64 KiB. Restrict socket access with filesystem permissions.

Lists are unfiltered and retain foreign and non-running objects. Status calls
are non-verbose and require an exact runtime ID. Invalid or oversized responses
fail without partial results; errors, including NotFound, never mean an empty
inventory. Separate calls are not an atomic snapshot and cannot authorize cleanup.
See the [initial Pod profile](../../docs/pod-profile.md) for the next execution
boundary and identity requirements.

Lifecycle calls have no automatic retries. A failed create/start may already
have committed; callers must retain durable intent and reconcile identities
before proceeding. `DefiniteRejection` identifies status codes that only a
completed runtime handler returns to refuse a request; it is necessary, not
sufficient, evidence that nothing was created. Socket access grants runtime mutation authority and must
remain restricted to the trusted node/runtime owner.
