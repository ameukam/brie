# CRI client

Reads CRI v1 `Version`, `Status`, `ListPodSandbox`, `ListContainers`,
`PodSandboxStatus`, and `ContainerStatus` over a local Unix socket.
The node agent reads only Version and Status at startup in registration-only
mode. Explicit `--enable-workloads` uses inventory and bounded image, network
configuration, sandbox, and container lifecycle calls.

Create a client with `New(socketPath, timeout)` and close it when done.
Calls run one at a time; the timeout includes queueing and connection setup.
Responses are limited to 64 KiB, except `ExecSync` output, which is limited to
1 MiB. Restrict socket access with filesystem permissions.

`Exec`, `Attach`, and `PortForward` return the runtime's streaming URL; they do
not stream. `ExecSync` runs a bounded command for probes, and
`ReopenContainerLog` supports log rotation. None of them resumes a paused
sandbox; callers check recorded lifecycle state first.

Lists are unfiltered and retain foreign and non-running objects. Status calls
are non-verbose and require an exact runtime ID. Invalid or oversized responses
fail without partial results; errors, including NotFound, never mean an empty
inventory. Separate calls are not an atomic snapshot and cannot authorize cleanup.
See the [initial Pod profile](../../docs/pod-profile.md) for the next execution
boundary and identity requirements.

Lifecycle calls have no automatic retries. A failed create/start may already
have committed; callers must retain durable intent and reconcile identities
before proceeding. Socket access grants runtime mutation authority and must
remain restricted to the trusted node/runtime owner.
