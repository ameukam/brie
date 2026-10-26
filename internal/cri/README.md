# Read-only CRI client

This is the first containerd integration increment for the selected Brie ->
containerd CRI -> Kata -> Firecracker stack. It is not wired into the node agent
and does not execute Pods or change Node readiness/capacity.

Kata owns guest control through its native agent protocol. Brie consumes CRI;
it does not implement a separate guest-control service or mutate Kata-owned VMs.

`New` takes an explicit local Unix-socket path and a positive per-call timeout.
It opens no connection until a call is made. Callers must `Close` the client.
`Version` requires complete runtime metadata and CRI v1. `Status` requests
non-verbose health and requires distinct RuntimeReady and NetworkReady
conditions; false conditions and their diagnostics are returned unchanged.
Neither method proves that Kata/Firecracker is configured or usable.

Calls are serialized; their timeout includes queueing, connecting, and receiving
the response. Caller cancellation and earlier deadlines take precedence.
Messages are bounded to 64 KiB, configured RPC retries are disabled, and errors
preserve gRPC codes and context cancellation/deadlines, including server-reported
cancellation/deadlines before the local context expires. Socket permissions must
protect the trusted endpoint: local plaintext gRPC is not peer authentication.
No runtime/image mutations or raw connection are exposed.

Fake Unix-socket tests exercise the protocol, not a real containerd deployment.
Pod reconciliation, ImageService, streaming, and the Kata-owned hibernation
extension remain separate increments.
