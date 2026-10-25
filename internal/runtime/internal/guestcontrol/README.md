# Guest control protocol

Protocol version 1 is a single-request-per-stream, 4-byte big-endian length
prefix followed by strict JSON. Frames are limited to 4096 bytes and reject
unknown fields, trailing values, and truncated payloads. The initial `ping`
request binds the reply to the Sandbox UID, Pod UID, runtime generation,
protocol version, operation, and random request ID. No command, image data,
credential, or mutation is accepted.

`DialFirecrackerVsock` implements Firecracker's host Unix-socket
`CONNECT <guest-port>` / `OK <host-port>` preamble before a protocol exchange.
`ServeConn` processes one stream after its caller authenticates and authorizes
the peer. The protocol package does not yet provide a guest AF_VSOCK listener,
host peer authentication, a guest executable, container execution, or
readiness. A matching identity is an association check, not authentication.
The ping response is not evidence that a container or Pod is running.

Tests exercise the complete framing and Firecracker preamble over Unix sockets
with an in-process mock peer. They do not launch Firecracker or prove that a
guest kernel or supervisor can use AF_VSOCK.
