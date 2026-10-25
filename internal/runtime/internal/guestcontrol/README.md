# Guest control

`guest_control.proto` defines the version-1 gRPC/Protobuf `GuestSupervisor`
service. Its initial read-only `GetInfo` RPC checks the Sandbox UID, Pod UID,
runtime generation, and API version. gRPC messages are bounded to 4096 bytes,
and each identity field is limited to 256 bytes.

`Dial` establishes a plaintext gRPC connection over Firecracker's host Unix
socket after performing the `CONNECT <guest-port>` / `OK <host-port>`
handshake. Generate bindings with `go generate ./internal/runtime/internal/guestcontrol`.
The pinned Go generators are `protoc-gen-go` v1.36.12-0.20260120151049-f2248ac996af
and `protoc-gen-go-grpc` v1.6.0; `protoc` must also be installed.

Vsock connectivity associates a peer with a VM but does not authenticate it.
This package does not yet provide a guest AF_VSOCK listener, host peer
authentication, a guest executable, container execution, or readiness. A
successful `GetInfo` proves only that a supervisor responded with the expected
identity and protocol version; it is not evidence that a container or Pod is
running.

Tests exercise gRPC and the Firecracker connection preamble over Unix sockets
with an in-process mock peer. They do not launch Firecracker or prove that a
guest kernel or supervisor can use AF_VSOCK.
