# CRI client

Reads containerd's CRI v1 `Version` and `Status` over a local Unix socket.
The node agent can read both once at startup with `--runtime-socket`.

Create a client with `New(socketPath, timeout)` and close it when done.
Calls run one at a time; the timeout includes queueing and connection setup.
Responses are limited to 64 KiB. Restrict socket access with filesystem permissions.
