# Cooperative counter example

`brie-counter` is a sample workload for the guest supervisor protocol
(`brie.guestsupervisor/v1`, DESIGN.md sections 3.1 and 3.2). It is
credential-free and demonstrates the application side of the contract only.
No snapshot, pause, or restore path exists in Brie or in the tested Kata
Firecracker backend, so running this example does not hibernate anything.

## Behavior

- `POST /increment` adds one to the counter. The new value is written to
  `/brie/counter.json` with a temporary file, `fsync`, rename, and a directory
  `fsync` before the reply, so every acknowledged request is durable on the
  container's block-backed root filesystem.
- `GET /status` reports the count, the per-execution instance ID, whether work
  is admitted, and the identity bound by the latest resume.
- `GET /readyz` returns 503 while work is not admitted.

The in-container supervisor listens on `/brie/supervisor.sock` with
owner-only permissions. It is not a network service.

- **prepare** closes admission (new requests receive 503), waits for in-flight
  requests until the prepare deadline, fsyncs the state file and its directory,
  and reports safe point `counter-<count>` with the acknowledged-request
  watermark `<count>`. On timeout or a state-file mismatch it reopens
  admission and refuses.
- **resume** re-reads the state file and refuses unless it still matches the
  safe point, binds the supplied Sandbox UID, Pod UID, and epoch, replaces the
  instance ID from fresh guest randomness mixed with the host nonce, and only
  then reopens admission. The supervisor refuses a resume whose wall clock
  differs from the guest clock by more than two seconds, so the guest must
  correct its clock first.
- **abort** reopens admission with the existing identity.

## Host path

The host never connects to the socket directly. It runs the relay through
CRI `ExecSync`, which Kata executes through its own agent:

```text
/brie-guestctl --socket=/brie/supervisor.sock '<canonical request JSON>'
```

`brie-guestctl` prints exactly one canonical response line. The Go host side is
`guestsupervisor.ExecClient`. Nothing in `brie-node` calls it yet.

## Image

[Dockerfile](Dockerfile) packages the two static binaries into a `scratch`
image that runs as UID/GID 65532, matching the restricted Pod profile's
non-root requirement. Use the image only by digest. The project has not built
or published it.
