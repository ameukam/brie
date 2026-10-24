# Registration e2e

Run `make registration-e2e` with Go from `go.mod`, Docker, kind v0.33.0,
and kubectl v1.37.0. The harness creates and deletes its own Kubernetes
v1.37.0 cluster and temporary kubeconfig; it never uses your current cluster.

The test builds Brie, issues a short-lived node certificate, and checks:

- Registration stays NotReady, cordoned, tainted, and at zero capacity.
- Lease renewals, heartbeat loss, and restart preserve Node identity.
- The node cannot read other Nodes, other Leases, or secrets, or modify the
  control-plane Node.
- Removing the scheduling guard stops the agent with an error.

Node and Lease resource-version conflicts are retried a bounded number of times,
with identity and scheduling guards checked again. Other API failures stop the
agent.

Results and kind logs default to `artifacts/registration`. Set `ARTIFACTS_DIR`
to a separate directory to retain each run. Credentials and agent binaries stay
in temporary directories and are not uploaded by CI.

This tests registration only, not Pod execution, Firecracker, or conformance.
