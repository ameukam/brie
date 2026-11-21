# Hardware lab harness

These scripts would provision, install, exercise, fence, and tear down a
disposable C4A lab for the milestones and gates in `DESIGN.md`. They are tested
only against a mock `gcloud` (`go test ./tests/hw`).

**None of these scripts ran against real Google Cloud in session r20261113.**
No project, zone, or spend ceiling was named for that session, so no cloud
resource was created, read, or deleted, and no hardware result exists. Treat
every script as unit-tested against a mock, not hardware-verified.

## Order of operations

1. Fill a copy of `checksums.template` and a price table; check both locally.
2. `provision.sh` (aborts and tears down on failure or a 45-minute stockout).
3. `run-on-host.sh` with `install-control-plane.sh` on a standard host and
   `install-host.sh` on each metal host; then install and start `brie-node`
   (not scripted here; see `docs/pod-profile.md`).
4. `m1-harness.sh`, then `node-conformance.sh` through `run-on-host.sh`.
5. `fence.sh` for M2 fencing experiments.
6. `teardown.sh`, which ends with `verify-empty.sh`. Run it even after a
   failure; it only deletes resources carrying the run's marker.

`go test ./tests/hw` (part of `make test`) runs every script against a mock
`gcloud` and `kubectl` placed first on `PATH`, with an empty `CLOUDSDK_CONFIG`
and `HOME`, and a simulated clock. The mock records each call and fails any
call without the explicit project or any `gcloud config` call.

## Safety guards

Every script that calls `gcloud` sources `lib.sh`, which:

- refuses to run unless `APPROVE_COSTS=yes`;
- requires `PROJECT`, `ZONE`, `ZONE2`, `RUN_ID`, and `SPEND_CEILING_USD`, with no
  defaults; `ZONE2` must be a different zone in the same region;
- never runs `gcloud config` or reads the active configuration, passes
  `--project=$PROJECT` as the first flag of every call, and also exports
  `CLOUDSDK_CORE_PROJECT=$PROJECT`;
- validates `RUN_ID` against `^[a-z][a-z0-9]{2,15}$`; resources are named
  `brie-hw-$RUN_ID-*` and labelled `brie-hw-run=$RUN_ID`. Networks, subnets,
  firewall rules, and routers have no labels, so their description must be
  exactly `brie-hw-run=$RUN_ID`;
- appends every action and gcloud exit status to `$HW_LOG_DIR/actions.log`
  (default `artifacts/hw/$RUN_ID`). The log is never truncated and must not be a
  symlink.

## Teardown and verification

`teardown.sh` deletes, in dependency order, instances, disks, the bucket
(`gcloud storage rm --recursive`), firewall rules, the Cloud NAT and router, the
subnet, and the VPC. It discovers candidates by run label or name prefix, then
re-describes each one immediately before deletion. Candidates without the
run's label or description marker are reported as `REFUSED` and kept. A NAT
has neither, so under an owned router only `brie-hw-$RUN_ID-nat` is deleted;
any other NAT is refused and the router is kept for it. It then
runs `verify-empty.sh`, which lists every type by label and prefix and fails
unless all lists succeed and are empty. Teardown exits nonzero on any refusal,
failed call, or remaining resource.

```sh
APPROVE_COSTS=yes PROJECT=<project> ZONE=<zone> ZONE2=<zone> RUN_ID=<id> \
  SPEND_CEILING_USD=<usd> tests/hw/teardown.sh
```

## Provisioning

`provision.sh` also requires `PRICE_TABLE`, `PLANNED_HOURS`, `METAL_COUNT`
(1 to 4 `c4a-highmem-96-metal` hosts), and `STANDARD_COUNT` (0 to 2
`c4a-standard-8` hosts). The operator supplies the price table; there is no
built-in price list. Each non-comment line is `<key> <USD per unit-hour>`, and
every key below must appear exactly once:

```text
c4a-highmem-96-metal <USD per instance-hour>
c4a-standard-8 <USD per instance-hour>
hyperdisk-balanced-gib <USD per GiB-hour>
cloud-nat-gateway <USD per gateway-hour>
fixed-overhead <USD per hour for the bucket, NAT data, egress, and anything else>
```

The script refuses the whole plan before any create if the projected hourly
rate times `PLANNED_HOURS` exceeds `SPEND_CEILING_USD`, and checks the running
total again before each priced create. It creates a custom-mode VPC, one
regional subnet with private Google access, internal and IAP-only SSH firewall
rules, a Cloud Router with NAT, one bucket (uniform access, public access
prevention, objects deleted after 7 days), and the hosts. Hosts use Ubuntu
24.04 Arm64 (`ubuntu-2404-lts-arm64`), Hyperdisk Balanced boot disks, no
external address, no service account, and IP forwarding for Pod routes. Each
metal host gets a labelled persistent data disk attached as `brie-data`.
Optional settings: `BOOT_DISK_GB` (100), `DATA_DISK_GB` (200), `SUBNET_RANGE`
(`10.200.0.0/20`), `POD_RANGE` (`10.244.0.0/16`), `STOCKOUT_RETRY_SECONDS` (60).

An instance create that fails with a stockout in `ZONE` is retried in `ZONE2`,
then both are retried after the retry interval. After 45 minutes of total
stockout, or on any other failure after the first create, provisioning aborts
and runs `teardown.sh`. The inventory is written to
`$HW_LOG_DIR/inventory.tsv`; a second run with the same log directory refuses.

The bucket and boot disks receive their labels in a second call after creation.
If that call fails, teardown refuses the unlabelled resource and provisioning
reports `TEARDOWN INCOMPLETE`; delete it by hand after checking it.

## Fencing (D5)

`fence.sh INSTANCE ZONE` is the provisional lab fence for DESIGN.md section
5.3. It re-describes the instance and refuses unless it carries the run label.
It records the instance id and creation timestamp as the incarnation, issues
`gcloud compute instances stop`, and polls (`FENCE_POLL_SECONDS`, default 10)
until the same incarnation reports `TERMINATED` (`FENCE_TIMEOUT_SECONDS`,
default 600). It then checks the label on `INSTANCE-data`, detaches it, and
verifies the disk no longer lists the instance as a user. It prints one JSON
evidence object (`brie.hw.fence/v1`) and appends it to
`$HW_LOG_DIR/fence-evidence.jsonl`. It exits zero only when `verified` is true.
If the instance never reaches `TERMINATED`, the disk is left attached and the
evidence records why the fence is unverified.

The fence does not stop an authorized principal from starting the instance
again; evidence describes one observed point in time. It has never been run
against a real instance.

## Host installation (D4, D9, D11)

Copy `checksums.template`, verify every URL and SHA-256 against the upstream
releases yourself, and fill in all placeholders. The scripts refuse any
placeholder, a D4 version other than the pinned one, non-HTTPS URLs or hosts
other than GitHub releases and `dl.k8s.io`, URLs that do not name their
version, malformed digests, and duplicate records. Check a file locally with
`install-host.sh --check-only FILE` and `install-control-plane.sh --check-only FILE`.

`run-on-host.sh INSTANCE ZONE SCRIPT [FILE...]` refuses instances without the
run label, copies the script, `checksums.sh`, and the input files over IAP
SSH, and runs the script as root. It forwards `NODE_NAME`, `POD_CIDR`,
`SERVICE_CIDR`, and `ADVERTISE_ADDRESS` when set, and refuses values outside
`[A-Za-z0-9./_-]`.

`install-host.sh CHECKSUMS` requires root, Arm64, Ubuntu 24.04, and `/dev/kvm`.
It installs containerd 2.4.2, Kata 4.3.1 (whose Firecracker must report
1.17.0), CNI plugins 1.9.1, and crictl 1.37.0; writes the D9 digest manifest
`/etc/brie/hw-digests.sha256` from the operator's `binary` records; and verifies
`firecracker`, `jailer`, `containerd-shim-kata-v2`, and `kata-runtime` against
it before configuring containerd. Re-verify later with
`install-host.sh --verify-manifest`, which first requires the manifest to list
each of those four `/opt/kata/bin` paths exactly once and nothing else. It configures a `kata-fc` runtime on a
loopback devmapper thin pool (16 GiB data, 256 MiB metadata, recreated at boot)
and a bridge CNI template filled from the Pod CIDR Brie sends through CRI. It
records the Kata Firecracker settings `jailer_path` and
`disable_guest_seccomp` without changing them. Evidence goes to
`/var/lib/brie-hw/evidence/install-host.txt` with a SHA-256 file.

`install-control-plane.sh CHECKSUMS` installs containerd with runc and kubeadm,
kubelet, and kubectl 1.37.x from `dl.k8s.io`, runs `kubeadm init` with the Node
authorizer, NodeRestriction, and kube-proxy in iptables mode, verifies those
settings, and writes a bridge CNI configuration for the control-plane Pod CIDR.
It passes `--skip-token-print` so the bootstrap join token never reaches the
output that `run-on-host.sh` forwards. Its evidence records versions and
settings, never a kubeconfig or token.

Not covered: Pod routes between hosts, kube-proxy on Brie workers, Brie node
credentials, and installing `brie-node` itself.

## M1 harness

`m1-harness.sh` runs the DESIGN.md section 15 M1 sequence against one labelled
workload host. Besides the `lib.sh` inputs it requires `M1_KUBECONFIG` (an
absolute path, passed to every `kubectl` call), `WORKER`, `WORKER_ZONE`,
`NODE_NAME`, and a digest-pinned BusyBox `M1_IMAGE`. `RUNTIME_CLASS` defaults
to `kata-fc` and `AGENT_UNIT` to `brie-node.service`. It submits a restricted
Pod without `spec.nodeName` and records each step in `results.tsv`:

| Step | How it is checked |
| --- | --- |
| startup | Pod Ready on `NODE_NAME`, Running, with an IP |
| pod-network | HTTP from the worker host to the Pod IP returns the guest boot ID |
| probes, exec, logs | Exercised only when listed in `M1_FEATURES`; otherwise `SKIPPED-UNSUPPORTED` |
| volumes, warm-pause-resume | Always `SKIPPED-UNSUPPORTED`; Brie has neither |
| agent-restart | Firecracker PIDs, CRI container IDs, and guest boot ID are identical before and after restarting `AGENT_UNIT` |
| cleanup | The Pod and its CRI sandbox are gone after deletion |
| service-and-dns, deletion-races, memory-footprint | `NOT-RUN`; the harness has no step for them yet |

A skipped or not-run step is never reported as PASS. The harness exits 0 only
if every step passed, 1 on any failure, and 2 when steps were skipped or not
run. Evidence goes to `$HW_LOG_DIR/m1-<timestamp>/` with `SHA256SUMS`; it holds
Pod manifests and status, versions, and host observations, never the
kubeconfig. The harness assumes `brie-node` already runs on the host as
`AGENT_UNIT`.

## Node Conformance (G1)

`node-conformance.sh CHECKSUMS` runs as root on the control-plane host with
`NODE_NAME` set to the Brie node. It downloads the pinned
`kubernetes-test-linux-arm64` 1.37 tarball, checks that the server version
matches, that the Brie node carries its `brie.dev/runtime` taint, and that
every other node keeps the kubeadm control-plane taint, so test Pods cannot
silently run on a kubelet node. It then runs `e2e.test` with
`--ginkgo.focus='\[NodeConformance\]'` and no skip filter.
`--non-blocking-taints` includes the Brie taint only so the suite can start.

Results go to `/var/lib/brie-hw/evidence/node-conformance-<timestamp>/`:
JUnit reports, `e2e.log`, `summary.txt`, `failures.txt` listing every failed
or errored test, and `SHA256SUMS`. Publish all of `failures.txt`. The script
exits nonzero if any test failed or none ran. `--summarize DIR` regenerates the
summary from existing JUnit files.

These are the `[NodeConformance]`-tagged tests of the cluster e2e suite, not
the kubelet-local `e2e_node` runner, which assumes a kubelet. The restricted
Pod profile rejects most test Pods, so failures are expected; they are results
to publish, not filters to add.
