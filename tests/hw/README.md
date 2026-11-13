# Hardware lab harness

These scripts would provision, install, exercise, fence, and tear down a
disposable C4A lab for the milestones and gates in `DESIGN.md`. They are tested
only against a mock `gcloud` (`go test ./tests/hw`).

**None of these scripts ran against real Google Cloud in session r20261113.**
No project, zone, or spend ceiling was named for that session, so no cloud
resource was created, read, or deleted, and no hardware result exists. Treat
every script as unit-tested against a mock, not hardware-verified.

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
run's label or description marker are reported as `REFUSED` and kept. It then
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
