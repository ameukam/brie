# Contributing to Brie

Brie is at the prototype stage. Start with small changes scoped to the
one-node milestones in [DESIGN.md](DESIGN.md). Discuss new dependencies,
runtime contracts, and architectural changes in an issue before implementing
them.

## Local workflow

Install the Go version specified in `go.mod`, Make, and the golangci-lint and
GoReleaser versions pinned in `.github/workflows/ci.yml`. Use the upstream
installation instructions and verify downloaded binaries. Git is needed for
local GoReleaser snapshot builds.

```sh
make build
make check
```

Use `make fmt` to format Go code. `make test` runs table-driven tests with the
race detector and enforces greater than 80% statement coverage.
`make fuzz` performs a bounded fuzzing run; failing inputs belong in the Go
fuzz corpus when they reproduce a real bug.

Keep APIs small, pass contexts to blocking operations, and wrap errors with
useful context. Prefer standard-library functionality and consumer-defined
interfaces. Add tests for behavior changes and retain regression cases.
Update `go.mod` and commit `go.sum` when adding dependencies; do not commit
generated binaries, coverage output, or release archives.

Update `CHANGELOG.md` under `Unreleased` for user-visible changes.

## E2E and Kubernetes baseline

`make e2e` requires Go from `go.mod` and `jq`. It builds and executes development
and release-version CLI binaries, checking exit codes, stdout, and stderr. It
also tests the baseline result gate against empty, skipped, failed, unknown,
and malformed results.

The [Kubernetes baseline workflow](.github/workflows/e2e.yml) runs Sonobuoy's
`quick` Pod creation/deletion test on PRs and main pushes. Weekly and manual
runs use `certified-conformance`: the complete upstream `[Conformance]` selection,
including serial and disruptive tests, without extra skip filters. Results
fail closed if selected tests are absent, skipped, or unsuccessful.

To reproduce on a **disposable local Docker environment**, install kind v0.33.0,
kubectl v1.37.0, Sonobuoy v0.57.5, and jq, then run:

```sh
make conformance-smoke
make conformance
```

The harness creates a uniquely named, single-node kind cluster using the stock
upstream scheduler and kubelet, an isolated temporary kubeconfig, and pinned node,
Sonobuoy, and conformance images. It never targets the current kubeconfig. Each
invocation deletes only its own cluster on exit. Full runs can take four hours;
do not interrupt cleanup. Set `ARTIFACTS_DIR` to retain separate run directories
(default: `artifacts/conformance`). The workflow checks tool download hashes
and preserves the Sonobuoy archive (including JUnit), detailed results, versions,
and kind logs for 14 days, including on failure. Kubeconfigs are not uploaded.

This is an **upstream Kubernetes cluster baseline**, not Brie Node Conformance
or CNCF certification. Brie still implements only a version CLI. No node-agent,
Brie runtime/guest execution, Firecracker, Arm64/KVM, restart-survival, or
hibernation behavior is exercised.
The chosen Kubernetes version is a baseline pin, not a resolved D4 support matrix.
Before claiming G1 in [DESIGN.md](DESIGN.md), implement and qualify the M0-M1
node/runtime stack, pin its complete compatibility profile, run the complete
upstream **Node Conformance** suite against Brie on that profile, publish the
results, and resolve every failure. Application lifecycle tests remain separate.

## Pull requests

Explain what changed and why. Keep unrelated work separate and make the
required checks pass. Changes need independent human review; an agent's review
does not replace it. Treat contributors respectfully and give actionable
feedback.

Repo-agent may prepare drafts and propose changes, but may not bypass branch
protection, approve its own work, or publish releases without maintainer
approval. Do not include credentials, tenant data, or private infrastructure
details in issues, logs, or patches.

For suspected vulnerabilities, follow [SECURITY.md](SECURITY.md) rather than
opening a public issue with sensitive details.

Contributions are licensed under [Apache-2.0](LICENSE).
