# Contributing to Brie

Brie currently provides a version CLI, not a working Kubernetes node agent.
Keep changes focused and discuss new dependencies or runtime interfaces before
implementing them.

## Local checks

Install Go from [go.mod](go.mod), Make, and the golangci-lint and GoReleaser
versions in [CI](.github/workflows/ci.yml). Then run:

```sh
make build
make check
make e2e
```

`make check` runs formatting checks, vet, race tests, bounded fuzzing, lint,
govulncheck, and release configuration validation. Statement coverage must
exceed 80%. `make e2e` also needs jq; it checks CLI binaries and the conformance
result parser.

Use `make fmt` to format Go code. Add regression tests for behavior changes and
keep reproducible fuzz failures in the Go fuzz corpus. Prefer the standard
library, pass contexts to blocking operations, and return useful errors.

Update [CHANGELOG.md](CHANGELOG.md) for user-visible changes. Commit `go.sum`
when adding dependencies, but do not commit binaries, coverage output,
credentials, or release artifacts.

## Kubernetes baseline

The [baseline workflow](.github/workflows/e2e.yml) runs Sonobuoy's `quick` Pod
creation/deletion test on pull requests and main pushes. Weekly and manual runs
use `certified-conformance`, including serial and disruptive tests without
extra skip filters. Missing, skipped, or unsuccessful selected tests fail the run.

For a local run, use a disposable Docker environment with kind v0.33.0,
kubectl v1.37.0, Sonobuoy v0.57.5, and jq:

```sh
make conformance-smoke
make conformance
```

The harness creates its own single-node kind cluster and temporary kubeconfig;
it never targets your current cluster. It deletes its cluster on exit. Full
runs can take four hours, so allow cleanup to finish.

Results default to `artifacts/conformance`; use a separate `ARTIFACTS_DIR` for
each run you want to retain. CI keeps results, versions, and kind logs for
14 days, including failures, but does not upload kubeconfigs.

This tests upstream Kubernetes, not Brie Node Conformance or CNCF certification.
It does not exercise Brie's runtime, node registration, or hibernation.

## Pull requests

Explain what changed and why. Keep unrelated work separate and wait for required
checks before merging. Agent output does not replace human review.

Report suspected vulnerabilities through [SECURITY.md](SECURITY.md), not in a
public issue with reproduction details.

Contributions are licensed under [Apache-2.0](LICENSE).
