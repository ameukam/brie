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
