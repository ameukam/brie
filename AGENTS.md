# Working on Brie

- Read `README.md` and the relevant sections of `DESIGN.md` before changing code.
  The current implementation is only a version CLI; do not claim the node agent
  or hibernation protocol works.
- Keep changes small and focused on the M0-M1 one-node prototype. Use the upstream
  Kubernetes scheduler; do not introduce a separate scheduler.
- Use the Go version in `go.mod`. Prefer the standard library, explicit errors,
  bounded concurrency, and contexts for blocking operations.
- Runtime owners retain exclusive VM mutation and lifecycle-journal ownership.
  Never bypass them with direct Firecracker mutations from the node agent.
- Use `make build` and `make check`. Tool versions are pinned in the CI workflow
  and Makefile. Do not weaken checks to make a change pass.
- Add table-driven tests and regression cases. Fuzz input-handling changes when
  appropriate. Update `CHANGELOG.md` for user-visible changes.
- Preserve unrelated work. Never commit generated binaries, release artifacts,
  credentials, or tenant data.
- Keep workflow actions pinned to full commit hashes and permissions scoped to
  the jobs that need them. Do not use privileged pull-request triggers to run
  untrusted code.
- Prepare reviewable changes and require independent human review. Do not bypass
  repository rules, publish releases, change GitHub settings, provision resources,
  or operate real VMs without explicit maintainer approval.
- Report blocked or unverified work plainly. Local checks do not prove GitHub
  publishing, repo-agent deployment, deps.dev indexing, or a perfect Scorecard.
