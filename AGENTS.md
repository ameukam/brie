# Working on Brie

- Read the relevant source and `CONTRIBUTING.md` before changing code.
  The CLI only reports its version; do not claim node registration, workload
  execution, or hibernation works.
- Keep changes small and focused on the requested task. Use the upstream
  Kubernetes scheduler; do not introduce a separate scheduler.
- Use the Go version in `go.mod`. Prefer the standard library, explicit errors,
  bounded concurrency, and contexts for blocking operations.
- Firecracker is the only planned runtime. Keep VM mutation behind runtime
  ownership rather than adding direct calls from the node agent.
- Use `make build` and `make check`, plus `make e2e` for CLI or test-harness
  changes. Follow the tool configuration in CI and the Makefile.
  Do not weaken checks to make a change pass.
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
- Report blocked or unverified work plainly. The kind/Sonobuoy baseline tests
  upstream Kubernetes, not Brie conformance.
