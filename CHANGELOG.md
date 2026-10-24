# Changelog

All notable changes to Brie are recorded here. Stable releases use semantic
versioning. Dated nightly prereleases are development builds, not stable releases.

## [Unreleased]

### Added

- A Go module and version-only `brie-node` command.
- CLI subprocess e2e tests and a pinned kind/Sonobuoy Kubernetes baseline
  workflow with PR smoke tests, weekly/manual full conformance, and retained
  diagnostics. These do not qualify Brie for Node Conformance.
- Race-enabled tests, CLI fuzzing, golangci-lint, and govulncheck.
- GitHub Actions for quality checks, CodeQL, and OpenSSF Scorecard.
- Linux amd64 and arm64 release archives, SHA-256 checksums, and signed build
  provenance on GitHub Releases.
- Dated nightly prereleases.
- Apache-2.0 licensing and contributor guidance.

### Changed

- Reduced the README to an abstract ten-word sentence.

[Unreleased]: https://github.com/ameukam/brie/commits/main
