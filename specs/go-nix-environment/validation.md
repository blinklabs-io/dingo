# Validation

Validated on x86_64 Linux with the committed input pins and Go 1.26.11.

## Passed

- Development shell: Go version, GOTOOLCHAIN=local, CGO_ENABLED=1, compiler,
  Make, gopls, golines, golangci-lint and treefmt available.
- `nix build` and `nix run . -- --help`.
- `make build` in the shell: all four command binaries; module files unchanged.
  Removed these generated binaries afterward with `make clean`; the Nix
  package remains available through `result`.
- `go test -race ./internal/version`.
- `go test ./internal/test/conformance/`.
- `go test ./internal/docsparity` and `go test ./internal/architecture`.
- `go test ./ouroboros/ -run TestConsensusConformance`.
- `go test -race ./...` from `sdk/`.
- Packaged `dingo devnet`: 44 successfully produced blocks; HTTP health
  reported healthy, latest-block endpoint returned height 19/slot 21 during
  the run; SIGINT caused graceful shutdown with exit 0. Temporary state was
  removed automatically.
- `nix build .#preview .#preprod .#mainnet --no-link`: wrappers and ShellCheck.
- All three network apps invoked through `nix run .#<network> -- --help`.
  Shell tracing also verified preview's default data directory, preservation of
  an explicit CARDANO_DATABASE_PATH, and forwarding of --data-dir/--help.
- `nix flake check`, Nix formatting and `git diff --check`.
- Clean `nix flake init -t path:<nix-templates>#go` outside either repository:
  flake checks, build, run (Hello, Go!), shell Go/compiler versions,
  GOTOOLCHAIN=local, `go test -race ./...` (sample has no tests), gofmt and
  Nix formatting checks.
- Template input pins match Dingo's. Existing templates remain registered.

## Scope

The full Go race suite, full lint/analyzers, configuration/container parity,
vulnerability scan and live multi-node/reference conformance were not run:
changes affect Nix and documentation, with no Go, SQL, embedded configuration,
consensus or protocol changes. Public-network wrappers were built and
invoked for help; no public-network chain synchronization was attempted.

Updated docs/development.md and the template README. DATABASE.md and
ARCHITECTURE.md were checked and are not affected. No commits or pushes.
