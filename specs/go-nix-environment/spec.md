# Go Nix development environment

Created: 2026-11-24. Work in the existing checkout, as requested; no numbered
branch or repository layout change.

## User stories

1. P1: As a Dingo contributor, enter a pinned Nix shell, build Dingo and run
   race tests without installing Go globally. Given only Nix and this checkout,
   `nix develop` provides Go at least as new as go.mod, Make, and a C compiler.
2. P1: As a node operator, build and run Dingo with `nix build` and `nix run`.
   Given the lock file, the build resolves dependencies by content hash and
   produces a binary that can start a local devnet and shut down cleanly.
3. P2: As a future Go project author, initialize the Go template from
   nix-templates, then enter its shell, build, test and run its sample CLI.
   The template follows the Rust template's flake-parts structure and contains
   no Dingo-specific names, dependencies or configuration.

## Requirements and success criteria

- Pin flake inputs and select Go explicitly; disable automatic toolchain downloads.
- Provide shell, default package, runnable default app and Nix formatting.
- Preserve Go modules, Make targets and node behavior.
- Validate shell tools, Dingo package build, ledger conformance, a race test and
  live devnet startup/block production/shutdown before extracting the template.
- Validate template initialization in a clean directory, its build/run, shell,
  tests and formatting. Register it as `templates.go`.
- Document normal commands, dependency hash refresh and customization.
- Provide preview, preprod and mainnet package/app wrappers that pass through
  arguments and signals and default to separate network data directories.

## Assumptions and edge cases

Target x86_64-linux first, matching the reference Rust template. Initial fetches
need network; subsequent builds use Nix caches. Dependency hash acquisition is
an intentional fixed-output discovery step, not a passing validation check.
An incompatible Go update must fail visibly rather than download a toolchain.
Do not commit or push. Keep existing template backup files untouched.
