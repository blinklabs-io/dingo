# Implementation plan

Use flake-parts and treefmt-nix with explicit imports of perSystem/{packages,
devShells,formatter}.nix. Use nixpkgs-unstable's go_1_26 (1.26.11, matching
go.mod); the reference's older nixos-25.11 pin is not appropriate for this Go
requirement. Pin all inputs in flake.lock and set GOTOOLCHAIN=local.

Build Dingo with buildGo126Module, subPackages = [ "cmd/dingo" ], extra plugin
tags and release-style linker flags. Use a fixed vendorHash, obtained through
the standard fixed-output hash discovery. Keep expensive node tests outside
the package's build check phase; run the mandatory conformance and a focused
race test explicitly in the shell. Supply Go, gopls, golines, golangci-lint,
GNU Make, a C compiler, Git and the treefmt wrapper in the shell.

Smoke-test a temporary local devnet using the built binary. Extract the
validated structure to nix-templates/go only after Dingo validation. Use a
dependency-free sample CLI, vendorHash = null and package tests enabled in the
template; document changing these for a real module. Use the same pinned
inputs for the template. Do not export Dingo's spec artifacts into the template.

Existing AGENTS.md/CLAUDE.md are the governing project rules. No constitution
or implementation behavior is changed. DATABASE.md and ARCHITECTURE.md require
no changes because only build and contributor configuration is affected.

Additional requested network wrappers use writeShellApplication and exec the
same package with --network. Set CARDANO_DATABASE_PATH only if unset, using
.dingo-<network>, so explicit environment and CLI overrides remain available.
These are Dingo-specific and do not belong in the reusable Go template.
