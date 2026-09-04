# Dingo documentation

This directory is the documentation entry point for the Dingo source tree. It
is intended for people, coding agents, and other tools that inspect a checkout
before changing it.

Dingo is under active development. For code behavior, prefer documentation
from the revision you have checked out. The public site describes supported
installation and operation and may track a released version rather than the
current branch.

## Find the right documentation

| Question | Source |
| --- | --- |
| How do I build, configure, or run Dingo? | The repository [README](../README.md) and [`dingo.yaml.example`](../dingo.yaml.example) |
| How are components wired and what owns a behavior? | [Architecture](../ARCHITECTURE.md) |
| How are storage, schemas, and persisted formats designed? | [Database reference](../DATABASE.md) |
| How does secure from-origin synchronization work? | [Ouroboros Genesis operator guide](../GENESIS_SYNC.md) |
| What does a Go package or exported symbol do? | [Go code reference](code-reference.md), package `doc.go` files, and `go doc` |
| How do I add a compiled-in provider? | [Plugin development](../database/plugin/PLUGIN_DEVELOPMENT.md) |
| How do I run conformance or end-to-end tests? | [Conformance tests](../internal/test/conformance/README.md) and [DevNet](../internal/test/devnet/README.md) |
| How do I exercise archive and history-expiry behavior? | [Archive node demo](../internal/test/archive-demo/README.md) |
| Where are runnable API examples? | [Examples](../examples/README.md) |
| How do I install the Grafana dashboards? | [Dashboards](dashboards/README.md) |

Contributor rules live in [`AGENTS.md`](../AGENTS.md) and
[`CLAUDE.md`](../CLAUDE.md). The repository `Makefile` is the source of truth
for build and validation targets.

## Code documentation

Go package comments live beside the implementation, usually in a `doc.go`
file. This makes them available to source browsers, language servers,
[pkg.go.dev](https://pkg.go.dev/github.com/blinklabs-io/dingo), and the `go doc`
command. Render them from the current checkout instead of relying on a cached
copy:

```sh
go doc ./ledger
go doc ./ledger LedgerState
go doc -all ./database
```

The [Go code reference](code-reference.md) maps the main documented packages
and includes commands for discovering additional package and symbol docs.

## Public documentation

Operator-facing installation, quick-start, Cardano CLI, stake-pool, monitoring,
and release documentation is published in the
[Dingo guide on docs.blinklabs.io](https://docs.blinklabs.io/guides/dingo/001-dingo/).

The same public documentation is available in machine-readable forms:

- [`llms.txt`](https://docs.blinklabs.io/llms.txt) indexes every documentation
  set.
- The [Cardano nodes and operations set](https://docs.blinklabs.io/_llms-txt/cardano-nodes-and-operations.txt)
  contains the focused Dingo and node-operations material.
- [`llms-full.txt`](https://docs.blinklabs.io/llms-full.txt) contains the full
  public documentation corpus.

Use repository documentation and `go doc` for details tied to the checked-out
source. Use the public site for released installation and operational guidance;
release notes are historical context, not the current code contract.

## Source priority for code changes

When sources overlap, use this order:

1. The checked-out implementation and its tests.
2. Package comments and exported-symbol comments rendered with `go doc`.
3. Versioned repository documents such as `ARCHITECTURE.md`, `DATABASE.md`, and
   the relevant test or subsystem README.
4. Public documentation for the supported operator experience.

Update the closest source of truth with a change. Link to it from this index
when a new documentation area is added instead of copying a large explanation
here.
