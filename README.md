# Dingo

<div align="center">
  <img src="./.github/assets/dingo-logo-with-text-horizontal.png" alt="Dingo Logo" width="640">
  <br>
  <img alt="GitHub" src="https://img.shields.io/github/license/blinklabs-io/dingo">
  <a href="https://pkg.go.dev/github.com/blinklabs-io/dingo"><img src="https://pkg.go.dev/badge/github.com/blinklabs-io/dingo.svg" alt="Go Reference"></a>
  <a href="https://discord.gg/5fPRZnX4qW"><img src="https://img.shields.io/badge/Discord-7289DA?style=flat&logo=discord&logoColor=white" alt="Discord"></a>
</div>

> ⚠️ **WARNING: Dingo is under heavy active development and is not yet ready for
> mainnet block production use. It should only be used on testnets (preview,
> preprod, musashi, dijkstra) and devnets. Do not use Dingo on mainnet for block
> production with real funds.**

Dingo is Blink Labs' Cardano node implementation in Go. It implements the
Ouroboros networking and consensus protocols, validates ledger state, and
provides pluggable storage and client interfaces.

## Documentation by audience

- **Node operators:** [Dingo
  guides](https://docs.blinklabs.io/guides/dingo/001-dingo/), including the
  [quick
  start](https://docs.blinklabs.io/guides/dingo/002-quick-start-overview/),
  [configuration and storage
  modes](https://docs.blinklabs.io/guides/dingo/005-node-configuration/),
  [bootstrap and data
  maintenance](https://docs.blinklabs.io/guides/dingo/007-bootstrap-and-data-maintenance/),
  and [stake pool
  operation](https://docs.blinklabs.io/guides/dingo/spo-guides/000-spo-guide/).
- **Application developers:** [APIs and archive
  services](https://docs.blinklabs.io/guides/dingo/006-apis-and-archive/) and
  [using Dingo with Cardano
  CLI](https://docs.blinklabs.io/guides/dingo/004-using-dingo-with-cardano-cli/).
- The Kupo-compatible API is disabled by default. To enable it, configure
  `DINGO_PLUGINS_API_KUPO_CONFIG_PORT` and use API storage mode.
- **Dingo contributors:** [development guide](docs/development.md), [local
  DevNet](docs/devnet.md), [benchmarks and profiling](docs/benchmarks.md),
  [architecture](ARCHITECTURE.md), [database design](DATABASE.md), and [plugin
  development](database/plugin/PLUGIN_DEVELOPMENT.md).

## Approximate network sync times and resource use

These baselines are from Dingo v0.90.5, captured November 4–5, 2026. Times
measure launch through Mithril bootstrap completion; API times include historical
metadata backfill and finalization. Mithril times use a snapshot and are not
from-genesis sync times. Hardware, snapshot size, and network conditions affect
results, so treat these as planning estimates rather than guarantees.

| Network | Core mode | API mode |
| --- | --- | --- |
| Preview | ~13 minutes (Mithril) | ~4 hours 41 minutes (Mithril) |
| Preprod | ~16 minutes (Mithril) | ~3 hours 43 minutes (Mithril) |
| Mainnet | ~6 hours 2 minutes (Mithril) | No completed measurement |
| Musashi | No full duration; full sync is from genesis | Not measured |
| Dijkstra | Not measured | Not measured |

Resource observations are approximate. CPU and RSS are sampled process values;
Preview and Preprod Core were sampled about once per minute on an 8-vCPU, 32-GiB
host. The Mithril cache was retained during these captures and is temporary.

| Network and mode | Approximate resource observations |
| --- | --- |
| Preview Core | Sampled peak ~16 GiB RSS and ~2.1 CPU cores; near completion, the Dingo database was ~21 GiB and the retained Mithril cache was ~16 GiB. |
| Preprod Core | Sampled peak ~17 GiB RSS and ~1.9 CPU cores; near completion, the Dingo database was ~23 GiB and the retained Mithril cache was ~20 GiB. |
| Mainnet Core | The input Mithril snapshot was ~221 GiB. The capture did not include process RSS or final Dingo database size. |
| Preview and Preprod API | No process RSS or storage time series was captured. |
| Musashi Core | A separate v0.89.0 from-genesis interval advanced 981 blocks in 302 seconds (~3.25 blocks/second). A 30-second CPU profile averaged ~1.9 cores; a nearby RSS sample was ~4.8 GiB. This is not a full-sync measurement. |
| Dijkstra and Musashi API | No comparable resource capture was available. |

The Musashi measurements are from v0.89.0; all other completed sync times above
are from v0.90.5. These figures do not represent v0.91.0 measurements.

## Codebase

| Area | Location |
| --- | --- |
| Application composition and CLI | `node.go`, `internal/node/`, `cmd/dingo/` |
| Ledger validation and state | `ledger/`, `ledgerstate/` |
| Ouroboros protocols and peer management | `ouroboros/`, `connmanager/`, `peergov/`, `topology/` |
| Persistence and transaction pool | `database/`, `mempool/` |
| Client APIs and Dingo archive service | `api/`, `bark/` |

## Build and test

The `@blinklabs/dingo` NPM package installs the matching release binary for
Linux, FreeBSD or macOS on amd64/arm64. It requires Node.js 22 or
later and `tar`. Run it with `npx @blinklabs/dingo --help`, or install it with
`npm install --global @blinklabs/dingo` and run `dingo --help`. Arguments, exit
codes and termination signals are passed to the release binary. Installation
fails if the platform has no released binary or the archive cannot be fetched
and extracted. If install scripts were disabled for a global installation,
`npm rebuild --global @blinklabs/dingo` runs the installer.

`npm test` checks the packed package and local, global and npx invocation using
an isolated release archive fixture.

Use Go 1.26 or later and `make`:

```sh
make build
./dingo --help
./dingo devnet

# Keep the chain between runs, then reset it when needed:
./dingo devnet --data-dir ./.dingo-devnet
./dingo devnet --data-dir ./.dingo-devnet --reset
```

The npm package supports the same commands, including
`npx @blinklabs/dingo devnet --data-dir ./.dingo-devnet`.

`dingo devnet` writes test funding credentials to `cardano/keys/` under its
state directory: `faucet.skey`, `faucet.vkey`, and `faucet.addr`. The faucet
address starts with 900,000 test ADA; use its signing key to make transactions
on network magic `42`. With the persistent command above, read the address with
`cat ./.dingo-devnet/cardano/keys/faucet.addr`. Without `--data-dir`, the files
are in the temporary directory printed at startup and are removed on exit.
These public test keys must never hold real funds. See the
[local DevNet guide](docs/devnet.md) for details.

Run the test suite with race detection using `make test`. See the [development
guide](docs/development.md) for repository checks, integration environments, and
profiling. For basic node setup and usage, follow the [operator quick
start](https://docs.blinklabs.io/guides/dingo/002-quick-start-overview/).

## Repository documentation

- [Development](docs/development.md)
- [Architecture](ARCHITECTURE.md)
- [Database](DATABASE.md)
- [MCP integration](docs/mcp/README.md)
- [Local DevNet](docs/devnet.md)
- [Benchmarks and profiling](docs/benchmarks.md)
- [Monitoring dashboards](docs/dashboards/README.md)
- [Badger garbage collection](docs/badger-gc.md)
