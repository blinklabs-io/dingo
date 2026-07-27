# Dingo

<div align="center">
  <img src="./.github/assets/dingo-logo-with-text-horizontal.png" alt="Dingo Logo" width="640">
  <br>
  <img alt="GitHub" src="https://img.shields.io/github/license/blinklabs-io/dingo">
  <a href="https://goreportcard.com/report/github.com/blinklabs-io/dingo"><img src="https://goreportcard.com/badge/github.com/blinklabs-io/dingo" alt="Go Report Card"></a>
  <a href="https://pkg.go.dev/github.com/blinklabs-io/dingo"><img src="https://pkg.go.dev/badge/github.com/blinklabs-io/dingo.svg" alt="Go Reference"></a>
  <a href="https://discord.gg/5fPRZnX4qW"><img src="https://img.shields.io/badge/Discord-7289DA?style=flat&logo=discord&logoColor=white" alt="Discord"></a>
</div>

> ⚠️ **WARNING: Dingo is under heavy active development and is not yet ready for production use. It should only be used on testnets (preview, preprod) and devnets. Do not use Dingo on mainnet with real funds.**

A high-performance Cardano blockchain node implementation in Go by Blink Labs. Dingo provides:
- Full chain synchronization and validation via Ouroboros consensus protocol
- UTxO tracking with 41 UTXO validation rules and Plutus V1/V2/V3 smart contract execution
- Block production with VRF leader election and stake snapshots
- Multi-peer chain selection with density comparison and VRF tie-breaking
- Client connectivity for wallets and applications
- Pluggable storage backends (Badger, SQLite, PostgreSQL, MySQL, GCS, S3)
- Tiered storage modes ("core" for consensus, "api" for full indexing)
- Peer governance with dynamic peer selection, ledger peers, and topology support
- Chain rollback support for handling forks with automatic state restoration
- Fast bootstrapping via built-in Mithril client
- Multiple external interfaces: general-purpose APIs (UTxO RPC, Blockfrost-compatible REST, Mesh/Rosetta) plus Bark for Dingo-to-Dingo C2 and archive services

Note: On Windows systems, named pipes are used instead of Unix sockets for node-to-client communication.

<div align="center">
  <img src="./.github/dingo-20241210.png" alt="dingo screenshot" width="640">
</div>

## Running

Dingo supports configuration via a YAML config file (`dingo.yaml`), environment variables, and command-line flags. Priority: CLI flags > environment variables > YAML config > defaults.

A sample configuration file is provided at `dingo.yaml.example`. You can copy and edit this file to configure Dingo for your local or production environment.

### Environment Variables

The following environment variables modify Dingo's behavior:

- `CARDANO_BIND_ADDR`
  - IP address to bind for listening (default: `0.0.0.0`)
- `CARDANO_CONFIG`
  - Full path to the Cardano node configuration (default:
    `./config/cardano/preview/config.json`)
  - Use your own configuration files for different networks
  - Genesis configuration files are read from the same directory by default
- `CARDANO_DATABASE_PATH`
  - A directory which contains the ledger database files (default:
    `.dingo`)
  - This is the location for persistent data storage for the ledger
- `CARDANO_INTERSECT_TIP`
  - Ignore prior chain history and start from current position (default:
    `false`)
  - This is experimental and will likely break... use with caution
- `CARDANO_METRICS_PORT`
  - TCP port to bind for listening for Prometheus metrics (default: `12798`)
- `CARDANO_NETWORK`
  - Named Cardano network (default: `preview`)
- `CARDANO_PRIVATE_BIND_ADDR`
  - IP address to bind for listening for Ouroboros NtC (default:
    `127.0.0.1`)
- `CARDANO_PRIVATE_PORT`
  - TCP port to bind for listening for Ouroboros NtC (default: `3002`)
- `CARDANO_RELAY_PORT`
  - TCP port to bind for listening for Ouroboros NtN (default: `3001`)
- `CARDANO_SOCKET_PATH`
  - UNIX socket path for listening (default: `dingo.socket`)
  - This socket speaks Ouroboros NtC and is used by client software
- `CARDANO_TOPOLOGY`
  - Full path to the Cardano node topology (default: "")
- `DINGO_PLUGINS_API_UTXORPC_CONFIG_PORT`
  - TCP port to bind for listening for UTxO RPC (default: `9090`)
  - Compatibility alias: `DINGO_UTXORPC_PORT`
- `DINGO_PLUGINS_API_BLOCKFROST_CONFIG_PORT`
  - TCP port for the Blockfrost-compatible REST API (default: `3000`)
  - Compatibility alias: `DINGO_BLOCKFROST_PORT`
- `DINGO_PLUGINS_API_MESH_CONFIG_PORT`
  - TCP port for the Mesh (Coinbase Rosetta) API (default: `8080`)
  - Compatibility alias: `DINGO_MESH_PORT`
- `DINGO_BARK_PORT`
  - TCP port for the Bark block archive API (default: `0`, disabled)
- `DINGO_BARK_BASE_URL`
  - Base URL of a remote Bark archive node used for archive fallback
    (default: empty, disabled)
- `DINGO_BARK_BLOCK_DOWNLOAD_HOSTS`
  - Comma-separated HTTPS hostnames additionally allowed for Bark-supplied
    block download URLs. The allowlist always includes the
    `DINGO_BARK_BASE_URL` hostname.
- `DINGO_HISTORY_EXPIRY_ENABLED`
  - Enable local expiry of immutable block CBOR older than the ledger stability
    window (default: `false`)
- `DINGO_HISTORY_EXPIRY_FREQUENCY`
  - How often a history-expiry node scans for old local blocks (default: `1h`)
- `DINGO_STORAGE_MODE`
  - Storage mode: `core` (default) or `api`
  - `core` stores only consensus data (UTxOs, certs, pools, protocol params)
  - `api` additionally stores witnesses, scripts, datums, redeemers, and tx metadata
  - API servers (Blockfrost, UTxO RPC, Mesh) require `api` mode
- `DINGO_RUN_MODE`
  - Run mode: `serve` (full node, default), `load` (batch import), `dev` (development mode), or `leios` (experimental Leios/Dijkstra protocol support)
- `DINGO_START_ERA`
  - Experimental startup era override. Set to `dijkstra` only for Dijkstra/Leios test networks; leave empty to follow genesis protocol version.
- `DINGO_LOGGING_FORMAT`
  - Log output format: `text` (default, human-readable) or `json` (machine-parseable, for ELK/Loki ingestion)
- `DINGO_LOGGING_LEVEL`
  - Minimum log level: `debug`, `info` (default), `warn`, or `error` (the `--debug` flag overrides this to `debug`)
- `TLS_CERT_FILE_PATH` - SSL certificate to use, requires `TLS_KEY_FILE_PATH`
    (default: empty)
- `TLS_KEY_FILE_PATH` - SSL certificate key to use (default: empty)

### Block Production (SPO Mode)

To run Dingo as a stake pool operator producing blocks:

- `CARDANO_BLOCK_PRODUCER` - Enable block production (default: `false`)
- `CARDANO_SHELLEY_VRF_KEY` - Path to VRF signing key file
- `CARDANO_SHELLEY_KES_KEY` - Path to KES signing key file
- `CARDANO_SHELLEY_OPERATIONAL_CERTIFICATE` - Path to operational certificate file

### Quick Start

```bash
# Preview network (default)
./dingo

# Mainnet
CARDANO_NETWORK=mainnet ./dingo

# Or with explicit config path
CARDANO_NETWORK=mainnet CARDANO_CONFIG=path/to/mainnet/config.json ./dingo
```

Dingo creates a `dingo.socket` file that speaks Ouroboros node-to-client and is compatible with `cardano-cli`, `adder`, `kupo`, and other Cardano client tools.

Cardano configuration files are bundled in the Docker image. For local builds, you can find them at [docker-cardano-configs](https://github.com/blinklabs-io/docker-cardano-configs/tree/main/config).

## Docker

```bash
# Run on preview (default)
docker run -p 3001:3001 ghcr.io/blinklabs-io/dingo

# Run on mainnet with persistent storage
docker run -p 3001:3001 \
  -e CARDANO_NETWORK=mainnet \
  -v dingo-data:/data/db \
  -v dingo-ipc:/ipc \
  ghcr.io/blinklabs-io/dingo
```

The image is based on Debian bookworm-slim and includes `cardano-cli`, `nview`, and `txtop`. Mithril snapshot support is built into dingo natively (`dingo mithril sync`). The Dockerfile sets `CARDANO_DATABASE_PATH=/data/db` and `CARDANO_SOCKET_PATH=/ipc/dingo.socket`, overriding the local defaults of `.dingo` and `dingo.socket` — the volume mounts above map to these container paths.

| Port | Service | Default |
|------|---------|---------|
| 3001 | Ouroboros NtN (node-to-node) | Enabled |
| 3002 | Ouroboros NtC over TCP | Enabled |
| 12798 | Prometheus metrics | Enabled |
| 3000 | Blockfrost REST API | Disabled |
| 8080 | Mesh (Rosetta) REST API | Disabled |
| 9090 | UTxO RPC (gRPC) | Disabled |
| — | Bark archive (gRPC) | Disabled (example when enabled: 9091) |

## Storage Modes

Dingo has two storage modes that control how much data is persisted:

| Mode | What's Stored | Use Case |
|------|---------------|----------|
| `core` (default) | UTxOs, certificates, pools, protocol parameters | Relays, block producers |
| `api` | Core data + witnesses, scripts, datums, redeemers, tx metadata | Nodes serving API queries |

```bash
# Relay or block producer (default)
./dingo

# API node
DINGO_STORAGE_MODE=api ./dingo
```

Or in `dingo.yaml`:

```yaml
storageMode: "api"
```

## API Servers and Bark

Dingo includes three general-purpose external APIs plus Bark. UTxO RPC, Blockfrost, and Mesh are client-facing APIs and require `storageMode: "api"`. Bark is different: it is Dingo's own protocol for Dingo-to-Dingo C2/archive services, not a general-purpose application API. Set an individual port to 0 to disable a specific interface. The Blockfrost server currently exposes the latest, epoch, network, and pool subset.

The shorter `DINGO_UTXORPC_PORT`, `DINGO_BLOCKFROST_PORT`, and
`DINGO_MESH_PORT` names remain supported for compatibility. If both a
compatibility name and its plugin-form name are set, the plugin-form value
takes precedence.

| Interface | Port Env Var | Default | Protocol | Role |
|-----------|--------------|---------|----------|------|
| UTxO RPC | `DINGO_PLUGINS_API_UTXORPC_CONFIG_PORT` | 9090 | gRPC | General-purpose client API |
| Blockfrost | `DINGO_PLUGINS_API_BLOCKFROST_CONFIG_PORT` | 3000 | REST | General-purpose client API |
| Mesh (Rosetta) | `DINGO_PLUGINS_API_MESH_CONFIG_PORT` | 8080 | REST | General-purpose client API |
| Bark | `DINGO_BARK_PORT` | disabled | Connect/gRPC | Dingo-to-Dingo C2/archive protocol |

```bash
# Enable Blockfrost API on port 3100 and UTxO RPC on port 9090
DINGO_STORAGE_MODE=api \
  DINGO_PLUGINS_API_BLOCKFROST_CONFIG_PORT=3100 \
  DINGO_PLUGINS_API_UTXORPC_CONFIG_PORT=9090 \
  ./dingo
```

Or in `dingo.yaml`:

```yaml
storageMode: "api"
plugins:
  api:
    blockfrost: {provider: builtin, config: {port: 3100}}
    utxorpc: {provider: builtin, config: {port: 9090}}
```

### Archive And History Expiry Nodes

Dingo can expire immutable block CBOR from a local blob store once blocks are
older than the ledger-derived stability window. This History Expiry mode is a
valid standalone operational mode: without an archive fallback, reads for
expired blocks return a clear history-expired error. When paired with Bark,
expired or missing historical block reads can be transparently served from a
remote archive node.

An archive node uses a signed-URL-capable blob plugin (`s3` or `gcs`) and
enables Bark with `barkPort`. Bark answers Dingo-to-Dingo archive requests by
returning a signed object-storage URL plus block metadata. Badger is valid for a
normal local blob store, but it does not provide signed URLs and should not be
used as the Bark archive backend.

For local source builds, the `s3` and `gcs` blob plugins require
`-tags dingo_extra_plugins` or `make build`. Official release binaries include
the extra plugin tag.

```yaml
storageMode: "core"
plugins:
  storage:
    blob:
      provider: s3
      config:
        bucket: "dingo-archive"
        region: "us-east-1"
        prefix: "preview"
barkPort: 9091
```

A history-expiry node keeps its normal local blob store and enables
`historyExpiry`. Dingo expires blocks older than the ledger-derived stability
window while keeping local indexes and metadata, so reads fail explicitly as
expired history unless an archive wrapper can serve them.

```yaml
storageMode: "core"
plugins:
  storage:
    blob:
      provider: badger
      config: {}
historyExpiry:
  enabled: true
  frequency: 1h
```

Add `barkBaseUrl` when expired historical reads should fall back to a Bark
archive:

```yaml
barkBaseUrl: "http://archive.example.internal:9091"
barkBlockDownloadHosts:
  - "dingo-archive.s3.us-east-1.amazonaws.com"
```

Bark archive RPC may use the configured `barkBaseUrl`, but the block download
URLs returned by that service must be HTTPS, must not contain credentials, and
must match either the `barkBaseUrl` hostname or `barkBlockDownloadHosts`.

The runnable demonstration in `internal/test/archive-demo/` brings up an S3
compatible Minio archive node, a local Badger history-expiry node, and an end-to-end
BlockFetch check through Bark.

### Deployment Patterns

**Relay node** (consensus only, no APIs):
```bash
./dingo
```

**API / data node** (full indexing, one or more APIs):
```bash
DINGO_STORAGE_MODE=api DINGO_PLUGINS_API_BLOCKFROST_CONFIG_PORT=3100 ./dingo
```

**Archive node** (cloud object storage plus Bark archive service):
```bash
DINGO_PLUGINS_STORAGE_BLOB_PROVIDER=s3 DINGO_BARK_PORT=9091 ./dingo
```

**History-expiry node** (local storage plus a remote Bark archive):
```bash
DINGO_HISTORY_EXPIRY_ENABLED=true \
DINGO_BARK_BASE_URL=http://archive.example.internal:9091 ./dingo
```

**Block producer** (consensus only, with SPO keys):
```bash
CARDANO_BLOCK_PRODUCER=true \
  CARDANO_SHELLEY_VRF_KEY=/keys/vrf.skey \
  CARDANO_SHELLEY_KES_KEY=/keys/kes.skey \
  CARDANO_SHELLEY_OPERATIONAL_CERTIFICATE=/keys/opcert.cert \
  ./dingo
```

When `storageMode=core`, the Badger blob store defaults to mmap-only settings: `block-cache-size=0`, `index-cache-size=0`, and `compression=false`. When `storageMode=api`, the default Badger profile is `block-cache-size=268435456`, `index-cache-size=0`, and `compression=true`. The `plugins.storage.blob.config` Badger settings (YAML or the matching `DINGO_PLUGINS_STORAGE_BLOB_CONFIG_*` environment variables) override those defaults only when explicitly set.

See `dingo.yaml.example` for the full set of configuration options.

## Fast Bootstrapping with Mithril

Instead of syncing from genesis (which can take days on mainnet), you can bootstrap Dingo using a [Mithril](https://mithril.network/) snapshot. Dingo has a built-in Mithril client that handles download, extraction, and import automatically. This is the fastest way to get a node running.

```bash
# Bootstrap from Mithril and start syncing
./dingo -n preview sync --mithril

# Then start the node
./dingo -n preview serve
```

Or use the subcommand form for more control:

```bash
# List available snapshots
./dingo -n preview mithril list

# Show snapshot details
./dingo -n preview mithril show <hash>

# Download and import
./dingo -n preview mithril sync
```

Two Mithril artifact backends are supported via `mithril.backend` (or
`--mithril-backend`): `v2` (default) restores from incremental per-immutable-file
archives verified against the certified merkle root, while `v1` uses the legacy
full snapshot tarballs, which upstream Mithril is phasing out. The `mithril list`
and `mithril show` subcommands follow the configured backend.

This imports:
- All blocks from genesis (stored in blob store for serving peers)
- Current UTxO set, stake accounts, pool registrations, DRep registrations
- Stake snapshots (mark/set/go) for leader election
- Protocol parameters, governance state, treasury/reserves
- Complete epoch history for slot-to-time calculations

Individual transaction records, certificate history, witness/script/datum storage, and governance vote records for blocks before the snapshot are not stored by the snapshot itself. In `core` mode these are not needed — consensus, block production, and serving blocks to peers work without them, and new blocks processed after bootstrap will have full metadata. In `api` mode, `dingo mithril sync` automatically runs a backfill step after loading the snapshot to populate this historical data, so API servers (Blockfrost, UTxO RPC, Mesh) have complete records from genesis.

Performance (preview network, ~4M blocks):

| Phase | core mode | api mode |
|-------|-----------|----------|
| Download snapshot (~2.6 GB) | ~1-2 min | ~1-2 min |
| Extract + download ancillary | ~1 min | ~1 min |
| Import ledger state (UTxOs, accounts, pools, DReps, epochs) | ~12 min | ~12 min |
| Load blocks into blob store | ~36 min | ~36 min |
| Backfill historical metadata | — | ~varies |
| Total | ~50 min | ~50 min + backfill |

### Disk Space Requirements

Bootstrapping requires temporary disk space for both the downloaded snapshot and the Dingo database:

| Network | Snapshot Size | Dingo DB | Total Needed |
|---------|--------------|----------|--------------|
| mainnet |      ~180 GB | ~200+ GB |      ~400 GB |
| preprod |       ~60 GB |   ~80 GB |      ~150 GB |
| preview |       ~15 GB |   ~25 GB |       ~50 GB |

These are approximate values that grow over time. The snapshot can be deleted after import, but you need sufficient space for both during the load process.

## Database Plugins

Dingo supports pluggable storage backends for both blob storage (blocks, transactions) and metadata storage. This allows you to choose the best storage solution for your use case.

### Available Plugins

For local source builds, `badger`, `sqlite`, the default mempool, and all three
built-in API providers are always available. GCS, S3, PostgreSQL, and MySQL
require `-tags dingo_extra_plugins` or an official release binary.

Blob Storage Plugins:
- `badger` - BadgerDB local key-value store (default)
- `gcs` - Google Cloud Storage blob store
- `s3` - AWS S3 blob store

Metadata Storage Plugins:
- `sqlite` - SQLite relational database (default)
- `postgres` - PostgreSQL relational database
- `mysql` - MySQL relational database

### Plugin Selection

Plugins can be selected via command-line flags, environment variables, or configuration file:

```bash
# Command line
./dingo --blob gcs --metadata sqlite

# Environment variables
DINGO_PLUGINS_STORAGE_BLOB_PROVIDER=gcs
DINGO_PLUGINS_STORAGE_METADATA_PROVIDER=sqlite

# Configuration file (dingo.yaml)
plugins:
  storage:
    blob:
      provider: gcs
      config:
        bucket: my-cardano-blocks
    metadata:
      provider: sqlite
      config: {}
```

### Plugin Configuration

Each capability has exactly one selected provider. Provider configuration is
strictly decoded; unknown fields fail startup. Generic environment variables
flatten the capability and config path, for example
`DINGO_PLUGINS_MEMPOOL_CONFIG_CAPACITY` and
`DINGO_PLUGINS_API_UTXORPC_CONFIG_PORT`. See `dingo.yaml.example`.

`CARDANO_DATABASE_PATH` (or `databasePath` / `--data-dir`) remains a shortcut
that supplies the data directory to both local storage providers. Set
`dataDir` on either local provider when blob and metadata storage need separate
paths; the provider value overrides the shared shortcut.

BadgerDB Options:
- `dataDir` - Badger data directory (defaults to the shared database path)
- `blockCacheSize` - Block cache size in bytes
- `indexCacheSize` - Index cache size in bytes
- `compression` - Enable ZSTD compression
- `gc` - Enable garbage collection

Leave mode-sensitive Badger settings unset to use storage-mode defaults.

Google Cloud Storage Options:
- `bucket` - GCS bucket name

AWS S3 Options:
- `endpoint` - Optional custom S3-compatible endpoint
- `bucket` - S3 bucket name
- `region` - AWS region
- `prefix` - Path prefix within bucket
- `timeout` - Request timeout

S3 credentials use the standard AWS credential chain.

SQLite Options:
- `dataDir` - SQLite data directory (defaults to the shared database path)
- `maxConnections` - Maximum connection count

PostgreSQL Options:
- `host` - PostgreSQL server hostname
- `port` - PostgreSQL server port
- `user` - Database user
- `password` - Database password
- `database` - Database name
- `sslMode` - PostgreSQL SSL mode
- `timeZone` - PostgreSQL time zone (default: UTC)
- `dsn` - Full PostgreSQL DSN (overrides the individual connection fields)

MySQL Options:
- `host` - MySQL server hostname
- `port` - MySQL server port
- `user` - Database user
- `password` - Database password
- `database` - Database name
- `sslMode` - MySQL TLS mode (mapped to `tls` in the DSN)
- `timeZone` - MySQL time zone location (default: UTC)
- `dsn` - Full MySQL DSN (overrides other options when set)

### Migrating From Pre-Plugin Configuration

The plugin platform replaces the earlier per-plugin CLI flags and environment
variables for storage, mempool, and API ports with the `plugins.*` config tree
(YAML), the generic `DINGO_PLUGINS_*` environment scheme, and the provider
selector flags. Every removed setting has an equivalent below; values are
unchanged, only where they are set has moved.

| Removed setting | New equivalent |
| --- | --- |
| `--mempool-capacity`, `CARDANO_MEMPOOL_CAPACITY` | `plugins.mempool.config.capacity` / `DINGO_PLUGINS_MEMPOOL_CONFIG_CAPACITY` |
| `--eviction-watermark`, `DINGO_MEMPOOL_EVICTION_WATERMARK` | `plugins.mempool.config.evictionWatermark` / `DINGO_PLUGINS_MEMPOOL_CONFIG_EVICTION_WATERMARK` |
| `--rejection-watermark`, `DINGO_MEMPOOL_REJECTION_WATERMARK` | `plugins.mempool.config.rejectionWatermark` / `DINGO_PLUGINS_MEMPOOL_CONFIG_REJECTION_WATERMARK` |
| `DINGO_DATABASE_BLOB_PLUGIN` | `--blob`, `plugins.storage.blob.provider`, or `DINGO_PLUGINS_STORAGE_BLOB_PROVIDER` |
| `DINGO_DATABASE_METADATA_PLUGIN` | `--metadata`, `plugins.storage.metadata.provider`, or `DINGO_PLUGINS_STORAGE_METADATA_PROVIDER` |
| `--blob-badger-*`, `DINGO_DATABASE_BLOB_BADGER_*` | `plugins.storage.blob.config.*` / `DINGO_PLUGINS_STORAGE_BLOB_CONFIG_*` |
| `--metadata-sqlite-*`, `DINGO_DATABASE_METADATA_SQLITE_*` | `plugins.storage.metadata.config.*` / `DINGO_PLUGINS_STORAGE_METADATA_CONFIG_*` |
| `MYSQL_*` MySQL connection aliases (`-tags dingo_extra_plugins`) | `plugins.storage.metadata.config.*` / `DINGO_PLUGINS_STORAGE_METADATA_CONFIG_*` |
| `--utxorpc-port`, `--blockfrost-port`, `--mesh-port` | `plugins.api.<name>.config.port` / `DINGO_PLUGINS_API_<NAME>_CONFIG_PORT` |

Provider config fields use lowerCamelCase in YAML; the environment form
uppercases them with underscore separators (`dataDir` becomes
`..._CONFIG_DATA_DIR`). The pre-plugin API port variables `DINGO_UTXORPC_PORT`,
`DINGO_BLOCKFROST_PORT`, and `DINGO_MESH_PORT` still work as compatibility
aliases, and setting an API port to `0` disables that server.

### Listing Available Plugins

You can see all available plugins and their descriptions:

```bash
./dingo list
```

## Plugin Development

For information on developing custom storage plugins, see [PLUGIN_DEVELOPMENT.md](PLUGIN_DEVELOPMENT.md).

## Features

- [x] Network
  - [x] UTxO RPC
  - [x] Ouroboros
    - [x] Node-to-node
      - [x] ChainSync
      - [x] BlockFetch
      - [x] TxSubmission2
    - [x] Node-to-client
      - [x] ChainSync
      - [x] LocalTxMonitor
      - [x] LocalTxSubmission
      - [x] LocalStateQuery
    - [x] Peer governor
      - [x] Topology config
      - [x] Peer churn (full PeerChurnEvent with gossip/public root churn, bootstrap events)
      - [x] Ledger peers
      - [x] Peer sharing
      - [x] Denied peers tracking
    - [x] Connection manager
      - [x] Inbound connections
        - [x] Node-to-client over TCP
        - [x] Node-to-client over UNIX socket
        - [x] Node-to-node over TCP
      - [x] Outbound connections
        - [x] Node-to-node over TCP
- [ ] Ledger
  - [x] Blocks
    - [x] Block storage
    - [x] Chain selection (density comparison, VRF tie-breaker, ChainForkEvent)
  - [x] UTxO tracking
  - [x] Protocol parameters
  - [x] Genesis validation
  - [x] Block header validation (VRF/KES/OpCert cryptographic verification)
  - [ ] Certificates
    - [x] Pool registration
    - [x] Stake registration/delegation
    - [x] Account registration checks
    - [x] DRep registration
    - [ ] Governance
  - [ ] Transaction validation
    - [ ] Phase 1 validation
      - [x] UTxO rules
      - [x] Fee validation (full fee calculation with script costs)
      - [x] Transaction size and ExUnit budget validation
      - [ ] Witnesses
      - [ ] Block body
      - [ ] Certificates
      - [ ] Delegation/pools
      - [ ] Governance
    - [x] Phase 2 validation
      - [x] Plutus V1 smart contract execution
      - [x] Plutus V2 smart contract execution
      - [x] Plutus V3 smart contract execution
- [x] Block production
  - [x] VRF leader election with stake snapshots
  - [x] Block forging with KES/OpCert signing
  - [x] Slot battle detection
- [x] Mempool
  - [x] Accept transactions from local clients
  - [x] Distribute transactions to other nodes
  - [x] Validation of transaction on add
  - [x] Consumer tracking
  - [x] Transaction purging on chain update
  - [x] Watermark-based eviction and rejection
- [x] Database Recovery
  - [x] Chain rollback support (SQLite, PostgreSQL, and MySQL plugins)
  - [x] State restoration on rollback
  - [x] WAL mode for crash recovery
  - [x] Automatic rollback on transaction error
- [x] Stake Snapshots
  - [x] Mark/Set/Go rotation at epoch boundaries
  - [x] Genesis snapshot capture
- [x] API Servers
  - [x] UTxO RPC (gRPC)
  - [ ] WIP Blockfrost-compatible REST API
  - [x] Mesh (Coinbase Rosetta) API
- [x] Mithril Bootstrap
  - [x] Built-in Mithril client
  - [x] Ledger state import (UTxOs, accounts, pools, DReps, epochs)
  - [x] Block loading from ImmutableDB

Additional planned features can be found in our issue tracker and project boards.

[Catalyst Fund 12 - Go Node (Dingo)](https://github.com/orgs/blinklabs-io/projects/16)<br/>
[Catalyst Fund 13 - Archive Node](https://github.com/orgs/blinklabs-io/projects/17)

Check the issue tracker for known issues. Due to rapid development, bugs happen
especially as there is functionality which has not yet been developed.

## Development / Building

This requires Go 1.25 or later. You also need `make`.

```bash
# Format, test, and build (default target)
make

# Build only
make build

# Run
./dingo

# Run without building a binary
go run ./cmd/dingo/
```

### Testing

```bash
make test                                    # All tests with race detection
go test -v -race -run TestName ./package/    # Single test
make bench                                   # Benchmarks
```

### Profiling

```bash
# Load testdata with CPU and memory profiling
make test-load-profile

# Analyze
go tool pprof cpu.prof
go tool pprof mem.prof
```

## DevNet

The DevNet runs a private Cardano network with Dingo and `cardano-node` producing blocks side by side. It validates that Dingo forges blocks, maintains consensus, and interoperates with the reference node.

### Architecture

The DevNet uses Docker Compose to run three Cardano nodes plus a load generator on a bridge network:

| Container | Role | Host Port |
|-----------|------|-----------|
| `dingo-producer` | Dingo block producer (pool 1) | 3010 |
| `cardano-producer` | cardano-node block producer (pool 2) | 3011 |
| `cardano-relay` | Relay node (no block production) | 3012 |
| `txpump` | Submits payment transactions into Dingo's mempool | — |

A `configurator` init container generates fresh pool keys and genesis files before nodes start. The `txpump` sidecar comes up after Dingo is healthy and continuously feeds the mempool so block bodies and tx-submission paths are exercised alongside consensus.

### Prerequisites

- Docker with the Compose plugin (`docker compose`)
- Go 1.25+

### Running the Automated Tests

The test suite builds the Dingo Docker image, starts all containers, waits for health checks, and runs Go integration tests tagged with `//go:build devnet`:

```bash
cd internal/test/devnet/

# Run all devnet tests
./run-tests.sh

# Run a specific test
./run-tests.sh -run TestBasicBlockForging

# Keep containers running after tests pass (for inspection)
./run-tests.sh --keep-up
```

Override host ports if needed:

```bash
DEVNET_DINGO_PORT=4010 DEVNET_CARDANO_PORT=4011 DEVNET_RELAY_PORT=4012 ./run-tests.sh
```

### Running the DevNet Manually

For longer-running manual tests (soak testing, observing behavior over multiple epochs, debugging):

```bash
cd internal/test/devnet/

# Start all containers
./start.sh

# Watch logs
docker compose -f docker-compose.yml logs -f

# Watch a specific node
docker compose -f docker-compose.yml logs -f dingo-producer

# Stop and clean up
./stop.sh
```

Containers remain running until you stop them. The DevNet parameters (in `testnet.yaml`) use 1-second slots and 500-slot epochs (~8 minutes per epoch) with `activeSlotsCoeff=0.4` and `securityParam (k)=40`, so you can observe epoch transitions, leader election, and stake snapshot rotation relatively quickly.

See [`internal/test/devnet/README.md`](internal/test/devnet/README.md) for full details on the harness, configurator, available test scenarios, and port/address overrides.

### Local DevNet (Without Docker)

For quick iteration without Docker, `devmode.sh` runs Dingo directly against a local devnet genesis. It resets state and updates genesis timestamps on each run:

```bash
# Run in devnet mode
./devmode.sh

# With debug logging
DEBUG=true ./devmode.sh
```

This stores state in `.devnet/` and uses genesis configs from `config/cardano/devnet/`. It runs a single Dingo node (no cardano-node counterpart), which is useful for testing startup, epoch transitions, and block production in isolation.
