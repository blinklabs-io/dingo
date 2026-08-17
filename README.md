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
- Pluggable storage backends (Badger, SQLite, GCS, S3, PostgreSQL, MySQL)
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
  - Application-wide operational mode for a bare `dingo` invocation:
    `serve` (default), `load`, `dev`, or `leios`
  - Explicit subcommands select their own effective operation; for example,
    `dingo sync` runs in sync mode regardless of the configured value
  - Relay, producer, storage, API, and validation settings do not select a run
    mode
- `DINGO_START_ERA`
  - Experimental startup era override. Set to `dijkstra` only for Dijkstra/Leios test networks; leave empty to follow genesis protocol version.
- `DINGO_LOGGING_FORMAT`
  - Log output format: `text` (default, human-readable) or `json` (machine-parseable, for ELK/Loki ingestion)
- `DINGO_LOGGING_LEVEL`
  - Minimum log level: `debug`, `info` (default), `warn`, or `error` (the `--debug` flag overrides this to `debug`)
- `TLS_CERT_FILE_PATH` - TLS certificate used by the built-in UTxO RPC
  listener, requires `TLS_KEY_FILE_PATH` (default: empty)
- `TLS_KEY_FILE_PATH` - TLS private key used by the built-in UTxO RPC listener
  (default: empty)

### Block Production (SPO Mode)

To run Dingo as a stake pool operator producing blocks:

- `CARDANO_BLOCK_PRODUCER` - Enable block production (default: `false`)
- `CARDANO_SHELLEY_VRF_KEY` - Path to VRF signing key file
- `CARDANO_SHELLEY_KES_KEY` - Path to KES signing key file
- `CARDANO_SHELLEY_OPERATIONAL_CERTIFICATE` - Path to operational certificate file

Dingo block production is exercised by the all-Dingo DevNet and has produced
blocks on preview and preprod. Current releases do not support mainnet
operation.

### Quick Start

```bash
# Preview network (default)
./dingo

# Preprod
CARDANO_NETWORK=preprod ./dingo

# Or with explicit config path
CARDANO_NETWORK=preprod CARDANO_CONFIG=path/to/preprod/config.json ./dingo
```

Dingo creates a `dingo.socket` file that speaks Ouroboros node-to-client and is compatible with `cardano-cli`, `adder`, `kupo`, and other Cardano client tools.

Cardano configuration files are bundled in the Docker image. For local builds, you can find them at [docker-cardano-configs](https://github.com/blinklabs-io/docker-cardano-configs/tree/main/config).

## Docker

```bash
# Run on preview (default)
docker run -p 3001:3001 ghcr.io/blinklabs-io/dingo

# Run on preprod with persistent storage
docker run -p 3001:3001 \
  -e CARDANO_NETWORK=preprod \
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

Dingo has two storage modes commonly used in three node configurations:

| Node configuration | Settings | Current behavior |
|---|---|---|
| Relay | `storageMode: core`, `blockProducer: false` | Validates and follows the chain, participates in NtN/NtC, relays blocks and transactions, and stores consensus state without API history |
| Block producer | `storageMode: core`, `blockProducer: true` plus VRF/KES/opcert paths | Includes the relay behavior, leader election, block forging, forged-block self-validation, and block diffusion |
| Data/API node | `storageMode: api`, `blockProducer: false` | Stores consensus state plus transaction, witness, script, datum, redeemer, governance, and metadata history; starts configured Blockfrost, Mesh, and UTxO RPC providers |

`core` is the default and smallest storage/runtime surface. The producer
profile adds forging and key operations to it. API mode adds historical
indexing and query services; it is not a separate consensus implementation.

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

Dingo includes three general-purpose external APIs plus Bark. UTxO RPC,
Blockfrost, and Mesh are client-facing APIs and require `storageMode: "api"`.
Their built-in providers are registered with the instance-owned plugin host,
start on their provider defaults in API mode, and can be configured
independently under `plugins.api`. Set an individual port to 0 to disable that
interface.

Bark is Dingo's own Dingo-to-Dingo archive protocol rather than an application
API. It is configured separately with `barkPort` and `barkBaseUrl`.

For public client access, place the API listeners behind a reverse proxy or API
gateway — that remains fully supported. In addition, UTxO RPC, Blockfrost, and
Mesh share one in-process TLS/authentication surface, so an operator can also
secure any subset of them without a proxy in front.

The shorter `DINGO_UTXORPC_PORT`, `DINGO_BLOCKFROST_PORT`, and
`DINGO_MESH_PORT` names remain supported for compatibility. If both a
compatibility name and its plugin-form name are set, the plugin-form value
takes precedence.

| Interface | Port Env Var | Default | Protocol | Role |
|-----------|--------------|---------|----------|------|
| UTxO RPC | `DINGO_PLUGINS_API_UTXORPC_CONFIG_PORT` | 9090 | gRPC | General-purpose client API (v1alpha and v1beta) |
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

### API TLS and Authentication

`api.tls`/`api.auth` set a shared default TLS and authentication policy for
every selected `plugins.api.*` provider (Blockfrost, Mesh, UTxO RPC). Each
field resolves independently: `plugins.api.<name>.config.tls`/`config.auth`
overrides any field for that provider only, and an explicit
`mode: disabled` at the provider level turns off an inherited policy rather
than merely leaving it unset. The default everywhere is `disabled`, so an
existing reverse-proxy/no-auth deployment is unaffected on upgrade.

```yaml
api:
  tls:
    mode: server
    certFilePath: /run/secrets/api.crt
    keyFilePath: /run/secrets/api.key
  auth:
    mode: token
    tokenFilePath: /run/secrets/api-token

plugins:
  api:
    # Inherits TLS and auth from api.tls/api.auth above unchanged.
    utxorpc:
      provider: builtin
      config:
        port: 9090
    # Explicitly opts out of the inherited token auth (e.g. this listener
    # sits behind its own gateway that already authenticates callers).
    mesh:
      provider: builtin
      config:
        port: 8080
        auth:
          mode: disabled
    # Overrides just the certificate/key for this provider; the inherited
    # api.tls.mode ("server") still applies.
    blockfrost:
      provider: builtin
      config:
        port: 3000
        tls:
          certFilePath: /run/secrets/blockfrost.crt
          keyFilePath: /run/secrets/blockfrost.key
```

Credential locations:

- **HTTP** (Blockfrost, Mesh, UTxO RPC's own REST/JSON access): send
  `Authorization: Bearer <token>`. Blockfrost also accepts its own
  `project_id: <token>` header as an alias for the same shared token — real
  Blockfrost clients already send their API key that way, so
  `auth.mode: token` secures Blockfrost against both header styles from one
  configured token.
- **Connect/gRPC** (UTxO RPC): send the identical
  `Authorization: Bearer <token>` request header; every Connect/gRPC
  handler UTxO RPC serves, including health checking and reflection,
  requires it once auth is enabled.
- **Liveness/readiness probes.** This is deliberate, not an oversight: once
  `auth.mode: token` is set for a provider, *every* route it serves requires
  the credential, with no separate unauthenticated allowlist for health
  checking — Blockfrost's `GET /health` and UTxO RPC's
  `grpc.health.v1.Health/Check` are no exception, matching how every other
  route on that listener behaves. A container-orchestrator probe (e.g. a
  Kubernetes liveness/readiness check) that cannot attach the shared
  credential will therefore fail once auth is enabled. Configure the probe
  to send the same `Authorization: Bearer <token>` header the rest of your
  clients use (most probe mechanisms support a custom header/exec command),
  or point liveness/readiness checks at a plain TCP connect to the listener
  port instead of the HTTP/gRPC health route, or run the probe against an
  unauthenticated in-cluster path (e.g. a `mode: disabled` provider carrying
  only observability traffic) rather than the public listener.
- A missing or invalid credential fails closed: `401` over HTTP,
  `Unauthenticated` over Connect/gRPC. A browser's CORS preflight
  (`OPTIONS`) never needs a credential — browsers never attach
  `Authorization` to one — but every other request, including a
  non-preflight `OPTIONS`, still authenticates normally.
- A partial `certFilePath`/`keyFilePath` pair (only one set) fails
  validation at startup, before any listener binds, with an error naming
  the full config path (e.g. `plugins.api.blockfrost.config.tls`).
- Tokens and certificate/key file *contents* are never written to logs,
  error messages, or effective-config output — only file paths and mode
  names are.

The pre-existing root `tlsCertFilePath`/`tlsKeyFilePath` fields remain a
supported, **UTxO RPC-only** compatibility default. They merge in as the
lowest-priority input, field by field, alongside the shared `api.tls`
default and UTxO RPC's own `tls` config — not as an all-or-nothing fallback
that only applies when both of those are completely unset. For example, if
the shared `api.tls` sets only `mode: server` with no `certFilePath`/
`keyFilePath` of its own, UTxO RPC still inherits those two fields from the
legacy root settings. They are not promoted onto Blockfrost or Mesh, since
doing so would silently switch a previously plaintext listener to TLS on
upgrade. `bindAddr` and `corsAllowedOrigins` are unrelated to this policy
and remain root-level settings shared by all listeners (`bindAddr` is also
used by the relay/NtN listener, not just the APIs).

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

The default `v2` backend restores incremental per-immutable-file archives only
after checking the genesis-rooted certificate chain, certified Merkle root, and
each immutable-file digest. It also requires the ancillary archive: its
ledger-state and in-progress immutable files are checked against the
manifest separately signed by the ancillary key. That signature authenticates
that payload; it is not a stake certificate and does not validate the volatile
blocks after the certified immutable point.

The legacy `v1` full-snapshot backend is available for inspection and
unverified library workflows, but it cannot be used for a verified fast
bootstrap because it has no signed ancillary-state boundary. The `mithril list`
and `mithril show` subcommands follow the configured backend.

This imports:
- All blocks from genesis (stored in blob store for serving peers)
- Current UTxO set, stake accounts, pool registrations, DRep registrations
- Stake snapshots (mark/set/go) for leader election
- Protocol parameters, governance state, treasury/reserves
- Complete epoch history for slot-to-time calculations

Individual transaction records, certificate history, witness/script/datum storage, and governance vote records for blocks before the snapshot are not stored by the snapshot itself. In `core` mode these are not needed — consensus, block production, and serving blocks to peers work without them, and new blocks processed after bootstrap will have full metadata. In `api` mode, `dingo mithril sync` automatically runs a backfill step after loading the snapshot to populate this historical data, so API servers (Blockfrost, UTxO RPC, Mesh) have complete records from genesis.

### Replay and bootstrap behavior

Dingo supports two working startup paths:

- A normal chain sync builds ledger and database state from downloaded blocks.
  The default configuration validates historical blocks from origin and fails
  closed when required ledger/UTxO state is missing. Operators intentionally
  using a non-genesis intersection without complete pre-intersect state must
  explicitly set `validateHistorical: false` and `strictUtxoValidation: false`.
- Mithril sync verifies the certificate chain and snapshot artifact, imports
  the separately ancillary-key-signed ledger state, stores certified
  immutable blocks, and strictly processes the gap between the imported state
  and immutable tip. Normal strict validation resumes at the imported point
  for the gap and all subsequently received network data. In API mode it then
  backfills historical query records before the APIs are used.

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

## Database Maintenance

`dingo database` provides offline snapshot, restore, and truncate operations.
Each subcommand operates directly against the configured data directory and
must not be run while a `dingo` node process has that directory open. All three
honor SIGINT and SIGTERM so an interrupt unwinds cleanly instead of leaving a
partial result behind.

```bash
# Capture a point-in-time snapshot. --dir must not already exist.
./dingo database snapshot --dir /backups/dingo-preview-2026-08-05

# Restore the configured data directory from a snapshot directory.
./dingo database restore /backups/dingo-preview-2026-08-05

# Rewind to a target point. Pass exactly one of --slot, --hash, --block-number.
./dingo database truncate --slot 12345678
```

Truncate makes the target block the new chain tip and removes every block and
metadata row added after it. Unlike a normal chain rollback it does not reject
a target beyond the security parameter, because it exists for disaster-recovery
scenarios (see CIP-0135) where the chain must be rewound further than Ouroboros
Praos allows. The resulting database is resync-ready from the target point.

The same operations are also exposed remotely through the Bark
`DatabaseService`. Bark has no built-in authentication, so do not expose its
port outside a trusted network.

## Database Plugins

Dingo supports pluggable storage backends for both blob storage (blocks, transactions) and metadata storage. This allows you to choose the best storage solution for your use case.

### Available Plugins

For local source builds, `badger`, `sqlite`, the default mempool, and all three
built-in API providers are always available. GCS and S3 require
`-tags dingo_extra_plugins` or an official release binary. The same tag adds
the operational PostgreSQL and MySQL metadata providers, backed by the shared
`database/sql` store and v1alpha1 schema.

Blob Storage Plugins:
- `badger` - BadgerDB local key-value store (default)
- `gcs` - Google Cloud Storage blob store
- `s3` - AWS S3 blob store

Metadata Storage Plugins:
- `sqlite` - SQLite relational database (default)
- `postgres` - PostgreSQL metadata store (requires `dingo_extra_plugins`)
- `mysql` - MySQL metadata store (requires `dingo_extra_plugins`)

Mempool Plugins:
- `fifo` - First-in, first-out transaction pool (default)
- `dag` - Dependency-graph transaction pool that makes transaction
  dependencies explicit. Ledger validation remains the source of truth for
  both providers; the DAG backend changes ordering and selection, not
  validation.

API Plugins:
- `blockfrost` - Blockfrost-compatible REST API
- `mesh` - Mesh (Coinbase Rosetta) REST API
- `utxorpc` - UTxO RPC gRPC API (serves both v1alpha and v1beta)

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

Reserved PostgreSQL Options:
- `host` - PostgreSQL server hostname
- `port` - PostgreSQL server port
- `user` - Database user
- `password` - Database password
- `database` - Database name
- `sslMode` - PostgreSQL SSL mode
- `timeZone` - PostgreSQL time zone (default: UTC)
- `dsn` - Full PostgreSQL DSN (overrides the individual connection fields)
- `poolMaxOpenConns` - Maximum open connections (default: 100)
- `poolMaxIdleConns` - Maximum idle connections (default: 10)
- `poolConnMaxLifetime` - Maximum connection lifetime (default: 1h)

Reserved MySQL Options:
- `host` - MySQL server hostname
- `port` - MySQL server port
- `user` - Database user
- `password` - Database password
- `database` - Database name
- `sslMode` - MySQL TLS mode (mapped to `tls` in the DSN)
- `timeZone` - MySQL time zone location (default: UTC)
- `dsn` - Full MySQL DSN (overrides other options when set)
- `poolMaxOpenConns` - Maximum open connections (default: 100)
- `poolMaxIdleConns` - Maximum idle connections (default: 10)
- `poolConnMaxLifetime` - Maximum connection lifetime (default: 1h)

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

For information on developing custom storage plugins, see [database/plugin/PLUGIN_DEVELOPMENT.md](database/plugin/PLUGIN_DEVELOPMENT.md).

## Features

This checklist is a compact map of implemented feature areas. The package tests,
conformance suite, DevNet, and public-network evidence provide the detailed
validation record.

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
  - [x] Selectable backend: FIFO (default) or DAG
- [x] Database Recovery
  - [x] Chain rollback support
  - [x] State restoration on rollback
  - [x] WAL mode for crash recovery
  - [x] Automatic rollback on transaction error
  - [x] Cross-store commit fence with durable blob sync and commit timestamps
  - [x] Partial-commit and blob-only timestamp divergence detection
  - [x] Startup chain/ledger tip reconciliation and orphaned-blob cleanup
  - [x] Recovery ordering ahead of history expiry
- [x] Database Lifecycle
  - [x] Offline snapshot, restore, and truncate (`dingo database`)
  - [x] Remote operation through the Bark `DatabaseService`
- [x] Stake Snapshots
  - [x] Mark/Set/Go rotation at epoch boundaries
  - [x] Genesis snapshot capture
- [x] API Servers
  - [x] UTxO RPC (gRPC), serving v1alpha and v1beta
  - [ ] WIP Blockfrost-compatible REST API (required endpoint families are
        implemented; compatibility hardening and reward parity are ongoing)
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

This requires Go 1.26 or later. You also need `make`.

The default target formats and builds. It does not run tests; use `make test`
for those.

```bash
# Format and build (default target)
make

# Build only
make build

# Run
./dingo

# Run without building a binary
go run ./cmd/dingo/
```

`make build` builds every command under `cmd/`: the `dingo` node itself and
`koios-parity`, which compares Dingo ledger state against Koios for a given
network and epoch.

Metadata storage uses typed `database/sql` code generated by
[sqlc](https://sqlc.dev) from `sqlc.yaml`. Regenerate it with `make sql` after
changing a query, and `make sql-check` fails when the checked-in output is
stale. The former ORM has been removed; `make gorm-check` fails if it returns
to the source tree or the dependency graph.

### Testing

```bash
make test                                    # All tests with race detection
go test -v -race -run TestName ./package/    # Single test
make bench                                   # Benchmarks
make bench-mempool                           # Compare FIFO and DAG mempools
make docs-parity                             # Docs agree with go.mod, Makefile, compose
make sql-check                               # Generated sqlc output is current
make gorm-check                              # The removed ORM has not returned
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

The default DevNet runs a private all-Dingo Cardano network: three Dingo block
producers, one Dingo relay, and `txpump`. It validates Dingo-to-Dingo consensus,
block diffusion, liveness, mempool behavior, and Dingo-only features.

Pass `--conformance` to run Dingo beside `cardano-node` for compatibility and
reference-conformance testing.

### Architecture

The default Docker Compose profile contains:

| Container | Role | Host Port |
|-----------|------|-----------|
| `dingo-1` | Dingo block producer (pool 1) | 3010 |
| `dingo-2` | Dingo block producer (pool 2) | 3013 |
| `dingo-3` | Dingo block producer (pool 3) | 3014 |
| `dingo-relay` | Dingo relay (no block production) | 3015 |
| `txpump-dingo` | Submits transactions into Dingo's mempool | — |

The opt-in conformance profile contains `dingo-producer`,
`cardano-producer`, `cardano-relay`, and `txpump`. A configurator init
container generates fresh pool keys and genesis files for either profile.

### Prerequisites

- Docker with the Compose plugin (`docker compose`)
- Go 1.26+

### Running the Automated Tests

The test suite builds the Dingo Docker image, starts all containers, waits for health checks, and runs Go integration tests tagged with `//go:build devnet`:

```bash
cd internal/test/devnet/

# Run the all-Dingo suite
./run-tests.sh

# Run Dingo beside cardano-node
./run-tests.sh --conformance

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
docker compose -f docker-compose.yml logs -f dingo-1

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
