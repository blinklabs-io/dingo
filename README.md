# Dingo

<div align="center">
  <img src="./.github/assets/dingo-logo-with-text-horizontal.png" alt="Dingo Logo" width="640">
  <br>
  <img alt="GitHub" src="https://img.shields.io/github/license/blinklabs-io/dingo">
  <a href="https://goreportcard.com/report/github.com/blinklabs-io/dingo"><img src="https://goreportcard.com/badge/github.com/blinklabs-io/dingo" alt="Go Report Card"></a>
  <a href="https://pkg.go.dev/github.com/blinklabs-io/dingo"><img src="https://pkg.go.dev/badge/github.com/blinklabs-io/dingo.svg" alt="Go Reference"></a>
  <a href="https://discord.gg/5fPRZnX4qW"><img src="https://img.shields.io/badge/Discord-7289DA?style=flat&logo=discord&logoColor=white" alt="Discord"></a>
</div>

# Dingo

⚠️ This is a work in progress and is currently under heavy development

**Note:** On Windows systems, named pipes are used instead of Unix sockets for node-to-client communication.

<div align="center">
  <img src="./.github/dingo-20241210.png" alt="dingo screenshot" width="640">
</div>

## Running

Dingo supports configuration via both a YAML config file (`dingo.yaml`) and uses environment
variables to modify its own behavior.

A sample configuration file is provided at `dingo.yaml.example`.You can copy and edit this file to configure Dingo for your local or production environment:
This behavior can be changed via the following environment variables:

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
- `CARDANO_UTXORPC_PORT`
  - TCP port to bind for listening for UTxO RPC (default: `9090`)
- `TLS_CERT_FILE_PATH` - SSL certificate to use, requires `TLS_KEY_FILE_PATH`
    (default: empty)
- `TLS_KEY_FILE_PATH` - SSL certificate key to use (default: empty)

## Database Plugins

Dingo supports pluggable storage backends for both blob storage (blocks, transactions) and metadata storage. This allows you to choose the best storage solution for your use case.

### Available Plugins

**Blob Storage Plugins:**
- `badger` - BadgerDB local key-value store (default)
- `gcs` - Google Cloud Storage blob store
- `s3` - AWS S3 blob store

**Metadata Storage Plugins:**
- `sqlite` - SQLite relational database (default)

### Plugin Selection

Plugins can be selected via command-line flags, environment variables, or configuration file:

```bash
# Command line
./dingo --blob gcs --metadata sqlite

# Environment variables
DINGO_DATABASE_BLOB_PLUGIN=gcs
DINGO_DATABASE_METADATA_PLUGIN=sqlite

# Configuration file (dingo.yaml)
database:
  blob:
    plugin: "gcs"
  metadata:
    plugin: "sqlite"
```

### Plugin Configuration

Each plugin supports specific configuration options. See `dingo.yaml.example` for detailed configuration examples.

**BadgerDB Options:**
- `data-dir` - Directory for database files
- `block-cache-size` - Block cache size in bytes
- `index-cache-size` - Index cache size in bytes
- `gc` - Enable garbage collection

**Google Cloud Storage Options:**
- `bucket` - GCS bucket name
- `project-id` - Google Cloud project ID
- `prefix` - Path prefix within bucket

**AWS S3 Options:**
- `bucket` - S3 bucket name
- `region` - AWS region
- `prefix` - Path prefix within bucket
- `access-key-id` - AWS access key ID (optional - uses default credential chain if not provided)
- `secret-access-key` - AWS secret access key (optional - uses default credential chain if not provided)

**SQLite Options:**
- `data-dir` - Path to SQLite database file

### Listing Available Plugins

You can see all available plugins and their descriptions:

```bash
./dingo list
```

## Plugin Development

For information on developing custom storage plugins, see [PLUGIN_DEVELOPMENT.md](PLUGIN_DEVELOPMENT.md).

### Example

Running on mainnet (:sweat_smile:):

```bash
CARDANO_NETWORK=mainnet CARDANO_CONFIG=path/to/cardano/configs/mainnet/config.json ./dingo
```

Note: you can find cardano configuration files at
<https://github.com/blinklabs-io/docker-cardano-configs/tree/main/config>

Dingo will drop a `dingo.socket` file which can be used by other clients, such
as `cardano-cli` or software like `adder` or `kupo`. This has only had limited
testing, so success/failure reports are very welcome and encouraged!

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
    - [ ] Peer governor
      - [x] Topology config
      - [ ] Peer churn
      - [ ] Ledger peers
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
    - [ ] Chain selection
  - [x] UTxO tracking
  - [x] Protocol parameters
  - [ ] Certificates
    - [x] Pool registration
    - [x] Stake registration/delegation
    - [ ] Governance
  - [ ] Transaction validation
    - [ ] Phase 1 validation
      - [x] UTxO rules
      - [ ] Witnesses
      - [ ] Block body
      - [ ] Certificates
      - [ ] Delegation/pools
      - [ ] Governance
    - [ ] Phase 2 validation
      - [ ] Smart contracts
- [x] Mempool
  - [x] Accept transactions from local clients
  - [x] Distribute transactions to other nodes
  - [x] Validation of transaction on add
  - [x] Consumer tracking
  - [x] Transaction purging on chain update

Additional planned features can be found in our issue tracker and project boards.

[Catalyst Fund 12 - Go Node (Dingo)](https://github.com/orgs/blinklabs-io/projects/16)<br/>
[Catalyst Fund 13 - Archive Node](https://github.com/orgs/blinklabs-io/projects/17)

Check the issue tracker for known issues. Due to rapid development, bugs happen
especially as there is functionality which has not yet been developed.

## Development / Building

This requires Go 1.23 or better is installed. You also need `make`.

```bash
# Build
make
# Run
./dingo
```

You can also run the code without building a binary, first

```bash
go run ./cmd/dingo/
```
