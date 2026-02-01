# Architecture

Dingo is a high-performance Cardano blockchain node implementation in Go. This document describes its architecture, core components, and design patterns.

## Table of Contents

- [Overview](#overview)
- [Directory Structure](#directory-structure)
- [Core Node Structure](#core-node-structure)
- [Event-Driven Communication](#event-driven-communication)
- [Storage Architecture](#storage-architecture)
- [Blockchain State Management](#blockchain-state-management)
- [Chain Management](#chain-management)
- [Network & Protocol Handling](#network--protocol-handling)
- [Peer Governance](#peer-governance)
- [Transaction Mempool](#transaction-mempool)
- [Design Patterns](#design-patterns)
- [Stake Snapshots](#stake-snapshots)

## Overview

Dingo's architecture is built on several key principles:

1. **Modular Component Design** - Uses dependency injection and composition
2. **Event-Driven Communication** - Components communicate via EventBus rather than direct coupling
3. **Pluggable Storage Backends** - Dual-layer database architecture (blob + metadata)
4. **Ouroboros Protocol Implementation** - Full Node-to-Node and Node-to-Client protocol support
5. **Multi-Peer Chain Synchronization** - Chain selection with Ouroboros Praos rules
6. **Peer Governance** - Dynamic peer management with topology support
7. **Graceful Shutdown** - Phased shutdown with resource cleanup

## Directory Structure

```
dingo/
├── cmd/dingo/           # CLI entry points
│   ├── main.go          # Cobra CLI setup, plugin management
│   ├── serve.go         # Node server command
│   ├── load.go          # Block loading from ImmutableDB/Mithril
│   └── version.go       # Version information
├── chain/               # Blockchain state and validation
│   ├── chain.go         # Chain struct, block management
│   ├── manager.go       # ChainManager, fork handling
│   ├── event.go         # Chain events (update, fork)
│   └── errors.go        # Chain-specific errors
├── chainselection/      # Multi-peer chain comparison
│   ├── selector.go      # ChainSelector struct
│   ├── comparison.go    # Ouroboros Praos chain selection rules
│   ├── event.go         # Selection events
│   ├── peer_tip.go      # Peer tip tracking
│   └── vrf.go           # VRF verification
├── chainsync/           # Block synchronization protocol state
│   └── chainsync.go     # ChainsyncState, peer tracking
├── connmanager/         # Network connection lifecycle
│   ├── connection_manager.go
│   └── event.go         # Connection events
├── database/            # Storage abstraction layer
│   ├── database.go      # Database struct, dual-layer design
│   ├── cbor_cache.go    # TieredCborCache implementation
│   ├── cbor_offset.go   # Offset-based CBOR references
│   ├── hot_cache.go     # Hot cache for frequently accessed data
│   ├── block_lru_cache.go # Block-level LRU cache
│   ├── immutable/       # ImmutableDB chunk reader
│   ├── models/          # Database models (epoch, tip, utxoid)
│   ├── types/           # Database types
│   ├── sops/            # Storage operations
│   └── plugin/          # Storage plugin system
│       ├── plugin.go    # Plugin registry and interfaces
│       ├── blob/        # Blob storage plugins
│       │   ├── badger/  # Badger (default local storage)
│       │   ├── aws/     # AWS S3
│       │   └── gcs/     # Google Cloud Storage
│       └── metadata/    # Metadata plugins
│           ├── sqlite/  # SQLite (default)
│           ├── postgres/# PostgreSQL
│           └── mysql/   # MySQL
├── event/               # Event bus for decoupled communication
│   ├── event.go         # EventBus, async delivery
│   └── metrics.go       # Event metrics
├── ledger/              # UTXO tracking, protocol parameters
│   ├── state.go         # LedgerState, validation
│   ├── view.go          # Ledger view queries
│   ├── queries.go       # State queries
│   ├── certs.go         # Certificate processing
│   ├── delta.go         # State delta tracking
│   ├── event.go         # Ledger events
│   ├── peer_provider.go # Ledger-based peer discovery
│   ├── timer.go         # Slot timing
│   ├── era_summary.go   # Era transition handling
│   └── eras/            # Era-specific ledger handling
├── mempool/             # Transaction pool
│   └── mempool.go       # Mempool, validation, capacity
├── ouroboros/           # Ouroboros protocol handlers
│   ├── ouroboros.go     # N2N and N2C protocol management
│   ├── chainsync.go     # Chain synchronization
│   ├── blockfetch.go    # Block fetching
│   ├── txsubmission.go  # TX submission (N2N)
│   ├── localtxsubmission.go # TX submission (N2C)
│   ├── localtxmonitor.go    # Mempool monitoring
│   ├── localstatequery.go   # Ledger queries
│   └── peersharing.go   # Peer discovery
├── peergov/             # Peer selection and governance
│   ├── peergov.go       # PeerGovernor
│   ├── churn.go         # Peer rotation
│   └── event.go         # Peer events
├── topology/            # Network topology handling
│   └── topology.go      # Topology configuration
├── utxorpc/             # UTxO RPC gRPC server
│   ├── utxorpc.go       # Server setup
│   ├── query.go         # Query service
│   ├── submit.go        # Submit service
│   ├── sync.go          # Sync service
│   └── watch.go         # Watch service
├── internal/
│   ├── config/          # Configuration parsing
│   ├── integration/     # Integration tests
│   ├── node/            # Node orchestration
│   │   ├── node.go      # Node.Run(), component initialization
│   │   └── load.go      # Block loading implementation
│   └── version/         # Version information
├── node.go              # Node struct definition
├── config.go            # Configuration management
└── tracing.go           # OpenTelemetry tracing
```

## Core Node Structure

The `Node` struct (defined in `node.go`) orchestrates all major components:

```go
type Node struct {
    connManager    *connmanager.ConnectionManager  // Network connections
    peerGov        *peergov.PeerGovernor          // Peer selection/governance
    chainsyncState *chainsync.State               // Multi-peer sync state
    chainSelector  *chainselection.ChainSelector  // Chain comparison
    eventBus       *event.EventBus                // Event routing
    mempool        *mempool.Mempool               // Transaction pool
    chainManager   *chain.ChainManager            // Blockchain state
    db             *database.Database             // Storage layer
    ledgerState    *ledger.LedgerState            // UTXO/state tracking
    utxorpc        *utxorpc.Utxorpc               // UTxO RPC server
    ouroboros      *ouroboros.Ouroboros           // Protocol handlers
}
```

### Initialization Flow

When `Node.Run()` is called, components are initialized in this order:

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Node.Run()                                   │
├─────────────────────────────────────────────────────────────────────┤
│  1. EventBus creation                                                │
│  2. Database loading (blob + metadata plugins)                       │
│  3. ChainManager initialization                                      │
│  4. Ouroboros initialization                                         │
│  5. LedgerState creation                                             │
│  6. Mempool setup                                                    │
│  7. ChainsyncState (multi-peer tracking)                             │
│  8. ChainSelector (density-based comparison)                         │
│  9. ConnectionManager (listeners)                                    │
│ 10. PeerGovernor (topology + churn + ledger peers)                   │
│ 11. UTxO RPC server startup                                          │
│ 12. Wait for shutdown signal                                         │
└─────────────────────────────────────────────────────────────────────┘
```

### Shutdown Flow

Graceful shutdown proceeds in phases:

```
┌─────────────────────────────────────────────────────────────────────┐
│  Phase 1: Stop accepting new work                                    │
│  └── ChainSelector, PeerGovernor, UTxO RPC                           │
├─────────────────────────────────────────────────────────────────────┤
│  Phase 2: Drain and close connections                                │
│  └── Mempool, ConnectionManager                                      │
├─────────────────────────────────────────────────────────────────────┤
│  Phase 3: Flush state and close database                             │
│  └── LedgerState, Database                                           │
├─────────────────────────────────────────────────────────────────────┤
│  Phase 4: Cleanup resources                                          │
│  └── Registered shutdown functions, EventBus                         │
└─────────────────────────────────────────────────────────────────────┘
```

## Event-Driven Communication

Components communicate via the `EventBus` (`event/event.go`) rather than direct coupling:

```
┌─────────────┐    publish     ┌───────────┐    deliver    ┌─────────────┐
│  Publisher  │ ─────────────► │  EventBus │ ────────────► │ Subscribers │
└─────────────┘                └───────────┘               └─────────────┘
                                    │
                                    │ async
                                    ▼
                            ┌───────────────┐
                            │  Worker Pool  │
                            │  (4 workers)  │
                            └───────────────┘
```

### Key Event Types

| Event | Source | Purpose |
|-------|--------|---------|
| `mempool.add_tx` | Mempool | Transaction added |
| `mempool.remove_tx` | Mempool | Transaction removed |
| `chain.update` | ChainManager | Block added to chain |
| `chain.fork-detected` | ChainManager | Fork detected |
| `chainselection.peer_tip_update` | ChainSync | Peer chain updated |
| `chainselection.chain_switch` | ChainSelector | Active peer changed |
| `chainselection.selection` | ChainSelector | Chain selection made |
| `connmanager.inbound-conn` | ConnManager | Inbound connection |
| `connmanager.conn-closed` | ConnManager | Connection closed |
| `blockfetch.event` | Ledger | Block fetch operation |
| `chainsync.event` | Ledger | Chainsync operation |
| `ledger.error` | Ledger | Ledger error occurred |
| `peergov.outbound-conn` | PeerGov | Outbound connection initiated |
| `peergov.peer-demoted` | PeerGov | Peer demoted |
| `peergov.peer-promoted` | PeerGov | Peer promoted |
| `peergov.peer-removed` | PeerGov | Peer removed |
| `peergov.peer-added` | PeerGov | Peer added |
| `peergov.peer-churn` | PeerGov | Peer rotation event |
| `peergov.quota-status` | PeerGov | Quota status update |
| `peergov.bootstrap-exited` | PeerGov | Exited bootstrap mode |
| `peergov.bootstrap-recovery` | PeerGov | Bootstrap recovery |

### EventBus Features

- **Asynchronous delivery**: Worker pool (4 workers, 1000-entry queue)
- **Buffered channels**: Timeout protection prevents blocking
- **Metrics**: Event delivery tracking and latency monitoring

## Storage Architecture

Dingo uses a dual-layer storage architecture with pluggable backends:

```
┌─────────────────────────────────────────────────────────────────┐
│                         Database                                 │
├─────────────────────────────┬───────────────────────────────────┤
│       Blob Store            │        Metadata Store             │
│   (blocks, UTxOs, txs)      │   (indexes, accounts, pools)      │
├─────────────────────────────┼───────────────────────────────────┤
│ Plugins:                    │ Plugins:                          │
│  • Badger (default)         │  • SQLite (default)               │
│  • AWS S3                   │  • PostgreSQL                     │
│  • Google Cloud Storage     │  • MySQL                          │
└─────────────────────────────┴───────────────────────────────────┘
```

### Tiered CBOR Cache

Instead of storing full CBOR data redundantly, Dingo uses offset-based references with a tiered cache:

```
┌─────────────────────────────────────────────────────────────────┐
│                     CBOR Data Request                            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Tier 1: Hot Cache (in-memory)                                   │
│  • UTxO entries: configurable count (HotUtxoEntries)             │
│  • Transaction entries: configurable count + byte limit          │
│  • O(1) access, LRU eviction                                     │
└─────────────────────────────────────────────────────────────────┘
                              │ miss
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Tier 2: Block LRU Cache                                         │
│  • Recently accessed blocks with pre-computed indexes            │
│  • Fast extraction without blob store access                     │
└─────────────────────────────────────────────────────────────────┘
                              │ miss
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Tier 3: Cold Extraction                                         │
│  • Fetch block from blob store                                   │
│  • Extract CBOR at stored offset                                 │
└─────────────────────────────────────────────────────────────────┘
```

### CborOffset Structure

Each CBOR reference is a fixed 48-byte `CborOffset` struct:

| Field | Size | Purpose |
|-------|------|---------|
| BlockSlot | 8 bytes | Block slot number |
| BlockHash | 32 bytes | Block hash |
| ByteOffset | 4 bytes | Offset within block CBOR |
| ByteLength | 4 bytes | Length of CBOR data |

### Plugin System

Plugins are registered via a global registry (`database/plugin/plugin.go`):

```go
// Plugin loading flow
plugin.SetPluginOption() → plugin.GetPlugin() → plugin.Start() → Use interface
```

**Interfaces:**
- `BlobStore` - Block/transaction storage operations
- `MetadataStore` - Index and query operations

## Blockchain State Management

The `LedgerState` (`ledger/state.go`) manages UTXO tracking and validation:

```
┌─────────────────────────────────────────────────────────────────┐
│                       LedgerState                                │
├─────────────────────────────────────────────────────────────────┤
│  • UTXO tracking and lookup                                      │
│  • Protocol parameter management                                 │
│  • Certificate processing (pools, stakes, DReps)                 │
│  • Transaction validation (Phase 1: UTXO rules)                  │
│  • Plutus script execution                                       │
│  • Block forging (dev mode)                                      │
│  • State restoration on rollback                                 │
│  • Ledger-based peer discovery                                   │
├─────────────────────────────────────────────────────────────────┤
│                    Database Worker Pool                          │
│  • Async database operations                                     │
│  • Configurable pool size (default: 5 workers)                   │
│  • Fire-and-forget or result-waiting                             │
└─────────────────────────────────────────────────────────────────┘
```

### Ledger View

The `LedgerView` interface provides query access to ledger state:
- UTXO lookups by address or output reference
- Protocol parameter queries
- Stake distribution queries
- Account registration checks

## Chain Management

The `ChainManager` (`chain/manager.go`) manages multiple chains:

```
┌─────────────────────────────────────────────────────────────────┐
│                      ChainManager                                │
├─────────────────────────────────────────────────────────────────┤
│  Primary Chain                                                   │
│  └── Persistent chain loaded from database                       │
│                                                                  │
│  Fork Chains                                                     │
│  └── Temporary chains for peer synchronization                   │
│                                                                  │
│  Block Cache                                                     │
│  └── In-memory cache for quick access                            │
│                                                                  │
│  Rollback Support                                                │
│  └── Reverts chain to previous point, emits rollback events      │
└─────────────────────────────────────────────────────────────────┘
```

### Chain Selection (Ouroboros Praos)

The `ChainSelector` (`chainselection/comparison.go`) implements Ouroboros Praos rules:

1. **Higher block number wins** (longer chain)
2. **At equal block number, lower slot wins** (denser chain)

## Network & Protocol Handling

### Ouroboros Protocol Stack

The `Ouroboros` struct (`ouroboros/ouroboros.go`) manages all protocol handlers:

```
┌─────────────────────────────────────────────────────────────────┐
│                      Ouroboros Protocols                         │
├────────────────────────────┬────────────────────────────────────┤
│     Node-to-Node (N2N)     │     Node-to-Client (N2C)           │
├────────────────────────────┼────────────────────────────────────┤
│  ChainSync                 │  ChainSync                         │
│  └── Block synchronization │  └── Wallet synchronization        │
│                            │                                    │
│  BlockFetch                │  LocalTxMonitor                    │
│  └── Block retrieval       │  └── Mempool queries               │
│                            │                                    │
│  TxSubmission2             │  LocalTxSubmission                 │
│  └── Transaction sharing   │  └── Transaction submission        │
│                            │                                    │
│  PeerSharing               │  LocalStateQuery                   │
│  └── Peer discovery        │  └── Ledger queries                │
└────────────────────────────┴────────────────────────────────────┘
```

### Connection Management

The `ConnectionManager` (`connmanager/connection_manager.go`) handles connection lifecycle:

```
┌─────────────────────────────────────────────────────────────────┐
│                    ConnectionManager                             │
├─────────────────────────────────────────────────────────────────┤
│  Inbound Listeners                                               │
│  ├── TCP N2N (default: 3001)                                     │
│  ├── TCP N2C (default: 3002)                                     │
│  └── Unix socket N2C                                             │
│                                                                  │
│  Outbound Clients                                                │
│  └── Source port selection                                       │
│                                                                  │
│  Connection Tracking                                             │
│  ├── Per-peer connection state                                   │
│  └── Duplex detection (bidirectional connections)                │
└─────────────────────────────────────────────────────────────────┘
```

## Peer Governance

The `PeerGovernor` (`peergov/peergov.go`) manages peer selection and topology:

```
┌─────────────────────────────────────────────────────────────────┐
│                      PeerGovernor                                │
├─────────────────────────────────────────────────────────────────┤
│  Peer Targets                                                    │
│  ├── Known peers: 150                                            │
│  ├── Established peers: 50                                       │
│  └── Active peers: 20                                            │
│                                                                  │
│  Per-Source Quotas                                               │
│  ├── Topology quota: 3 peers                                     │
│  ├── Gossip quota: 12 peers                                      │
│  └── Ledger quota: 5 peers                                       │
│                                                                  │
│  Peer Churn                                                      │
│  ├── Gossip churn: 5 min interval, 20%                           │
│  └── Public root churn: 30 min interval, 20%                     │
│                                                                  │
│  Ledger Peer Discovery                                           │
│  └── Discovers peers from stake pool relays                      │
│                                                                  │
│  Denied List                                                     │
│  └── Prevents reconnection to bad peers (30 min timeout)         │
└─────────────────────────────────────────────────────────────────┘
```

## Transaction Mempool

The `Mempool` (`mempool/mempool.go`) manages pending transactions:

```
┌─────────────────────────────────────────────────────────────────┐
│                        Mempool                                   │
├─────────────────────────────────────────────────────────────────┤
│  Transaction Management                                          │
│  ├── Validation on add                                           │
│  ├── Capacity limits (configurable)                              │
│  └── Automatic purging on chain updates                          │
│                                                                  │
│  Consumer Tracking                                               │
│  └── Per-consumer state for transaction distribution             │
│                                                                  │
│  Metrics                                                         │
│  ├── Transaction count                                           │
│  ├── Total size                                                  │
│  └── Validation statistics                                       │
└─────────────────────────────────────────────────────────────────┘
```

## Design Patterns

### Dependency Injection

The `Node` creates and injects dependencies into components during initialization. Components receive their dependencies through constructors rather than creating them internally.

### Interface Segregation

Small, focused interfaces allow swapping implementations:
- `BlobStore` for blob storage
- `MetadataStore` for metadata storage
- Protocol handler interfaces for Ouroboros

### Plugin Architecture

Storage backends are loaded dynamically through a plugin registry, allowing extension without modifying core code.

### Observer Pattern

The `EventBus` implements publisher/subscriber communication, decoupling components that produce events from those that consume them.

### Iterator Pattern

`ChainIterator` provides sequential access to blocks without exposing internal chain structure.

### Manager Pattern

`ChainManager` and `PeerGovernor` orchestrate related operations and maintain consistent state across multiple entities.

### Worker Pool Pattern

Database operations and event delivery use worker pools for controlled concurrency and backpressure.

## Threading & Concurrency

| Pattern | Usage |
|---------|-------|
| Goroutine Management | Tracked WaitGroups for clean shutdown |
| Mutex Protection | RWMutex for read-heavy operations |
| Atomic Operations | Atomic types for metrics counters |
| Channel Communication | EventBus async delivery |
| Context Cancellation | Graceful shutdown signals |
| Worker Pools | Database operations and event delivery |
| sync.Once | Ensure single shutdown execution |

## Configuration

Configuration priority (highest to lowest):

1. CLI flags
2. Environment variables
3. YAML config file (`dingo.yaml`)
4. Hardcoded defaults

Key configuration areas:
- Network selection (preview, preprod, mainnet)
- Database path and plugins
- Listen addresses and ports
- Mempool capacity
- Peer targets and quotas
- CBOR cache sizing (hot entries, block LRU)

## Stake Snapshots

Stake snapshots capture the stake distribution at epoch boundaries for use in Ouroboros Praos leader election. The block producer must know the stake distribution from 2 epochs ago to determine if they are the slot leader.

### Ouroboros Praos Snapshot Model

```
Epoch N-2        Epoch N-1        Epoch N (current)
   |                |                |
   v                v                v
[Go Snapshot] <- [Set Snapshot] <- [Mark Snapshot]
   |                                    |
   +------------------------------------+
   Used for leader election             Captured at
   in current epoch                     epoch boundary
```

- **Mark Snapshot**: Captured at the end of epoch N, becomes Set at epoch N+1
- **Set Snapshot**: Previous Mark, becomes Go at epoch N+1
- **Go Snapshot**: Active snapshot used for leader election (2 epochs old)

### Stake Snapshot Components

```
┌─────────────────────────────────────────────────────────────────┐
│                   STAKE SNAPSHOT DATA FLOW                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Block Processing                                                │
│       │                                                          │
│       ▼                                                          │
│  ┌─────────────┐     ┌──────────────────┐     ┌───────────────┐ │
│  │ LedgerState │────▶│ Epoch Transition │────▶│ EventBus      │ │
│  │             │     │ Detection        │     │ (EpochEvent)  │ │
│  └─────────────┘     └──────────────────┘     └───────┬───────┘ │
│                                                       │         │
│                                                       ▼         │
│                                            ┌──────────────────┐ │
│                                            │ SnapshotManager  │ │
│                                            │ (Subscribe)      │ │
│                                            └────────┬─────────┘ │
│                                                     │           │
│            ┌────────────────────────────────────────┼─────┐     │
│            │                                        │     │     │
│            ▼                                        ▼     ▼     │
│  ┌─────────────────┐              ┌─────────────────┐  ┌──────┐ │
│  │ Calculate Stake │              │ Rotate Snapshots│  │Cleanup│ │
│  │ Distribution    │              │ Mark→Set→Go     │  │      │ │
│  └────────┬────────┘              └────────┬────────┘  └──────┘ │
│           │                                │                    │
│           ▼                                ▼                    │
│  ┌─────────────────────────────────────────────────────┐       │
│  │                   Database                           │       │
│  │  ┌─────────────────┐  ┌───────────────┐             │       │
│  │  │PoolStakeSnapshot│  │ EpochSummary  │             │       │
│  │  └─────────────────┘  └───────────────┘             │       │
│  └─────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────┘
```

### Database Models

| Model | Purpose |
|-------|---------|
| `PoolStakeSnapshot` | Per-pool stake at epoch boundary (epoch, type, pool hash, stake, delegator count) |
| `EpochSummary` | Network-wide aggregates (total stake, pool count, delegator count, epoch nonce) |

Snapshot types: `"mark"`, `"set"`, `"go"`

### Query Interface

The `LedgerView` provides stake distribution queries:

```go
// Get full stake distribution for leader election
dist, err := ledgerView.GetStakeDistribution(epoch)

// Get stake for a specific pool
poolStake, err := ledgerView.GetPoolStake(epoch, poolKeyHash)

// Get total active stake
totalStake, err := ledgerView.GetTotalActiveStake(epoch)
```

### Event-Driven Capture

`EpochTransitionEvent` triggers snapshot capture:

```go
type EpochTransitionEvent struct {
    PreviousEpoch     uint64
    NewEpoch          uint64
    BoundarySlot      uint64
    EpochNonce        []byte
    ProtocolVersion   uint
    SnapshotSlot      uint64  // Typically boundary - 1
}
```

### Rollback Support

On chain rollback past an epoch boundary:
- Delete snapshots for epochs after rollback point
- Recalculate affected snapshots on forward replay
