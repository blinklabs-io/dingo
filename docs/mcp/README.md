# Model Context Protocol (MCP) Server in Dingo

Dingo includes a built-in **Model Context Protocol (MCP)** server that allows Large Language Models (LLMs) and AI agents (such as Claude Code CLI, Claude Desktop, Cursor, and custom agents) to safely inspect the live Cardano blockchain and query Dingo's SQLite metadata directly.

---

## 1. Quick Start (60 Seconds)

Get Dingo's MCP server running and connected to your AI assistant:

### Step 1: Start Dingo with MCP Enabled
```bash
DINGO_PLUGINS_API_MCP_CONFIG_PORT=8088 ./dingo serve --config dingo.yaml --mcp-provider builtin
```
MCP is disabled by default (`port: 0`): node startup skips the server entirely,
so zero does not allocate a random port. This makes the SQL and node-inspection
endpoint opt-in while authentication is unset by default. The command above
enables it at `http://127.0.0.1:8088`; the default host is loopback.

### Step 2: Verify Listener Health
```bash
curl -s http://127.0.0.1:8088/healthz
# {"service":"dingo-mcp","status":"ok"}
```

### Step 3: Connect Your AI Assistant
- **Claude Code CLI** (SSE):
  ```bash
  claude mcp add --transport sse dingo http://127.0.0.1:8088/sse
  ```
- **Cursor / Windsurf** (SSE):
  In Settings > Features > MCP, add an SSE server with URL `http://127.0.0.1:8088/sse`.
- **Claude Desktop** (Streamable HTTP):
  Add to `claude_desktop_config.json`:
  ```json
  {
    "mcpServers": {
      "dingo": {
        "url": "http://127.0.0.1:8088/mcp"
      }
    }
  }
  ```
- **Codex / Custom Agents** (Streamable HTTP):
  Target `http://127.0.0.1:8088/mcp` with header `Accept: application/json`.

### Step 4: Run an Inspection Prompt
Ask your AI assistant:
> *"What is the current sync slot of my Dingo node, and which blocks did Dingo mint?"*

---

## 2. Architecture & Subsystem Overview

### Transports
- **Streamable HTTP** (`POST/GET /mcp`): Modern bi-directional JSON-RPC 2.0 communication with session state.
- **Server-Sent Events (SSE)** (`GET /sse`): Streaming event transport for CLI and IDE agents.
- **Health Endpoint** (`GET /healthz`): Listener health probe returning `{"service":"dingo-mcp","status":"ok"}`; it does not probe database connectivity.

### 21 Registered Tools
Dingo registers 21 typed, read-only tools:
- **SQLite Tools (3)**: `sqlite_query` (safe SELECT execution), `sqlite_explain` (query plan analysis), `sqlite_table_schema` (DDL schema inspection).
- **Cardano Domain Tools (18)**:
  - Consensus & Chain Tip: `get_cardano_tip`, `get_block`, `get_transaction`.
  - Ledger, Protocol & UTxO: `get_utxos`, `get_account`, `get_asset_info`, `get_utxos_by_asset`, `get_protocol_parameters`, `calculate_min_utxo`, `decode_address`.
  - Mempool & Propagation: `get_mempool_info`.
  - SPO & Governance: `get_pool_performance`, `get_governance_state`, `get_governance_proposal`, `get_epoch_summary`.
  - Smart Contracts: `resolve_datum_or_script`, `evaluate_tx` (pure-Go Plutus V1/V2/V3 simulation).
  - Node Identity & Docs: `get_node_info` (identity, faq, architecture).

### MCP Resources
Passively accessible reference context and schema DDL. Six fixed resources are
always registered; the server also registers one schema resource for each
discovered non-internal SQLite table, so the total varies by database:
- `dingo://docs/identity`: Node architecture, block header fingerprint (Protocol Minor Version 69), durable forge fencing.
- `dingo://docs/faq`: Storage profiles, memory footprint, Mithril fast-sync.
- `dingo://docs/architecture`: Node pipeline and indexer overview.
- `dingo://node/status`: Live tip slot, block height, block hash, and sync status.
- `dingo://dbsync/cheatsheet`: PostgreSQL-to-SQLite schema translation reference.
- `dingo://schema/tables` and `dingo://schema/table/{name}`: DDL and column catalog for discovered SQLite tables.

### 6 Pre-Engineered Prompts (Official Agent Workflows)
Pre-packaged diagnostic and forensic protocols exposed via `prompts/list` and `prompts/get`:
- `diagnose_node_health`: Reported tip and sync status, minor 69 fingerprint, and any forge fence in `sync_state`.
- `simulate_and_diagnose_tx`: Pre-flight dry-run of Plutus redeemers, ExUnits, fees, and datums.
- `audit_pool_rewards`: Recorded block, stake, pledge, cost, margin, performance, and reward snapshots plus a current active account count.
- `track_asset_portfolio`: Asset metadata, circulating supply, holding UTxOs, and paginated asset UTxO inspection.
- `conway_governance_brief`: CIP-1694 Constitution, CC quorum, DReps, and active proposals.
- `investigate_address`: EUTxO forensic audit, token portfolio, datums, and staking delegation.

### Read-Only Security Boundary
- **Pure-Go SQLite Enforcement**: The `modernc.org/sqlite` connection uses `mode=ro` and `PRAGMA query_only = ON`.
- **SQL Statement Validation**: A tokenizer identifies the main statement after any CTE definitions and rejects writes before execution. Read-only EXPLAIN and metadata PRAGMAs are supported.
- **Immutable Ledger State**: Cardano domain tools read immutable in-memory ledger state or read-only database connections.
- **Rate Limiting**: Per-IP token bucket (default 60 req/s, burst 15) prevents runaway agent loops from exhausting CPU or disk I/O.
- **Output Sanitization**: External asset text is length-bounded, stripped of control characters and invisible formatting, JSON-quoted, and escaped for Markdown. Results mark this metadata as untrusted data; clients must not follow instructions contained in it.

---

## 3. Architecture & Request Workflow Diagrams

The Dingo MCP server's end-to-end request flow, security boundary, tool registries, storage layer, and execution lifecycle are illustrated below:

### Visual Subsystem Architecture

![Dingo MCP Subsystem Architecture](architecture.svg)

Edit the [Excalidraw source](architecture.excalidraw) and export an updated SVG to refresh this preview.

---

## 4. Semantic Architecture & Intent Mapping Taxonomy

### 4.1 Why Semantic Categorization Matters
In automated AI agent systems, role-based archetypes ("personas") do not correspond to the physical architecture of the blockchain or the protocol primitives of the Model Context Protocol. An LLM reasoning over an MCP request does not operate on *who* is asking; it reasons over **what state machine, mutability boundary, graph topology, or data lifecycle** it must inspect.

Dingo organizes its MCP subsystem across four primary semantic angles:

#### A. Protocol State & Temporal Mutability (The Cardano Ledger Dimension)
Cardano's ledger divides information into distinct temporal and mutability tiers. Slicing tools along this axis reflects how the node, consensus engine, and indexer handle data lifecycles:

| Semantic State Tier | Semantics & Data Lifecycle | Dingo Tools / Resources | Under-the-Hood Subsystem |
| :--- | :--- | :--- | :--- |
| **Live Consensus & Ephemeral State** | Dynamic, volatile point-in-time consensus state (changes every slot/block). Volatile; cannot be cached long-term. | `get_cardano_tip`, `get_node_info`, `dingo://node/status` | In-memory Go ledger state & Ouroboros Praos engine (~1.5ms). |
| **Settled / Historical Ledger** | Fully validated, immutable on-chain records (blocks, transactions, historical outputs, stake certificates). | `get_block`, `get_transaction`, `get_account`, `sqlite_query` | Metadata SQLite indexer & Badger DB blob storage. |
| **Active UTxO Set & Asset Graph** | Current live unspent outputs (`deleted_slot = 0`), multi-asset balances, and attached datum hashes. | `get_utxos`, `get_utxos_by_asset`, `get_asset_info` | Relational SQLite indexes covering policy, credential, and address. |
| **Deterministic Simulation (Sandboxed)** | Off-chain predictive dry-runs against active protocol parameters before broadcasting. Zero side-effects. | `evaluate_tx`, `sqlite_explain` | Pure-Go Plutus CEK interpreter & SQLite query planner. |
| **Off-Chain & Semantic Anchors** | Off-chain metadata referenced by on-chain hashes (Token Registry, IPFS Constitution, CIP-25 NFT labels). | `get_asset_info`, `resolve_datum_or_script`, `get_governance_state` | CIP-26 registry cache and decoded CBOR payloads. |

#### B. MCP Protocol Primitives: Tools, Resources, and Prompts
The Model Context Protocol specification defines three distinct server capabilities:

1. **Tools (`tools/*`)**: *Active execution & computation* — parameterized functions that the LLM invokes dynamically (e.g., simulating a transaction via `evaluate_tx` or filtering assets via `get_utxos_by_asset`).
2. **Resources (`resources/*`)**: *Passive context injection* — deterministic, read-only documents and schemas read directly into the context window (e.g., table DDLs via `dingo://schema/tables` or node operational state via `dingo://node/status`).
3. **Prompts (`prompts/*`)**: *Structured semantic playbooks* — pre-engineered, multi-turn prompt templates guiding agents through standard analytical workflows (e.g., automated transaction failure diagnostics, SPO reward audits, or Conway governance briefs).

#### C. The EUTxO Graph Dimension (Graph vs Key-Value Semantics)
Cardano's Extended UTxO (EUTxO) model is fundamentally a **directed acyclic graph (DAG)** of value and state transformations, rather than an account balance table:
- **Graph Nodes (State Carriers)**: UTxOs, Datums (`resolve_datum_or_script`), Asset bundles (`get_utxos_by_asset`).
- **Graph Edges (State Transitions)**: Transactions (`get_transaction`), Inputs/Outputs, Redeemer spending paths.
- **Graph Validators (Guards)**: Plutus scripts (V1/V2/V3), Native multisig scripts, ExUnit budgets (`evaluate_tx`).
- **Global Invariants (State Machines)**: Conway CIP-1694 governance proposals, CC votes, DRep delegations (`get_governance_state`).

#### D. Execution Cost & Latency Tiers (Systems Engineering Dimension)
For automated agents operating in tight loops, tools differ significantly in resource utilization and execution isolation:
- **L1 Microsecond (Lock-free In-Memory)**: `get_cardano_tip`, `get_node_info` (atomic memory pointers; $<2\text{ms}$).
- **L2 Millisecond (Indexed Relational I/O)**: `sqlite_query`, `get_utxos`, `get_transaction` (bounded by SQLite read pool and indexes; $5\text{--}25\text{ms}$).
- **L3 Sandboxed Compute (Pure-Go Plutus VM)**: `evaluate_tx` (executes untrusted Plutus scripts in a step-budgeted CEK machine; CPU and memory bound).

---

### 4.2 Intent Mapping Matrix Across Core Cardano Domains

When an operator, developer, or AI agent interacts with Dingo via MCP, the user poses a question in natural language (e.g., *"What is my node's sync tip?"*, *"Find UTxOs holding token policy X"*, or *"Why is my Plutus script failing?"*).

During MCP initialization (`initialize`), Dingo delivers schemas for **21 tools**, six fixed resources plus a database-dependent number of table-schema resources, and **6 prompt workflows**, accompanied by system instructions. The LLM maps the user's intent to the appropriate tool or resource.

The matrix below details how question styles and intents map across 5 core Cardano domains:

### 1. Cardano Core & Ledger State
| Question Style / Intent | Example Natural Language Query | Mapped Tool / Resource | Parameters | Under-the-Hood Behavior |
| :--- | :--- | :--- | :--- | :--- |
| **Chain Tip & Sync** | *"What slot and sync status does Dingo report on Preview?"* | `get_cardano_tip` | `{}` | Reads the live ledger tip when available, otherwise the latest indexed metadata tip; reports source, slot, height, hash, and tracked slots-behind status. It does not report epoch or network propagation latency. |
| **Block Details** | *"Show the indexed block summary for slot 123372879."* | `get_block` | `{"hash_or_slot": "123372879"}` | Returns slot/hash and whichever epoch, height, transaction-count, and fee fields the connected metadata schema provides. It does not decode block headers. |
| **Transaction Lookup** | *"Show the indexed summary and output total for tx X."* | `get_transaction` | `{"tx_hash": "01ab..."}` | Returns transaction ID, slot, block index, fee, output count, and total output lovelace; it does not enumerate inputs, outputs, or metadata. |
| **Protocol Parameters** | *"What are the current linear fee parameters and min UTxO cost per byte?"* | `get_protocol_parameters` | `{}` | Reads active protocol parameters: linear fee coefficients $a, b$, coins per UTxO byte, max ExUnits, Plutus execution prices, collateral percentage, and governance action deposits. |
| **Address Cryptography** | *"Decode this Bech32 address and extract its payment and stake credentials."* | `decode_address` | `{"address": "addr_test1..."}` | Cryptographically decodes Bech32 or raw hex into network ID, address type (Base, Enterprise, Reward, Pointer, Byron), payment credential (key vs script hash), stake credential, and derived stake address. |
| **Minimum UTxO Deposit** | *"Calculate the minimum required Lovelace deposit for an output with 2 native assets and an inline datum."* | `calculate_min_utxo` | `{"output_cbor_hex": "<complete serialized output>"}` | Computes CIP-55 minimum required Lovelace using active ledger `coins_per_utxo_byte` and exact serialized output byte size formula. |
| **Schema Introspection** | *"What are the exact column names and types for the live UTxO table?"* | `sqlite_table_schema` | `{"table_name": "utxo"}` | Queries `sqlite_master` DDL schema and returns structured column catalog. |
| **Ad-Hoc Read Query** | *"Query 3 live unspent outputs that have not been spent."* | `sqlite_query` | `{"query": "SELECT * FROM utxo WHERE deleted_slot = 0 LIMIT 3"}` | Executes read-only `SELECT` with automatic BLOB hex formatting and row caps. |
| **Plutus Simulation** | *"Evaluate execution budget and redeemers for this transaction."* | `evaluate_tx` | `{"cbor": "84a4..."}` | Simulates Plutus V1/V2/V3 script execution in pure Go and returns exact ExUnits (CPU/Memory). |

### 2. Stake Pool Operations
| Question Style / Intent | Example Natural Language Query | Mapped Tool / Resource | Parameters | Under-the-Hood Behavior |
| :--- | :--- | :--- | :--- | :--- |
| **Pool Performance** | *"Inspect recent performance, pledge, and rewards for pool 003A75D8..."* | `get_pool_performance` | `{"pool_id": "003A75D8...", "limit": 10}` | Reads recorded epoch reward and stake snapshots; `limit` defaults to 10 and is capped at 50 epochs. Stake and pledge amounts are lovelace. |
| **Delegator Status** | *"What account fields are indexed for this stake key?"* | `get_account` | `{"stake_address_or_credential": "stake_test1..."}` | Decodes the Bech32 stake credential and returns its stored account row. |
| **Epoch Randomness** | *"What are the active epoch randomness nonces for slot leader checks?"* | `get_epoch_summary` | `{}` | Returns epoch boundary summary, active epoch number, and candidate block counts. |
| **OpCert Sequences** | *"Verify pool operational certificate counter sequence numbers."* | `sqlite_query` | `{"query": "SELECT pool_key_hash, latest_op_cert_sequence FROM pool LIMIT 5"}` | Scans pool state table for OpCert sequence to verify anti-rollback forge fences. |
| **Node Identity** | *"How does Dingo identify itself and comply with Minor 69?"* | `get_node_info` | `{"topic": "identity"}` | Returns official markdown guide detailing pure-Go block header minting and identity. |

### 3. Decentralized Exchange & Liquidity
| Question Style / Intent | Example Natural Language Query | Mapped Tool / Resource | Parameters | Under-the-Hood Behavior |
| :--- | :--- | :--- | :--- | :--- |
| **Asset UTxO Matching** | *"Kupo-style search: Find all live UTxOs holding token policy X."* | `get_utxos_by_asset` | `{"policy_id": "00000000..."}` | Performs indexed multi-asset join across `asset` and `utxo` tables where `deleted_slot = 0`. |
| **CIP-26 Token Registry** | *"Lookup decimals, ticker, and circulating supply for token Y."* | `get_asset_info` | `{"policy_id": "0000...", "asset_name": "TEST"}` | Calculates total on-chain circulating supply and queries CIP-26 registry metadata. |
| **Mempool In-Flight** | *"Inspect pending transactions in the mempool or check if tx X is queued for admission."* | `get_mempool_info` | `{"tx_hash": "01ab..."}` | Queries live Go mempool service: pending transaction count, total memory footprint, capacity saturation percentage, and individual transaction presence. |
| **DEX Contract UTxOs** | *"Fetch all unspent liquidity outputs for DEX script credential 5e68f6..."* | `get_utxos` | `{"address_or_credential": "5e68f6..."}` | Resolves 28-byte payment credential and returns list of UTxOs with amounts and datums. |
| **Query Plan Tuning** | *"Explain the query execution plan for UTxO amount indexing."* | `sqlite_explain` | `{"query": "SELECT * FROM utxo WHERE amount > 10000000 LIMIT 10"}` | Executes `EXPLAIN QUERY PLAN` to reveal table scans, covering indexes, and opcode costs. |
| **Asset Schema** | *"Show schema and indexes for native asset table."* | `sqlite_table_schema` | `{"table_name": "asset"}` | Inspects `asset` table DDL including `policy_id`, `name`, `fingerprint`, and `utxo_id`. |

### 4. Lending, Borrowing & CDP
| Question Style / Intent | Example Natural Language Query | Mapped Tool / Resource | Parameters | Under-the-Hood Behavior |
| :--- | :--- | :--- | :--- | :--- |
| **Collateral Liquidations**| *"Query all active UTxOs that have an attached datum hash for liquidations."* | `sqlite_query` | `{"query": "SELECT * FROM utxo WHERE datum_hash IS NOT NULL LIMIT 5"}` | Finds Plutus contract outputs with datum hashes awaiting off-chain bot liquidations. |
| **Borrower Delegation** | *"Check indexed stake key delegation status before issuing a loan."* | `get_account` | `{"stake_address_or_credential": "stake_test1..."}` | Inspects the stored account row for delegation fields. It does not return reward withdrawal history. |
| **Index Verification** | *"Explain query execution plan for liquidation scanning across datum hashes."* | `sqlite_explain` | `{"query": "SELECT * FROM utxo WHERE datum_hash IS NOT NULL LIMIT 20"}` | Verifies whether query planner utilizes index or full table scan for datum lookups. |
| **Stake Account Schema** | *"Inspect the indexed stake account columns."* | `sqlite_table_schema` | `{"table_name": "account"}` | Returns the account table definition, including credential, delegation, and reward fields. It is not a historical balance snapshot table. |
| **Engine Footprint** | *"Inspect node architecture, storage mode, and memory profile for high-frequency trading."* | `get_node_info` | `{"topic": "architecture"}` | Returns architecture guide explaining Badger DB blob storage and SQLite indexer caching. |

### 5. Governance & Voltaire (CIP-1694)
| Question Style / Intent | Example Natural Language Query | Mapped Tool / Resource | Parameters | Under-the-Hood Behavior |
| :--- | :--- | :--- | :--- | :--- |
| **Conway State** | *"What is the active Constitution IPFS anchor and CC quorum?"* | `get_governance_state` | `{}` | Inspects live Conway governance ledger: extracts constitution anchor, committee size, and DReps. |
| **Governance Proposals**| *"Audit active CIP-1694 governance proposals and tally Constitutional Committee, DRep, and SPO votes."* | `get_governance_proposal` | `{"status": "active"}` | Queries Conway governance proposals from SQLite and computes live voting breakdown tallies across CC members, DReps, and Stake Pool Operators. |
| **Node Diversity** | *"How does Dingo prevent client monoculture on Cardano?"* | `get_node_info` | `{"topic": "identity"}` | Explains Dingo's pure-Go implementation of Ouroboros Praos, eliminating Haskell dependencies. |
| **Consensus Architecture**| *"How does Dingo perform fast ledger sync using Mithril certificates?"* | `get_node_info` | `{"topic": "faq"}` | Details Mithril snapshot downloading and cryptographic signature verification pipeline. |
| **Network Synchronization**| *"What sync status does this node currently report?"* | `get_cardano_tip` | `{}` | Reports the local ledger or metadata tip and tracked slots behind head when live ledger state is available; it does not measure peer latency or compare against remote network time. |

---

## 5. How Dingo MCP Works Under the Hood

The complete lifecycle of a request consists of 6 distinct phases:

1. **Phase 1: Handshake & Discovery (`initialize`)**:
   - The AI client (Claude, Cursor, Codex) initiates a session via `POST /mcp` or `GET /sse`.
   - Dingo responds with server capabilities, registering 21 tools, six fixed resources plus one per discovered non-internal SQLite table, and system steering instructions detailing block identity and forge fencing.
2. **Phase 2: Intent Classification & Tool Invocation (`tools/call`)**:
   - The LLM parses the user prompt, determines the required data, and emits a JSON-RPC 2.0 tool invocation.
3. **Phase 3: Ingress Security Perimeter**:
   - **Authentication**: If configured, `Authorization: Bearer <token>` is validated using `crypto/subtle.ConstantTimeCompare`.
   - **Rate Limiting**: Per-client IP token bucket (default 60 req/s, burst 15) decrements tokens. Excess requests receive HTTP 429.
   - **SQL Statement Validation**: SQL tools tokenize comments, quoted data, and CTE definitions to identify the main statement. Rejected queries return an MCP tool error before execution.
4. **Phase 4: Tool Routing & Engine Execution**:
   - The MCP dispatcher routes the call:
     - **Cardano Domain Tools**: Access immutable in-memory ledger state or compiled pure-Go Ouroboros APIs.
     - **SQLite Tools**: Execute queries against `metadata.sqlite` using a dedicated read-only connection pool (`PRAGMA query_only = ON`).
5. **Phase 5: Output Formatting & Sanitization**:
   - Binary hashes (TX IDs, pool hashes, policy IDs) are formatted into uppercase hex strings.
   - Results are rendered into structured GitHub-flavored Markdown tables with automatic row and column caps.
   - External text is length-bounded and stripped of invisible formatting and control characters; metadata remains untrusted.
6. **Phase 6: AI Synthesis & Response Delivery**:
   - The AI client ingests the sanitized Markdown table and synthesizes a natural language answer with verified facts.

---

## 6. Enabling the MCP Server

The MCP server is compiled in as a modular API provider under capability `api.mcp`. It is disabled until a positive port is configured.

### Command Line Flags

To start Dingo with MCP enabled on port `8088`:

```bash
DINGO_PLUGINS_API_MCP_CONFIG_PORT=8088 ./dingo serve \
  --network preview \
  --storage-mode api \
  --mcp-provider builtin
```

> **Note**: SQL-backed tools require the SQLite metadata provider. Live ledger tools remain available with other metadata providers; `storage-mode` controls indexing depth independently of the provider.

To explicitly disable MCP:
```bash
DINGO_PLUGINS_API_MCP_CONFIG_PORT=0 ./dingo serve
```

### Environment Variables

You can configure MCP via canonical plugin environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `DINGO_PLUGINS_API_MCP_CONFIG_PORT` | `0` (disabled) | Port for the MCP HTTP/SSE listener (also accepts legacy `DINGO_MCP_PORT`) |
| `DINGO_PLUGINS_API_MCP_CONFIG_AUTH_TOKEN` | *empty* (disabled) | Optional Bearer token for authentication |
| `DINGO_PLUGINS_API_MCP_CONFIG_RATE_LIMIT` | `60.0` | Sustained requests per second per IP (0 = unlimited) |
| `DINGO_PLUGINS_API_MCP_CONFIG_BURST` | `15` | Maximum burst requests per IP |
| `DINGO_PLUGINS_API_MCP_CONFIG_QUERY_TIMEOUT` | `5s` | Maximum execution timeout for SQLite queries |
| `DINGO_PLUGINS_API_MCP_CONFIG_MAX_ROWS` | `100` | Maximum rows returned per query by default |

Example with authentication and custom port:
```bash
export DINGO_PLUGINS_API_MCP_CONFIG_PORT=8088
export DINGO_PLUGINS_API_MCP_CONFIG_AUTH_TOKEN="secret-agent-key-123"
./dingo serve --network preview --storage-mode api --mcp-provider builtin
```

### Configuration File (`dingo.yaml`)

Add the `mcp` block under `plugins.api`:

```yaml
plugins:
  storage:
    metadata:
      provider: "sqlite"
  api:
    mcp:
      provider: "builtin"
      config:
        port: 8088
```

---

## 7. Supported Endpoints

When running on port `8088`, Dingo serves:

| Endpoint | Method | Description |
| --- | --- | --- |
| `/mcp` | `POST` | Modern Streamable HTTP JSON-RPC 2.0 MCP endpoint |
| `/sse` | `GET`, `POST` | Legacy & streaming Server-Sent Events MCP endpoint |
| `/health` | `GET` | Plaintext health check (`ok\n`) |
| `/healthz` | `GET` | JSON health check (`{"status":"ok","service":"dingo-mcp"}`) |

---

### Authentication and browser origins

Set `DINGO_MCP_AUTH_TOKEN` or the canonical
`DINGO_PLUGINS_API_MCP_CONFIG_AUTH_TOKEN` to require a Bearer token or
`X-API-Key`. The canonical variable takes precedence when both are present;
otherwise the compatibility variable overrides YAML `authToken`. The
compatibility variable preserves its value verbatim. Canonical plugin variables
use the generic YAML scalar convention; quote token values that resemble YAML
numbers or booleans. Non-loopback binds require both an auth token and server
TLS; loopback remains the default and supports local clients without credentials.

Clients without an `Origin` header remain supported. Browser requests must be
same-origin on loopback or match an explicit entry in root `corsAllowedOrigins`; an empty
list or `"*"` does not allow arbitrary browser origins for MCP. Invalid or
untrusted origins receive HTTP 403 on both transports, including preflight.
For a browser Inspector, add the exact origin shown by the Inspector, for example:

```yaml
corsAllowedOrigins:
  - "http://localhost:6274"
```

`localhost` and `127.0.0.1` are distinct origins. Origin validation happens
before authentication; an allowed browser must also supply the configured token.

### Metadata and query semantics

The `dingo://schema/tables` catalog reads column names and declared types from
the connected SQLite database on each request. The `dingo://dbsync/cheatsheet`
resource provides bounded, executable Dingo SQL examples and explains binary
credentials, lovelace text fields, retained spent outputs and exact-address
lookup. These examples are checked against the production migrations.

MCP opens a separate read-only connection to the active SQLite provider's
resolved file. Provider `dataDir` overrides are honored. With a different
metadata backend or an in-memory SQLite store, file-based SQL tools are
unavailable. Full-address UTxO lookup uses the active database's exact CBOR
address matcher in ascending ledger order. Full addresses use `cursor` pagination
and reject nonzero `offset`; raw payment credentials retain offset pagination
and intentionally search across address forms. Stake addresses and CIP-129 DRep
identifiers preserve key/script types; raw hashes mean key credentials. An
all-zero stake credential remains a valid lookup.

Each full-address request examines at most 2,000 candidates and returns at most
100 matches (default 20). Responses include structured `utxos`, `next_cursor`,
`complete`, `candidates_scanned`, `stop_reason` and `consistency` fields. Continue
with `next_cursor` until `complete` is true, even when a page contains no matches.
Cursors identify the last fully examined candidate and are bound to the address
and network. Do not combine a cursor with an offset.

The query deadline reaches the coordinated read transaction and SQL operations;
cancellation is also checked between CBOR loads/decodes. If the deadline expires
after progress, the response carries the completed results and continuation
cursor with `stop_reason: deadline`. With no progress, it returns a timeout error.
Client cancellation stops the operation. Blob-store operations already in progress
must return before cooperative cancellation can be observed.

Pages observe live ledger state independently, not a fixed snapshot. Outputs may
be spent or added between requests. Restart pagination after a rollback. These
pages do not provide a complete balance at a single ledger snapshot.

`sqlite_query` applies `offset` and the effective row limit while reading results,
including PRAGMA and EXPLAIN results and SQL containing its own LIMIT. The
result cap is the smallest of the requested limit (default 50), configured
`maxRows`, and 200.

Epoch nonce reporting reads `epoch_summary.epoch_nonce` with `epoch.nonce` as
fallback. Recorded block counts cover retained `block_nonce` rows inside the
stored epoch slot boundaries, so pruning can reduce the reported count.
Transaction totals cover retained `utxo` rows and cannot reconstruct pruned
outputs. Pool stake and pledge are reported in lovelace.

Governance registration and recorded-expiry counts describe stored DRep fields;
a recorded expiry is not a claim of current voting eligibility.


## 8. Testing with the MCP Inspector

SQL tools and resource examples can be checked against a running Preview node's
metadata file without starting another node or enabling its MCP listener:

```sh
DINGO_MCP_PREVIEW_TEST_DB=/path/to/metadata.sqlite \
go test -race ./api/mcp -run TestPreviewLiveSQL -v -count=1
```

This test opens the database read-only, starts an in-memory MCP session using the
current implementation, and executes the resource SQL examples and a metadata
PRAGMA. It does not evaluate or submit transactions.

Opt-in live evaluation testing (requires an MCP endpoint; no submission):

```sh
DINGO_MCP_PREVIEW_TEST_URL=http://127.0.0.1:8088/mcp \
DINGO_MCP_PREVIEW_TEST_DB=/path/to/metadata.sqlite \
go test -race ./api/mcp -run TestPreviewLiveEvaluation -v -count=1
```

The database must belong to the Preview endpoint. This checks a no-script
transaction and a Plutus minting template using an unspent input. Templates are
unsigned; successful evaluation is not full transaction validation or acceptance.
Deterministic real-chain evaluator fixtures are covered by
`TestEvaluateTxBabbagePreviewDuplicateRequiredSigner`,
`TestEvaluateTxConwayPreprodFixtures`, and
`TestEvaluateTxConwayPreprodSerialiseData` in `ledger/eras`.

The official [MCP Inspector](https://github.com/modelcontextprotocol/inspector) is the easiest way to test and interact with the server.

### A. Web UI (Interactive Dashboard)

Start the Inspector pointing to Dingo:

```bash
npx @modelcontextprotocol/inspector --web --server-url http://127.0.0.1:8088/mcp
```

1. Open the URL shown in the terminal (e.g. `http://127.0.0.1:6274?...`).
2. Click the **Tools** tab at the top.
3. Select any tool (e.g. `get_cardano_tip` or `sqlite_query`) and click **Execute Tool**.

If Dingo requires an authentication token:
```bash
npx @modelcontextprotocol/inspector --web --server-url http://127.0.0.1:8088/mcp --header "Authorization: Bearer secret-agent-key-123"
```

### B. Command-Line Interface (CLI)

You can run automated queries or test without a browser:

- **List available tools**:
  ```bash
  npx @modelcontextprotocol/inspector --cli --server-url http://127.0.0.1:8088/mcp --method tools/list
  ```

- **Get Cardano chain tip**:
  ```bash
  npx @modelcontextprotocol/inspector --cli --server-url http://127.0.0.1:8088/mcp --method tools/call --tool-name get_cardano_tip
  ```

- **Run an SQL query**:
  ```bash
  npx @modelcontextprotocol/inspector --cli --server-url http://127.0.0.1:8088/mcp \
    --method tools/call \
    --tool-name sqlite_query \
    --tool-args-json '{"query":"SELECT name FROM sqlite_master WHERE type=\"table\" LIMIT 5"}'
  ```

- **List resources (table schemas & guides)**:
  ```bash
  npx @modelcontextprotocol/inspector --cli --server-url http://127.0.0.1:8088/mcp --method resources/list
  ```

- **Read a resource**:
  ```bash
  npx @modelcontextprotocol/inspector --cli --server-url http://127.0.0.1:8088/mcp --method resources/read --uri "dingo://node/status"
  ```

---

## 9. Connecting AI Clients

### Claude Code CLI

Anthropic's [Claude Code](https://docs.anthropic.com/en/docs/agents-and-tools/claude-code/overview) CLI supports MCP servers out-of-the-box over HTTP/SSE.

#### A. Add Dingo MCP via CLI Command

Run in your terminal (with Dingo running on port `8088`):

```bash
claude mcp add --transport sse dingo http://127.0.0.1:8088/sse
```

If Dingo is configured with an authentication token:
```bash
claude mcp add --transport sse dingo http://127.0.0.1:8088/sse --header "Authorization: Bearer secret-agent-key-123"
```

#### B. Verify or Manage Servers in Claude Code

- **List active MCP servers**:
  ```bash
  claude mcp list
  ```
- **Remove a server**:
  ```bash
  claude mcp remove dingo
  ```

#### C. Manual Configuration (`.claude.json`)

Alternatively, configure MCP directly in your global `~/.claude.json` or project-root `.claude.json`:

```json
{
  "mcpServers": {
    "dingo": {
      "url": "http://127.0.0.1:8088/sse"
    }
  }
}
```

---

### Codex CLI / AI Agents

For Codex-based CLI utilities or custom agent frameworks supporting the Model Context Protocol:

1. **Remote SSE Endpoint**: Point your client's MCP configuration to `http://127.0.0.1:8088/sse` (or Streamable HTTP at `http://127.0.0.1:8088/mcp`).
2. **Session Lifecycle**: Dingo handles standard MCP JSON-RPC 2.0 handshakes (`initialize` -> `notifications/initialized`). During `initialize`, Dingo automatically provides **System Instructions** steering the model to its Cardano tools and documentation resources.

---

### Claude Desktop

Edit your Claude Desktop configuration file:
- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
- **Linux**: `~/.config/Claude/claude_desktop_config.json`

#### Option 1: HTTP / SSE (Recommended if Dingo is already running)

```json
{
  "mcpServers": {
    "dingo": {
      "url": "http://127.0.0.1:8088/sse"
    }
  }
}
```

If you configured an authentication token:
```json
{
  "mcpServers": {
    "dingo": {
      "url": "http://127.0.0.1:8088/sse",
      "headers": {
        "Authorization": "Bearer secret-agent-key-123"
      }
    }
  }
}
```

#### Stdio-only clients

Dingo serves MCP over HTTP/SSE and does not implement a stdio transport.
Do not configure the Dingo node executable as a stdio MCP command; use an
HTTP-capable client or an independently configured HTTP-to-stdio bridge.

### Cursor / VS Code / Windsurf

In Cursor or VS Code (with an MCP extension):
1. Navigate to **Settings > Features > MCP**.
2. Add a new server:
   - **Type**: `SSE` or `HTTP`
   - **URL**: `http://127.0.0.1:8088/sse` (or `/mcp`)

---

## 10. Prompting & Steering the Agent to Use MCP Docs

When an AI agent connects to Dingo MCP, Dingo automatically transmits guidance instructions via the `initialize` handshake.

You can steer the agent in conversation with prompts like:

| What You Want to Ask | Prompt Example | Tool / Resource Invoked |
| :--- | :--- | :--- |
| **Node & Block Identity** | *"How does Dingo identify itself and its forged blocks on-chain?"* | `get_node_info(topic="identity")` or `dingo://docs/identity` |
| **Dingo FAQ** | *"What are Dingo's storage modes and how does Mithril bootstrap work?"* | `get_node_info(topic="faq")` or `dingo://docs/faq` |
| **Node Architecture** | *"Explain Dingo's consensus engine and storage subsystems."* | `get_node_info(topic="architecture")` or `dingo://docs/architecture` |
| **Live Chain Tip** | *"What is the current Cardano slot and sync status?"* | `get_cardano_tip` |
| **Schema Mapping** | *"How do I query transactions using cardano-db-sync style in Dingo?"* | `dingo://dbsync/cheatsheet` |
| **SPO Performance** | *"Check block production and rewards for pool 5e4e3bb... in epoch 1426"* | `get_pool_performance` |

---

## 11. Tools Reference

The Dingo MCP server provides 21 specialized tools:

| Tool | Purpose | Key Parameters |
| --- | --- | --- |
| `get_node_info` | Retrieve Dingo node architecture, FAQs, block-production identity (minor 69), storage modes, and documentation | `topic` (string, optional: `'identity'`, `'faq'`, `'architecture'`) |
| `sqlite_query` | Run read-only SQL `SELECT` against metadata SQLite | `query` (string, required), `limit` (int, default 50), `offset` (int) |
| `sqlite_table_schema` | Inspect table columns, types, primary keys, and indexes | `table_name` (string, required) |
| `sqlite_explain` | Run `EXPLAIN QUERY PLAN` to audit index usage and query performance | `query` (string, required) |
| `get_cardano_tip` | Live Cardano slot, height, block hash, and sync state | *none* |
| `get_block` | Look up block details by slot number or block hash | `hash_or_slot` (string, required) |
| `get_transaction` | Fetch transaction details by 64-character hex hash | `tx_hash` (string, required) |
| `get_utxos` | Query unspent outputs for a Bech32 address or payment credential | `address_or_credential` (string, required), `limit`, `offset` |
| `get_account` | Query the stored stake-account row, including indexed delegation, reward, and DRep fields | `stake_address_or_credential` (string, required), `credential_type` (`key` or `script`, required for hex credentials) |
| `get_epoch_summary` | Summary stats and boundary nonces for an epoch | `epoch` (int, optional) |
| `resolve_datum_or_script` | Look up and decode Plutus CIP-32 datum or script metadata from hash | `hash` (string, required - 56 or 64 hex chars) |
| `evaluate_tx` | Simulate Plutus execution units (CPU steps, memory) & fee before submission | `cbor` (string, required - hex or base64) |
| `get_utxos_by_asset` | Kupo-style pattern matching querying unspent UTxOs holding a specific policy or asset | `policy_id` (56-hex, required), `asset_name` (optional ASCII or hex), `limit`, `offset` |
| `get_asset_info` | Detailed Cardano native asset metadata, Token Registry info, on-chain supply, count of holding UTxOs, and CIP-25 NFT metadata | `policy_id` (56-hex, required), `asset_name` (optional ASCII or hex) |
| `get_governance_state` | Conway CIP-1694 governance state (constitution, committee quorum, members, DReps) | `drep_credential` (optional Bech32 `drep1...` or 56-hex) |
| `get_pool_performance` | Historical SPO reward performance, blocks, delegated stake and pledge (lovelace), and apparent performance | `pool_id` (Bech32 `pool1...` or 56-hex, required), `epoch` (optional int), `limit` (default 10, max 50) |
| `get_protocol_parameters` | Active ledger protocol parameters (fees, min UTxO, max ExUnits, Plutus prices, governance deposits) | *none* |
| `get_mempool_info` | Live mempool metrics (pending tx count, byte volume, saturation percentage) or tx status | `tx_hash` (optional 64-character hex) |
| `decode_address` | Cryptographically parse Bech32 or raw hex address into network ID and payment/stake credentials | `address` (string, required) |
| `get_governance_proposal` | Query CIP-1694 governance proposals with CC, DRep, and SPO voting breakdown tallies | `tx_hash` (optional), `action_index` (optional), `status` (optional: `active`, `ratified`, `enacted`, `expired`, `all`), `limit` |
| `calculate_min_utxo` | Compute CIP-55 minimum required Lovelace for output shapes with multi-assets and datums | `output_cbor_hex`, `address`, `coins_per_utxo_byte` |

For `get_account`, a Bech32 stake reward address encodes whether its stake
credential is a key or script. A 56-character hex hash does not encode that
distinction; pair it with `credential_type` set to `key` or `script`. If both
a reward address and `credential_type` are supplied, they must agree.

### Protocol Parameters & Ledger Economy

#### `get_protocol_parameters`
Retrieves current active protocol parameters governing ledger consensus, fee computation, and Plutus execution. Explicit `epoch` requests return an unsupported error; the tool does not forecast or label current values as historical parameters:
- **Linear Fee Formula**: Base linear parameters $a$ (`min_fee_a`) and $b$ (`min_fee_b`) for calculating minimum transaction fee ($\text{fee} = a \times \text{size}(tx) + b$).
- **Min UTxO Value**: Shelley/Allegra/Mary report `min_utxo_value` in Lovelace; Alonzo reports `coins_per_utxo_word` in Lovelace per 8-byte word; Babbage/Conway/Dijkstra report `coins_per_utxo_byte` in Lovelace per byte.
- **Plutus Execution Unit Prices**: Step price per CPU unit and memory unit fractions for Plutus script validation.
- **Execution Budget Caps**: Maximum ExUnits allowable per individual transaction and per block (`max_tx_ex_mem`, `max_tx_ex_steps`, `max_block_ex_mem`, `max_block_ex_steps`).
- **Collateral & Security Constraints**: Minimum collateral percentage (e.g. 150%) and maximum collateral inputs (`max_collateral_inputs`).
- **Voltaire Governance Deposits**: Minimum DRep registration deposit (`drep_deposit`) and governance proposal action deposit (`gov_action_deposit`).

#### `calculate_min_utxo`

Calculates the Babbage/Conway minimum using gouroboros `babbage.MinCoinTxOut`:
`(160 + serialized output CBOR bytes) * coins_per_utxo_byte`.

- Supply `output_cbor_hex` with the complete serialized output, including its
  address, coin value, assets, datum and reference script. The result applies to
  those exact bytes; recalculate after modifying the output.
- For an ADA-only output, supply `address` or omit it to use a testnet enterprise
  address with a zero key hash. The tool adjusts the coin value until the output
  contains its own minimum deposit.
- `coins_per_utxo_byte` overrides the active Babbage/Conway parameter. When no
  parameter is available, the fallback is 4,310 lovelace per byte.
- Counts and partial datum/script descriptions cannot determine an exact size.
  The legacy `assets_count`, `policies_count`, `has_datum`, `inline_datum_hex`
  and `ref_script_hex` inputs now return an error requesting `output_cbor_hex`.

An ADA-only enterprise output at 4,310 lovelace per byte requires 857,690
lovelace. Full CBOR supports long asset names and arbitrary valid output detail
without relying on average asset sizes.

### Mempool & In-Flight Diagnostics

#### `get_mempool_info`
Inspects Dingo's live in-memory transaction buffer (mempool) awaiting inclusion in upcoming forged blocks:
- **Global Mempool Health (when called with `{}`)**:
  - `pending_tx_count`: Total number of validated transactions currently queued in memory.
  - `mempool_bytes`: Aggregated memory volume consumed by buffered transactions.
  - `max_mempool_bytes`: Configured maximum memory pool capacity (default 64 MiB).
  - `saturation_pct`: Real-time percentage saturation of the node's mempool buffer.
  - `transactions`: Summary listing of pending transaction hashes, fees, and sizes in bytes.
- **Single Transaction Inspection (when called with `{"tx_hash": "..."}`)**:
  - Verifies whether an emitted transaction has been admitted into the local mempool, its queued slot, fee, and byte size.

### Cryptographic Address Inspection

#### `decode_address`
Cryptographically parses any Cardano address (Bech32 or uppercase/lowercase hex) using pure-Go Ouroboros ledger primitives without external network lookups:
- **Network ID**: Distinguishes Mainnet (`1`) from Testnet/Preview/Preprod (`0`).
- **Address Type**: Identifies address standard (`Shelley Base`, `Enterprise`, `Reward Account`, `Pointer`, `Byron`).
- **Payment Credential**:
  - `Kind`: `KeyHash` (standard Ed25519 public key hash) or `ScriptHash` (Plutus/Native script).
  - `Hash`: 28-byte hex credential hash.
- **Stake Credential**:
  - `Kind`: `KeyHash` or `ScriptHash` if delegated, or `None` for enterprise/byron addresses.
  - `Hash`: 28-byte hex stake credential hash.
- **Derived Stake Address**: Formats the associated Bech32 reward account (`stake1...` or `stake_test1...`) directly from the stake credential.

### Native Asset & Kupo-Style Pattern Matching

#### `get_utxos_by_asset`
Mirrors Kupo-style indexing by finding all unspent UTxOs containing assets under a specific minting policy:
- Supports matching by `policy_id` alone (wildcard asset name) or exact `policy_id` + `asset_name` (accepts ASCII string like `HOSKY` or hex).
- Returns a markdown table detailing UTxO reference (`tx_hash#index`), payment credential hash, lovelace balance, asset amount, asset fingerprint, and datum hash.
- Filters out spent outputs (`deleted_slot = 0`).

#### `get_asset_info`
Consolidates on-chain and off-chain asset intelligence into a unified profile:
- **Cardano Token Registry**: Pulls off-chain name, ticker, description, website URL, and decimals from the synchronized token registry.
- **On-Chain Circulation**: Aggregates total live unspent quantity and the count of holding UTxOs. This count is not a unique wallet or address count.
- **Mint / Burn History**: Shows initial mint transaction hash, slot, and total minted/burned quantity.
- **CIP-25 NFT Metadata**: Returns sanitized, JSON-quoted text from transaction label 721 when available; the result is marked as untrusted data.

### Conway CIP-1694 Governance

#### `get_governance_state`
Inspects on-chain governance state introduced in the Conway era:
- **Constitution**: Anchor URL, 32-byte anchor hash, script hash, and slot ratified.
- **Constitutional Committee**: Voting quorum threshold (e.g. `2/3`), cold credential hashes, term start slot, and expiration epoch.
- **Delegated Representatives (DReps)**: Active registered DRep counts, activity and expiry epochs. If `drep_credential` is specified (in Bech32 `drep1...` or hex), inspects that specific DRep's registration anchor and status.

#### `get_governance_proposal`
Queries submitted CIP-1694 governance proposals from the SQLite indexer and tallies live votes:
- **Filtering Options**: Filter by separate `tx_hash` and `action_index` parameters or governance lifecycle status:
  - `active`: Currently undergoing voting in the current voting window.
  - `ratified`: Passed vote thresholds and approved for enactment.
  - `enacted`: Executed on-chain across epoch boundaries.
  - `expired`: Dropped without passing required quorum.
  - `all`: Unfiltered inspection.
- **Live Multi-Role Voting Breakdown**: Automatically counts and tallies votes across:
  - **Constitutional Committee (CC)**: Yes / No / Abstain counts.
  - **Delegated Representatives (DReps)**: Yes / No / Abstain counts.
  - **Stake Pool Operators (SPOs)**: Yes / No / Abstain counts.
- **Proposal Metadata**: Displays proposal action type (e.g. `ParameterChange`, `HardForkInitiation`, `TreasuryWithdrawals`, `InfoAction`), Lovelace deposit, return address, submission slot, and anchor URL/hash.

### Stake Pool Operator (SPO) Performance

#### `get_pool_performance`
Provides historical staking and rewards analytics for any stake pool:
- Accepts pool ID in Bech32 format (`pool1...`) or 56-character hex hash.
- Displays recorded blocks, delegated stake and pledge (lovelace), fixed cost (lovelace), margin, apparent performance ratio, and reward totals per epoch.
- Allows filtering by a specific epoch or viewing recent epochs (`limit` defaults to 10 and is capped at 50).

### Plutus Smart Contract & DevOps Tools

#### `resolve_datum_or_script`
Resolves 28-byte script hashes or 32-byte datum hashes stored on-chain:
- For **Datums**: Decodes raw Plutus CBOR data into structured JSON, displaying constructor tags, fields, and the slot it was added.
- For **Scripts**: Identifies script type (`Native / MultiSig`, `PlutusV1`, `PlutusV2`, `PlutusV3`), size in bytes, and slot created.
- **Coverage**: Core mode does not populate the API-mode datum, script, or redeemer detail indexes. API mode indexes details from transactions it processes; available and retained history still limits results. Dingo rejects switching an existing core-mode database to API mode in place, so use a separate API-mode database when these indexes are required. An empty table or missing hash is not evidence that the blockchain has no such data. SQL failures are reported as query errors, separately from missing rows.

#### `evaluate_tx`
Dry-runs a signed or unsigned transaction against Dingo's active ledger state and protocol parameters without broadcasting it to the network:
- Computes exact **CPU steps** and **Memory units** consumed by each Plutus redeemer.
- Formats results in a clean markdown breakdown table indexed by redeemer purpose (`spend`, `mint`, `cert`, `reward`, `voting`, `proposing`, `guarding`) and index.
- Calculates total estimated Plutus script execution fee.
- If evaluation fails (e.g. Phase-2 validation failure or missing input UTxO), returns structured diagnostic guidance for the agent to recommend fixes.

### Read-Only Query Guardrails

The `sqlite_query` tool strictly verifies queries before execution:
- SELECT and VALUES reads, including recursive CTEs, are permitted. The main statement after WITH must be a read. EXPLAIN and EXPLAIN QUERY PLAN can inspect ordinary SQL, including DDL and DML, without executing the statement.
- Multi-statement execution (semicolons separating queries) is blocked.
- Executable mutations are rejected. Metadata PRAGMAs `table_info`, `index_list`, `index_info`, and `foreign_key_list` are allowed and paginated during row iteration. Other PRAGMAs, including `EXPLAIN PRAGMA query_only=OFF`, are rejected.

---

## 12. Resources Reference

MCP Resources allow LLMs to passively read context without invoking functions:

| Resource URI | Description |
| --- | --- |
| `dingo://docs/identity` | Complete guide to Dingo's on-chain block fingerprint (`BlockHeaderProtocolMinor = 69`), OpCert sequence tracking, and peer handshakes. |
| `dingo://docs/faq` | Frequently asked questions covering Dingo architecture, storage modes (`core` vs `api`), Mithril fast-sync, and tool usage. |
| `dingo://docs/architecture` | Detailed breakdown of Ouroboros Praos consensus, Plutus interpreter, block forging, and Badger/SQLite storage subsystems. |
| `dingo://node/status` | Current tip slot, block height, block hash, and sync status. |
| `dingo://schema/tables` | Catalog of all metadata tables with column summaries and descriptions. |
| `dingo://schema/table/<name>` | Full dynamic SQL schema definition and index list for any table `<name>`. |
| `dingo://dbsync/cheatsheet` | Rosetta stone mapping queries from PostgreSQL `cardano-db-sync` schema to Dingo's SQLite schema. |

---

## 13. Prompts Reference (Official Agent Workflows)

Dingo implements official agent workflows natively in Go using the Model Context Protocol prompts capability (`prompts/list` and `prompts/get`). These workflows package domain-expert Cardano diagnostic and forensic procedures into standardized prompt templates that AI clients and CLI inspectors can execute interactively.

| Prompt | Title | Key Arguments | Target Persona & Workflow Protocol |
| :--- | :--- | :--- | :--- |
| `diagnose_node_health` | Diagnose Cardano Node Sync & Health | `detailed` (optional bool) | **Node Operations Engineer**: Evaluates reported tip and sync status, identity minor 69, and any forge fence in `sync_state`. It does not assess storage health. |
| `simulate_and_diagnose_tx` | Simulate and Diagnose Plutus Transaction | `tx_cbor` (required hex/b64), `purpose` (optional string) | **Smart Contract Auditor**: Executes dry-run in pure-Go Plutus CEK VM via `evaluate_tx`, computes ExUnits (CPU/Memory), audits fee sufficiency, validates collateral, and checks inline datums. |
| `audit_pool_rewards` | Audit Stake Pool Performance & Rewards | `pool_id` (required Bech32/hex), `epoch` (optional non-negative integer string) | **Staking Analyst**: Audits recorded block/reward snapshots and queries the current active account count for the pool. Account rows do not provide a delegated stake total. |
| `track_asset_portfolio` | Track Cardano Native Asset & Circulation | `policy_id` (required 56-hex), `asset_name` (optional, max 32 bytes) | **Financial Analyst**: Reads asset metadata and paginates matching UTxOs. The asset-info count is holding UTxOs, not unique addresses; no holder concentration is calculated. |
| `conway_governance_brief` | Conway CIP-1694 Governance & Constitutional Audit | `drep_id` (optional), `stake_address` (optional) | **Governance Delegate**: Audits active Constitution anchor, Constitutional Committee quorum, DRep delegation, and active governance proposals. |
| `investigate_address` | Investigate Address & EUTxO State | `address` (required Bech32/credential) | **Forensic Investigator**: Queries live unspent outputs, lovelace balance, multi-asset token holdings, CIP-32 datums, staking delegation, and pool rewards. |

### Inspecting & Triggering Prompts via CLI / JSON-RPC

Clients supporting MCP prompts can list and trigger these workflows directly:

#### 1. Listing Available Prompts (`prompts/list`)
```json
// Request (JSON-RPC 2.0)
{"jsonrpc":"2.0","id":1,"method":"prompts/list"}

// Response
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "prompts": [
      {
        "name": "diagnose_node_health",
        "description": "Workflow to inspect the reported Dingo node tip and sync status, identity fingerprint, and any forge fence in sync_state.",
        "arguments": [
          {"name": "detailed", "description": "Set to 'true' to include database schema and peer connectivity checks.", "required": false}
        ]
      },
      {
        "name": "simulate_and_diagnose_tx",
        "description": "Pre-flight simulation and diagnostic workflow for Cardano transactions, Plutus scripts, ExUnits, and datums.",
        "arguments": [
          {"name": "tx_cbor", "description": "Hexadecimal or base64 encoded transaction CBOR.", "required": true},
          {"name": "purpose", "description": "Specific focus area: 'exunits', 'fee', 'collateral', 'datums', or 'all'.", "required": false}
        ]
      },
      {
        "name": "audit_pool_rewards",
        "description": "Review recorded pool block, stake, pledge, cost, margin, performance, and reward snapshots, plus the current active account count.",
        "arguments": [
          {"name": "pool_id", "description": "Bech32 pool ID (pool1...) or 56-character hex pool key hash.", "required": true},
          {"name": "epoch", "description": "Specific epoch number to analyze (default: current/latest epoch).", "required": false}
        ]
      },
      {
        "name": "track_asset_portfolio",
        "description": "Asset workflow to inspect Token Registry and CIP-25 metadata, circulating supply, the count of holding UTxOs, and paginated matching UTxOs.",
        "arguments": [
          {"name": "policy_id", "description": "56-character hex minting policy ID.", "required": true},
          {"name": "asset_name", "description": "ASCII asset name (e.g., 'HOSKY') or hex-encoded asset name.", "required": false}
        ]
      },
      {
        "name": "conway_governance_brief",
        "description": "Constitutional and governance audit workflow for Conway CIP-1694 state, DReps, and CC quorum.",
        "arguments": [
          {"name": "drep_id", "description": "Optional Bech32 DRep ID (drep1...) or hex credential.", "required": false},
          {"name": "stake_address", "description": "Optional Bech32 stake address to audit DRep delegation.", "required": false}
        ]
      },
      {
        "name": "investigate_address",
        "description": "Forensic investigation workflow for an address or payment credential (UTxOs, tokens, datums, staking).",
        "arguments": [
          {"name": "address", "description": "Bech32 address (addr1...) or payment credential hash.", "required": true}
        ]
      }
    ]
  }
}
```

#### 2. Executing a Prompt Workflow (`prompts/get`)
```json
// Request
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "prompts/get",
  "params": {
    "name": "diagnose_node_health",
    "arguments": {
      "detailed": "true"
    }
  }
}

// Response
{
  "jsonrpc": "2.0",
  "id": 2,
  "result": {
    "description": "Cardano Node Health & Sync Diagnosis Workflow",
    "messages": [
      {
        "role": "user",
        "content": {
          "type": "text",
          "text": "You are a Cardano node operations engineer diagnosing the health and synchronization state of this Dingo node. Follow this step-by-step diagnostic workflow:\n\n1. Node Tip & Consensus State: Invoke `get_cardano_tip`...\n2. Operational Status & Storage Mode: Read `dingo://node/status`...\n3. Forge Fencing & Sync State: Execute `sqlite_query`...\n4. Node Identity & Protocol Compliance: Invoke `get_node_info(topic=\"identity\")`...\n5. Diagnostic Summary & Remediation: Synthesize findings into an operational brief..."
        }
      }
    ]
  }
}
```


### Governance query contracts

`get_governance_proposal` excludes deleted proposals and votes. Database query
or scan failures return tool errors; they do not imply no proposals or zero
votes. List status accepts `active`, `ratified`, `enacted`, `expired`, or `all`
(case-insensitive, with surrounding whitespace ignored); other values are
errors. `action_index` is an unsigned 32-bit integer and requires `tx_hash`.
Omitting it when supplying a hash selects action zero.
