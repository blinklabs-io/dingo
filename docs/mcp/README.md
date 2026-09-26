# Model Context Protocol (MCP) Server in Dingo

Dingo includes a built-in **Model Context Protocol (MCP)** server that allows Large Language Models (LLMs) and AI agents (such as Claude Code CLI, Claude Desktop, Cursor, and custom agents) to safely inspect the live Cardano blockchain and query Dingo's SQLite metadata directly.

---

## 1. Quick Start (60 Seconds)

Get Dingo's MCP server running and connected to your AI assistant:

### Step 1: Start Dingo with MCP Enabled
```bash
./dingo serve --config dingo.yaml --mcp-provider builtin
```
*(By default, the in-process MCP listener binds to `http://127.0.0.1:8088`)*

### Step 2: Verify Listener Health
```bash
curl -s http://127.0.0.1:8088/healthz
# {"status":"healthy","uptime_seconds":12,"database_connected":true}
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
- **Health Endpoint** (`GET /healthz`): Health probe returning uptime and database connection status.

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

### 81 MCP Resources
Passively accessible reference context and schema DDL:
- `dingo://docs/identity`: Node architecture, block header fingerprint (Protocol Minor Version 69), durable forge fencing.
- `dingo://docs/faq`: Storage profiles, memory footprint, Mithril fast-sync.
- `dingo://docs/architecture`: Node pipeline and indexer overview.
- `dingo://node/status`: Live tip slot, epoch, and sync percentage.
- `dingo://dbsync/cheatsheet`: PostgreSQL-to-SQLite schema translation reference.
- `dingo://schema/tables` and `dingo://schema/table/{name}`: Complete DDL and column catalog for all 78 SQLite tables.

### 6 Pre-Engineered Prompts (Official Agent Workflows)
Pre-packaged diagnostic and forensic protocols exposed via `prompts/list` and `prompts/get`:
- `diagnose_node_health`: Tip latency, sync progress, minor 69 fingerprint, and forge fences.
- `simulate_and_diagnose_tx`: Pre-flight dry-run of Plutus redeemers, ExUnits, fees, and datums.
- `audit_pool_rewards`: Block production track record, apparent performance ratio, active stake, and fees.
- `track_asset_portfolio`: Kupo-style UTxO pattern matching, circulating supply, holders, and CIP-25 NFT metadata.
- `conway_governance_brief`: CIP-1694 Constitution, CC quorum, DReps, and active proposals.
- `investigate_address`: EUTxO forensic audit, token portfolio, datums, and staking delegation.

### Read-Only Security Boundary
- **CGO Driver Enforcement**: SQLite connections run with `PRAGMA query_only = ON`.
- **AST Parser Validation**: Any mutating SQL statement (`INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `ATTACH`) is rejected before execution.
- **Immutable Ledger State**: Cardano domain tools read immutable in-memory ledger state or read-only database connections.
- **Rate Limiting**: Per-IP token bucket (default 60 req/s, burst 15) prevents runaway agent loops from exhausting CPU or disk I/O.
- **Output Sanitization**: Control characters and prompt-injection vectors are sanitized before transmission.

---

## 3. Architecture & Request Workflow Diagrams

The Dingo MCP server's end-to-end request flow, security boundary, tool registries, storage layer, and execution lifecycle are illustrated below:

### Visual Subsystem Architecture

![Dingo MCP Subsystem Architecture](architecture.svg)

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

The AI agent does not guess randomly. During MCP initialization (`initialize`), Dingo delivers structured JSON schemas for **21 tools**, **81 resources**, and **6 prompt workflows**, accompanied by system steering instructions. The LLM's reasoning engine parses the user's intent and maps it directly to the optimal tool.

The matrix below details how question styles and intents map across 5 core Cardano domains:

### 1. Cardano Core & Ledger State
| Question Style / Intent | Example Natural Language Query | Mapped Tool / Resource | Parameters | Under-the-Hood Behavior |
| :--- | :--- | :--- | :--- | :--- |
| **Chain Tip & Sync** | *"What slot and epoch is Dingo synced to on Preview?"* | `get_cardano_tip` | `{}` | Reads live Ouroboros consensus tip from in-memory Go ledger state (~1.5ms). |
| **Block Details** | *"Show block header and tx count for slot 123372879."* | `get_block` | `{"hash_or_slot": "123372879"}` | Fetches block CBOR header, extracts block hash, slot, issuer VKey, and VRF certificate. |
| **Transaction Lookup** | *"Inspect transaction inputs, outputs, and fee for tx X."* | `get_transaction` | `{"tx_hash": "01ab..."}` | Reads transaction record from SQLite indexer and returns formatted inputs/outputs. |
| **Protocol Parameters** | *"What are the current linear fee parameters and min UTxO cost per byte?"* | `get_protocol_parameters` | `{}` | Reads active protocol parameters: linear fee coefficients $a, b$, coins per UTxO byte, max ExUnits, Plutus execution prices, collateral percentage, and governance action deposits. |
| **Address Cryptography** | *"Decode this Bech32 address and extract its payment and stake credentials."* | `decode_address` | `{"address": "addr_test1..."}` | Cryptographically decodes Bech32 or raw hex into network ID, address type (Base, Enterprise, Reward, Pointer, Byron), payment credential (key vs script hash), stake credential, and derived stake address. |
| **Minimum UTxO Deposit** | *"Calculate the minimum required Lovelace deposit for an output with 2 native assets and an inline datum."* | `calculate_min_utxo` | `{"has_inline_datum": true, "assets": [{"policy_id": "0000...", "asset_name": "TEST", "quantity": 1}]}` | Computes CIP-55 minimum required Lovelace using active ledger `coins_per_utxo_byte` and exact serialized output byte size formula. |
| **Schema Introspection** | *"What are the exact column names and types for the live UTxO table?"* | `sqlite_table_schema` | `{"table_name": "utxo"}` | Queries `sqlite_master` DDL schema and returns structured column catalog. |
| **Ad-Hoc Read Query** | *"Query 3 live unspent outputs that have not been spent."* | `sqlite_query` | `{"query": "SELECT * FROM utxo WHERE deleted_slot = 0 LIMIT 3"}` | Executes read-only `SELECT` with automatic BLOB hex formatting and row caps. |
| **Plutus Simulation** | *"Evaluate execution budget and redeemers for this transaction."* | `evaluate_tx` | `{"cbor": "84a4..."}` | Simulates Plutus V1/V2/V3 script execution in pure Go and returns exact ExUnits (CPU/Memory). |

### 2. Stake Pool Operations
| Question Style / Intent | Example Natural Language Query | Mapped Tool / Resource | Parameters | Under-the-Hood Behavior |
| :--- | :--- | :--- | :--- | :--- |
| **Pool Performance** | *"Inspect performance, pledge, and margin for pool 003A75D8..."* | `get_pool_performance` | `{"pool_id": "003A75D8..."}` | Queries pool registration certificates, historical minted blocks, and pledge satisfaction. |
| **Delegator Status** | *"What is the reward balance and delegated pool for this stake key?"* | `get_account` | `{"stake_address_or_credential": "stake_test1..."}` | Decodes Bech32 stake address, reads reward account balance and active delegation. |
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
| **Borrower Delegation** | *"Verify borrower stake key delegation status before issuing loan."* | `get_account` | `{"stake_address_or_credential": "stake_test1..."}` | Inspects live account table for delegation status and reward withdrawal activity. |
| **Index Verification** | *"Explain query execution plan for liquidation scanning across datum hashes."* | `sqlite_explain` | `{"query": "SELECT * FROM utxo WHERE datum_hash IS NOT NULL LIMIT 20"}` | Verifies whether query planner utilizes index or full table scan for datum lookups. |
| **Balance Snapshots** | *"Inspect account table schema for staking balance snapshots."* | `sqlite_table_schema` | `{"table_name": "account"}` | Returns account table columns: staking_key, credential_tag, pool, drep, rewards. |
| **Engine Footprint** | *"Inspect node architecture, storage mode, and memory profile for high-frequency trading."* | `get_node_info` | `{"topic": "architecture"}` | Returns architecture guide explaining Badger DB blob storage and SQLite indexer caching. |

### 5. Governance & Voltaire (CIP-1694)
| Question Style / Intent | Example Natural Language Query | Mapped Tool / Resource | Parameters | Under-the-Hood Behavior |
| :--- | :--- | :--- | :--- | :--- |
| **Conway State** | *"What is the active Constitution IPFS anchor and CC quorum?"* | `get_governance_state` | `{}` | Inspects live Conway governance ledger: extracts constitution anchor, committee size, and DReps. |
| **Governance Proposals**| *"Audit active CIP-1694 governance proposals and tally Constitutional Committee, DRep, and SPO votes."* | `get_governance_proposal` | `{"status": "active"}` | Queries Conway governance proposals from SQLite and computes live voting breakdown tallies across CC members, DReps, and Stake Pool Operators. |
| **Node Diversity** | *"How does Dingo prevent client monoculture on Cardano?"* | `get_node_info` | `{"topic": "identity"}` | Explains Dingo's pure-Go implementation of Ouroboros Praos, eliminating Haskell dependencies. |
| **Consensus Architecture**| *"How does Dingo perform fast ledger sync using Mithril certificates?"* | `get_node_info` | `{"topic": "faq"}` | Details Mithril snapshot downloading and cryptographic signature verification pipeline. |
| **Network Synchronization**| *"Check synchronization tip, slot drift, and block propagation latency."* | `get_cardano_tip` | `{}` | Checks local tip vs network consensus time to measure slot drift and block propagation. |

---

## 5. How Dingo MCP Works Under the Hood

The complete lifecycle of a request consists of 6 distinct phases:

1. **Phase 1: Handshake & Discovery (`initialize`)**:
   - The AI client (Claude, Cursor, Codex) initiates a session via `POST /mcp` or `GET /sse`.
   - Dingo responds with server capabilities, registering 21 tools, 81 resources, and system steering instructions detailing block identity and forge fencing.
2. **Phase 2: Intent Classification & Tool Invocation (`tools/call`)**:
   - The LLM parses the user prompt, determines the required data, and emits a JSON-RPC 2.0 tool invocation.
3. **Phase 3: Ingress Security Perimeter**:
   - **Authentication**: If configured, `Authorization: Bearer <token>` is validated using `crypto/subtle.ConstantTimeCompare`.
   - **Rate Limiting**: Per-client IP token bucket (default 60 req/s, burst 15) decrements tokens. Excess requests receive HTTP 429.
   - **Abstract Syntax Tree (AST) Validation**: For SQL tools, an AST parser verifies that the statement is strictly read-only. Statements containing `DROP`, `DELETE`, `INSERT`, `UPDATE`, `ALTER`, or `ATTACH` are immediately rejected with HTTP 400.
4. **Phase 4: Tool Routing & Engine Execution**:
   - The MCP dispatcher routes the call:
     - **Cardano Domain Tools**: Access immutable in-memory ledger state or compiled pure-Go Ouroboros APIs.
     - **SQLite Tools**: Execute queries against `metadata.sqlite` using a dedicated read-only connection pool (`PRAGMA query_only = ON`).
5. **Phase 5: Output Formatting & Sanitization**:
   - Binary hashes (TX IDs, pool hashes, policy IDs) are formatted into uppercase hex strings.
   - Results are rendered into structured GitHub-flavored Markdown tables with automatic row and column caps.
   - Special control characters and prompt injection markers are sanitized.
6. **Phase 6: AI Synthesis & Response Delivery**:
   - The AI client ingests the sanitized Markdown table and synthesizes a natural language answer with verified facts.

---

## 6. Enabling the MCP Server

The MCP server is compiled in as a modular API provider plugin under capability `api/mcp` and is controlled by a feature flag.

### Command Line Flags

To start Dingo with MCP enabled on port `8088`:

```bash
./dingo run \
  --network preview \
  --storage-mode api \
  --mcp-provider builtin \
  --plugins.api.mcp.config.port 8088
```

> **Note**: MCP requires SQLite metadata to be active, which is standard in `--storage-mode api`.

To explicitly disable MCP:
```bash
./dingo run --mcp-provider none
```

### Environment Variables

You can configure MCP via canonical plugin environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `DINGO_PLUGINS_API_MCP_CONFIG_PORT` | `8088` | Port for the MCP HTTP/SSE listener (also accepts legacy `DINGO_MCP_PORT`) |
| `DINGO_PLUGINS_API_MCP_CONFIG_AUTHTOKEN` | *empty* (disabled) | Optional Bearer token for authentication |
| `DINGO_PLUGINS_API_MCP_CONFIG_RATELIMIT` | `60.0` | Sustained requests per second per IP (0 = unlimited) |
| `DINGO_PLUGINS_API_MCP_CONFIG_BURST` | `15` | Maximum burst requests per IP |
| `DINGO_PLUGINS_API_MCP_CONFIG_QUERYTIMEOUT` | `5s` | Maximum execution timeout for SQLite queries |
| `DINGO_PLUGINS_API_MCP_CONFIG_MAXROWS` | `100` | Maximum rows returned per query by default |

Example with authentication and custom port:
```bash
export DINGO_PLUGINS_API_MCP_CONFIG_PORT=8088
export DINGO_PLUGINS_API_MCP_CONFIG_AUTHTOKEN="secret-agent-key-123"
./dingo run --network preview --storage-mode api --mcp-provider builtin
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

## 8. Testing with the MCP Inspector

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

#### Option 2: Local Stdio Process (Claude spawns Dingo)

```json
{
  "mcpServers": {
    "dingo": {
      "command": "/absolute/path/to/dingo",
      "args": [
        "run",
        "--network", "preview",
        "--storage-mode", "api",
        "--mcp-provider", "builtin"
      ]
    }
  }
}
```

---

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
| `get_account` | Query stake account delegation, pool ID, and DRep | `stake_address_or_credential` (string, required) |
| `get_epoch_summary` | Summary stats and boundary nonces for an epoch | `epoch` (int, optional) |
| `resolve_datum_or_script` | Look up and decode Plutus CIP-32 datum or script metadata from hash | `hash` (string, required - 56 or 64 hex chars) |
| `evaluate_tx` | Simulate Plutus execution units (CPU steps, memory) & fee before submission | `cbor` (string, required - hex or base64) |
| `get_utxos_by_asset` | Kupo-style pattern matching querying unspent UTxOs holding a specific policy or asset | `policy_id` (56-hex, required), `asset_name` (optional ASCII or hex), `limit`, `offset` |
| `get_asset_info` | Detailed Cardano native asset metadata, Token Registry info, on-chain supply, and CIP-25 NFT metadata | `policy_id` (56-hex, required), `asset_name` (optional ASCII or hex) |
| `get_governance_state` | Conway CIP-1694 governance state (constitution, committee quorum, members, DReps) | `drep_credential` (optional Bech32 `drep1...` or 56-hex) |
| `get_pool_performance` | Historical SPO reward performance, blocks produced, apparent performance, and stake metrics | `pool_id` (Bech32 `pool1...` or 56-hex, required), `epoch` (optional int) |
| `get_protocol_parameters` | Active ledger protocol parameters (fees, min UTxO, max ExUnits, Plutus prices, governance deposits) | *none* |
| `get_mempool_info` | Live mempool metrics (pending tx count, byte volume, saturation percentage) or tx status | `tx_hash` (optional 64-character hex) |
| `decode_address` | Cryptographically parse Bech32 or raw hex address into network ID and payment/stake credentials | `address` (string, required) |
| `get_governance_proposal` | Query CIP-1694 governance proposals with CC, DRep, and SPO voting breakdown tallies | `proposal_id` (optional), `status` (optional: `active`, `ratified`, `enacted`, `expired`, `all`), `limit` |
| `calculate_min_utxo` | Compute CIP-55 minimum required Lovelace for output shapes with multi-assets and datums | `has_inline_datum`, `reference_script_bytes`, `has_datum_hash`, `assets`, `coins_per_utxo_byte` |

### Protocol Parameters & Ledger Economy

#### `get_protocol_parameters`
Retrieves live protocol parameters governing ledger consensus, fee computation, and Plutus execution:
- **Linear Fee Formula**: Base linear parameters $a$ (`min_fee_a`) and $b$ (`min_fee_b`) for calculating minimum transaction fee ($\text{fee} = a \times \text{size}(tx) + b$).
- **Min UTxO Value**: `coins_per_utxo_byte` (e.g. 4,310 Lovelace/byte) enforcing CIP-55 minimum deposits to prevent dust UTxO bloat.
- **Plutus Execution Unit Prices**: Step price per CPU unit and memory unit fractions for Plutus script validation.
- **Execution Budget Caps**: Maximum ExUnits allowable per individual transaction and per block (`max_tx_ex_mem`, `max_tx_ex_steps`, `max_block_ex_mem`, `max_block_ex_steps`).
- **Collateral & Security Constraints**: Minimum collateral percentage (e.g. 150%) and maximum collateral inputs (`max_collateral_inputs`).
- **Voltaire Governance Deposits**: Minimum DRep registration deposit (`drep_deposit`) and governance proposal action deposit (`gov_action_deposit`).

#### `calculate_min_utxo`
Calculates the exact CIP-55 minimum required Lovelace deposit for any arbitrary Cardano transaction output before building transactions:
- **Input Parameters**:
  - `has_inline_datum` (boolean): Whether the output carries an attached CIP-32 inline datum (+64 bytes overhead).
  - `reference_script_bytes` (integer): Serialized size in bytes of attached CIP-33 reference script, if any.
  - `has_datum_hash` (boolean): Whether the output attaches a 32-byte legacy datum hash.
  - `assets` (array): List of native assets containing `policy_id`, `asset_name`, and `quantity`.
  - `coins_per_utxo_byte` (optional integer): Cost per UTxO byte (defaults to active ledger parameter or 4,310 Lovelace).
- **Exact CIP-55 Calculation**:
  $$\text{minLovelace} = (160 + \text{addressSize} + \text{datumOverhead} + \text{assetBundleSize}) \times \text{coinsPerUtxoByte}$$
- Prevents submission rejections caused by ledger error `OutputTooSmallUTxO`.

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
- **On-Chain Circulation**: Aggregates total live unspent quantity and unique wallet holder count from UTxO ledger state.
- **Mint / Burn History**: Shows initial mint transaction hash, slot, and total minted/burned quantity.
- **CIP-25 NFT Metadata**: Extracts structured metadata from transaction label 721 when available.

### Conway CIP-1694 Governance

#### `get_governance_state`
Inspects on-chain governance state introduced in the Conway era:
- **Constitution**: Anchor URL, 32-byte anchor hash, script hash, and slot ratified.
- **Constitutional Committee**: Voting quorum threshold (e.g. `2/3`), cold credential hashes, term start slot, and expiration epoch.
- **Delegated Representatives (DReps)**: Active registered DRep counts, activity and expiry epochs. If `drep_credential` is specified (in Bech32 `drep1...` or hex), inspects that specific DRep's registration anchor and status.

#### `get_governance_proposal`
Queries submitted CIP-1694 governance proposals from the SQLite indexer and tallies live votes:
- **Filtering Options**: Filter by `proposal_id` (`tx_hash#action_index`) or governance lifecycle status:
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
- Displays blocks produced, apparent performance ratio, total reward, leader reward, member rewards, declared margin, fixed cost, and active delegated stake per epoch.
- Allows filtering by a specific epoch or viewing recent epochs.

### Plutus Smart Contract & DevOps Tools

#### `resolve_datum_or_script`
Resolves 28-byte script hashes or 32-byte datum hashes stored on-chain:
- For **Datums**: Decodes raw Plutus CBOR data into structured JSON, displaying constructor tags, fields, and the slot it was added.
- For **Scripts**: Identifies script type (`Native / MultiSig`, `PlutusV1`, `PlutusV2`, `PlutusV3`), size in bytes, and slot created.
- **Self-Correcting Guidance**: If a hash is missing, reminds the AI agent that CIP-32 inline datums are stored directly on the spending UTxO rather than the global datum table.

#### `evaluate_tx`
Dry-runs a signed or unsigned transaction against Dingo's active ledger state and protocol parameters without broadcasting it to the network:
- Computes exact **CPU steps** and **Memory units** consumed by each Plutus redeemer.
- Formats results in a clean markdown breakdown table indexed by redeemer purpose (`spend`, `mint`, `cert`, `reward`, `voting`, `proposing`, `guarding`) and index.
- Calculates total estimated Plutus script execution fee.
- If evaluation fails (e.g. Phase-2 validation failure or missing input UTxO), returns structured diagnostic guidance for the agent to recommend fixes.

### Read-Only Query Guardrails

The `sqlite_query` tool strictly verifies queries before execution:
- Only statements starting with `SELECT` or `WITH ... SELECT` are permitted.
- Multi-statement execution (semicolons separating queries) is blocked.
- Mutation keywords (`INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `ATTACH`, `VACUUM`, `PRAGMA`) are rejected with clear error explanations so the LLM can self-correct.

---

## 12. Resources Reference

MCP Resources allow LLMs to passively read context without invoking functions:

| Resource URI | Description |
| --- | --- |
| `dingo://docs/identity` | Complete guide to Dingo's on-chain block fingerprint (`BlockHeaderProtocolMinor = 69`), OpCert sequence tracking, and peer handshakes. |
| `dingo://docs/faq` | Frequently asked questions covering Dingo architecture, storage modes (`core` vs `api`), Mithril fast-sync, and tool usage. |
| `dingo://docs/architecture` | Detailed breakdown of Ouroboros Praos consensus, Plutus interpreter, block forging, and Badger/SQLite storage subsystems. |
| `dingo://node/status` | Real-time node operational status, ledger sync progress, and tip details. |
| `dingo://schema/tables` | Catalog of all metadata tables with column summaries and descriptions. |
| `dingo://schema/table/<name>` | Full dynamic SQL schema definition and index list for any table `<name>`. |
| `dingo://dbsync/cheatsheet` | Rosetta stone mapping queries from PostgreSQL `cardano-db-sync` schema to Dingo's SQLite schema. |

---

## 13. Prompts Reference (Official Agent Workflows)

Dingo implements official agent workflows natively in Go using the Model Context Protocol prompts capability (`prompts/list` and `prompts/get`). These workflows package domain-expert Cardano diagnostic and forensic procedures into standardized prompt templates that AI clients and CLI inspectors can execute interactively.

| Prompt | Title | Key Arguments | Target Persona & Workflow Protocol |
| :--- | :--- | :--- | :--- |
| `diagnose_node_health` | Diagnose Cardano Node Sync & Health | `detailed` (optional bool) | **Node Operations Engineer**: Evaluates tip slot, sync lag, identity minor 69, forge fence in `sync_state`, Badger DB blob storage, and ledger readiness. |
| `simulate_and_diagnose_tx` | Simulate and Diagnose Plutus Transaction | `tx_cbor` (required hex/b64), `purpose` (optional string) | **Smart Contract Auditor**: Executes dry-run in pure-Go Plutus CEK VM via `evaluate_tx`, computes ExUnits (CPU/Memory), audits fee sufficiency, validates collateral, and checks inline datums. |
| `audit_pool_rewards` | Audit Stake Pool Performance & Rewards | `pool_id` (required Bech32/hex), `epoch` (optional string) | **Staking Analyst**: Audits historical block production, apparent performance ratio, active delegators, margin, fixed cost, and leader rewards. |
| `track_asset_portfolio` | Track Cardano Native Asset & Circulation | `policy_id` (required hex), `asset_name` (optional ASCII/hex) | **Financial Analyst**: Kupo-style UTxO pattern matching, live circulating supply, unique wallet holder count, and CIP-25 NFT metadata. |
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
        "description": "Comprehensive diagnostic workflow to inspect Dingo node tip, sync latency, forge fences, and ledger readiness.",
        "arguments": [
          {"name": "detailed", "description": "Set to 'true' to include detailed peer and storage diagnostics.", "required": false}
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
        "description": "Deep-dive audit workflow for stake pool block production, apparent performance, delegators, and rewards.",
        "arguments": [
          {"name": "pool_id", "description": "Bech32 pool ID (pool1...) or 56-character hex pool key hash.", "required": true},
          {"name": "epoch", "description": "Specific epoch number to analyze (default: current/latest epoch).", "required": false}
        ]
      },
      {
        "name": "track_asset_portfolio",
        "description": "Portfolio and circulation analysis workflow for Cardano native tokens using Kupo-style UTxO matching.",
        "arguments": [
          {"name": "policy_id", "description": "56-character hex policy ID of the native asset.", "required": true},
          {"name": "asset_name", "description": "Asset name in UTF-8/ASCII or hex format.", "required": false}
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


