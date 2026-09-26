// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mcp

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"

	"github.com/blinklabs-io/dingo/ledger"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const dbsyncCheatsheetContent = `# Cardano db-sync to Dingo SQLite Schema Cheatsheet

This cheatsheet provides a direct conceptual and schema mapping for developers familiar with **cardano-db-sync** who are querying **Dingo's SQLite metadata store** directly.

---

## 1. Core Architecture & Storage Differences

| Concept | cardano-db-sync (PostgreSQL) | Dingo (SQLite WAL + Badger) |
| :--- | :--- | :--- |
| **Storage Engine** | PostgreSQL with relational foreign keys | SQLite with WAL mode (metadata.sqlite) + Badger for raw block blobs |
| **Transactions Table** | tx (referenced by block_id) | transaction (contains id, hash, slot, block_index, fee, size) |
| **Outputs / UTxOs** | tx_out (consumed via tx_in) | utxo (active unspent outputs stored directly; spent outputs are pruned/indexed) |
| **Stake Accounts** | stake_address | account (keyed by credential, tracks pool_id, drep_id, resigned) |
| **Datums** | datum (hashes and raw CBOR/JSON) | datum (stores hash and value) |
| **Assets & Minting** | multi_asset + ma_tx_mint | asset table and transaction-associated minting metadata |
| **Governance (Conway)** | gov_action_proposal, voting_procedure | governance_proposal, governance_vote, drep, committee_member |

---

## 2. Table Mappings & Key Columns

### Transactions
* **db-sync**:
  SELECT id, hash, block_id, fee, out_sum FROM tx WHERE hash = ...;
* **Dingo SQLite**:
  SELECT id, hash, slot, block_index, fee, size FROM transaction WHERE hash = ...;
  *(Note: transaction hash in SQLite is stored as binary BLOB. Use hex strings like X'...' or sqlite_query tool).*

### UTxOs / Addresses
* **db-sync**:
  SELECT tx_out.address, tx_out.value FROM tx_out LEFT JOIN tx_in ...;
* **Dingo SQLite**:
  SELECT tx_id, tx_index, address, value, datum_hash FROM utxo WHERE address = 'addr1...';
  *(Dingo's utxo table represents currently active unspent outputs).*

### Stake Accounts & Delegations
* **db-sync**:
  SELECT view, hash_raw FROM stake_address JOIN pool_owner ...;
* **Dingo SQLite**:
  SELECT credential, pool_id, drep_id, resigned FROM account WHERE credential = '...';

### Datums
* **db-sync**:
  SELECT hash, value FROM datum WHERE hash = ...;
* **Dingo SQLite**:
  SELECT hash, value FROM datum WHERE hash = ...;

---

## 3. Query Best Practices for Dingo SQLite
1. **Always use parameterized or properly typed queries**: Transaction hashes and hashes are stored as 32-byte binary blobs.
2. **Keep queries focused**: Use LIMIT and indexed columns (slot, hash, address, credential) to avoid full table scans.
3. **Analyze execution plans**: Use the sqlite_explain tool to ensure SQLite utilizes existing B-Tree indexes.
`

const schemaTablesCatalogContent = `# Dingo SQLite Database Tables Catalog

The following tables are available in Dingo's SQLite metadata store:

| Table Name | Description | Key Columns |
| :--- | :--- | :--- |
| **transaction** | Confirmed blockchain transactions | id, hash, slot, block_index, fee, size, deposit |
| **utxo** | Current unspent transaction outputs | id, tx_id, tx_index, address, value, datum_hash |
| **account** | Stake credentials, pool & DRep delegations | credential, pool_id, drep_id, resigned |
| **asset** | Native multi-assets and token policies | id, policy_id, name, fingerprint |
| **block_nonce** | Epoch boundary nonces and randomness | epoch, slot, nonce |
| **datum** | Plutus datum hashes and serialized values | hash, value |
| **drep** | Registered Delegated Representatives (DReps) | id, credential, deposit, anchor_url, anchor_hash |
| **committee_member** | Constitutional Committee members | cold_cred, hot_cred, resigned, expiration |
| **governance_proposal** | Conway governance action proposals | tx_id, cert_index, action_type, deposit, return_address |
| **governance_vote** | Votes cast by DReps, SPOs, and CC | proposal_tx_id, proposal_index, voter_role, vote |
| **node_settings** | Node state checkpoints and metadata flags | key, value |
`

const docsIdentityContent = `# How Dingo Identifies Itself and Its Blocks

This document explains how **Dingo** (a pure-Go Cardano node implementation by Blink Labs) identifies itself across the Cardano network, on-chain, and in node telemetry.

---

## 1. On-Chain Block Identification (Protocol Minor Version 69)

When Dingo is configured as an active Block Producer (` + "`--block-producer`" + `), every block it forges contains a distinct protocol version fingerprint in its header:

- **Protocol Major Version**: Matches the active ledger era (e.g., ` + "`9`" + ` or ` + "`10`" + ` in Conway). This guarantees ledger consensus and hard fork compatibility with the entire network.
- **Protocol Minor Version**: Explicitly set to **` + "`69`" + `** (` + "`BlockHeaderProtocolMinor = 69`" + ` in ` + "`internal/version/version.go`" + `).

### Why Minor Version 69?
Cardano's ledger rules use ` + "`ProtocolMajorVersion`" + ` to govern era transitions and hard forks. The ` + "`ProtocolMinorVersion`" + ` in the block header is an informational field that block-producing implementations use to signal software identity without altering consensus rules or causing network forks.

Whenever a block is inspected on-chain:
- Header: ` + "`{ \"proto_major\": 9, \"proto_minor\": 69 }`" + `
- Any block with ` + "`proto_minor == 69`" + ` was forged by a **Dingo** node.

---

## 2. Operational Certificates & Block Minting

When forging blocks, Dingo utilizes standard Cardano operational certificates:
- **Pool Cold Key**: Identifies the stake pool that won the slot lottery (` + "`PoolKeyHash`" + `).
- **Operational Certificate (` + "`OpCert`" + `)**: Binds the pool cold key to the current hot KES verification key and records the operational certificate sequence number (` + "`OpCertSequenceNumber`" + `).
- **VRF Key & Leader Proof**: Validates that the pool was elected leader for the exact slot under Praos consensus.

In Dingo's SQLite metadata store, whenever an operational certificate is observed or validated, it is tracked in:
` + "`SELECT slot, sequence FROM pool_opcert_sequence WHERE pool_key_hash = ...;`" + `

---

## 3. Durable Forge Fence (Double-Signing Prevention)

To prevent forging two blocks in the same slot across process crashes, restarts, or node migrations, Dingo implements a durable **Forge Fence**:
- The forge fence is backed by the SQLite ` + "`sync_state`" + ` table with key ` + "`forge_fence:<pool_key_hash>`" + `.
- Format: JSON record storing the highest forged slot and timestamp.
- A Dingo block producer will strictly refuse to start or forge if the current slot is less than or equal to the persisted fence.

---

## 4. Peer-to-Peer Network Identity

During Ouroboros network protocol handshakes:
- **Node User Agent**: Handshake identification strings use ` + "`dingo:<version> (<commit>)`" + `.
- **Node Version**: Displayed via ` + "`dingo version`" + ` or ` + "`version.GetVersionString()`" + `.

---

## 5. Telemetry & Metrics

Dingo exports Prometheus metrics with the ` + "`dingo_`" + ` prefix:
- ` + "`dingo_forge_blocks_forged_total`" + `: Total blocks successfully minted.
- ` + "`dingo_forge_slots_elected_total`" + `: Slots won in leader election.
- ` + "`dingo_ledger_leader_threshold_margin`" + `: Margin between VRF output and leader threshold.
`

const docsFAQContent = `# Dingo Frequently Asked Questions (FAQ)

### Q1: What is Dingo?
**Dingo** is an independent, pure-Go modular implementation of the Cardano blockchain node developed by Blink Labs. It provides high performance, memory efficiency, and native queryability without requiring Haskell dependencies.

### Q2: How does Dingo identify blocks it forged on Cardano?
Dingo sets the block header **Protocol Minor Version to ` + "`69`" + `** (` + "`BlockHeaderProtocolMinor = 69`" + `). While the major version indicates the protocol era (e.g. Conway 9 or 10), the minor version 69 identifies Dingo block producers on-chain.

### Q3: How does Dingo prevent slot double-signing?
Dingo records a durable **Forge Fence** in SQLite (` + "`sync_state`" + ` table under ` + "`forge_fence:<pool_id>`" + `). It records the highest slot signed and fences against double-signing even across process restarts.

### Q4: What storage modes does Dingo support?
- **Core Mode (` + "`--storage-mode core`" + `)**: Uses an embedded Badger DB for raw block blobs and an embedded SQLite database (` + "`metadata.sqlite`" + ` in WAL mode) for relational ledger state, accounts, UTxOs, and governance.
- **API Mode (` + "`--storage-mode api`" + `)**: Extends storage with external databases (e.g., PostgreSQL) for heavy API indexing.

### Q5: Can Dingo bootstrap quickly without full historical sync?
Yes! Dingo supports fast bootstrapping from **Mithril** certified snapshots. It verifies Mithril cryptographic multi-signatures, reconstructs the active UTxO set and stake distributions, and begins following the tip in minutes rather than days.

### Q6: How does the Dingo MCP server help AI agents?
Dingo provides a native Model Context Protocol (MCP) server over HTTP/SSE. AI assistants can:
- Check sync tip and peer status (` + "`get_cardano_tip`" + `)
- Retrieve node architecture & FAQs (` + "`get_node_info`" + `)
- Query pool delegation, performance, and rewards (` + "`get_pool_performance`" + `)
- Inspect Conway Voltaire governance state (` + "`get_governance_state`" + `)
- Resolve UTxOs and accounts (` + "`get_utxos`" + `, ` + "`get_account`" + `)
- Run safe, read-only SQL queries directly against ledger tables (` + "`sqlite_query`" + `, ` + "`sqlite_explain`" + `)
- Learn table structures and guides via passive resources (` + "`dingo://docs/identity`" + `, ` + "`dingo://docs/faq`" + `, ` + "`dingo://schema/tables`" + `, ` + "`dingo://dbsync/cheatsheet`" + `)

### Q7: Where can I find more documentation?
Read MCP resources:
- ` + "`dingo://docs/identity`" + `: How Dingo identifies itself and its blocks.
- ` + "`dingo://docs/faq`" + `: General FAQ.
- ` + "`dingo://docs/architecture`" + `: Internal components and storage subsystems.
- ` + "`dingo://dbsync/cheatsheet`" + `: Schema differences between cardano-db-sync and Dingo.
- ` + "`dingo://schema/tables`" + `: Database table definitions.
`

const docsArchitectureContent = `# Dingo Node Architecture

Dingo is a modular, high-performance Cardano node written in Go.

## Component Breakdown

1. **Ouroboros Protocol Engine**:
   - Implements Ouroboros Praos consensus and mini-protocols (Handshake, ChainSync, BlockFetch, TxSubmission, KeepAlive).
   - Manages inbound and outbound peer connections with dynamic peer governance.

2. **Ledger & Validation**:
   - Era-aware ledger supporting Byron, Shelley, Allegra, Mary, Alonzo, Babbage, and Conway.
   - Plutus Core V1/V2/V3 script evaluation engine for smart contracts.
   - Stake reward calculations, epoch boundaries, and Voltaire governance tallying.

3. **Block Forging & Leader Election**:
   - VRF-based slot lottery evaluation under Praos rules.
   - Key Evolving Signatures (KES) and Operational Certificates (OpCert).
   - Stamps Block Header Protocol Minor Version 69 on forged blocks.
   - Durable Forge Fence in SQLite to prevent double-signing.

4. **Storage Subsystem**:
   - **Core Mode**: Badger DB key-value store for immutable block blobs + SQLite WAL database ('metadata.sqlite') for ledger state, UTxO index, transactions, and governance.
   - **API Mode**: Pluggable storage providers (e.g. PostgreSQL) for comprehensive API queries.
   - **Mithril Bootstrap**: Certified snapshot downloading and rapid ledger state reconstruction.

5. **Model Context Protocol (MCP)**:
   - Embedded JSON-RPC MCP server over HTTP/SSE.
   - Exposes tools, resources, and documentation for autonomous AI agents and operator interfaces.
`

// RegisterResources registers passive MCP resources for schema exploration and db-sync guidance.
func RegisterResources(
	server *mcp.Server,
	db *sql.DB,
	ls *ledger.LedgerState,
	network string,
) {
	// Resource: dingo://docs/identity
	server.AddResource(&mcp.Resource{
		URI:         "dingo://docs/identity",
		Name:        "Dingo Node & Block Identification Guide",
		Description: "Detailed explanation of how Dingo identifies itself to peers, on-chain via block header minor version 69, and in telemetry",
		MIMEType:    "text/markdown",
	}, func(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{
				{
					URI:      req.Params.URI,
					MIMEType: "text/markdown",
					Text:     docsIdentityContent,
				},
			},
		}, nil
	})

	// Resource: dingo://docs/faq
	server.AddResource(&mcp.Resource{
		URI:         "dingo://docs/faq",
		Name:        "Dingo Frequently Asked Questions (FAQ)",
		Description: "Comprehensive FAQ about Dingo architecture, storage modes, Mithril fast-sync, block production, and MCP usage",
		MIMEType:    "text/markdown",
	}, func(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{
				{
					URI:      req.Params.URI,
					MIMEType: "text/markdown",
					Text:     docsFAQContent,
				},
			},
		}, nil
	})

	// Resource: dingo://docs/architecture
	server.AddResource(&mcp.Resource{
		URI:         "dingo://docs/architecture",
		Name:        "Dingo Node Architecture",
		Description: "Detailed overview of Dingo's consensus engine, ledger, storage subsystems, and block builder",
		MIMEType:    "text/markdown",
	}, func(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{
				{
					URI:      req.Params.URI,
					MIMEType: "text/markdown",
					Text:     docsArchitectureContent,
				},
			},
		}, nil
	})

	// Resource: dingo://dbsync/cheatsheet
	server.AddResource(&mcp.Resource{
		URI:         "dingo://dbsync/cheatsheet",
		Name:        "Cardano db-sync to Dingo SQLite Cheatsheet",
		Description: "Conceptual guide and schema mapping for querying Dingo SQLite using cardano-db-sync mental models",
		MIMEType:    "text/markdown",
	}, func(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{
				{
					URI:      req.Params.URI,
					MIMEType: "text/markdown",
					Text:     dbsyncCheatsheetContent,
				},
			},
		}, nil
	})

	// Resource: dingo://schema/tables
	server.AddResource(&mcp.Resource{
		URI:         "dingo://schema/tables",
		Name:        "Dingo SQLite Tables Catalog",
		Description: "Overview of all tables in Dingo's metadata SQLite store with descriptions and primary columns",
		MIMEType:    "text/markdown",
	}, func(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{
				{
					URI:      req.Params.URI,
					MIMEType: "text/markdown",
					Text:     schemaTablesCatalogContent,
				},
			},
		}, nil
	})

	// Resource: dingo://node/status
	server.AddResource(&mcp.Resource{
		URI:         "dingo://node/status",
		Name:        "Dingo Node & Chain Status",
		Description: "Current passive status of the Dingo node, configured network, and synchronization tip",
		MIMEType:    "text/markdown",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		var slot uint64
		var hashHex string
		var blockHeight uint64
		var behindHead uint64

		if ls != nil {
			tip := ls.Tip()
			slot = tip.Point.Slot
			hashHex = hex.EncodeToString(tip.Point.Hash)
			blockHeight = tip.BlockNumber
			behindHead = ls.SlotsBehindHead()
		} else if db != nil {
			var rawHash []byte
			_ = db.QueryRowContext(ctx, "SELECT slot, hash FROM transaction ORDER BY slot DESC LIMIT 1").Scan(&slot, &rawHash)
			hashHex = hex.EncodeToString(rawHash)
		}

		syncStatus := "in sync"
		if behindHead > 100 {
			syncStatus = fmt.Sprintf("syncing (%d slots behind)", behindHead)
		}

		statusMarkdown := fmt.Sprintf("# Dingo Node Status\n\n"+
			"- **Network**: `%s`\n"+
			"- **Tip Slot**: `%d`\n"+
			"- **Block Height**: `%d`\n"+
			"- **Block Hash**: `%s`\n"+
			"- **Sync Status**: `%s`\n",
			network, slot, blockHeight, hashHex, syncStatus)

		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{
				{
					URI:      req.Params.URI,
					MIMEType: "text/markdown",
					Text:     statusMarkdown,
				},
			},
		}, nil
	})

	// Register dynamic resource for tables if db is available
	if db != nil {
		rows, err := db.Query(
			"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
		)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var tbl string
				if err := rows.Scan(&tbl); err == nil {
					tableName := tbl
					server.AddResource(&mcp.Resource{
						URI:         "dingo://schema/table/" + tableName,
						Name:        "Schema: " + tableName,
						Description: "SQLite schema and column definitions for " + tableName,
						MIMEType:    "text/markdown",
					}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
						var sqlDDL sql.NullString
						_ = db.QueryRowContext(ctx, "SELECT sql FROM sqlite_master WHERE name = ?", tableName).
							Scan(&sqlDDL)

						infoRows, qErr := db.QueryContext(
							ctx,
							fmt.Sprintf("PRAGMA table_info(%s)", tableName),
						)
						var cols []string
						var tableData [][]string
						if qErr == nil && infoRows != nil {
							defer infoRows.Close()
							cols, _ = infoRows.Columns()
							valPtrs := make([]any, len(cols))
							vals := make([]any, len(cols))
							for i := range vals {
								valPtrs[i] = &vals[i]
							}
							for infoRows.Next() {
								if err := infoRows.Scan(valPtrs...); err == nil {
									row := make([]string, len(cols))
									for i, v := range vals {
										row[i] = FormatCell(v)
									}
									tableData = append(tableData, row)
								}
							}
							if err := infoRows.Err(); err != nil {
								return nil, err
							}
						}

						content := fmt.Sprintf(
							"# Table Schema: `%s`\n\n```sql\n%s\n```\n\n### Columns\n\n%s",
							tableName,
							sqlDDL.String,
							FormatMarkdownTable(cols, tableData),
						)

						return &mcp.ReadResourceResult{
							Contents: []*mcp.ResourceContents{
								{
									URI:      req.Params.URI,
									MIMEType: "text/markdown",
									Text:     content,
								},
							},
						}, nil
					})
				}
			}
			if err := rows.Err(); err != nil {
				return
			}
		}
	}
}
