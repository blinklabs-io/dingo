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

These examples target Dingo's SQLite metadata schema, not cardano-db-sync's
PostgreSQL schema. SQLite WAL stores metadata; the configured blob provider
stores block/output CBOR. Core/API storage mode controls metadata coverage and
is independent of the metadata provider. Mithril core bootstrap does not
backfill all historical transaction metadata. Pruning also limits history.

## 1. Conceptual mappings

| Concept | cardano-db-sync | Dingo SQLite |
| :--- | :--- | :--- |
| Transactions | tx | "transaction": hash, block_hash, slot, block_index, fee |
| Outputs | tx_out, tx_in | utxo: tx_id, output_idx, amount, deleted_slot |
| Stake accounts | stake_address and delegation/reward tables | account: staking_key, credential_tag, pool, drep, reward, active |
| Datums | datum | datum: hash, raw_datum (CBOR bytes) |
| Native assets | multi_asset and output/mint relations | asset: policy_id, name, amount, utxo_id (output holdings) |
| Governance | gov_action_proposal, voting_procedure | governance_proposal, governance_vote, drep, committee_member |

## 2. Executable Dingo SQLite examples

Each example is a bounded read of retained data. An empty result can mean the
relevant history was not indexed or has been pruned.

### Recent retained transactions

~~~sql
SELECT hex(hash) AS tx_hash, hex(block_hash) AS block_hash,
       slot, block_index, fee AS fee_lovelace
FROM "transaction"
ORDER BY id DESC
LIMIT 20;
~~~

The transaction table has no size or deposit column. Transaction hash and block
hash are different identities. The utxo.transaction_id integer refers to the
transaction row's id; utxo.tx_id is the binary transaction hash.

### Unspent outputs

~~~sql
SELECT hex(tx_id) AS tx_hash, output_idx, amount AS lovelace,
       hex(payment_key) AS payment_credential, payment_script,
       hex(staking_key) AS stake_credential, credential_tag,
       hex(datum_hash) AS datum_hash
FROM utxo
WHERE deleted_slot = 0
LIMIT 20;
~~~

Retained spent rows also exist: deleted_slot = 0 selects unspent outputs.
There is no utxo.address text column. A payment_key filter deliberately searches
across address forms sharing a credential; payment_script distinguishes key
from script payment credentials. For a complete Bech32 address, use get_utxos
with address_or_credential so Dingo verifies exact address bytes from output
CBOR. Matching payment/stake hashes alone does not establish full address identity.

### Registered stake accounts

~~~sql
SELECT hex(staking_key) AS stake_credential, credential_tag,
       hex(pool) AS pool_key_hash, hex(drep) AS drep_credential,
       drep_type, reward AS reward_lovelace, active
FROM account
WHERE active = 1
ORDER BY id DESC
LIMIT 20;
~~~

Stake identity is (credential_tag, staking_key): tag 0 is a key hash and tag 1
a script hash. Include both when filtering an account. pool and drep are binary
credentials, not numeric foreign-key IDs. Interpret drep with drep_type, which
also represents special delegation choices. active records account registration;
it is not a DRep voting-eligibility flag.

For get_account, a stake reward address (stake1... or stake_test1...) carries
its credential type. A raw 28-byte hexadecimal credential hash does not: pass
credential_type as "key" or "script" with a hexadecimal hash. Do not infer the
type from the hash bytes.

### Stored datums

~~~sql
SELECT hex(hash) AS datum_hash, hex(raw_datum) AS datum_cbor_hex
FROM datum
ORDER BY id DESC
LIMIT 5;
~~~

raw_datum contains serialized CBOR, not JSON. Use resolve_datum_or_script with
the datum hash for decoded content. Core storage mode does not populate the
API-mode datum, script, or redeemer detail indexes. Empty results from those
tables in core mode do not mean the chain has no such data. API mode indexes
details for transactions it processes, subject to available and retained
history; an existing core-mode database cannot be switched to API mode in
place. Use a separate API-mode database when those indexes are required.

## 3. Query guidance

- Transaction, block and datum hashes are 32-byte BLOBs; payment/stake credentials,
  pool key hashes and asset policy IDs are 28-byte BLOBs. Asset names are bytes.
- Display BLOBs with hex(...). Filter a BLOB using an SQLite X'...' literal with
  the complete hexadecimal bytes, not a quoted Bech32 identifier or hex string.
- Lovelace and asset quantities can be stored as decimal TEXT to preserve range.
  Do not cast to floating point for exact balances or assume SQLite integer SUM
  can represent every aggregate.
- Inspect dingo://schema/tables and dingo://schema/table/<table>, or use
  sqlite_table_schema, for the connected database's columns and DDL.
- Use sqlite_explain to check query plans. LIMIT bounds returned rows, not the
  total work needed for a scan or sort.
`

func sqliteTablesCatalog(ctx context.Context, db *sql.DB) (string, error) {
	const heading = "# Dingo SQLite Database Tables Catalog\n\n"
	if db == nil {
		return heading + "SQLite database is unavailable; live schema cannot be inspected.\n", nil
	}
	rows, err := db.QueryContext(ctx, `
SELECT m.name, p.name, p.type
FROM sqlite_master AS m
JOIN pragma_table_info(m.name) AS p
WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite_%'
ORDER BY m.name, p.cid`)
	if err != nil {
		return "", fmt.Errorf("read SQLite table catalog: %w", err)
	}
	defer rows.Close()
	descriptions := map[string]string{
		"transaction":         "Retained transaction metadata",
		"utxo":                "Retained outputs; deleted_slot = 0 selects unspent outputs",
		"account":             "Stake accounts identified by credential_tag and staking_key",
		"asset":               "Native asset holdings linked by utxo_id",
		"datum":               "Datum hashes and serialized CBOR",
		"block_nonce":         "Block hashes and nonces by slot",
		"drep":                "DRep registration state",
		"committee_member":    "Committee cold credentials and membership terms",
		"governance_proposal": "Governance actions identified by tx_hash and action_index",
		"governance_vote":     "Votes linked to governance_proposal by proposal_id",
		"node_settings":       "Storage mode and network settings",
	}
	var data [][]string
	for rows.Next() {
		var table, column, columnType string
		if err := rows.Scan(&table, &column, &columnType); err != nil {
			return "", err
		}
		if len(data) == 0 || data[len(data)-1][0] != table {
			data = append(data, []string{table, descriptions[table], ""})
		}
		row := data[len(data)-1]
		if row[2] != "" {
			row[2] += ", "
		}
		row[2] += column + " " + columnType
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return heading + "Columns and declared types are read from the connected SQLite database. " +
		"Read dingo://schema/table/<table> for DDL, keys and constraints.\n\n" +
		FormatMarkdownTable(
			[]string{"Table", "Description", "Columns (declared types)"},
			data,
		), nil
}

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
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		content, err := sqliteTablesCatalog(ctx, db)
		if err != nil {
			return nil, err
		}
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
