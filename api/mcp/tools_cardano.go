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
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/btcsuite/btcd/btcutil/bech32"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// TipParams defines the input schema for get_cardano_tip.
type TipParams struct{}

// NodeInfoParams defines the input schema for get_node_info.
type NodeInfoParams struct {
	Topic string `json:"topic,omitempty" jsonschema:"Optional topic filter: 'identity' (how Dingo identifies itself and its blocks on-chain), 'faq' (frequently asked questions), 'architecture' (internal components and storage modes), or leave empty for general summary"`
}

// BlockParams defines the input schema for get_block.
type BlockParams struct {
	HashOrSlot string `json:"hash_or_slot" jsonschema:"Hex-encoded block hash (64 chars) or decimal slot number"`
}

// TransactionParams defines the input schema for get_transaction.
type TransactionParams struct {
	TxHash string `json:"tx_hash" jsonschema:"Hex-encoded 64-character transaction hash"`
}

// UTxOsParams defines the input schema for get_utxos.
type UTxOsParams struct {
	AddressOrCredential string `json:"address_or_credential" jsonschema:"Bech32 Cardano address or hex payment credential"`
	Limit               int    `json:"limit,omitempty"       jsonschema:"Maximum UTxOs to return (default 20, max 100)"`
	Offset              int    `json:"offset,omitempty"      jsonschema:"Pagination offset (default 0)"`
}

// AccountParams defines the input schema for get_account.
type AccountParams struct {
	StakeAddressOrCredential string `json:"stake_address_or_credential" jsonschema:"Stake address (e.g. stake1...) or hex stake credential"`
}

// EpochSummaryParams defines the input schema for get_epoch_summary.
type EpochSummaryParams struct {
	Epoch *int `json:"epoch,omitempty" jsonschema:"Epoch number (optional, defaults to current epoch)"`
}

// ResolveHashParams defines the input schema for resolve_datum_or_script.
type ResolveHashParams struct {
	Hash string `json:"hash" jsonschema:"Hex-encoded datum hash (32 bytes / 64 hex characters) or script hash (28 bytes / 56 hex characters)"`
}

// EvaluateTxParams defines the input schema for evaluate_tx.
type EvaluateTxParams struct {
	Cbor string `json:"cbor" jsonschema:"Hex-encoded or base64-encoded raw transaction CBOR"`
}

// UtxosByAssetParams defines the input schema for get_utxos_by_asset.
type UtxosByAssetParams struct {
	PolicyID  string `json:"policy_id"            jsonschema:"Hex-encoded 28-byte (56 hex characters) minting policy ID"`
	AssetName string `json:"asset_name,omitempty" jsonschema:"Asset name either as ASCII text (e.g. 'HOSKY') or hex-encoded (optional)"`
	Limit     int    `json:"limit,omitempty"      jsonschema:"Maximum UTxOs to return (default 50, max 200)"`
	Offset    int    `json:"offset,omitempty"     jsonschema:"Pagination offset (default 0)"`
}

// AssetInfoParams defines the input schema for get_asset_info.
type AssetInfoParams struct {
	PolicyID  string `json:"policy_id"            jsonschema:"Hex-encoded 28-byte (56 hex characters) minting policy ID"`
	AssetName string `json:"asset_name,omitempty" jsonschema:"Asset name either as ASCII text or hex-encoded (optional)"`
}

// GovernanceStateParams defines the input schema for get_governance_state.
type GovernanceStateParams struct {
	DrepCredential string `json:"drep_credential,omitempty" jsonschema:"Optional hex credential or Bech32 DRep ID (drep1...) to inspect a specific DRep"`
}

// PoolPerformanceParams defines the input schema for get_pool_performance.
type PoolPerformanceParams struct {
	PoolID string `json:"pool_id"         jsonschema:"Bech32 pool ID (e.g. pool1...) or hex pool key hash (56 hex characters)"`
	Epoch  *int   `json:"epoch,omitempty" jsonschema:"Specific epoch number to inspect (optional, defaults to last 10 epochs)"`
	Limit  int    `json:"limit,omitempty" jsonschema:"Maximum epochs of history to return (default 10, max 50)"`
}

func formatHash(val any) string {
	switch v := val.(type) {
	case []byte:
		return hex.EncodeToString(v)
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}

// RegisterCardanoTools registers domain-specific Cardano and db-sync aligned tools.
func RegisterCardanoTools(
	server *mcp.Server,
	db *sql.DB,
	ls *ledger.LedgerState,
	mp mempool.Service,
	network string,
	queryTimeout time.Duration,
) {
	if queryTimeout <= 0 {
		queryTimeout = 5 * time.Second
	}

	registerExtendedCardanoTools(server, db, ls, mp, network, queryTimeout)

	// Tool: get_node_info
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_node_info",
		Description: "Retrieve Dingo node architecture, FAQs, block-production identity (protocol minor version 69), storage modes, and documentation.",
	}, func(_ context.Context, _ *mcp.CallToolRequest, input NodeInfoParams) (*mcp.CallToolResult, any, error) {
		topic := strings.ToLower(strings.TrimSpace(input.Topic))
		var text string
		switch topic {
		case "identity", "identifies", "identification", "block_identity", "version":
			text = docsIdentityContent
		case "faq", "faqs", "questions":
			text = docsFAQContent
		case "architecture", "arch", "components", "storage":
			text = docsArchitectureContent
		default:
			text = fmt.Sprintf("# Dingo Node Information\n\n"+
				"- **Software**: Dingo (pure-Go Cardano node implementation by Blink Labs)\n"+
				"- **Configured Network**: `%s`\n"+
				"- **Block Header Protocol Minor Version**: `69` (identifies Dingo-forged blocks on-chain without altering ledger protocol major version)\n"+
				"- **Block Production**: Enabled via `--block-producer` with VRF, KES, and OpCert; tracks slots in SQLite `sync_state` (`forge_fence:<pool_id>`) and `pool_opcert_sequence`\n"+
				"- **Storage Modes**: `core` (Badger block blob store + SQLite WAL metadata) and `api` (PostgreSQL / indexed store)\n"+
				"- **Mithril Bootstrap**: Supported for rapid certified state synchronisation\n\n"+
				"### Available MCP Documentation Resources\n"+
				"- `dingo://docs/identity`: How Dingo identifies itself to peers and on-chain via protocol minor version 69\n"+
				"- `dingo://docs/faq`: Frequently asked questions about node architecture and usage\n"+
				"- `dingo://docs/architecture`: Component breakdown and storage engine details\n"+
				"- `dingo://dbsync/cheatsheet`: Schema differences between cardano-db-sync and Dingo SQLite\n"+
				"- `dingo://schema/tables`: Database table catalog\n\n"+
				"*(Tip: call with `topic: \"identity\"` or `topic: \"faq\"` to read full guides directly)*\n",
				network)
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: text},
			},
		}, nil, nil
	})

	// Tool: get_cardano_tip
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_cardano_tip",
		Description: "Get the current Cardano blockchain tip from Dingo, including slot number, block hash, block height, and sync progress.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ TipParams) (*mcp.CallToolResult, any, error) {
		var slot uint64
		var hashHex string
		var blockHeight uint64
		var behindHead uint64
		var source string

		if ls != nil {
			tip := ls.Tip()
			slot = tip.Point.Slot
			hashHex = hex.EncodeToString(tip.Point.Hash)
			blockHeight = tip.BlockNumber
			behindHead = ls.SlotsBehindHead()
			source = "ledger_state"
		} else if db != nil {
			qCtx, cancel := context.WithTimeout(ctx, queryTimeout)
			defer cancel()
			var rawHash any
			// Try tip table first
			row := db.QueryRowContext(qCtx, "SELECT slot, hash, block_number FROM tip ORDER BY id DESC LIMIT 1")
			if err := row.Scan(&slot, &rawHash, &blockHeight); err == nil {
				hashHex = formatHash(rawHash)
				source = "metadata_tip"
			} else {
				// Try blocks table
				row = db.QueryRowContext(qCtx, "SELECT slot, hash, height FROM blocks ORDER BY slot DESC LIMIT 1")
				if err := row.Scan(&slot, &rawHash, &blockHeight); err == nil {
					hashHex = formatHash(rawHash)
					source = "metadata_blocks"
				} else {
					// Try transaction table
					row = db.QueryRowContext(qCtx, "SELECT slot, hash FROM \"transaction\" ORDER BY slot DESC LIMIT 1")
					if err := row.Scan(&slot, &rawHash); err == nil {
						hashHex = formatHash(rawHash)
						source = "metadata_transaction"
					}
				}
			}
		}

		if source == "" {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: neither ledger state nor SQLite database are accessible",
					},
				},
			}, nil, nil
		}

		syncStatus := "in sync"
		if behindHead > 100 {
			syncStatus = fmt.Sprintf(
				"syncing (%d slots behind head)",
				behindHead,
			)
		}

		markdown := fmt.Sprintf("### Cardano Chain Tip\n\n"+
			"- **Network**: `%s`\n"+
			"- **Slot**: `%d`\n"+
			"- **Block Height**: `%d`\n"+
			"- **Block Hash**: `%s`\n"+
			"- **Sync Status**: %s\n"+
			"- **Source**: `%s`\n",
			network, slot, blockHeight, hashHex, syncStatus, source)

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: markdown},
			},
		}, nil, nil
	})

	// Tool: get_block
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_block",
		Description: "Look up block details by slot number or block hash from Dingo's SQLite store.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input BlockParams) (*mcp.CallToolResult, any, error) {
		if db == nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: SQLite database is not available",
					},
				},
			}, nil, nil
		}

		id := strings.TrimSpace(input.HashOrSlot)
		qCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		defer cancel()

		var slot uint64
		var hashHex string
		var epoch uint64
		var height uint64
		var txCount int
		var totalFees uint64

		if slotNum, err := strconv.ParseUint(id, 10, 64); err == nil &&
			len(id) < 64 {
			// Look up by slot
			// 1. Try blocks table
			var rawHash any
			row := db.QueryRowContext(
				qCtx,
				"SELECT hash, slot, epoch, height FROM blocks WHERE slot = ?",
				slotNum,
			)
			if err := row.Scan(&rawHash, &slot, &epoch, &height); err == nil {
				hashHex = formatHash(rawHash)
				_ = db.QueryRowContext(qCtx, "SELECT count(*), coalesce(sum(fee), 0) FROM tx WHERE slot = ?", slotNum).
					Scan(&txCount, &totalFees)
				if txCount == 0 {
					_ = db.QueryRowContext(qCtx, "SELECT count(*), coalesce(sum(fee), 0) FROM \"transaction\" WHERE slot = ?", slotNum).
						Scan(&txCount, &totalFees)
				}
				markdown := fmt.Sprintf("### Block Summary\n\n"+
					"- **Slot**: `%d`\n"+
					"- **Hash**: `%s`\n"+
					"- **Epoch**: `%d`\n"+
					"- **Height**: `%d`\n"+
					"- **Transactions**: %d\n"+
					"- **Total Fees (lovelace)**: %d\n",
					slot, hashHex, epoch, height, txCount, totalFees)
				return &mcp.CallToolResult{
					Content: []mcp.Content{
						&mcp.TextContent{Text: markdown},
					},
				}, nil, nil
			}

			// 2. Try tip table
			var tipHash any
			if err := db.QueryRowContext(qCtx, "SELECT hash, slot, block_number FROM tip WHERE slot = ?", slotNum).Scan(&tipHash, &slot, &height); err == nil {
				hashHex = formatHash(tipHash)
				markdown := fmt.Sprintf("### Block Summary\n\n"+
					"- **Slot**: `%d`\n"+
					"- **Hash**: `%s`\n"+
					"- **Height**: `%d`\n",
					slot, hashHex, height)
				return &mcp.CallToolResult{
					Content: []mcp.Content{
						&mcp.TextContent{Text: markdown},
					},
				}, nil, nil
			}

			// 3. Try transaction / tx table
			var rawTxHash any
			row = db.QueryRowContext(
				qCtx,
				"SELECT count(*), coalesce(sum(fee), 0), min(slot), min(hash) FROM \"transaction\" WHERE slot = ?",
				slotNum,
			)
			if err := row.Scan(&txCount, &totalFees, &slot, &rawTxHash); err == nil &&
				txCount > 0 {
				hashHex = formatHash(rawTxHash)
			} else {
				row = db.QueryRowContext(qCtx, "SELECT count(*), coalesce(sum(fee), 0), min(slot), min(hash) FROM tx WHERE slot = ?", slotNum)
				if err := row.Scan(&txCount, &totalFees, &slot, &rawTxHash); err == nil && txCount > 0 {
					hashHex = formatHash(rawTxHash)
				} else {
					return &mcp.CallToolResult{
						IsError: true,
						Content: []mcp.Content{
							&mcp.TextContent{Text: fmt.Sprintf("No block found for slot %d", slotNum)},
						},
					}, nil, nil
				}
			}
		} else {
			// Look up by hex hash
			cleanHash := strings.TrimPrefix(id, "0x")
			hashBytes, _ := hex.DecodeString(cleanHash)

			// 1. Try blocks table
			var rawHash any
			row := db.QueryRowContext(qCtx, "SELECT hash, slot, epoch, height FROM blocks WHERE hash = ? OR hash = ?", cleanHash, hashBytes)
			if err := row.Scan(&rawHash, &slot, &epoch, &height); err == nil {
				hashHex = formatHash(rawHash)
				markdown := fmt.Sprintf("### Block Summary\n\n"+
					"- **Slot**: `%d`\n"+
					"- **Hash**: `%s`\n"+
					"- **Epoch**: `%d`\n"+
					"- **Height**: `%d`\n",
					slot, hashHex, epoch, height)
				return &mcp.CallToolResult{
					Content: []mcp.Content{
						&mcp.TextContent{Text: markdown},
					},
				}, nil, nil
			}

			// 2. Try tip table
			var tipHash any
			if db.QueryRowContext(qCtx, "SELECT hash, slot, block_number FROM tip WHERE hash = ? OR hash = ?", hashBytes, cleanHash).Scan(&tipHash, &slot, &height) == nil {
				hashHex = formatHash(tipHash)
				markdown := fmt.Sprintf("### Block Summary\n\n"+
					"- **Slot**: `%d`\n"+
					"- **Hash**: `%s`\n"+
					"- **Height**: `%d`\n",
					slot, hashHex, height)
				return &mcp.CallToolResult{
					Content: []mcp.Content{
						&mcp.TextContent{Text: markdown},
					},
				}, nil, nil
			}

			// 3. Try transaction / tx table
			var rawTxHash any
			if len(hashBytes) > 0 {
				_ = db.QueryRowContext(qCtx, "SELECT count(*), coalesce(sum(fee), 0), min(slot), min(hash) FROM \"transaction\" WHERE hash = ? OR block_hash = ?", hashBytes, hashBytes).Scan(&txCount, &totalFees, &slot, &rawTxHash)
			}
			if txCount == 0 {
				_ = db.QueryRowContext(qCtx, "SELECT count(*), coalesce(sum(fee), 0), min(slot), min(hash) FROM tx WHERE hash = ?", cleanHash).Scan(&txCount, &totalFees, &slot, &rawTxHash)
			}
			if txCount == 0 {
				return &mcp.CallToolResult{
					IsError: true,
					Content: []mcp.Content{
						&mcp.TextContent{Text: fmt.Sprintf("No block found with hash '%s'", id)},
					},
				}, nil, nil
			}
			hashHex = cleanHash
		}

		markdown := fmt.Sprintf("### Block Summary\n\n"+
			"- **Slot**: `%d`\n"+
			"- **Hash**: `%s`\n"+
			"- **Transactions**: %d\n"+
			"- **Total Fees (lovelace)**: %d\n",
			slot, hashHex, txCount, totalFees)

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: markdown},
			},
		}, nil, nil
	})

	// Tool: get_transaction
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_transaction",
		Description: "Look up transaction details by 64-character hex hash from Dingo's SQLite store.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input TransactionParams) (*mcp.CallToolResult, any, error) {
		if db == nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: SQLite database is not available",
					},
				},
			}, nil, nil
		}

		cleanHash := strings.TrimPrefix(strings.TrimSpace(input.TxHash), "0x")
		hashBytes, err := hex.DecodeString(cleanHash)
		if err != nil || len(hashBytes) != 32 {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: fmt.Sprintf(
							"Invalid transaction hash '%s': expected 64 hex characters (32 bytes)",
							input.TxHash,
						),
					},
				},
			}, nil, nil
		}

		qCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		defer cancel()

		var txID uint64
		var slot uint64
		var fee uint64
		var blockIndex sql.NullInt64

		err = db.QueryRowContext(
			qCtx,
			"SELECT id, slot, fee, block_index FROM \"transaction\" WHERE hash = ? OR hash = ?",
			hashBytes,
			cleanHash,
		).Scan(&txID, &slot, &fee, &blockIndex)
		if err != nil {
			// Fallback to tx table
			err = db.QueryRowContext(qCtx,
				"SELECT id, slot, fee, 0 FROM tx WHERE hash = ? OR hash = ?",
				cleanHash, hashBytes).Scan(&txID, &slot, &fee, &blockIndex)
		}
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: fmt.Sprintf(
							"Transaction not found for hash '%s'",
							cleanHash,
						),
					},
				},
			}, nil, nil
		}

		// Count UTxOs created by this tx
		var outCount int
		var totalOut uint64
		_ = db.QueryRowContext(qCtx, "SELECT count(*), coalesce(sum(value), 0) FROM utxo WHERE tx_hash = ? OR tx_id = ?", cleanHash, txID).
			Scan(&outCount, &totalOut)

		bIdx := "N/A"
		if blockIndex.Valid {
			bIdx = strconv.FormatInt(blockIndex.Int64, 10)
		}

		markdown := fmt.Sprintf("### Transaction Details\n\n"+
			"- **Hash**: `%s`\n"+
			"- **Internal ID**: `%d`\n"+
			"- **Slot**: `%d`\n"+
			"- **Block Index**: `%s`\n"+
			"- **Fee (lovelace)**: %d\n"+
			"- **Outputs Created**: %d\n"+
			"- **Total Output Lovelace**: %d\n",
			cleanHash, txID, slot, bIdx, fee, outCount, totalOut)

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: markdown},
			},
		}, nil, nil
	})

	// Tool: get_utxos
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_utxos",
		Description: "Query unspent transaction outputs (UTxOs) for a Cardano address or payment credential with pagination.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input UTxOsParams) (*mcp.CallToolResult, any, error) {
		if db == nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: SQLite database is not available",
					},
				},
			}, nil, nil
		}

		addr := strings.TrimSpace(input.AddressOrCredential)
		limit := input.Limit
		if limit <= 0 || limit > 100 {
			limit = 20
		}
		offset := input.Offset
		if offset < 0 {
			offset = 0
		}

		qCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		defer cancel()

		var cols []string
		colsErr := func() error {
			rows, err := db.QueryContext(qCtx, "SELECT * FROM utxo LIMIT 1")
			if err != nil {
				return err
			}
			defer rows.Close()
			cols, err = rows.Columns()
			if err != nil {
				return err
			}
			return rows.Err()
		}()
		if colsErr != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "UTxO query error: " + colsErr.Error(),
					},
				},
			}, nil, nil
		}

		hasCol := func(name string) bool {
			for _, c := range cols {
				if c == name {
					return true
				}
			}
			return false
		}

		var qRows *sql.Rows
		var queryCols []string
		var err error

		if hasCol("address") {
			// Legacy or test schema with address string column
			qRows, err = db.QueryContext(qCtx,
				"SELECT * FROM utxo WHERE address = ? LIMIT ? OFFSET ?",
				addr, limit, offset)
			if err != nil {
				return &mcp.CallToolResult{
					IsError: true,
					Content: []mcp.Content{
						&mcp.TextContent{
							Text: "UTxO query error: " + err.Error(),
						},
					},
				}, nil, nil
			}
			defer qRows.Close()
			queryCols, _ = qRows.Columns()
		} else if hasCol("payment_key") {
			// Real Dingo schema with payment_key and optional staking_key / deleted_slot
			pk, sk, parseErr := parseAddressOrCredential(addr)
			if parseErr != nil {
				return &mcp.CallToolResult{
					IsError: true,
					Content: []mcp.Content{
						&mcp.TextContent{Text: "Invalid address or payment credential: " + parseErr.Error()},
					},
				}, nil, nil
			}

			var whereClauses []string
			var args []any

			if len(pk) > 0 && len(sk) > 0 {
				whereClauses = append(whereClauses, "(payment_key = ? AND staking_key = ?)")
				args = append(args, pk, sk)
			} else if len(pk) > 0 {
				whereClauses = append(whereClauses, "payment_key = ?")
				args = append(args, pk)
			} else if len(sk) > 0 {
				whereClauses = append(whereClauses, "staking_key = ?")
				args = append(args, sk)
			}

			if hasCol("deleted_slot") {
				whereClauses = append(whereClauses, "(deleted_slot = 0 OR deleted_slot IS NULL)")
			}

			orderClause := ""
			if hasCol("added_slot") {
				orderClause = " ORDER BY added_slot DESC"
			}

			whereStr := strings.Join(whereClauses, " AND ")
			// #nosec G201 //nolint:gosec // whereStr and orderClause are constructed from safe tokens
			query := fmt.Sprintf("SELECT * FROM utxo WHERE %s%s LIMIT ? OFFSET ?", whereStr, orderClause)
			args = append(args, limit, offset)

			qRows, err = db.QueryContext(qCtx, query, args...)
			if err != nil {
				return &mcp.CallToolResult{
					IsError: true,
					Content: []mcp.Content{
						&mcp.TextContent{Text: "UTxO query error: " + err.Error()},
					},
				}, nil, nil
			}
			defer qRows.Close()
			queryCols, _ = qRows.Columns()
		} else {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{Text: "Unsupported utxo table schema: neither address nor payment_key column found"},
				},
			}, nil, nil
		}

		var utxoRows [][]string
		for qRows.Next() {
			vals := make([]any, len(queryCols))
			valPtrs := make([]any, len(queryCols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := qRows.Scan(valPtrs...); err != nil {
				continue
			}
			rowStr := make([]string, len(queryCols))
			for i, v := range vals {
				switch val := v.(type) {
				case []byte:
					rowStr[i] = FormatCell(hex.EncodeToString(val))
				case nil:
					rowStr[i] = "null"
				default:
					rowStr[i] = FormatCell(fmt.Sprintf("%v", val))
				}
			}
			utxoRows = append(utxoRows, rowStr)
		}

		if err := qRows.Err(); err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "UTxO row iteration error: " + err.Error(),
					},
				},
			}, nil, nil
		}

		table := FormatMarkdownTable(queryCols, utxoRows)
		header := fmt.Sprintf(
			"### UTxOs for `%s`\n\n*(Showing %d results, offset %d)*\n\n",
			addr,
			len(utxoRows),
			offset,
		)

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: header + table},
			},
		}, nil, nil
	})

	// Tool: get_account
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_account",
		Description: "Query stake account details (delegation, pool ID, DRep, resignation status) from Dingo's SQLite store.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input AccountParams) (*mcp.CallToolResult, any, error) {
		if db == nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: SQLite database is not available",
					},
				},
			}, nil, nil
		}

		cred := strings.TrimSpace(input.StakeAddressOrCredential)
		qCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		defer cancel()

		var cols []string
		colsErr := func() error {
			rows, err := db.QueryContext(qCtx, "SELECT * FROM account LIMIT 1")
			if err != nil {
				return err
			}
			defer rows.Close()
			cols, err = rows.Columns()
			if err != nil {
				return err
			}
			return rows.Err()
		}()
		if colsErr != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Stake account query error: " + colsErr.Error(),
					},
				},
			}, nil, nil
		}

		// Build WHERE clause based on existing column
		whereCol := "stake_address"
		for _, c := range cols {
			if c == "credential" {
				whereCol = "credential"
				break
			} else if c == "staking_key" {
				whereCol = "staking_key"
				break
			}
		}

		//nolint:gosec // whereCol is whitelisted to stake_address, credential, or staking_key
		qRows, err := db.QueryContext(
			qCtx,
			fmt.Sprintf("SELECT * FROM account WHERE %s = ? LIMIT 1", whereCol),
			cred,
		)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Stake account query error: " + err.Error(),
					},
				},
			}, nil, nil
		}
		defer qRows.Close()

		if !qRows.Next() {
			if err := qRows.Err(); err != nil {
				return &mcp.CallToolResult{
					IsError: true,
					Content: []mcp.Content{
						&mcp.TextContent{
							Text: "Stake account query error: " + err.Error(),
						},
					},
				}, nil, nil
			}
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: fmt.Sprintf(
							"Stake account not found for credential '%s'",
							cred,
						),
					},
				},
			}, nil, nil
		}

		vals := make([]any, len(cols))
		valPtrs := make([]any, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		_ = qRows.Scan(valPtrs...)
		if err := qRows.Err(); err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Stake account row error: " + err.Error(),
					},
				},
			}, nil, nil
		}

		var rowStr []string
		for _, v := range vals {
			switch val := v.(type) {
			case []byte:
				rowStr = append(rowStr, FormatCell(hex.EncodeToString(val)))
			case nil:
				rowStr = append(rowStr, "null")
			default:
				rowStr = append(rowStr, FormatCell(fmt.Sprintf("%v", val)))
			}
		}

		table := FormatMarkdownTable(cols, [][]string{rowStr})
		markdown := fmt.Sprintf("### Stake Account: `%s`\n\n%s", cred, table)

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: markdown},
			},
		}, nil, nil
	})

	// Tool: get_epoch_summary
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_epoch_summary",
		Description: "Query epoch summary statistics including block counts and boundary nonces from SQLite.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input EpochSummaryParams) (*mcp.CallToolResult, any, error) {
		if db == nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: SQLite database is not available",
					},
				},
			}, nil, nil
		}

		qCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		defer cancel()

		var epochNum int
		if input.Epoch != nil {
			epochNum = *input.Epoch
		} else {
			// Find max epoch from blocks, epoch_summary, or block_nonce
			_ = db.QueryRowContext(qCtx, "SELECT coalesce(max(epoch), 0) FROM blocks").Scan(&epochNum)
			if epochNum == 0 {
				_ = db.QueryRowContext(qCtx, "SELECT coalesce(max(epoch), 0) FROM epoch_summary").Scan(&epochNum)
			}
			if epochNum == 0 {
				_ = db.QueryRowContext(qCtx, "SELECT coalesce(max(epoch), 0) FROM block_nonce").Scan(&epochNum)
			}
		}

		nonceHex := "unknown / not recorded"
		var blockCount int

		// Try blocks table count
		_ = db.QueryRowContext(qCtx, "SELECT count(*) FROM blocks WHERE epoch = ?", epochNum).
			Scan(&blockCount)
		if blockCount == 0 {
			var rawNonce []byte
			_ = db.QueryRowContext(qCtx, "SELECT nonce FROM block_nonce WHERE epoch = ?", epochNum).
				Scan(&rawNonce)
			if len(rawNonce) > 0 {
				nonceHex = hex.EncodeToString(rawNonce)
			}
			_ = db.QueryRowContext(qCtx, "SELECT count(*) FROM block_nonce WHERE epoch = ?", epochNum).
				Scan(&blockCount)
		}

		markdown := fmt.Sprintf("### Epoch Summary: `%d`\n\n"+
			"- **Network**: `%s`\n"+
			"- **Epoch Nonce**: `%s`\n"+
			"- **Recorded Blocks**: %d\n",
			epochNum, network, nonceHex, blockCount)

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: markdown},
			},
		}, nil, nil
	})

	// Tool: resolve_datum_or_script
	mcp.AddTool(server, &mcp.Tool{
		Name:        "resolve_datum_or_script",
		Description: "Resolve an on-chain datum hash (32 bytes / 64 hex) or script hash (28 bytes / 56 hex) into structured JSON or decoded script metadata.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input ResolveHashParams) (*mcp.CallToolResult, any, error) {
		trimmed := strings.TrimSpace(input.Hash)
		if trimmed == "" {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: Hash parameter is empty. Please provide a 28-byte script hash (56 hex chars) or 32-byte datum hash (64 hex chars).",
					},
				},
			}, nil, nil
		}

		hashBytes, err := hex.DecodeString(trimmed)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: fmt.Sprintf(
							"Error: Invalid hex-encoded hash: %v. Hash must contain only valid hexadecimal characters.",
							err,
						),
					},
				},
			}, nil, nil
		}

		if len(hashBytes) != 28 && len(hashBytes) != 32 {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: fmt.Sprintf(
							"Error: Invalid hash length: expected 28 bytes / 56 hex chars (script hash) or 32 bytes / 64 hex chars (datum hash), got %d bytes (%d hex chars).",
							len(hashBytes),
							len(trimmed),
						),
					},
				},
			}, nil, nil
		}

		if db == nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: SQLite database is not available to resolve datums or scripts.",
					},
				},
			}, nil, nil
		}

		qCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		defer cancel()

		// 1. Check datum table
		var rawDatum []byte
		var addedSlot sql.NullInt64
		err = db.QueryRowContext(qCtx, "SELECT raw_datum, added_slot FROM datum WHERE hash = ? LIMIT 1", hashBytes).
			Scan(&rawDatum, &addedSlot)
		if err == nil {
			slotStr := "unknown"
			if addedSlot.Valid {
				slotStr = strconv.FormatInt(addedSlot.Int64, 10)
			}
			jsonText, jsonErr := cborToPrettyJSON(rawDatum)
			if jsonErr != nil {
				jsonText = fmt.Sprintf(
					"/* raw cbor parse error: %v */\n%s",
					jsonErr,
					hex.EncodeToString(rawDatum),
				)
			}

			md := fmt.Sprintf(
				"### Resolved Datum: `%s`\n\n"+
					"- **Hash**: `%s`\n"+
					"- **Type**: Plutus Datum (CIP-32)\n"+
					"- **Added Slot**: `%s`\n"+
					"- **Size**: %d bytes\n"+
					"- **Raw CBOR (hex)**: `%s`\n\n"+
					"#### Decoded Plutus Data JSON\n```json\n%s\n```\n",
				trimmed,
				trimmed,
				slotStr,
				len(rawDatum),
				hex.EncodeToString(rawDatum),
				jsonText,
			)

			return &mcp.CallToolResult{
				Content: []mcp.Content{
					&mcp.TextContent{Text: md},
				},
			}, nil, nil
		}

		// 2. Check script table
		var content []byte
		var createdSlot sql.NullInt64
		var scriptType sql.NullInt64
		err = db.QueryRowContext(qCtx, "SELECT content, created_slot, type FROM script WHERE hash = ? LIMIT 1", hashBytes).
			Scan(&content, &createdSlot, &scriptType)
		if err == nil {
			slotStr := "unknown"
			if createdSlot.Valid {
				slotStr = strconv.FormatInt(createdSlot.Int64, 10)
			}
			stName := "Unknown"
			if scriptType.Valid {
				stName = scriptTypeName(scriptType.Int64)
			}

			preview := hex.EncodeToString(content)
			if len(preview) > 512 {
				preview = preview[:512] + "..."
			}

			md := fmt.Sprintf("### Resolved Script: `%s`\n\n"+
				"- **Hash**: `%s`\n"+
				"- **Script Type**: `%s`\n"+
				"- **Created Slot**: `%s`\n"+
				"- **Size**: %d bytes\n"+
				"- **CBOR (hex)**: `%s`\n",
				trimmed, trimmed, stName, slotStr, len(content), preview)

			return &mcp.CallToolResult{
				Content: []mcp.Content{
					&mcp.TextContent{Text: md},
				},
			}, nil, nil
		}

		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{
				&mcp.TextContent{
					Text: fmt.Sprintf(
						"Hash %q was not found in the local database (checked 'datum' and 'script' tables).\n\n"+
							"**Troubleshooting Suggestions**:\n"+
							"1. If looking for a transaction datum, verify whether the UTxO uses an inline datum rather than a datum hash.\n"+
							"2. If this transaction was recently submitted, the block containing it may not be indexed into SQLite yet.\n"+
							"3. Check for typos in the hash (datum hashes are 64 hex chars, script hashes are 56 hex chars).",
						trimmed,
					),
				},
			},
		}, nil, nil
	})

	// Tool: evaluate_tx
	mcp.AddTool(server, &mcp.Tool{
		Name:        "evaluate_tx",
		Description: "Simulate and evaluate Plutus script execution units (CPU steps, Memory units) and script fees for a transaction before on-chain submission.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input EvaluateTxParams) (*mcp.CallToolResult, any, error) {
		trimmed := strings.TrimSpace(input.Cbor)
		if trimmed == "" {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: Transaction payload is empty. Please provide hex-encoded or base64-encoded transaction CBOR.",
					},
				},
			}, nil, nil
		}

		var txBytes []byte
		var decErr error
		if b, err := hex.DecodeString(trimmed); err == nil {
			txBytes = b
		} else if b, err := base64.StdEncoding.DecodeString(trimmed); err == nil {
			txBytes = b
		} else {
			decErr = err
		}

		if len(txBytes) == 0 {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: fmt.Sprintf(
							"Error: Failed to decode transaction CBOR: %v. Body must be hex or base64 encoded.",
							decErr,
						),
					},
				},
			}, nil, nil
		}

		txType, err := gledger.DetermineTransactionType(txBytes)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: fmt.Sprintf(
							"Error: Invalid transaction CBOR: failed to determine transaction type: %v",
							err,
						),
					},
				},
			}, nil, nil
		}

		tx, err := gledger.NewTransactionFromCbor(txType, txBytes)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: fmt.Sprintf(
							"Error: Invalid transaction CBOR: failed to decode transaction: %v",
							err,
						),
					},
				},
			}, nil, nil
		}

		if ls == nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: Ledger state is not initialized on this node. Plutus transaction evaluation requires active ledger state and protocol parameters.",
					},
				},
			}, nil, nil
		}

		fee, totalExUnits, redeemerExUnits, err := ls.EvaluateTx(tx)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: fmt.Sprintf(
							"### Plutus Transaction Evaluation Failed\n\n"+
								"The transaction could not be evaluated against current ledger state:\n"+
								"- **Error**: `%v`\n\n"+
								"**Troubleshooting Guidance for Developer / AI**:\n"+
								"1. Verify that all referenced UTxO inputs exist and are unspent.\n"+
								"2. Confirm that required script witnesses and redeemers are attached.\n"+
								"3. Ensure required signers in the transaction body match the script expectations.\n"+
								"4. Ensure script execution units (CPU steps and memory units) are within protocol parameter maximums.",
							err,
						),
					},
				},
			}, nil, nil
		}

		cols := []string{"Purpose", "Index", "CPU Steps", "Memory Units"}
		var rows [][]string
		for key, ex := range redeemerExUnits {
			rows = append(rows, []string{
				redeemerPurposeString(key.Tag),
				strconv.FormatUint(uint64(key.Index), 10),
				strconv.FormatInt(ex.Steps, 10),
				strconv.FormatInt(ex.Memory, 10),
			})
		}

		var redeemerSection string
		if len(rows) > 0 {
			redeemerSection = fmt.Sprintf(
				"#### Redeemers Execution Breakdown\n\n%s\n",
				FormatMarkdownTable(cols, rows),
			)
		} else {
			redeemerSection = "_No script redeemers found in transaction._\n"
		}

		md := fmt.Sprintf("### Plutus Transaction Evaluation: SUCCESS\n\n"+
			"- **Estimated Script Fee**: %d lovelace\n"+
			"- **Total CPU Steps**: %d units\n"+
			"- **Total Memory**: %d units\n\n%s",
			fee, totalExUnits.Steps, totalExUnits.Memory, redeemerSection)

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: md},
			},
		}, nil, nil
	})

	// Tool: get_utxos_by_asset
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_utxos_by_asset",
		Description: "Query live unspent UTxOs holding a specific Cardano native asset by policy ID (and optional asset name). Kupo-style pattern matching directly against Dingo SQLite store.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input UtxosByAssetParams) (*mcp.CallToolResult, any, error) {
		if db == nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: SQLite database is not available",
					},
				},
			}, nil, nil
		}

		policyHex := strings.TrimSpace(input.PolicyID)
		if len(policyHex) != 56 {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: fmt.Sprintf(
							"Error: policy_id must be a 56-character hex string (28 bytes), got length %d",
							len(policyHex),
						),
					},
				},
			}, nil, nil
		}
		policyBytes, err := hex.DecodeString(policyHex)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: invalid hex in policy_id: " + err.Error(),
					},
				},
			}, nil, nil
		}

		limit := input.Limit
		if limit <= 0 {
			limit = 50
		} else if limit > 200 {
			limit = 200
		}
		offset := input.Offset
		if offset < 0 {
			offset = 0
		}

		qCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		defer cancel()

		var q string
		var args []any
		if strings.TrimSpace(input.AssetName) != "" {
			rawName, hexName := parseAssetName(input.AssetName)
			q = `SELECT u.tx_id, u.output_idx, u.payment_key, u.amount, a.name, a.fingerprint, a.amount, u.datum_hash
				 FROM asset a
				 JOIN utxo u ON a.utxo_id = u.id
				 WHERE a.policy_id = ? AND (a.name = ? OR hex(a.name) = UPPER(?) OR a.name = ?) AND (u.deleted_slot = 0 OR u.deleted_slot IS NULL)
				 ORDER BY u.id DESC
				 LIMIT ? OFFSET ?`
			args = []any{
				policyBytes,
				rawName,
				hexName,
				[]byte(input.AssetName),
				limit,
				offset,
			}
		} else {
			q = `SELECT u.tx_id, u.output_idx, u.payment_key, u.amount, a.name, a.fingerprint, a.amount, u.datum_hash
				 FROM asset a
				 JOIN utxo u ON a.utxo_id = u.id
				 WHERE a.policy_id = ? AND (u.deleted_slot = 0 OR u.deleted_slot IS NULL)
				 ORDER BY u.id DESC
				 LIMIT ? OFFSET ?`
			args = []any{policyBytes, limit, offset}
		}

		rows, err := db.QueryContext(qCtx, q, args...)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Asset UTxO query error: " + err.Error(),
					},
				},
			}, nil, nil
		}
		defer rows.Close()

		cols := []string{
			"UTxO",
			"Payment Credential",
			"Lovelace",
			"Asset Name",
			"Fingerprint",
			"Asset Amount",
			"Datum Hash",
		}
		var resRows [][]string
		for rows.Next() {
			var txID []byte
			var outIdx int
			var paymentKey []byte
			var lovelace sql.NullString
			var aName []byte
			var aFingerprint []byte
			var aAmount sql.NullString
			var datumHash []byte

			if err := rows.Scan(&txID, &outIdx, &paymentKey, &lovelace, &aName, &aFingerprint, &aAmount, &datumHash); err != nil {
				continue
			}

			utxoRef := fmt.Sprintf("%s#%d", hex.EncodeToString(txID), outIdx)
			payCred := hex.EncodeToString(paymentKey)
			loveStr := "0"
			if lovelace.Valid {
				loveStr = lovelace.String
			}
			nameDisplay := string(aName)
			if nameDisplay == "" {
				nameDisplay = hex.EncodeToString(aName)
			}
			fprint := string(aFingerprint)
			amtStr := "0"
			if aAmount.Valid {
				amtStr = aAmount.String
			}
			dHashStr := "null"
			if len(datumHash) > 0 {
				dHashStr = hex.EncodeToString(datumHash)
			}

			resRows = append(resRows, []string{
				FormatCell(utxoRef),
				FormatCell(payCred),
				FormatCell(loveStr),
				FormatCell(nameDisplay),
				FormatCell(fprint),
				FormatCell(amtStr),
				FormatCell(dHashStr),
			})
		}

		if err := rows.Err(); err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Asset UTxO row iteration error: " + err.Error(),
					},
				},
			}, nil, nil
		}

		if len(resRows) == 0 {
			nameHint := ""
			if input.AssetName != "" {
				nameHint = fmt.Sprintf(" (asset name: `%s`)", input.AssetName)
			}
			msg := fmt.Sprintf(
				"No live unspent UTxOs found holding policy `%s`%s.\n\n"+
					"**Possible reasons:**\n"+
					"1. All UTxOs holding this asset have been spent or burned.\n"+
					"2. The policy ID or asset name does not match on-chain records.\n"+
					"3. The node is still syncing blocks up to the asset's creation slot.",
				policyHex,
				nameHint,
			)
			return &mcp.CallToolResult{
				Content: []mcp.Content{
					&mcp.TextContent{Text: msg},
				},
			}, nil, nil
		}

		md := fmt.Sprintf(
			"### Live UTxOs for Policy `%s`\n\n*(Found %d UTxOs, offset %d)*\n\n%s",
			policyHex,
			len(resRows),
			offset,
			FormatMarkdownTable(cols, resRows),
		)
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: md},
			},
		}, nil, nil
	})

	// Tool: get_asset_info
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_asset_info",
		Description: "Detailed asset discovery and metadata inspection: queries off-chain Token Registry cache, on-chain circulating supply, mint/burn events, and CIP-25 NFT metadata (label 721).",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input AssetInfoParams) (*mcp.CallToolResult, any, error) {
		if db == nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: SQLite database is not available",
					},
				},
			}, nil, nil
		}

		policyHex := strings.TrimSpace(input.PolicyID)
		if len(policyHex) != 56 {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: fmt.Sprintf(
							"Error: policy_id must be a 56-character hex string (28 bytes), got %d chars",
							len(policyHex),
						),
					},
				},
			}, nil, nil
		}
		policyBytes, err := hex.DecodeString(policyHex)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: invalid hex in policy_id: " + err.Error(),
					},
				},
			}, nil, nil
		}

		rawName, hexName := parseAssetName(input.AssetName)
		subject := policyHex + hexName

		qCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		defer cancel()

		// 1. Check Token Registry
		var regName, regTicker, regDesc, regURL sql.NullString
		var regDecimals sql.NullInt64
		_ = db.QueryRowContext(
			qCtx,
			"SELECT name, ticker, description, url, decimals FROM token_registry_entry WHERE subject = ? LIMIT 1",
			subject,
		).Scan(&regName, &regTicker, &regDesc, &regURL, &regDecimals)

		// 2. Circulating Supply and live UTxO holder count
		var circSupply sql.NullString
		var holderCount int
		var circQuery string
		var circArgs []any
		if len(rawName) > 0 {
			circQuery = `SELECT COALESCE(SUM(CAST(a.amount AS INTEGER)), 0), COUNT(DISTINCT u.id)
						 FROM asset a
						 JOIN utxo u ON a.utxo_id = u.id
						 WHERE a.policy_id = ? AND (a.name = ? OR hex(a.name) = UPPER(?) OR a.name = ?) AND (u.deleted_slot = 0 OR u.deleted_slot IS NULL)`
			circArgs = []any{
				policyBytes,
				rawName,
				hexName,
				[]byte(input.AssetName),
			}
		} else {
			circQuery = `SELECT COALESCE(SUM(CAST(a.amount AS INTEGER)), 0), COUNT(DISTINCT u.id)
						 FROM asset a
						 JOIN utxo u ON a.utxo_id = u.id
						 WHERE a.policy_id = ? AND (u.deleted_slot = 0 OR u.deleted_slot IS NULL)`
			circArgs = []any{policyBytes}
		}
		_ = db.QueryRowContext(qCtx, circQuery, circArgs...).
			Scan(&circSupply, &holderCount)

		// 3. Mint / Burn history
		var initTxHash []byte
		var initSlot sql.NullInt64
		var mintCount int
		var mintQuery string
		var mintArgs []any
		if len(rawName) > 0 {
			mintQuery = `SELECT tx_hash, slot FROM asset_mint_burn WHERE policy_id = ? AND (name = ? OR fingerprint = ?) ORDER BY slot ASC, tx_index ASC LIMIT 1`
			mintArgs = []any{policyBytes, rawName, rawName}
		} else {
			mintQuery = `SELECT tx_hash, slot FROM asset_mint_burn WHERE policy_id = ? ORDER BY slot ASC, tx_index ASC LIMIT 1`
			mintArgs = []any{policyBytes}
		}
		_ = db.QueryRowContext(qCtx, mintQuery, mintArgs...).
			Scan(&initTxHash, &initSlot)

		if len(rawName) > 0 {
			_ = db.QueryRowContext(qCtx, `SELECT COUNT(*) FROM asset_mint_burn WHERE policy_id = ? AND (name = ? OR fingerprint = ?)`, policyBytes, rawName, rawName).
				Scan(&mintCount)
		} else {
			_ = db.QueryRowContext(qCtx, `SELECT COUNT(*) FROM asset_mint_burn WHERE policy_id = ?`, policyBytes).Scan(&mintCount)
		}

		// 4. Check CIP-25 NFT metadata (Label 721)
		var cip25JSON sql.NullString
		if len(initTxHash) > 0 {
			_ = db.QueryRowContext(
				qCtx,
				`SELECT tml.json_value
				 FROM transaction_metadata_label tml
				 JOIN "transaction" t ON tml.transaction_id = t.id
				 WHERE t.hash = ? AND tml.label = '721'
				 LIMIT 1`,
				initTxHash,
			).Scan(&cip25JSON)
		}

		hasRegistry := regName.Valid || regTicker.Valid
		hasOnChain := holderCount > 0 || len(initTxHash) > 0

		if !hasRegistry && !hasOnChain {
			return &mcp.CallToolResult{
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: fmt.Sprintf(
							"Asset with policy `%s` (name `%s`) was not found in the local token registry or on-chain mint records.\n\n"+
								"**Suggestion:** Verify that the 56-character policy ID is correct, or check if the asset has been minted yet.",
							policyHex,
							input.AssetName,
						),
					},
				},
			}, nil, nil
		}

		var sb strings.Builder
		fmt.Fprintf(
			&sb,
			"### Asset Intelligence: `%s.%s`\n\n",
			policyHex,
			input.AssetName,
		)

		sb.WriteString("#### Token Registry (Off-Chain)\n")
		if hasRegistry {
			if regName.Valid {
				fmt.Fprintf(&sb, "- **Name**: %s\n", regName.String)
			}
			if regTicker.Valid {
				fmt.Fprintf(&sb, "- **Ticker**: `%s`\n", regTicker.String)
			}
			if regDecimals.Valid {
				fmt.Fprintf(&sb, "- **Decimals**: %d\n", regDecimals.Int64)
			}
			if regDesc.Valid && regDesc.String != "" {
				fmt.Fprintf(&sb, "- **Description**: %s\n", regDesc.String)
			}
			if regURL.Valid && regURL.String != "" {
				fmt.Fprintf(&sb, "- **URL**: %s\n", regURL.String)
			}
		} else {
			sb.WriteString("_No off-chain Cardano Token Registry entry cached for this subject._\n")
		}
		sb.WriteString("\n")

		sb.WriteString("#### On-Chain Circulation & Activity\n")
		supplyStr := "0"
		if circSupply.Valid {
			supplyStr = circSupply.String
		}
		fmt.Fprintf(&sb, "- **Live Circulating Supply**: %s units\n", supplyStr)
		fmt.Fprintf(&sb, "- **Active Holding UTxOs**: %d\n", holderCount)
		if len(initTxHash) > 0 {
			fmt.Fprintf(
				&sb,
				"- **Initial Mint Transaction**: `%s`\n",
				hex.EncodeToString(initTxHash),
			)
			if initSlot.Valid {
				fmt.Fprintf(
					&sb,
					"- **Initial Mint Slot**: %d\n",
					initSlot.Int64,
				)
			}
			fmt.Fprintf(&sb, "- **Total Mint/Burn Events**: %d\n", mintCount)
		}
		sb.WriteString("\n")

		if cip25JSON.Valid && strings.TrimSpace(cip25JSON.String) != "" {
			sb.WriteString("#### CIP-25 NFT Metadata (Label 721)\n```json\n")
			sb.WriteString(strings.TrimSpace(cip25JSON.String))
			sb.WriteString("\n```\n")
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: sb.String()},
			},
		}, nil, nil
	})

	// Tool: get_governance_state
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_governance_state",
		Description: "Inspect Voltaire CIP-1694 governance: Cardano Constitution anchor URL/hash, Constitutional Committee members & quorum, and active DRep ecosystem metrics.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input GovernanceStateParams) (*mcp.CallToolResult, any, error) {
		if db == nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: SQLite database is not available",
					},
				},
			}, nil, nil
		}

		qCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		defer cancel()

		var sb strings.Builder
		sb.WriteString("### Cardano Conway (CIP-1694) Governance State\n\n")

		// 1. Constitution
		var constURL sql.NullString
		var constHash, constPolicyHash []byte
		var constSlot sql.NullInt64
		err := db.QueryRowContext(
			qCtx,
			"SELECT anchor_url, anchor_hash, policy_hash, added_slot FROM constitution WHERE deleted_slot IS NULL OR deleted_slot = 0 ORDER BY added_slot DESC LIMIT 1",
		).Scan(&constURL, &constHash, &constPolicyHash, &constSlot)

		sb.WriteString("#### 1. Constitution\n")
		if err == nil && constURL.Valid {
			fmt.Fprintf(&sb, "- **Anchor URL**: %s\n", constURL.String)
			fmt.Fprintf(
				&sb,
				"- **Anchor Hash**: `%s`\n",
				hex.EncodeToString(constHash),
			)
			if len(constPolicyHash) > 0 {
				fmt.Fprintf(
					&sb,
					"- **Guardrail Script Hash**: `%s`\n",
					hex.EncodeToString(constPolicyHash),
				)
			}
			if constSlot.Valid {
				fmt.Fprintf(&sb, "- **Ratified Slot**: %d\n", constSlot.Int64)
			}
		} else {
			sb.WriteString("_No active ratified constitution found in local store (network may be bootstrapping Conway)._\n")
		}
		sb.WriteString("\n")

		// 2. Constitutional Committee & Quorum
		var quorumStr sql.NullString
		var quorumSlot sql.NullInt64
		_ = db.QueryRowContext(
			qCtx,
			"SELECT quorum, added_slot FROM committee_quorum ORDER BY added_slot DESC LIMIT 1",
		).Scan(&quorumStr, &quorumSlot)

		commRows, err := db.QueryContext(
			qCtx,
			"SELECT cold_cred_hash, expires_epoch, term_start_slot FROM committee_member WHERE deleted_slot IS NULL OR deleted_slot = 0 ORDER BY expires_epoch ASC",
		)

		sb.WriteString("#### 2. Constitutional Committee\n")
		if quorumStr.Valid {
			fmt.Fprintf(&sb, "- **Quorum Threshold**: %s\n", quorumStr.String)
		}

		if err == nil {
			defer commRows.Close()
			var members [][]string
			commCols := []string{
				"Cold Credential Hash",
				"Expires Epoch",
				"Term Start Slot",
			}
			for commRows.Next() {
				var credHash []byte
				var expEpoch, termSlot int64
				if err := commRows.Scan(&credHash, &expEpoch, &termSlot); err == nil {
					members = append(members, []string{
						FormatCell(hex.EncodeToString(credHash)),
						strconv.FormatInt(expEpoch, 10),
						strconv.FormatInt(termSlot, 10),
					})
				}
			}
			if err := commRows.Err(); err != nil {
				sb.WriteString("_Error iterating committee members._\n\n")
			} else if len(members) > 0 {
				fmt.Fprintf(&sb, "\n%s\n", FormatMarkdownTable(commCols, members))
			} else {
				sb.WriteString("_No active committee members recorded._\n\n")
			}
		} else {
			sb.WriteString("_Committee member table query error or not yet populated._\n\n")
		}

		// 3. DRep Ecosystem Status
		var activeDReps, expiringDReps int
		_ = db.QueryRowContext(qCtx, "SELECT COUNT(*) FROM drep WHERE active = TRUE").
			Scan(&activeDReps)
		_ = db.QueryRowContext(qCtx, "SELECT COUNT(*) FROM drep WHERE active = TRUE AND expiry_epoch > 0").
			Scan(&expiringDReps)

		sb.WriteString("#### 3. Delegated Representatives (DReps)\n")
		fmt.Fprintf(&sb, "- **Active Registered DReps**: %d\n", activeDReps)
		fmt.Fprintf(
			&sb,
			"- **DReps with Active Expiry Term**: %d\n\n",
			expiringDReps,
		)

		// 4. Specific DRep details if supplied
		if strings.TrimSpace(input.DrepCredential) != "" {
			drepBytes, err := parseDrepID(input.DrepCredential)
			if err != nil {
				fmt.Fprintf(
					&sb,
					"> [!WARNING]\n> Could not parse DRep identifier `%s`: %s\n\n",
					input.DrepCredential,
					err.Error(),
				)
			} else if len(drepBytes) > 0 {
				var anchorURL sql.NullString
				var anchorHash []byte
				var addedSlot, lastAct, expiry int64
				var active bool
				drepErr := db.QueryRowContext(
					qCtx,
					"SELECT anchor_url, anchor_hash, added_slot, last_activity_epoch, expiry_epoch, active FROM drep WHERE credential = ? LIMIT 1",
					drepBytes,
				).Scan(&anchorURL, &anchorHash, &addedSlot, &lastAct, &expiry, &active)

				fmt.Fprintf(&sb, "#### Specific DRep: `%s`\n", input.DrepCredential)
				if drepErr == nil {
					fmt.Fprintf(&sb, "- **Status**: Active=%v\n", active)
					if anchorURL.Valid {
						fmt.Fprintf(&sb, "- **Anchor URL**: %s\n", anchorURL.String)
					}
					if len(anchorHash) > 0 {
						fmt.Fprintf(&sb, "- **Anchor Hash**: `%s`\n", hex.EncodeToString(anchorHash))
					}
					fmt.Fprintf(&sb, "- **Registration Slot**: %d\n", addedSlot)
					fmt.Fprintf(&sb, "- **Last Activity Epoch**: %d\n", lastAct)
					fmt.Fprintf(&sb, "- **Expiry Epoch**: %d\n", expiry)
				} else {
					sb.WriteString("_DRep credential not found in local DRep table._\n")
				}
			}
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: sb.String()},
			},
		}, nil, nil
	})

	// Tool: get_pool_performance
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_pool_performance",
		Description: "Query stake pool historical performance and rewards from Dingo's SQLite store: blocks produced, apparent performance, pledge, cost/margin, and member/leader rewards.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input PoolPerformanceParams) (*mcp.CallToolResult, any, error) {
		if db == nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: SQLite database is not available",
					},
				},
			}, nil, nil
		}

		poolBytes, err := parsePoolID(input.PoolID)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error parsing pool ID: " + err.Error(),
					},
				},
			}, nil, nil
		}

		limit := input.Limit
		if limit <= 0 {
			limit = 10
		} else if limit > 50 {
			limit = 50
		}

		qCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		defer cancel()

		var q string
		var args []any
		if input.Epoch != nil {
			q = `SELECT o.epoch, o.apparent_performance, o.optimal_reward, o.total_reward, o.leader_reward, o.member_reward_total,
						i.pledge, i.cost, i.margin, i.delegated_stake, i.blocks_produced
				 FROM reward_pool_output o
				 LEFT JOIN reward_pool_input i ON o.epoch = i.epoch AND o.pool_key_hash = i.pool_key_hash
				 WHERE o.pool_key_hash = ? AND o.epoch = ?
				 ORDER BY o.epoch DESC LIMIT 1`
			args = []any{poolBytes, *input.Epoch}
		} else {
			q = `SELECT o.epoch, o.apparent_performance, o.optimal_reward, o.total_reward, o.leader_reward, o.member_reward_total,
						i.pledge, i.cost, i.margin, i.delegated_stake, i.blocks_produced
				 FROM reward_pool_output o
				 LEFT JOIN reward_pool_input i ON o.epoch = i.epoch AND o.pool_key_hash = i.pool_key_hash
				 WHERE o.pool_key_hash = ?
				 ORDER BY o.epoch DESC LIMIT ?`
			args = []any{poolBytes, limit}
		}

		rows, err := db.QueryContext(qCtx, q, args...)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Pool performance query error: " + err.Error(),
					},
				},
			}, nil, nil
		}
		defer rows.Close()

		cols := []string{
			"Epoch",
			"Blocks",
			"Delegated Stake (ADA)",
			"Pledge (ADA)",
			"Apparent Performance",
			"Total Reward",
			"Leader Reward",
			"Member Rewards",
		}
		var poolRows [][]string
		for rows.Next() {
			var epoch int64
			var perf, optRew, totRew, leadRew, memRew sql.NullString
			var pledge, cost, margin, delStake sql.NullString
			var blocks sql.NullInt64

			if err := rows.Scan(&epoch, &perf, &optRew, &totRew, &leadRew, &memRew, &pledge, &cost, &margin, &delStake, &blocks); err != nil {
				continue
			}

			blocksStr := "0"
			if blocks.Valid {
				blocksStr = strconv.FormatInt(blocks.Int64, 10)
			}
			delStakeStr := "-"
			if delStake.Valid {
				delStakeStr = delStake.String
			}
			pledgeStr := "-"
			if pledge.Valid {
				pledgeStr = pledge.String
			}
			perfStr := "-"
			if perf.Valid {
				perfStr = perf.String
			}
			totRewStr := "0"
			if totRew.Valid {
				totRewStr = totRew.String
			}
			leadRewStr := "0"
			if leadRew.Valid {
				leadRewStr = leadRew.String
			}
			memRewStr := "0"
			if memRew.Valid {
				memRewStr = memRew.String
			}

			poolRows = append(poolRows, []string{
				strconv.FormatInt(epoch, 10),
				blocksStr,
				FormatCell(delStakeStr),
				FormatCell(pledgeStr),
				FormatCell(perfStr),
				FormatCell(totRewStr),
				FormatCell(leadRewStr),
				FormatCell(memRewStr),
			})
		}

		if err := rows.Err(); err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Pool performance row iteration error: " + err.Error(),
					},
				},
			}, nil, nil
		}

		if len(poolRows) == 0 {
			msg := fmt.Sprintf(
				"No historical reward performance records found for pool `%s` (hash `%s`).\n\n"+
					"**Possible reasons:**\n"+
					"1. The pool did not produce blocks in recent epochs.\n"+
					"2. Reward snapshot calculations for this epoch have not finalized yet.\n"+
					"3. The pool ID is valid but not currently registered on this network.",
				input.PoolID,
				hex.EncodeToString(poolBytes),
			)
			return &mcp.CallToolResult{
				Content: []mcp.Content{
					&mcp.TextContent{Text: msg},
				},
			}, nil, nil
		}

		md := fmt.Sprintf(
			"### Performance History for Pool `%s`\n\n*(Showing %d epoch snapshots)*\n\n%s",
			input.PoolID,
			len(poolRows),
			FormatMarkdownTable(cols, poolRows),
		)
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: md},
			},
		}, nil, nil
	})
}

func cborToPrettyJSON(raw []byte) (string, error) {
	var val any
	if _, err := cbor.Decode(raw, &val); err != nil {
		return "", err
	}
	cleaned := sanitizeCborValue(val)
	jsonBytes, err := json.MarshalIndent(cleaned, "", "  ")
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

func sanitizeCborValue(v any) any {
	switch val := v.(type) {
	case []byte:
		return hex.EncodeToString(val)
	case cbor.Tag:
		return map[string]any{
			"constructor": val.Number,
			"fields":      sanitizeCborValue(val.Content),
		}
	case map[any]any:
		res := make(map[string]any, len(val))
		for k, v := range val {
			res[fmt.Sprintf("%v", k)] = sanitizeCborValue(v)
		}
		return res
	case []any:
		res := make([]any, len(val))
		for i, elem := range val {
			res[i] = sanitizeCborValue(elem)
		}
		return res
	default:
		return val
	}
}

func scriptTypeName(t int64) string {
	switch t {
	case 0:
		return "Native / MultiSig (Phase-1)"
	case 1:
		return "PlutusV1"
	case 2:
		return "PlutusV2"
	case 3:
		return "PlutusV3"
	default:
		return fmt.Sprintf("Unknown (%d)", t)
	}
}

func redeemerPurposeString(tag lcommon.RedeemerTag) string {
	switch tag {
	case lcommon.RedeemerTagSpend:
		return "spend"
	case lcommon.RedeemerTagMint:
		return "mint"
	case lcommon.RedeemerTagCert:
		return "cert"
	case lcommon.RedeemerTagReward:
		return "reward"
	case lcommon.RedeemerTagVoting:
		return "voting"
	case lcommon.RedeemerTagProposing:
		return "proposing"
	case lcommon.RedeemerTagGuarding:
		return "guarding"
	default:
		return fmt.Sprintf("tag(%d)", tag)
	}
}

func parseAssetName(name string) ([]byte, string) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, ""
	}
	if h, err := hex.DecodeString(name); err == nil && len(name)%2 == 0 {
		return h, strings.ToLower(name)
	}
	return []byte(name), hex.EncodeToString([]byte(name))
}

func parsePoolID(id string) ([]byte, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, errors.New("pool ID cannot be empty")
	}
	if len(id) == 56 {
		if hash, err := hex.DecodeString(id); err == nil {
			return hash, nil
		}
	}
	hrp, data, err := bech32.Decode(id)
	if err != nil {
		return nil, fmt.Errorf(
			"invalid pool ID format (expected 56 hex chars or bech32 'pool1...'): %w",
			err,
		)
	}
	if strings.ToLower(hrp) != "pool" {
		return nil, fmt.Errorf("invalid pool prefix %q (expected 'pool')", hrp)
	}
	payload, err := bech32.ConvertBits(data, 5, 8, false)
	if err != nil || len(payload) != 28 {
		return nil, errors.New(
			"invalid pool bech32 payload length (expected 28 bytes)",
		)
	}
	return payload, nil
}

func parseDrepID(id string) ([]byte, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, nil
	}
	if len(id) == 56 {
		if hash, err := hex.DecodeString(id); err == nil {
			return hash, nil
		}
	}
	hrp, data, err := bech32.Decode(id)
	if err == nil &&
		(strings.ToLower(hrp) == "drep" || strings.ToLower(hrp) == "drep_script") {
		payload, err := bech32.ConvertBits(data, 5, 8, false)
		if err == nil && len(payload) == 28 {
			return payload, nil
		}
	}
	if h, err := hex.DecodeString(id); err == nil {
		return h, nil
	}
	return nil, errors.New(
		"invalid DRep identifier (expected 56 hex characters or bech32 'drep1...')",
	)
}

func parseAddressOrCredential(addrStr string) ([]byte, []byte, error) {
	clean := strings.TrimSpace(addrStr)
	if clean == "" {
		return nil, nil, errors.New("address or credential cannot be empty")
	}

	// 1. Check if 56-character hex string (28-byte Blake2b-224 hash)
	if len(clean) == 56 {
		if b, err := hex.DecodeString(clean); err == nil {
			return b, nil, nil
		}
	}

	// 2. Try parsing as a Cardano address via gouroboros
	if addr, err := lcommon.NewAddress(clean); err == nil {
		zeroHash := lcommon.NewBlake2b224(nil)
		var pk, sk []byte
		if pkh := addr.PaymentKeyHash(); pkh != zeroHash {
			pk = pkh.Bytes()
		}
		if skh := addr.StakeKeyHash(); skh != zeroHash {
			sk = skh.Bytes()
		}
		if len(pk) > 0 || len(sk) > 0 {
			return pk, sk, nil
		}
	}

	// 3. Try Bech32 decode for addr_vkh / stake_vkh / addr / stake
	if hrp, data, err := bech32.Decode(clean); err == nil {
		converted, err := bech32.ConvertBits(data, 5, 8, false)
		if err == nil {
			if len(converted) == 28 {
				if strings.HasPrefix(strings.ToLower(hrp), "stake") {
					return nil, converted, nil
				}
				return converted, nil, nil
			} else if len(converted) > 28 {
				// Header byte + 28 bytes payment key
				return converted[1:29], nil, nil
			}
		}
	}

	return nil, nil, fmt.Errorf(
		"invalid address or credential format %q",
		clean,
	)
}
