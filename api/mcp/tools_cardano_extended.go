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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mcp

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ProtocolParametersParams defines input schema for get_protocol_parameters.
type ProtocolParametersParams struct {
	Epoch *uint64 `json:"epoch,omitempty" jsonschema:"Optional epoch number to query protocol parameters for. Omit to query current active parameters."`
}

// MempoolInfoParams defines input schema for get_mempool_info.
type MempoolInfoParams struct {
	TxHash string `json:"tx_hash,omitempty" jsonschema:"Optional 64-character hex transaction hash to inspect in the mempool."`
	Limit  int    `json:"limit,omitempty"   jsonschema:"Maximum number of pending transactions to list when tx_hash is omitted (default 10, max 50)."`
}

// DecodeAddressParams defines input schema for decode_address.
type DecodeAddressParams struct {
	Address string `json:"address" jsonschema:"Cardano address in Bech32 (addr..., addr_test..., stake...) or Byron base58 format."`
}

// GovernanceProposalParams defines input schema for get_governance_proposal.
type GovernanceProposalParams struct {
	TxHash      string `json:"tx_hash,omitempty"      jsonschema:"Optional 64-character hex transaction hash of the governance proposal."`
	ActionIndex *int   `json:"action_index,omitempty" jsonschema:"Optional governance action index within the transaction (default 0)."`
	Status      string `json:"status,omitempty"       jsonschema:"Filter by proposal status: 'active', 'ratified', 'enacted', 'expired', or 'all' (default 'active')."`
	Limit       int    `json:"limit,omitempty"        jsonschema:"Maximum number of proposals to return when listing (default 10, max 50)."`
}

// CalculateMinUtxoParams defines input schema for calculate_min_utxo.
type CalculateMinUtxoParams struct {
	Address        string  `json:"address,omitempty"             jsonschema:"Destination Cardano Bech32 address (defaults to a standard 29-byte Shelley enterprise address if omitted)."`
	CoinsPerByte   *uint64 `json:"coins_per_utxo_byte,omitempty"    jsonschema:"Lovelace cost per UTxO byte (defaults to active ledger parameter, or 4310 if unconfigured)."`
	HasDatum       bool    `json:"has_datum,omitempty"           jsonschema:"Whether the output includes a 32-byte datum hash."`
	InlineDatumHex string  `json:"inline_datum_hex,omitempty"    jsonschema:"Optional hex-encoded CBOR of an inline datum attached to the output."`
	RefScriptHex   string  `json:"ref_script_hex,omitempty"      jsonschema:"Optional hex-encoded reference script attached to the output."`
	AssetsCount    int     `json:"assets_count,omitempty"        jsonschema:"Total number of distinct native asset entries in the multi-asset value."`
	PoliciesCount  int     `json:"policies_count,omitempty"      jsonschema:"Total number of distinct policy IDs in the multi-asset value (default 1 if assets_count > 0)."`
}

// registerExtendedCardanoTools registers the 5 additional semantic tools.
func registerExtendedCardanoTools(
	server *mcp.Server,
	db *sql.DB,
	ls *ledger.LedgerState,
	mp mempool.Service,
	network string,
	queryTimeout time.Duration,
) {
	// 1. Tool: get_protocol_parameters
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_protocol_parameters",
		Description: "Query active or historical Cardano protocol parameters (fee coefficients a/b, coins_per_utxo_byte, Plutus execution unit prices, deposits, and cost models).",
	}, func(_ context.Context, _ *mcp.CallToolRequest, input ProtocolParametersParams) (*mcp.CallToolResult, any, error) {
		var pparams lcommon.ProtocolParameters
		epochLabel := "current active"
		if input.Epoch != nil {
			epochLabel = fmt.Sprintf("epoch %d", *input.Epoch)
		}

		if ls != nil {
			if input.Epoch == nil {
				pparams = ls.GetCurrentPParamsForReporting()
			} else {
				// Query parameters for specific slot projected from epoch
				slot := *input.Epoch * 432000 // default epoch length fallback
				pparams = ls.ProtocolParamsForSlot(slot)
			}
		}

		if pparams == nil {
			return &mcp.CallToolResult{
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: fmt.Sprintf("Protocol parameters are currently unavailable for %s (ledger state not initialized or parameters not yet decoded).", epochLabel),
					},
				},
			}, nil, nil
		}

		text := formatProtocolParameters(pparams, epochLabel, network)
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: text},
			},
		}, nil, nil
	})

	// 2. Tool: get_mempool_info
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_mempool_info",
		Description: "Inspect pending in-flight transactions, mempool byte volume, capacity, and check specific transaction status before block inclusion.",
	}, func(_ context.Context, _ *mcp.CallToolRequest, input MempoolInfoParams) (*mcp.CallToolResult, any, error) {
		if mp == nil {
			return &mcp.CallToolResult{
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "### Mempool Status\nThe mempool service is currently inactive in this node configuration (or the node is running in passive client mode).",
					},
				},
			}, nil, nil
		}

		cleanHash := strings.ToLower(strings.TrimSpace(input.TxHash))
		cleanHash = strings.TrimPrefix(cleanHash, "0x")

		if cleanHash != "" {
			tx, found := mp.GetTransaction(cleanHash)
			if !found {
				return &mcp.CallToolResult{
					Content: []mcp.Content{
						&mcp.TextContent{
							Text: fmt.Sprintf("### Mempool Transaction Lookup\n\nTransaction `%s` is **not currently present** in Dingo's in-memory mempool.\n\n*Possible reasons*:\n- The transaction was already minted into a block (inspect via `get_transaction`).\n- The transaction expired or was rejected during ledger validation.\n- The transaction has not yet been relayed to this node.", cleanHash),
						},
					},
				}, nil, nil
			}

			age := time.Since(tx.LastSeen).Truncate(time.Millisecond)
			result := fmt.Sprintf("### Mempool Transaction Details\n\n"+
				"- **Transaction Hash**: `%s`\n"+
				"- **Status**: Pending Block Inclusion\n"+
				"- **Era / Type ID**: `%d`\n"+
				"- **Payload Size**: %d bytes\n"+
				"- **First Seen**: %s (%s ago)\n",
				tx.Hash, tx.Type, len(tx.Cbor),
				tx.LastSeen.UTC().Format(time.RFC3339), age,
			)
			return &mcp.CallToolResult{
				Content: []mcp.Content{
					&mcp.TextContent{Text: result},
				},
			}, nil, nil
		}

		// Summary listing
		txs := mp.Transactions()
		capacityBytes := mp.CapacityBytes()
		var totalBytes int64
		for _, tx := range txs {
			totalBytes += int64(len(tx.Cbor))
		}

		saturation := 0.0
		if capacityBytes > 0 {
			saturation = (float64(totalBytes) / float64(capacityBytes)) * 100
		}

		limit := input.Limit
		if limit <= 0 {
			limit = 10
		} else if limit > 50 {
			limit = 50
		}

		var sb strings.Builder
		sb.WriteString("### Dingo In-Memory Mempool Status\n\n")
		fmt.Fprintf(&sb, "- **Pending Transactions**: %d\n", len(txs))
		fmt.Fprintf(&sb, "- **Buffered Volume**: %d bytes (%s)\n", totalBytes, formatBytes(totalBytes))
		fmt.Fprintf(&sb, "- **Mempool Capacity**: %d bytes (%s)\n", capacityBytes, formatBytes(capacityBytes))
		fmt.Fprintf(&sb, "- **Saturation**: %.2f%%\n\n", saturation)

		if len(txs) == 0 {
			sb.WriteString("*Mempool is currently empty. No pending in-flight transactions.*\n")
		} else {
			fmt.Fprintf(&sb, "#### Recent Pending Transactions (showing up to %d of %d)\n\n", limit, len(txs))
			sb.WriteString("| Transaction Hash | Size (bytes) | Type | Age |\n")
			sb.WriteString("| :--- | :--- | :--- | :--- |\n")
			count := 0
			for i := len(txs) - 1; i >= 0 && count < limit; i-- {
				tx := txs[i]
				age := time.Since(tx.LastSeen).Truncate(time.Second)
				fmt.Fprintf(&sb, "| `%s` | %d | %d | %s |\n", tx.Hash, len(tx.Cbor), tx.Type, age)
				count++
			}
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: sb.String()},
			},
		}, nil, nil
	})

	// 3. Tool: decode_address
	mcp.AddTool(server, &mcp.Tool{
		Name:        "decode_address",
		Description: "Parse and cryptographically inspect any Cardano Bech32 or Byron address: extracts network ID, payment credential (key vs script), staking credential, and raw byte hashes.",
	}, func(_ context.Context, _ *mcp.CallToolRequest, input DecodeAddressParams) (*mcp.CallToolResult, any, error) {
		clean := strings.TrimSpace(input.Address)
		if clean == "" {
			return nil, nil, errors.New("address parameter is required")
		}

		addr, err := lcommon.NewAddress(clean)
		if err != nil {
			return nil, nil, fmt.Errorf("decode address: %w", err)
		}

		var networkName string
		switch addr.NetworkId() {
		case 1:
			networkName = "Mainnet (ID 1)"
		case 0:
			networkName = "Testnet / Preview / Preprod (ID 0)"
		default:
			networkName = fmt.Sprintf("Network ID %d", addr.NetworkId())
		}

		var addrTypeName string
		switch addr.Type() {
		case lcommon.AddressTypeKeyKey, lcommon.AddressTypeScriptKey, lcommon.AddressTypeKeyScript, lcommon.AddressTypeScriptScript:
			addrTypeName = "Shelley Base Address (Payment + Stake)"
		case lcommon.AddressTypeKeyNone, lcommon.AddressTypeScriptNone:
			addrTypeName = "Shelley Enterprise Address (Payment Only, No Stake)"
		case lcommon.AddressTypeNoneKey, lcommon.AddressTypeNoneScript:
			addrTypeName = "Shelley Reward Address (Stake Account)"
		case lcommon.AddressTypeKeyPointer, lcommon.AddressTypeScriptPointer:
			addrTypeName = "Shelley Pointer Address"
		case lcommon.AddressTypeByron:
			addrTypeName = "Byron Legacy Address (Base58)"
		default:
			addrTypeName = fmt.Sprintf("Address Type %d", addr.Type())
		}

		zeroHash := lcommon.NewBlake2b224(nil)
		paymentType := "none"
		paymentHash := "none"
		if pkh := addr.PaymentKeyHash(); pkh != zeroHash {
			paymentHash = hex.EncodeToString(pkh.Bytes())
			switch addr.Type() {
			case lcommon.AddressTypeScriptKey, lcommon.AddressTypeScriptScript, lcommon.AddressTypeScriptPointer, lcommon.AddressTypeScriptNone:
				paymentType = "Script Hash (Plutus / Native Script)"
			default:
				paymentType = "Public Key Hash (Ed25519 VKey)"
			}
		}

		stakeType := "none"
		stakeHash := "none"
		stakeAddrStr := "none"
		if skh := addr.StakeKeyHash(); skh != zeroHash {
			stakeHash = hex.EncodeToString(skh.Bytes())
			switch addr.Type() {
			case lcommon.AddressTypeKeyScript, lcommon.AddressTypeScriptScript, lcommon.AddressTypeNoneScript:
				stakeType = "Stake Script Hash (Script Delegator)"
			default:
				stakeType = "Stake Public Key Hash (VKey Delegator)"
			}
			if sa := addr.StakeAddress(); sa != nil {
				stakeAddrStr = sa.String()
			}
		}

		addrBytes, _ := addr.Bytes()
		rawHex := hex.EncodeToString(addrBytes)

		report := fmt.Sprintf("### Cardano Address Decomposition\n\n"+
			"| Property | Value |\n"+
			"| :--- | :--- |\n"+
			"| **Canonical Address** | `%s` |\n"+
			"| **Address Type** | %s |\n"+
			"| **Network** | %s |\n"+
			"| **Payment Credential Type** | %s |\n"+
			"| **Payment Credential Hash** | `%s` |\n"+
			"| **Staking Credential Type** | %s |\n"+
			"| **Staking Credential Hash** | `%s` |\n"+
			"| **Associated Stake Address** | `%s` |\n"+
			"| **Raw Address Bytes (Hex)** | `%s` |\n",
			addr.String(), addrTypeName, networkName,
			paymentType, paymentHash,
			stakeType, stakeHash, stakeAddrStr,
			rawHex,
		)

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: report},
			},
		}, nil, nil
	})

	// 4. Tool: get_governance_proposal
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_governance_proposal",
		Description: "Query Conway CIP-1694 governance action proposals and vote tallies across DReps, Stake Pool Operators (SPOs), and Constitutional Committee (CC).",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input GovernanceProposalParams) (*mcp.CallToolResult, any, error) {
		if db == nil {
			return nil, nil, errors.New("SQLite metadata database connection is nil")
		}

		ctxTimeout, cancel := context.WithTimeout(ctx, queryTimeout)
		defer cancel()

		cleanHash := strings.ToLower(strings.TrimSpace(input.TxHash))
		cleanHash = strings.TrimPrefix(cleanHash, "0x")

		if cleanHash != "" {
			txHashBytes, err := hex.DecodeString(cleanHash)
			if err != nil || len(txHashBytes) != 32 {
				return nil, nil, fmt.Errorf("invalid transaction hash %q: must be 64 hex characters (32 bytes)", cleanHash)
			}

			actionIdx := 0
			if input.ActionIndex != nil {
				actionIdx = *input.ActionIndex
			}

			row := db.QueryRowContext(ctxTimeout, `
				SELECT id, tx_hash, action_index, action_type, proposed_epoch, expires_epoch,
				       enacted_epoch, ratified_epoch, expired_epoch, anchor_url, anchor_hash, deposit, return_address
				FROM governance_proposal
				WHERE tx_hash = ? AND action_index = ?
				LIMIT 1`,
				txHashBytes, actionIdx,
			)

			var (
				id                                                   int64
				txHashRaw, anchorHashRaw, returnAddressRaw           []byte
				actionIndex, actionType, proposedEpoch, expiresEpoch int
				enactedEpoch, ratifiedEpoch, expiredEpoch            sql.NullInt64
				anchorURL                                            sql.NullString
				deposit                                              uint64
			)

			err = row.Scan(
				&id, &txHashRaw, &actionIndex, &actionType, &proposedEpoch, &expiresEpoch,
				&enactedEpoch, &ratifiedEpoch, &expiredEpoch, &anchorURL, &anchorHashRaw, &deposit, &returnAddressRaw,
			)
			if errors.Is(err, sql.ErrNoRows) {
				return &mcp.CallToolResult{
					Content: []mcp.Content{
						&mcp.TextContent{
							Text: fmt.Sprintf("Governance action proposal `%s#%d` was not found in Dingo's governance database.", cleanHash, actionIdx),
						},
					},
				}, nil, nil
			} else if err != nil {
				return nil, nil, fmt.Errorf("query governance proposal: %w", err)
			}

			status := "Active"
			if enactedEpoch.Valid {
				status = fmt.Sprintf("Enacted (epoch %d)", enactedEpoch.Int64)
			} else if ratifiedEpoch.Valid {
				status = fmt.Sprintf("Ratified (epoch %d)", ratifiedEpoch.Int64)
			} else if expiredEpoch.Valid {
				status = fmt.Sprintf("Expired (epoch %d)", expiredEpoch.Int64)
			}

			urlStr := "none"
			if anchorURL.Valid && anchorURL.String != "" {
				urlStr = anchorURL.String
			}
			anchorHashHex := "none"
			if len(anchorHashRaw) > 0 {
				anchorHashHex = hex.EncodeToString(anchorHashRaw)
			}

			// Query voting tallies
			voteRows, err := db.QueryContext(ctxTimeout, `
				SELECT voter_type, vote, count(*)
				FROM governance_vote
				WHERE proposal_id = ?
				GROUP BY voter_type, vote`,
				id,
			)
			tallyMap := make(map[string]map[string]int) // voter -> vote -> count
			tallyMap["CC"] = map[string]int{"Yes": 0, "No": 0, "Abstain": 0}
			tallyMap["DRep"] = map[string]int{"Yes": 0, "No": 0, "Abstain": 0}
			tallyMap["SPO"] = map[string]int{"Yes": 0, "No": 0, "Abstain": 0}

			if err == nil {
				defer voteRows.Close()
				for voteRows.Next() {
					var vType, voteVal, count int
					if scanErr := voteRows.Scan(&vType, &voteVal, &count); scanErr == nil {
						voterRole := "Unknown"
						switch vType {
						case 0:
							voterRole = "CC"
						case 1:
							voterRole = "DRep"
						case 2:
							voterRole = "SPO"
						}
						voteStr := "Unknown"
						switch voteVal {
						case 0:
							voteStr = "No"
						case 1:
							voteStr = "Yes"
						case 2:
							voteStr = "Abstain"
						}
						if m, ok := tallyMap[voterRole]; ok {
							m[voteStr] = count
						}
					}
				}
				if vErr := voteRows.Err(); vErr != nil {
					return nil, nil, fmt.Errorf("read vote tallies: %w", vErr)
				}
			}

			var sb strings.Builder
			fmt.Fprintf(&sb, "### Governance Action Proposal `%s#%d`\n\n", cleanHash, actionIdx)
			sb.WriteString("| Property | Value |\n")
			sb.WriteString("| :--- | :--- |\n")
			fmt.Fprintf(&sb, "| **Action Type** | %s (ID %d) |\n", govActionTypeName(actionType), actionType)
			fmt.Fprintf(&sb, "| **Lifecycle Status** | %s |\n", status)
			fmt.Fprintf(&sb, "| **Proposed Epoch** | %d |\n", proposedEpoch)
			fmt.Fprintf(&sb, "| **Expires Epoch** | %d |\n", expiresEpoch)
			fmt.Fprintf(&sb, "| **Proposal Deposit** | %d Lovelace (%.6f ADA) |\n", deposit, float64(deposit)/1000000.0)
			fmt.Fprintf(&sb, "| **Metadata Anchor URL** | %s |\n", urlStr)
			fmt.Fprintf(&sb, "| **Metadata Anchor Hash** | `%s` |\n\n", anchorHashHex)

			sb.WriteString("#### CIP-1694 Voting Procedure Breakdown\n\n")
			sb.WriteString("| Voter Role | Yes | No | Abstain |\n")
			sb.WriteString("| :--- | :--- | :--- | :--- |\n")
			fmt.Fprintf(&sb, "| **Constitutional Committee (CC)** | %d | %d | %d |\n", tallyMap["CC"]["Yes"], tallyMap["CC"]["No"], tallyMap["CC"]["Abstain"])
			fmt.Fprintf(&sb, "| **Delegated Representatives (DReps)** | %d | %d | %d |\n", tallyMap["DRep"]["Yes"], tallyMap["DRep"]["No"], tallyMap["DRep"]["Abstain"])
			fmt.Fprintf(&sb, "| **Stake Pool Operators (SPOs)** | %d | %d | %d |\n", tallyMap["SPO"]["Yes"], tallyMap["SPO"]["No"], tallyMap["SPO"]["Abstain"])

			return &mcp.CallToolResult{
				Content: []mcp.Content{
					&mcp.TextContent{Text: sb.String()},
				},
			}, nil, nil
		}

		// Listing mode
		limit := input.Limit
		if limit <= 0 {
			limit = 10
		} else if limit > 50 {
			limit = 50
		}

		statusFilter := strings.ToLower(strings.TrimSpace(input.Status))
		whereClause := "1=1"
		switch statusFilter {
		case "active", "":
			whereClause = "enacted_epoch IS NULL AND ratified_epoch IS NULL AND expired_epoch IS NULL"
		case "ratified":
			whereClause = "ratified_epoch IS NOT NULL"
		case "enacted":
			whereClause = "enacted_epoch IS NOT NULL"
		case "expired":
			whereClause = "expired_epoch IS NOT NULL"
		case "all":
			whereClause = "1=1"
		}

		query := fmt.Sprintf(`
			SELECT id, tx_hash, action_index, action_type, proposed_epoch, expires_epoch,
			       enacted_epoch, ratified_epoch, expired_epoch, deposit, anchor_url
			FROM governance_proposal
			WHERE %s
			ORDER BY id DESC
			LIMIT ?`, whereClause)

		rows, err := db.QueryContext(ctxTimeout, query, limit)
		if err != nil {
			return nil, nil, fmt.Errorf("list governance proposals: %w", err)
		}
		defer rows.Close()

		var sb strings.Builder
		sb.WriteString("### Conway CIP-1694 Governance Action Proposals\n\n")
		sb.WriteString("| Proposal Tx#Index | Action Type | Status | Proposed | Expires | Deposit (ADA) |\n")
		sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- |\n")

		foundCount := 0
		for rows.Next() {
			var (
				id                                                   int64
				txHashRaw                                            []byte
				actionIndex, actionType, proposedEpoch, expiresEpoch int
				enactedEpoch, ratifiedEpoch, expiredEpoch            sql.NullInt64
				deposit                                              uint64
				anchorURL                                            sql.NullString
			)
			if scanErr := rows.Scan(&id, &txHashRaw, &actionIndex, &actionType, &proposedEpoch, &expiresEpoch, &enactedEpoch, &ratifiedEpoch, &expiredEpoch, &deposit, &anchorURL); scanErr != nil {
				continue
			}
			foundCount++
			txHex := hex.EncodeToString(txHashRaw)
			status := "Active"
			if enactedEpoch.Valid {
				status = "Enacted"
			} else if ratifiedEpoch.Valid {
				status = "Ratified"
			} else if expiredEpoch.Valid {
				status = "Expired"
			}

			fmt.Fprintf(&sb, "| `%s#%d` | %s | %s | Epoch %d | Epoch %d | %.2f |\n",
				txHex, actionIndex, govActionTypeName(actionType), status, proposedEpoch, expiresEpoch, float64(deposit)/1000000.0,
			)
		}
		if rErr := rows.Err(); rErr != nil {
			return nil, nil, fmt.Errorf("read governance proposals: %w", rErr)
		}

		if foundCount == 0 {
			sb.WriteString("\n*No governance action proposals found matching the selected filter criteria.*\n")
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: sb.String()},
			},
		}, nil, nil
	})

	// 5. Tool: calculate_min_utxo
	mcp.AddTool(server, &mcp.Tool{
		Name:        "calculate_min_utxo",
		Description: "Calculate the exact CIP-55 minimum required Lovelace deposit for a Cardano UTxO output given address, multi-assets, inline datums, and reference scripts.",
	}, func(_ context.Context, _ *mcp.CallToolRequest, input CalculateMinUtxoParams) (*mcp.CallToolResult, any, error) {
		var coinsPerByte uint64
		if input.CoinsPerByte != nil && *input.CoinsPerByte > 0 {
			coinsPerByte = *input.CoinsPerByte
		} else if ls != nil {
			if pp := ls.GetCurrentPParamsForReporting(); pp != nil {
				switch p := pp.(type) {
				case *conway.ConwayProtocolParameters:
					coinsPerByte = p.AdaPerUtxoByte
				case *babbage.BabbageProtocolParameters:
					coinsPerByte = p.AdaPerUtxoByte
				case *alonzo.AlonzoProtocolParameters:
					coinsPerByte = p.AdaPerUtxoByte
				}
			}
		}

		if coinsPerByte == 0 {
			coinsPerByte = 4310 // Default Cardano Mainnet/Preview parameter
		}

		addrSize := 29 // Standard Shelley base / enterprise address byte size
		destAddress := "Standard Shelley (29 bytes)"
		if cleanAddr := strings.TrimSpace(input.Address); cleanAddr != "" {
			if addr, err := lcommon.NewAddress(cleanAddr); err == nil {
				if b, bErr := addr.Bytes(); bErr == nil {
					addrSize = len(b)
				}
				destAddress = addr.String()
			} else {
				return nil, nil, fmt.Errorf("invalid address format %q: %w", cleanAddr, err)
			}
		}

		// CIP-55 Calculation breakdown
		const utxoEntrySizeWithoutVal = 160
		const lovelaceValueSize = 8
		const outputFramingOverhead = 16

		assetSize := 0
		assetsCount := input.AssetsCount
		policiesCount := input.PoliciesCount
		if assetsCount > 0 && policiesCount == 0 {
			policiesCount = 1
		}
		if assetsCount > 0 {
			// 28 bytes per policy ID + ~20 bytes average for asset name and quantity + map framing
			assetSize = policiesCount*28 + assetsCount*20 + 8
		}

		datumSize := 0
		datumDesc := "None"
		if cleanDatum := strings.TrimSpace(input.InlineDatumHex); cleanDatum != "" {
			cleanDatum = strings.TrimPrefix(cleanDatum, "0x")
			datumBytes, err := hex.DecodeString(cleanDatum)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid inline datum hex: %w", err)
			}
			datumSize = len(datumBytes) + 4 // CIP-32 tag overhead
			datumDesc = fmt.Sprintf("Inline Datum (%d bytes serialized)", len(datumBytes))
		} else if input.HasDatum {
			datumSize = 34 // 32-byte datum hash + 2 bytes CBOR framing
			datumDesc = "Datum Hash (32 bytes)"
		}

		refScriptSize := 0
		refScriptDesc := "None"
		if cleanScript := strings.TrimSpace(input.RefScriptHex); cleanScript != "" {
			cleanScript = strings.TrimPrefix(cleanScript, "0x")
			scriptBytes, err := hex.DecodeString(cleanScript)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid reference script hex: %w", err)
			}
			refScriptSize = len(scriptBytes) + 4 // CIP-33 tag overhead
			refScriptDesc = fmt.Sprintf("Reference Script (%d bytes)", len(scriptBytes))
		}

		txOutSize := addrSize + lovelaceValueSize + assetSize + datumSize + refScriptSize + outputFramingOverhead
		totalSizedBytes := uint64(utxoEntrySizeWithoutVal + txOutSize)
		minLovelace := totalSizedBytes * coinsPerByte
		minAda := float64(minLovelace) / 1000000.0

		report := fmt.Sprintf("### CIP-55 Minimum UTxO Lovelace Calculation\n\n"+
			"| Parameter | Value | Contribution |\n"+
			"| :--- | :--- | :--- |\n"+
			"| **Destination Address** | `%s` | %d bytes |\n"+
			"| **Lovelace Storage** | Standard CBOR uint64 | %d bytes |\n"+
			"| **Multi-Asset Tokens** | %d assets across %d policies | %d bytes |\n"+
			"| **Datum Payload** | %s | %d bytes |\n"+
			"| **Reference Script** | %s | %d bytes |\n"+
			"| **Output Framing** | CBOR map / tuple overhead | %d bytes |\n"+
			"| **Fixed Protocol Overhead** | `utxoEntrySizeWithoutVal` | %d bytes |\n"+
			"| **Total Serialized Entry Size** | **%d bytes** | — |\n"+
			"| **Rate (`coins_per_utxo_byte`)** | **%d Lovelace/byte** | — |\n\n"+
			"**Required Minimum Deposit**:\n"+
			"- **%d Lovelace**\n"+
			"- **%.6f ADA**\n",
			destAddress, addrSize,
			lovelaceValueSize,
			assetsCount, policiesCount, assetSize,
			datumDesc, datumSize,
			refScriptDesc, refScriptSize,
			outputFramingOverhead,
			utxoEntrySizeWithoutVal,
			totalSizedBytes,
			coinsPerByte,
			minLovelace,
			minAda,
		)

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: report},
			},
		}, nil, nil
	})
}

// formatProtocolParameters formats protocol parameters into a structured markdown report.
func formatProtocolParameters(pparams lcommon.ProtocolParameters, epochLabel string, network string) string {
	var (
		eraName              = "Unknown"
		protoMajor           uint
		protoMinor           uint
		minFeeA              uint
		minFeeB              uint
		maxTxSize            uint
		maxBlockBodySize     uint
		keyDeposit           uint64
		poolDeposit          uint64
		coinsPerUtxoByte     uint64
		minPoolCost          uint64
		priceMem             float64
		priceStep            float64
		maxTxExMem           int64
		maxTxExSteps         int64
		collateralPercent    uint
		maxCollateralInputs  uint
		costModels           []string
		drepDeposit          uint64
		govActionDeposit     uint64
		refScriptCostPerByte *float64
	)

	switch pp := pparams.(type) {
	case *conway.ConwayProtocolParameters:
		eraName = "Conway"
		protoMajor = pp.ProtocolVersion.Major
		protoMinor = pp.ProtocolVersion.Minor
		minFeeA = pp.MinFeeA
		minFeeB = pp.MinFeeB
		maxTxSize = pp.MaxTxSize
		maxBlockBodySize = pp.MaxBlockBodySize
		keyDeposit = uint64(pp.KeyDeposit)
		poolDeposit = uint64(pp.PoolDeposit)
		coinsPerUtxoByte = pp.AdaPerUtxoByte
		minPoolCost = pp.MinPoolCost
		priceMem = ratToFloat64(pp.ExecutionCosts.MemPrice)
		priceStep = ratToFloat64(pp.ExecutionCosts.StepPrice)
		maxTxExMem = pp.MaxTxExUnits.Memory
		maxTxExSteps = pp.MaxTxExUnits.Steps
		collateralPercent = pp.CollateralPercentage
		maxCollateralInputs = pp.MaxCollateralInputs
		drepDeposit = pp.DRepDeposit
		govActionDeposit = pp.GovActionDeposit
		refScriptCostPerByte = ratPointerPtr(pp.MinFeeRefScriptCostPerByte)
		costModels = extractCostModelNames(pp.CostModels)
	case *babbage.BabbageProtocolParameters:
		eraName = "Babbage"
		protoMajor = pp.ProtocolMajor
		protoMinor = pp.ProtocolMinor
		minFeeA = pp.MinFeeA
		minFeeB = pp.MinFeeB
		maxTxSize = pp.MaxTxSize
		maxBlockBodySize = pp.MaxBlockBodySize
		keyDeposit = uint64(pp.KeyDeposit)
		poolDeposit = uint64(pp.PoolDeposit)
		coinsPerUtxoByte = pp.AdaPerUtxoByte
		minPoolCost = pp.MinPoolCost
		priceMem = ratToFloat64(pp.ExecutionCosts.MemPrice)
		priceStep = ratToFloat64(pp.ExecutionCosts.StepPrice)
		maxTxExMem = pp.MaxTxExUnits.Memory
		maxTxExSteps = pp.MaxTxExUnits.Steps
		collateralPercent = pp.CollateralPercentage
		maxCollateralInputs = pp.MaxCollateralInputs
		costModels = extractCostModelNames(pp.CostModels)
	case *alonzo.AlonzoProtocolParameters:
		eraName = "Alonzo"
		protoMajor = pp.ProtocolMajor
		protoMinor = pp.ProtocolMinor
		minFeeA = pp.MinFeeA
		minFeeB = pp.MinFeeB
		maxTxSize = pp.MaxTxSize
		maxBlockBodySize = pp.MaxBlockBodySize
		keyDeposit = uint64(pp.KeyDeposit)
		poolDeposit = uint64(pp.PoolDeposit)
		coinsPerUtxoByte = pp.AdaPerUtxoByte
		minPoolCost = pp.MinPoolCost
		priceMem = ratToFloat64(pp.ExecutionCosts.MemPrice)
		priceStep = ratToFloat64(pp.ExecutionCosts.StepPrice)
		maxTxExMem = pp.MaxTxExUnits.Memory
		maxTxExSteps = pp.MaxTxExUnits.Steps
		collateralPercent = pp.CollateralPercentage
		maxCollateralInputs = pp.MaxCollateralInputs
		costModels = extractCostModelNames(pp.CostModels)
	case *mary.MaryProtocolParameters:
		eraName = "Mary"
		protoMajor = pp.ProtocolMajor
		protoMinor = pp.ProtocolMinor
		minFeeA = pp.MinFeeA
		minFeeB = pp.MinFeeB
		maxTxSize = pp.MaxTxSize
		maxBlockBodySize = pp.MaxBlockBodySize
		keyDeposit = uint64(pp.KeyDeposit)
		poolDeposit = uint64(pp.PoolDeposit)
		minPoolCost = pp.MinPoolCost
	case *shelley.ShelleyProtocolParameters:
		eraName = "Shelley"
		protoMajor = pp.ProtocolMajor
		protoMinor = pp.ProtocolMinor
		minFeeA = pp.MinFeeA
		minFeeB = pp.MinFeeB
		maxTxSize = pp.MaxTxSize
		maxBlockBodySize = pp.MaxBlockBodySize
		keyDeposit = uint64(pp.KeyDeposit)
		poolDeposit = uint64(pp.PoolDeposit)
		coinsPerUtxoByte = uint64(pp.MinUtxoValue)
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "### Cardano Protocol Parameters (%s, Network: %s)\n\n", epochLabel, network)
	sb.WriteString("| Category | Parameter | Value |\n")
	sb.WriteString("| :--- | :--- | :--- |\n")
	fmt.Fprintf(&sb, "| **Era & Protocol** | Era Name | **%s** |\n", eraName)
	fmt.Fprintf(&sb, "| **Era & Protocol** | Protocol Version | Major %d, Minor %d |\n", protoMajor, protoMinor)
	fmt.Fprintf(&sb, "| **Transaction Fees** | `min_fee_a` (linear coefficient) | %d Lovelace/byte |\n", minFeeA)
	fmt.Fprintf(&sb, "| **Transaction Fees** | `min_fee_b` (constant base fee) | %d Lovelace (%.3f ADA) |\n", minFeeB, float64(minFeeB)/1000000.0)
	if refScriptCostPerByte != nil {
		fmt.Fprintf(&sb, "| **Transaction Fees** | `min_fee_ref_script_cost_per_byte` | %.4f Lovelace/byte |\n", *refScriptCostPerByte)
	}
	fmt.Fprintf(&sb, "| **Sizing Limits** | `max_tx_size` | %d bytes (%s) |\n", maxTxSize, formatBytes(int64(maxTxSize)))
	fmt.Fprintf(&sb, "| **Sizing Limits** | `max_block_body_size` | %d bytes (%s) |\n", maxBlockBodySize, formatBytes(int64(maxBlockBodySize)))
	fmt.Fprintf(&sb, "| **Deposits** | `coins_per_utxo_byte` (min UTxO cost) | %d Lovelace/byte |\n", coinsPerUtxoByte)
	fmt.Fprintf(&sb, "| **Deposits** | `key_deposit` (stake registration) | %d Lovelace (%.1f ADA) |\n", keyDeposit, float64(keyDeposit)/1000000.0)
	fmt.Fprintf(&sb, "| **Deposits** | `pool_deposit` (SPO registration) | %d Lovelace (%.1f ADA) |\n", poolDeposit, float64(poolDeposit)/1000000.0)
	fmt.Fprintf(&sb, "| **Staking** | `min_pool_cost` | %d Lovelace (%.1f ADA) |\n", minPoolCost, float64(minPoolCost)/1000000.0)

	if eraName == "Conway" {
		fmt.Fprintf(&sb, "| **Governance (CIP-1694)** | `drep_deposit` | %d Lovelace (%.1f ADA) |\n", drepDeposit, float64(drepDeposit)/1000000.0)
		fmt.Fprintf(&sb, "| **Governance (CIP-1694)** | `gov_action_deposit` | %d Lovelace (%.1f ADA) |\n", govActionDeposit, float64(govActionDeposit)/1000000.0)
	}

	if priceMem > 0 || priceStep > 0 {
		fmt.Fprintf(&sb, "| **Plutus Script Execution** | `price_mem` (Memory unit price) | %.6f Lovelace/unit |\n", priceMem)
		fmt.Fprintf(&sb, "| **Plutus Script Execution** | `price_step` (CPU step price) | %.8f Lovelace/step |\n", priceStep)
		fmt.Fprintf(&sb, "| **Plutus Script Execution** | `max_tx_ex_mem` (Tx Memory Budget) | %d units |\n", maxTxExMem)
		fmt.Fprintf(&sb, "| **Plutus Script Execution** | `max_tx_ex_steps` (Tx CPU Budget) | %d steps |\n", maxTxExSteps)
		fmt.Fprintf(&sb, "| **Plutus Script Execution** | `collateral_percentage` | %d%% |\n", collateralPercent)
		fmt.Fprintf(&sb, "| **Plutus Script Execution** | `max_collateral_inputs` | %d |\n", maxCollateralInputs)
	}

	if len(costModels) > 0 {
		fmt.Fprintf(&sb, "| **Plutus Script Execution** | Active Cost Models | %s |\n", strings.Join(costModels, ", "))
	}

	return sb.String()
}

func govActionTypeName(actionType int) string {
	switch actionType {
	case 0:
		return "ParameterChange"
	case 1:
		return "HardForkInitiation"
	case 2:
		return "TreasuryWithdrawals"
	case 3:
		return "NoConfidence"
	case 4:
		return "NewCommittee"
	case 5:
		return "NewConstitution"
	case 6:
		return "InfoAction"
	default:
		return fmt.Sprintf("ActionType(%d)", actionType)
	}
}

func ratToFloat64(r *cbor.Rat) float64 {
	if r == nil || r.Rat == nil {
		return 0
	}
	f, _ := r.Float64()
	return f
}

func ratPointerPtr(r *cbor.Rat) *float64 {
	if r == nil || r.Rat == nil {
		return nil
	}
	f, _ := r.Float64()
	return &f
}

func extractCostModelNames(models map[uint][]int64) []string {
	if len(models) == 0 {
		return nil
	}
	keys := make([]uint, 0, len(models))
	for k := range models {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	names := make([]string, 0, len(keys))
	for _, k := range keys {
		switch k {
		case 0:
			names = append(names, "PlutusV1")
		case 1:
			names = append(names, "PlutusV2")
		case 2:
			names = append(names, "PlutusV3")
		default:
			names = append(names, "PlutusV"+strconv.FormatUint(uint64(k)+1, 10))
		}
	}
	return names
}

func formatBytes(b int64) string {
	if b < 1024 {
		return strconv.FormatInt(b, 10) + " B"
	}
	kb := float64(b) / 1024.0
	if kb < 1024 {
		return fmt.Sprintf("%.1f KiB", kb)
	}
	mb := kb / 1024.0
	if mb < 1024 {
		return fmt.Sprintf("%.2f MiB", mb)
	}
	gb := mb / 1024.0
	return fmt.Sprintf("%.2f GiB", gb)
}

// mathMaxInt is used if needed.
var _ = math.MaxInt
