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
	"errors"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/ledger"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func getPromptArg(req *mcp.GetPromptRequest, key string) string {
	if req == nil || req.Params == nil || req.Params.Arguments == nil {
		return ""
	}
	return strings.TrimSpace(req.Params.Arguments[key])
}

// RegisterPrompts registers standard, pre-engineered agent workflows with the MCP server.
func RegisterPrompts(
	server *mcp.Server,
	_ *sql.DB,
	_ *ledger.LedgerState,
	_ string,
) {
	// 1. diagnose_node_health
	server.AddPrompt(&mcp.Prompt{
		Name:        "diagnose_node_health",
		Title:       "Diagnose Cardano Node Sync & Health",
		Description: "Comprehensive diagnostic workflow to inspect Dingo node tip, sync latency, forge fences, and ledger readiness.",
		Arguments: []*mcp.PromptArgument{
			{
				Name:        "detailed",
				Title:       "Detailed Diagnostics",
				Description: "Set to 'true' to include detailed peer and storage diagnostics.",
				Required:    false,
			},
		},
	}, func(_ context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		detailed := getPromptArg(req, "detailed")
		detailInstruction := ""
		if strings.EqualFold(detailed, "true") {
			detailInstruction = "\n- Detailed mode enabled: also inspect database schemas via 'dingo://schema/tables' and peer connectivity."
		}

		promptText := fmt.Sprintf(
			"You are a Cardano node operations engineer diagnosing the health and synchronization state of this Dingo node.%s\n\n"+
				"Follow this diagnostic protocol:\n"+
				"1. Retrieve the node's current tip using 'get_cardano_tip'. Record the current slot, epoch, block height, and sync status.\n"+
				"2. Inspect the node identity and block production fingerprint using 'get_node_info' with topic='identity'. Verify that BlockHeaderProtocolMinor is 69.\n"+
				"3. Query the 'sync_state' table using 'sqlite_query' (SELECT * FROM sync_state) to inspect the local sync watermark and check for any active forge fence ('forge_fence:<pool_id>').\n"+
				"4. Read 'dingo://node/status' to review live operational metrics and storage stats.\n"+
				"5. Synthesize a structured diagnostic report with:\n"+
				"   - Sync status and lag behind the Cardano network\n"+
				"   - Current ledger slot and epoch progress\n"+
				"   - Block production readiness and forge fencing status\n"+
				"   - Storage engine health and recommendations.",
			detailInstruction,
		)

		return &mcp.GetPromptResult{
			Description: "Cardano Node Health & Sync Diagnosis Workflow",
			Messages: []*mcp.PromptMessage{
				{
					Role:    mcp.Role("user"),
					Content: &mcp.TextContent{Text: promptText},
				},
			},
		}, nil
	})

	// 2. simulate_and_diagnose_tx
	server.AddPrompt(&mcp.Prompt{
		Name:        "simulate_and_diagnose_tx",
		Title:       "Simulate and Diagnose Plutus Transaction",
		Description: "Pre-flight dry-run of a Cardano transaction CBOR to evaluate Plutus execution units, verify fees, check input UTxOs, and resolve datums.",
		Arguments: []*mcp.PromptArgument{
			{
				Name:        "tx_cbor",
				Title:       "Transaction CBOR",
				Description: "Hex or base64 encoded transaction CBOR string.",
				Required:    true,
			},
			{
				Name:        "purpose",
				Title:       "Transaction Purpose",
				Description: "Transaction intent or protocol context (e.g., 'swap', 'mint', 'governance', 'spend').",
				Required:    false,
			},
		},
	}, func(_ context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		txCBOR := getPromptArg(req, "tx_cbor")
		if txCBOR == "" {
			return nil, errors.New("missing required argument 'tx_cbor'")
		}
		purpose := getPromptArg(req, "purpose")
		purposeText := ""
		if purpose != "" {
			purposeText = "\nDeclared Intent / Context: " + purpose + "\n"
		}

		promptText := fmt.Sprintf(
			"You are a Cardano smart contract auditor and DevOps engineer performing a pre-flight simulation and diagnosis for a Cardano transaction.\n\n"+
				"Transaction CBOR:\n%s\n%s\n"+
				"Execute the following inspection protocol:\n"+
				"1. Execute 'evaluate_tx' with the provided transaction CBOR to dry-run all Plutus redeemers through Dingo's pure-Go Plutus CEK interpreter.\n"+
				"2. Analyze the evaluation result:\n"+
				"   - If evaluation succeeds: Record the CPU steps, memory units, and total estimated execution fee. Verify that each redeemer's consumption is within the network's max execution budget per transaction.\n"+
				"   - If evaluation fails: Inspect the exact error message. If the failure indicates a missing datum, determine the missing hash and check 'resolve_datum_or_script'. Note that CIP-32 inline datums are stored directly on the input UTxO.\n"+
				"3. For each input referenced in the transaction, verify that the UTxO is currently unspent by querying 'get_utxos' or checking 'deleted_slot = 0' via 'sqlite_query'.\n"+
				"4. Check if the transaction contains required collateral inputs adequate for the calculated execution fee.\n"+
				"5. Synthesize a structured report detailing:\n"+
				"   - Plutus redeemer execution units (CPU steps & Memory) and budget utilization\n"+
				"   - Fee sufficiency and collateral validation\n"+
				"   - Identified Phase-1 or Phase-2 validation issues with concrete, actionable remediation steps.",
			txCBOR,
			purposeText,
		)

		return &mcp.GetPromptResult{
			Description: "Plutus Transaction Simulation & Diagnosis Workflow",
			Messages: []*mcp.PromptMessage{
				{
					Role:    mcp.Role("user"),
					Content: &mcp.TextContent{Text: promptText},
				},
			},
		}, nil
	})

	// 3. audit_pool_rewards
	server.AddPrompt(&mcp.Prompt{
		Name:        "audit_pool_rewards",
		Title:       "Audit Stake Pool Performance & Rewards",
		Description: "In-depth audit workflow for a stake pool: analyze historical block minting, apparent performance ratio, active stake, and reward distribution.",
		Arguments: []*mcp.PromptArgument{
			{
				Name:        "pool_id",
				Title:       "Pool ID",
				Description: "Bech32 pool ID (pool1...) or 56-character hex pool hash.",
				Required:    true,
			},
			{
				Name:        "epoch",
				Title:       "Epoch Number",
				Description: "Target epoch number to audit (leave empty for recent epochs).",
				Required:    false,
			},
		},
	}, func(_ context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		poolID := getPromptArg(req, "pool_id")
		if poolID == "" {
			return nil, errors.New("missing required argument 'pool_id'")
		}
		epoch := getPromptArg(req, "epoch")
		epochParam := ""
		if epoch != "" {
			epochParam = " in epoch " + epoch
		}

		promptText := fmt.Sprintf(
			"You are a Cardano staking analyst auditing the performance and reward distribution of stake pool '%s'%s.\n\n"+
				"Execute the following audit workflow:\n"+
				"1. Call 'get_pool_performance' with pool_id='%s' (and epoch='%s' if specified) to retrieve historical block production, apparent performance ratio, declared margin, fixed cost, and reward distribution.\n"+
				"2. Query the 'account' table via 'sqlite_query' (SELECT count(*), sum(controlled_amount) FROM account WHERE pool_id = '%s') to check the pool's current active delegators and total stake.\n"+
				"3. Compare actual blocks minted against the pool's apparent performance ratio and active stake proportion.\n"+
				"4. Audit fee deductions: verify that the fixed cost and margin are applied according to protocol rules and that leader vs member reward splits are accurate.\n"+
				"5. Synthesize an SPO performance dossier with:\n"+
				"   - Block production track record and efficiency ratio\n"+
				"   - Delegated stake trends and delegator count\n"+
				"   - Pool profitability, declared fees, and delegator net return analysis.",
			poolID,
			epochParam,
			poolID,
			epoch,
			poolID,
		)

		return &mcp.GetPromptResult{
			Description: "Stake Pool Performance Audit for " + poolID,
			Messages: []*mcp.PromptMessage{
				{
					Role:    mcp.Role("user"),
					Content: &mcp.TextContent{Text: promptText},
				},
			},
		}, nil
	})

	// 4. track_asset_portfolio
	server.AddPrompt(&mcp.Prompt{
		Name:        "track_asset_portfolio",
		Title:       "Track Cardano Native Asset & Circulation",
		Description: "Kupo-style asset intelligence workflow: inspect live unspent outputs, circulating supply, unique wallet holders, Token Registry metadata, and CIP-25 NFT attributes.",
		Arguments: []*mcp.PromptArgument{
			{
				Name:        "policy_id",
				Title:       "Policy ID",
				Description: "56-character hex minting policy ID.",
				Required:    true,
			},
			{
				Name:        "asset_name",
				Title:       "Asset Name",
				Description: "ASCII asset name (e.g., 'HOSKY') or hex-encoded asset name.",
				Required:    false,
			},
		},
	}, func(_ context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		policyID := getPromptArg(req, "policy_id")
		if policyID == "" {
			return nil, errors.New("missing required argument 'policy_id'")
		}
		assetName := getPromptArg(req, "asset_name")
		assetText := ""
		if assetName != "" {
			assetText = fmt.Sprintf(" [asset name: '%s']", assetName)
		}

		promptText := fmt.Sprintf(
			"You are a Cardano on-chain financial analyst compiling an intelligence report for the native asset under policy '%s'%s.\n\n"+
				"Follow this investigative playbook:\n"+
				"1. Call 'get_asset_info' with policy_id='%s' and asset_name='%s' to retrieve metadata from the Cardano Token Registry (name, ticker, decimals, description, website) and CIP-25 NFT metadata from transaction label 721.\n"+
				"2. Call 'get_utxos_by_asset' with policy_id='%s' and asset_name='%s' to inspect all live unspent outputs carrying this asset.\n"+
				"3. Analyze the distribution of unspent outputs:\n"+
				"   - Calculate the total live circulating supply across all active UTxOs.\n"+
				"   - Count the total number of unique holding addresses and identify concentration (top holders).\n"+
				"   - Verify if any outputs are locked at script addresses or carry datum hashes.\n"+
				"4. Synthesize an Asset Dossier containing:\n"+
				"   - Verified asset identity, ticker, and decimals\n"+
				"   - Circulating supply, mint/burn history, and holder distribution metrics\n"+
				"   - Liquidity distribution across personal wallets vs smart contract pools.",
			policyID,
			assetText,
			policyID,
			assetName,
			policyID,
			assetName,
		)

		return &mcp.GetPromptResult{
			Description: "Native Asset Intelligence for policy " + policyID,
			Messages: []*mcp.PromptMessage{
				{
					Role:    mcp.Role("user"),
					Content: &mcp.TextContent{Text: promptText},
				},
			},
		}, nil
	})

	// 5. conway_governance_brief
	server.AddPrompt(&mcp.Prompt{
		Name:        "conway_governance_brief",
		Title:       "Conway CIP-1694 Governance & Constitutional Audit",
		Description: "Governance workflow to inspect active constitutional parameters, Constitutional Committee voting status, and DRep delegation.",
		Arguments: []*mcp.PromptArgument{
			{
				Name:        "drep_id",
				Title:       "DRep ID",
				Description: "Bech32 (drep1...) or 56-hex DRep credential to audit.",
				Required:    false,
			},
			{
				Name:        "stake_address",
				Title:       "Stake Address",
				Description: "Bech32 stake address to check DRep voting delegation.",
				Required:    false,
			},
		},
	}, func(_ context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		drepID := getPromptArg(req, "drep_id")
		stakeAddress := getPromptArg(req, "stake_address")

		contextNotes := ""
		if drepID != "" {
			contextNotes += "\n- Target DRep: " + drepID
		}
		if stakeAddress != "" {
			contextNotes += "\n- Target Stake Address: " + stakeAddress
		}

		promptText := fmt.Sprintf(
			"You are a Cardano governance delegate auditing on-chain governance state in the Conway era under CIP-1694.%s\n\n"+
				"Execute the following governance inspection protocol:\n"+
				"1. Call 'get_governance_state' (with drep_credential='%s' if specified) to retrieve the active Constitution anchor hash, script hash, Constitutional Committee (CC) member list, and committee voting threshold quorum.\n"+
				"2. If a stake address is provided ('%s'), call 'get_account' to determine the current DRep voting delegation, delegated pool, and active voting power.\n"+
				"3. If a DRep credential was specified, audit that DRep's status, registration anchor URL, and active/expired state.\n"+
				"4. Query active governance action proposals via 'sqlite_query' (SELECT * FROM gov_action_proposal WHERE expiration > (SELECT max(epoch) FROM blocks) LIMIT 10) to review pending on-chain governance actions.\n"+
				"5. Synthesize a comprehensive governance briefing:\n"+
				"   - Constitutional state and Committee threshold compliance\n"+
				"   - DRep delegation status and voting power representation\n"+
				"   - Active governance proposals and pending voting deadlines.",
			contextNotes,
			drepID,
			stakeAddress,
		)

		return &mcp.GetPromptResult{
			Description: "Conway CIP-1694 Governance & Constitutional Brief",
			Messages: []*mcp.PromptMessage{
				{
					Role:    mcp.Role("user"),
					Content: &mcp.TextContent{Text: promptText},
				},
			},
		}, nil
	})

	// 6. investigate_address
	server.AddPrompt(&mcp.Prompt{
		Name:        "investigate_address",
		Title:       "Investigate Address & EUTxO State",
		Description: "Deep-dive EUTxO state analysis of a Cardano address: inspect unspent outputs, lovelace balance, native assets, attached datums, and staking status.",
		Arguments: []*mcp.PromptArgument{
			{
				Name:        "address",
				Title:       "Cardano Address",
				Description: "Bech32 address (addr... or addr_test...) or 56-character payment credential.",
				Required:    true,
			},
		},
	}, func(_ context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		address := getPromptArg(req, "address")
		if address == "" {
			return nil, errors.New("missing required argument 'address'")
		}

		promptText := fmt.Sprintf(
			"You are a Cardano blockchain forensic investigator analyzing the EUTxO state of address '%s'.\n\n"+
				"Execute the following investigation workflow:\n"+
				"1. Call 'get_utxos' with address_or_credential='%s' to retrieve all live unspent transaction outputs.\n"+
				"2. Calculate the total liquid lovelace balance (and formatted ADA) across all unspent outputs.\n"+
				"3. Enumerate all Cardano native assets and NFTs held at the address, noting distinct policy IDs and quantities.\n"+
				"4. For any UTxO that carries a datum hash or inline datum, call 'resolve_datum_or_script' to inspect and decode the Plutus data.\n"+
				"5. If the address contains a staking credential, call 'get_account' to inspect the associated stake key, pool delegation, available rewards, and DRep delegation.\n"+
				"6. Synthesize an EUTxO State Dossier:\n"+
				"   - Total spendable ADA and multi-asset token portfolio\n"+
				"   - UTxO fragmentation and coin selection considerations\n"+
				"   - Staking delegation, pool identity, and unclaimed rewards\n"+
				"   - Active smart contract datums or script interactions.",
			address,
			address,
		)

		return &mcp.GetPromptResult{
			Description: "EUTxO State Investigation for " + address,
			Messages: []*mcp.PromptMessage{
				{
					Role:    mcp.Role("user"),
					Content: &mcp.TextContent{Text: promptText},
				},
			},
		}, nil
	})
}
