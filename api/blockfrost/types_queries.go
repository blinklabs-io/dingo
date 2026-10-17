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

package blockfrost

import "encoding/json"

// BlockTransactionCBORResponse is one entry of the block transaction CBOR
// listings.
type BlockTransactionCBORResponse struct {
	TxHash string `json:"tx_hash"`
	CBOR   string `json:"cbor"`
}

// BlockAddressTransactionResponse names one transaction touching an address.
type BlockAddressTransactionResponse struct {
	TxHash string `json:"tx_hash"`
}

// BlockAddressResponse is one entry of the addresses affected by a block.
type BlockAddressResponse struct {
	Address      string                            `json:"address"`
	Transactions []BlockAddressTransactionResponse `json:"transactions"`
}

// AddressExtendedAmountResponse is one balance entry of an extended address.
type AddressExtendedAmountResponse struct {
	Unit                  string `json:"unit"`
	Quantity              string `json:"quantity"`
	Decimals              *int   `json:"decimals"`
	HasNFTOnchainMetadata bool   `json:"has_nft_onchain_metadata"`
}

// AddressExtendedResponse represents a Blockfrost extended address object.
type AddressExtendedResponse struct {
	Address      string                          `json:"address"`
	Amount       []AddressExtendedAmountResponse `json:"amount"`
	StakeAddress *string                         `json:"stake_address"`
	Type         string                          `json:"type"`
	Script       bool                            `json:"script"`
}

// AddressTotalResponse represents the Blockfrost address totals object.
type AddressTotalResponse struct {
	Address     string                  `json:"address"`
	ReceivedSum []AddressAmountResponse `json:"received_sum"`
	SentSum     []AddressAmountResponse `json:"sent_sum"`
	TxCount     int                     `json:"tx_count"`
}

// AssetListItemResponse is one entry of an asset listing.
type AssetListItemResponse struct {
	Asset    string `json:"asset"`
	Quantity string `json:"quantity"`
}

// AssetHistoryResponse is one mint or burn event of an asset.
type AssetHistoryResponse struct {
	TxHash string `json:"tx_hash"`
	Action string `json:"action"`
	Amount string `json:"amount"`
}

// AssetUTXOResponse is one current UTxO holding an asset. InlineDatumJSON is
// null for an output without an inline datum.
type AssetUTXOResponse struct {
	Address             string                  `json:"address"`
	TxHash              string                  `json:"tx_hash"`
	OutputIndex         int                     `json:"output_index"`
	Amount              []AddressAmountResponse `json:"amount"`
	Block               string                  `json:"block"`
	BlockHeight         uint64                  `json:"block_height"`
	BlockTime           int64                   `json:"block_time"`
	DataHash            *string                 `json:"data_hash"`
	InlineDatum         *string                 `json:"inline_datum"`
	InlineDatumJSON     json.RawMessage         `json:"inline_datum_json"`
	ReferenceScriptHash *string                 `json:"reference_script_hash"`
}

// ScriptListItemResponse is one entry of the script listing.
type ScriptListItemResponse struct {
	ScriptHash string `json:"script_hash"`
}

// ScriptResponse represents a Blockfrost script object. SerialisedSize is
// null for a native script.
type ScriptResponse struct {
	ScriptHash     string `json:"script_hash"`
	Type           string `json:"type"`
	SerialisedSize *int   `json:"serialised_size"`
}

// ScriptJSONResponse carries the JSON form of a native script, or null for a
// Plutus script.
type ScriptJSONResponse struct {
	JSON json.RawMessage `json:"json"`
}

// ScriptCBORResponse carries the CBOR of a Plutus script, or null for a native
// script.
type ScriptCBORResponse struct {
	CBOR *string `json:"cbor"`
}

// ScriptRedeemerResponse is one redeemer that executed a script.
type ScriptRedeemerResponse struct {
	TxHash           string  `json:"tx_hash"`
	TxIndex          int     `json:"tx_index"`
	Purpose          string  `json:"purpose"`
	RedeemerDataHash string  `json:"redeemer_data_hash"`
	DatumHash        *string `json:"datum_hash"`
	UnitMem          string  `json:"unit_mem"`
	UnitSteps        string  `json:"unit_steps"`
	Fee              string  `json:"fee"`
}

// ScriptUTXOResponse is one UTxO carrying a script as a reference script.
type ScriptUTXOResponse struct {
	Address             string                  `json:"address"`
	TxHash              string                  `json:"tx_hash"`
	OutputIndex         int                     `json:"output_index"`
	Amount              []AddressAmountResponse `json:"amount"`
	Block               string                  `json:"block"`
	DataHash            *string                 `json:"data_hash"`
	InlineDatum         *string                 `json:"inline_datum"`
	ReferenceScriptHash string                  `json:"reference_script_hash"`
}

// DatumResponse carries the detailed-schema JSON of a datum.
type DatumResponse struct {
	JSONValue json.RawMessage `json:"json_value"`
}

// DatumCBORResponse carries the hex CBOR of a datum.
type DatumCBORResponse struct {
	CBOR string `json:"cbor"`
}

// MempoolHashResponse is one entry of a mempool listing.
type MempoolHashResponse struct {
	TxHash string `json:"tx_hash"`
}

// MempoolTransactionSummaryResponse holds the fields a transaction has before
// it is in a block.
type MempoolTransactionSummaryResponse struct {
	Hash               string                  `json:"hash"`
	OutputAmount       []AddressAmountResponse `json:"output_amount"`
	Fees               string                  `json:"fees"`
	Deposit            string                  `json:"deposit"`
	Size               int                     `json:"size"`
	InvalidBefore      *string                 `json:"invalid_before"`
	InvalidHereafter   *string                 `json:"invalid_hereafter"`
	UtxoCount          int                     `json:"utxo_count"`
	WithdrawalCount    int                     `json:"withdrawal_count"`
	MirCertCount       int                     `json:"mir_cert_count"`
	DelegationCount    int                     `json:"delegation_count"`
	StakeCertCount     int                     `json:"stake_cert_count"`
	PoolUpdateCount    int                     `json:"pool_update_count"`
	PoolRetireCount    int                     `json:"pool_retire_count"`
	AssetMintBurnCount int                     `json:"asset_mint_or_burn_count"`
	RedeemerCount      int                     `json:"redeemer_count"`
	ValidContract      bool                    `json:"valid_contract"`
}

// MempoolInputResponse is one input of a mempool transaction. The address is
// omitted when the input does not resolve to a confirmed output.
type MempoolInputResponse struct {
	Address     *string `json:"address,omitempty"`
	TxHash      string  `json:"tx_hash"`
	OutputIndex int     `json:"output_index"`
	Collateral  bool    `json:"collateral"`
	Reference   *bool   `json:"reference,omitempty"`
}

// MempoolOutputResponse is one output of a mempool transaction.
type MempoolOutputResponse struct {
	Address             string                  `json:"address"`
	Amount              []AddressAmountResponse `json:"amount"`
	OutputIndex         int                     `json:"output_index"`
	DataHash            *string                 `json:"data_hash"`
	InlineDatum         *string                 `json:"inline_datum"`
	Collateral          bool                    `json:"collateral"`
	ReferenceScriptHash *string                 `json:"reference_script_hash"`
}

// MempoolRedeemerResponse is one redeemer of a mempool transaction.
type MempoolRedeemerResponse struct {
	TxIndex   int    `json:"tx_index"`
	Purpose   string `json:"purpose"`
	UnitMem   string `json:"unit_mem"`
	UnitSteps string `json:"unit_steps"`
}

// MempoolTransactionResponse represents a transaction in the mempool.
type MempoolTransactionResponse struct {
	Tx        MempoolTransactionSummaryResponse `json:"tx"`
	Inputs    []MempoolInputResponse            `json:"inputs"`
	Outputs   []MempoolOutputResponse           `json:"outputs"`
	Redeemers []MempoolRedeemerResponse         `json:"redeemers,omitempty"`
}
