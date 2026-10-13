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
