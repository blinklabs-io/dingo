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

package blockfrost

import "encoding/json"

// RootResponse is returned by GET /.
type RootResponse struct {
	URL     string `json:"url"`
	Version string `json:"version"`
}

// HealthResponse is returned by GET /health.
type HealthResponse struct {
	IsHealthy bool `json:"is_healthy"`
}

// BlockResponse represents a Blockfrost block object.
type BlockResponse struct {
	Time          int64   `json:"time"`
	Height        uint64  `json:"height"`
	Hash          string  `json:"hash"`
	Slot          uint64  `json:"slot"`
	Epoch         uint64  `json:"epoch"`
	EpochSlot     uint64  `json:"epoch_slot"`
	SlotLeader    string  `json:"slot_leader"`
	Size          uint64  `json:"size"`
	TxCount       int     `json:"tx_count"`
	Output        *string `json:"output"`
	Fees          *string `json:"fees"`
	BlockVRF      *string `json:"block_vrf"`
	OPCert        *string `json:"op_cert"`
	OPCertCounter *string `json:"op_cert_counter"`
	PreviousBlock string  `json:"previous_block"`
	NextBlock     *string `json:"next_block"`
	Confirmations uint64  `json:"confirmations"`
}

// EpochResponse represents a Blockfrost epoch object.
type EpochResponse struct {
	Epoch          uint64  `json:"epoch"`
	StartTime      int64   `json:"start_time"`
	EndTime        int64   `json:"end_time"`
	FirstBlockTime int64   `json:"first_block_time"`
	LastBlockTime  int64   `json:"last_block_time"`
	BlockCount     int     `json:"block_count"`
	TxCount        int     `json:"tx_count"`
	Output         string  `json:"output"`
	Fees           string  `json:"fees"`
	ActiveStake    *string `json:"active_stake"`
}

// ProtocolParamsResponse represents Blockfrost protocol
// parameters.
type ProtocolParamsResponse struct {
	Epoch              uint64  `json:"epoch"`
	MinFeeA            int     `json:"min_fee_a"`
	MinFeeB            int     `json:"min_fee_b"`
	MaxBlockSize       int     `json:"max_block_size"`
	MaxTxSize          int     `json:"max_tx_size"`
	MaxBlockHeaderSize int     `json:"max_block_header_size"`
	KeyDeposit         string  `json:"key_deposit"`
	PoolDeposit        string  `json:"pool_deposit"`
	EMax               int     `json:"e_max"`
	NOpt               int     `json:"n_opt"`
	A0                 float64 `json:"a0"`
	Rho                float64 `json:"rho"`
	Tau                float64 `json:"tau"`
	//nolint:tagliatelle
	DecentralisationParam float64         `json:"decentralisation_param"`
	ExtraEntropy          *map[string]any `json:"extra_entropy"`
	ProtocolMajorVer      int             `json:"protocol_major_ver"`
	ProtocolMinorVer      int             `json:"protocol_minor_ver"`
	MinUtxo               string          `json:"min_utxo"`
	MinPoolCost           string          `json:"min_pool_cost"`
	Nonce                 string          `json:"nonce"`
	CoinsPerUtxoSize      *string         `json:"coins_per_utxo_size"`
	CoinsPerUtxoWord      string          `json:"coins_per_utxo_word"`
	CostModels            *any            `json:"cost_models"`
	PriceMem              *float64        `json:"price_mem"`
	PriceStep             *float64        `json:"price_step"`
	MaxTxExMem            *string         `json:"max_tx_ex_mem"`
	MaxTxExSteps          *string         `json:"max_tx_ex_steps"`
	MaxBlockExMem         *string         `json:"max_block_ex_mem"`
	MaxBlockExSteps       *string         `json:"max_block_ex_steps"`
	MaxValSize            *string         `json:"max_val_size"`
	CollateralPercent     *int            `json:"collateral_percent"`
	MaxCollateralInputs   *int            `json:"max_collateral_inputs"`
}

// NetworkResponse represents Blockfrost network info.
type NetworkResponse struct {
	Supply NetworkSupply `json:"supply"`
	Stake  NetworkStake  `json:"stake"`
}

// NetworkSupply holds supply information.
type NetworkSupply struct {
	Max         string `json:"max"`
	Total       string `json:"total"`
	Circulating string `json:"circulating"`
	Locked      string `json:"locked"`
	Treasury    string `json:"treasury"`
	Reserves    string `json:"reserves"`
}

// NetworkStake holds stake information.
type NetworkStake struct {
	Live   string `json:"live"`
	Active string `json:"active"`
}

// AddressAmountResponse represents a Blockfrost address
// amount object.
type AddressAmountResponse struct {
	Unit     string `json:"unit"`
	Quantity string `json:"quantity"`
}

// AddressUTXOResponse represents a Blockfrost address
// UTxO object.
type AddressUTXOResponse struct {
	Address             string                  `json:"address"`
	TxHash              string                  `json:"tx_hash"`
	OutputIndex         int                     `json:"output_index"`
	Amount              []AddressAmountResponse `json:"amount"`
	Block               string                  `json:"block"`
	DataHash            *string                 `json:"data_hash"`
	InlineDatum         *string                 `json:"inline_datum"`
	ReferenceScriptHash *string                 `json:"reference_script_hash"`
}

// AddressTransactionResponse represents a Blockfrost
// address transaction object.
type AddressTransactionResponse struct {
	TxHash      string `json:"tx_hash"`
	TxIndex     int    `json:"tx_index"`
	BlockHeight uint64 `json:"block_height"`
	BlockTime   int    `json:"block_time"`
}

// AssetResponse represents a Blockfrost native asset
// object.
type AssetResponse struct {
	Asset                   string  `json:"asset"`
	PolicyID                string  `json:"policy_id"`
	AssetName               string  `json:"asset_name"`
	AssetNameASCII          string  `json:"asset_name_ascii"`
	Fingerprint             string  `json:"fingerprint"`
	Quantity                string  `json:"quantity"`
	InitialMintTxHash       string  `json:"initial_mint_tx_hash"`
	MintOrBurnCount         int     `json:"mint_or_burn_count"`
	OnchainMetadata         *any    `json:"onchain_metadata"`
	OnchainMetadataStandard *string `json:"onchain_metadata_standard"`
	OnchainMetadataExtra    *string `json:"onchain_metadata_extra"`
	Metadata                *any    `json:"metadata"`
}

// ErrorResponse represents a Blockfrost error response.
type ErrorResponse struct {
	StatusCode int    `json:"status_code"`
	Error      string `json:"error"`
	Message    string `json:"message"`
}

// PoolRelayResponse represents a stake pool relay.
type PoolRelayResponse struct {
	IPv4 *string `json:"ipv4"`
	IPv6 *string `json:"ipv6"`
	DNS  *string `json:"dns"`
	Port *int    `json:"port"`
}

// PoolExtendedResponse represents an extended stake pool
// list item.
type PoolExtendedResponse struct {
	PoolID         string              `json:"pool_id"`
	Hex            string              `json:"hex"`
	VrfKey         string              `json:"vrf_key"`
	ActiveStake    string              `json:"active_stake"`
	LiveStake      string              `json:"live_stake"`
	DeclaredPledge string              `json:"declared_pledge"`
	FixedCost      string              `json:"fixed_cost"`
	MarginCost     float64             `json:"margin_cost"`
	Relays         []PoolRelayResponse `json:"relays"`
}

// MetadataTransactionJSONResponse represents a Blockfrost
// metadata label transaction item with JSON content.
type MetadataTransactionJSONResponse struct {
	TxHash       string          `json:"tx_hash"`
	JSONMetadata json.RawMessage `json:"json_metadata"`
}

// MetadataTransactionCBORResponse represents a Blockfrost
// metadata label transaction item with CBOR content.
type MetadataTransactionCBORResponse struct {
	TxHash       string  `json:"tx_hash"`
	CborMetadata *string `json:"cbor_metadata"`
	Metadata     string  `json:"metadata"`
}
