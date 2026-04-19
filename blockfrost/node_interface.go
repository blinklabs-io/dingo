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

// BlockfrostNode is the interface that the Blockfrost API
// server uses to query the node for blockchain data. This
// decouples the HTTP server from the concrete Node struct
// and enables testing with mock implementations.
type BlockfrostNode interface {
	// ChainTip returns the current chain tip info.
	ChainTip() (ChainTipInfo, error)

	// LatestBlock returns information about the latest
	// block on the chain.
	LatestBlock() (BlockInfo, error)

	// LatestBlockTxHashes returns the transaction hashes
	// for the latest block.
	LatestBlockTxHashes() ([]string, error)

	// CurrentEpoch returns information about the current
	// epoch.
	CurrentEpoch() (EpochInfo, error)

	// CurrentProtocolParams returns the current protocol
	// parameters.
	CurrentProtocolParams() (ProtocolParamsInfo, error)

	// EpochProtocolParams returns protocol parameters for a
	// specific epoch.
	EpochProtocolParams(epoch uint64) (ProtocolParamsInfo, error)

	// PoolsExtended returns the current active pools with
	// extended details.
	PoolsExtended() ([]PoolExtendedInfo, error)

	// AddressUTXOs returns the paginated current UTxOs for
	// an address along with the total number of matching
	// results before pagination.
	AddressUTXOs(
		address string,
		params PaginationParams,
	) ([]AddressUTXOInfo, int, error)

	// AddressTransactions returns the paginated transaction
	// history for an address along with the total number of
	// matching results before pagination.
	AddressTransactions(
		address string,
		params PaginationParams,
	) ([]AddressTransactionInfo, int, error)

	// MetadataTransactions returns the paginated transactions
	// containing the requested metadata label, with JSON values.
	MetadataTransactions(
		label uint64,
		params PaginationParams,
	) ([]MetadataTransactionJSONInfo, int, error)

	// MetadataTransactionsCBOR returns the paginated transactions
	// containing the requested metadata label, with CBOR values.
	MetadataTransactionsCBOR(
		label uint64,
		params PaginationParams,
	) ([]MetadataTransactionCBORInfo, int, error)

	// Asset returns native asset information for a
	// concatenated hex asset ID ({policy_id}{asset_name}).
	Asset(
		policyID string,
		assetName []byte,
	) (AssetInfo, error)
}

// ChainTipInfo holds chain tip data needed by the API.
type ChainTipInfo struct {
	BlockHash   string
	Slot        uint64
	BlockNumber uint64
}

// BlockInfo holds block data needed by the API.
type BlockInfo struct {
	Hash          string
	Slot          uint64
	Epoch         uint64
	EpochSlot     uint64
	Height        uint64
	Time          int64
	Size          uint64
	TxCount       int
	SlotLeader    string
	PreviousBlock string
	Confirmations uint64
}

// EpochInfo holds epoch data needed by the API.
type EpochInfo struct {
	Epoch          uint64
	StartTime      int64
	EndTime        int64
	FirstBlockTime int64
	LastBlockTime  int64
	BlockCount     int
	TxCount        int
}

// ProtocolParamsInfo holds protocol parameter data needed
// by the API.
type ProtocolParamsInfo struct {
	Epoch               uint64
	MinFeeA             int
	MinFeeB             int
	MaxBlockSize        int
	MaxTxSize           int
	MaxBlockHeaderSize  int
	KeyDeposit          string
	PoolDeposit         string
	EMax                int
	NOpt                int
	A0                  float64
	Rho                 float64
	Tau                 float64
	ProtocolMajorVer    int
	ProtocolMinorVer    int
	MinPoolCost         string
	CoinsPerUtxoSize    string
	PriceMem            float64
	PriceStep           float64
	MaxTxExMem          string
	MaxTxExSteps        string
	MaxBlockExMem       string
	MaxBlockExSteps     string
	MaxValSize          string
	CollateralPercent   int
	MaxCollateralInputs int
}

// PoolExtendedInfo holds pool data needed by the
// /pools/extended endpoint.
type PoolExtendedInfo struct {
	PoolID         string
	Hex            string
	VrfKey         string
	ActiveStake    string
	LiveStake      string
	DeclaredPledge string
	FixedCost      string
	MarginCost     float64
	Relays         []PoolRelayInfo
}

// PoolRelayInfo holds relay data for pool responses.
type PoolRelayInfo struct {
	IPv4 string
	IPv6 string
	DNS  string
	Port *int
}

// AddressAmountInfo holds amount data needed by address
// UTxO responses.
type AddressAmountInfo struct {
	Unit     string
	Quantity string
}

// AddressUTXOInfo holds address UTxO data needed by the
// API.
type AddressUTXOInfo struct {
	Address             string
	TxHash              string
	OutputIndex         uint32
	Amount              []AddressAmountInfo
	Block               string
	DataHash            *string
	InlineDatum         *string
	ReferenceScriptHash *string
}

// AddressTransactionInfo holds address transaction data
// needed by the API.
type AddressTransactionInfo struct {
	TxHash      string
	TxIndex     uint32
	BlockHeight uint64
	BlockTime   int64
}

// MetadataTransactionJSONInfo holds metadata label query
// data for the JSON endpoint.
type MetadataTransactionJSONInfo struct {
	TxHash       string
	JSONMetadata json.RawMessage
}

// MetadataTransactionCBORInfo holds metadata label query
// data for the CBOR endpoint.
type MetadataTransactionCBORInfo struct {
	TxHash   string
	Metadata string
}

// AssetInfo holds native asset data needed by the API.
type AssetInfo struct {
	Asset             string
	PolicyID          string
	AssetName         string
	AssetNameASCII    string
	Fingerprint       string
	Quantity          string
	InitialMintTxHash string
	MintOrBurnCount   int
	OnchainMetadata   *any
}
