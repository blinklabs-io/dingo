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

import (
	"context"
	"encoding/json"
)

// BlockQueryNode is the block navigation and block content surface of
// BlockfrostNode. Block identifiers are 64-character hex hashes or decimal
// heights, as for BlockByHashOrNumber.
type BlockQueryNode interface {
	// BlockNext returns a page of the blocks following the identified
	// block and the number of heights between it and the tip. Page one holds
	// the nearest blocks; heights ascend within a page unless params.Order
	// is descending.
	BlockNext(
		ctx context.Context,
		id string,
		params PaginationParams,
	) ([]BlockInfo, int, error)

	// BlockPrevious returns a page of the blocks preceding the identified
	// block and the number of heights below it. Page one holds the nearest
	// blocks; heights ascend within a page unless params.Order is
	// descending.
	BlockPrevious(
		ctx context.Context,
		id string,
		params PaginationParams,
	) ([]BlockInfo, int, error)

	// BlockBySlot returns the block produced in an absolute slot.
	BlockBySlot(ctx context.Context, slot uint64) (BlockInfo, error)

	// BlockByEpochSlot returns the block produced in a slot counted from
	// the start of an epoch.
	BlockByEpochSlot(
		ctx context.Context,
		epoch uint64,
		epochSlot uint64,
	) (BlockInfo, error)

	// BlockTransactionHashes returns every transaction hash in the
	// identified block, in block order.
	BlockTransactionHashes(
		ctx context.Context,
		id string,
	) ([]string, error)

	// BlockTransactionCBOR returns every transaction in the identified block
	// with its raw CBOR, in block order.
	BlockTransactionCBOR(
		ctx context.Context,
		id string,
	) ([]BlockTransactionCBORInfo, error)

	// LatestBlockTransactionCBOR returns every transaction of the chain tip
	// block with its raw CBOR, in block order.
	LatestBlockTransactionCBOR(
		ctx context.Context,
	) ([]BlockTransactionCBORInfo, error)

	// BlockAddresses returns every address affected by the identified
	// block together with the transactions that touch it, ordered by
	// address.
	BlockAddresses(
		ctx context.Context,
		id string,
	) ([]BlockAddressInfo, error)
}

// AddressQueryNode is the address aggregate surface of BlockfrostNode.
type AddressQueryNode interface {
	// AddressExtended returns the address summary with per-asset decimals
	// and on-chain metadata flags.
	AddressExtended(
		ctx context.Context,
		address string,
	) (AddressExtendedInfo, error)

	// AddressTotal returns the lifetime received and sent sums and the
	// transaction count of an address.
	AddressTotal(
		ctx context.Context,
		address string,
	) (AddressTotalInfo, error)

	// AddressAssetUTXOs returns the paginated current UTxOs of an address
	// that hold the given asset, along with the total number of matching
	// results before pagination.
	AddressAssetUTXOs(
		ctx context.Context,
		address string,
		policyID string,
		assetName []byte,
		params PaginationParams,
	) ([]AddressUTXOInfo, int, error)
}

// AssetQueryNode is the native asset listing surface of BlockfrostNode.
type AssetQueryNode interface {
	// Assets returns the paginated list of every asset with a recorded
	// mint or burn, oldest first, along with the total before pagination.
	Assets(
		ctx context.Context,
		params PaginationParams,
	) ([]AssetListItemInfo, int, error)

	// AssetsByPolicy returns the paginated assets minted under a policy,
	// along with the total before pagination.
	AssetsByPolicy(
		ctx context.Context,
		policyID string,
		params PaginationParams,
	) ([]AssetListItemInfo, int, error)

	// AssetHistory returns the paginated mint and burn events of an asset,
	// along with the total before pagination.
	AssetHistory(
		ctx context.Context,
		policyID string,
		assetName []byte,
		params PaginationParams,
	) ([]AssetHistoryItemInfo, int, error)

	// AssetTransactions returns the paginated transactions that mint,
	// burn, create or spend an output holding the asset, optionally
	// restricted to an inclusive from/to block-range position.
	AssetTransactions(
		ctx context.Context,
		policyID string,
		assetName []byte,
		params TransactionRangeParams,
	) ([]AssetTransactionInfo, int, error)

	// AssetUTXOs returns the paginated current UTxOs holding the asset,
	// optionally restricted to an inclusive from/to block-range position of
	// the producing transaction.
	AssetUTXOs(
		ctx context.Context,
		policyID string,
		assetName []byte,
		params TransactionRangeParams,
	) ([]AssetUTXOInfo, int, error)
}

// ScriptQueryNode is the script and datum surface of BlockfrostNode.
type ScriptQueryNode interface {
	// Scripts returns the paginated hashes of every known script, along
	// with the total before pagination.
	Scripts(
		ctx context.Context,
		params PaginationParams,
	) ([]string, int, error)

	// Script returns the type and serialised size of a script.
	Script(ctx context.Context, hash []byte) (ScriptInfo, error)

	// ScriptJSON returns the JSON form of a native script, or nil for a
	// Plutus script.
	ScriptJSON(ctx context.Context, hash []byte) (json.RawMessage, error)

	// ScriptCBOR returns the hex CBOR of a Plutus script, or nil for a
	// native script.
	ScriptCBOR(ctx context.Context, hash []byte) (*string, error)

	// ScriptRedeemers returns the paginated redeemers that executed the
	// script, along with the total before pagination.
	ScriptRedeemers(
		ctx context.Context,
		hash []byte,
		params PaginationParams,
	) ([]ScriptRedeemerInfo, int, error)

	// ScriptUTXOs returns the paginated current UTxOs carrying the script
	// as a reference script, along with the total before pagination.
	ScriptUTXOs(
		ctx context.Context,
		hash []byte,
		params PaginationParams,
	) ([]ScriptUTXOInfo, int, error)

	// DatumJSON returns the detailed-schema JSON of a datum by hash.
	DatumJSON(ctx context.Context, hash []byte) (json.RawMessage, error)

	// DatumCBOR returns the hex CBOR of a datum by hash.
	DatumCBOR(ctx context.Context, hash []byte) (string, error)
}

// MempoolQueryNode is the mempool read surface of BlockfrostNode.
type MempoolQueryNode interface {
	// MempoolTransactions returns the paginated hashes of the transactions
	// in the mempool, along with the total before pagination.
	MempoolTransactions(
		ctx context.Context,
		params PaginationParams,
	) ([]string, int, error)

	// MempoolTransaction returns one mempool transaction.
	MempoolTransaction(
		ctx context.Context,
		hash []byte,
	) (MempoolTransactionInfo, error)

	// MempoolAddressTransactions returns the paginated hashes of the
	// mempool transactions that pay to or spend from an address, along with
	// the total before pagination.
	MempoolAddressTransactions(
		ctx context.Context,
		address string,
		params PaginationParams,
	) ([]string, int, error)
}

// BlockTransactionCBORInfo holds a transaction hash and its raw hex CBOR.
type BlockTransactionCBORInfo struct {
	TxHash string
	CBOR   string
}

// BlockAddressInfo holds an address affected by a block and the hashes of
// the block transactions touching it.
type BlockAddressInfo struct {
	Address  string
	TxHashes []string
}

// AddressExtendedAmountInfo is one balance entry of an extended address.
type AddressExtendedAmountInfo struct {
	Unit     string
	Quantity string
	// Decimals comes from the token registry; nil when unregistered.
	Decimals              *int
	HasNFTOnchainMetadata bool
}

// AddressExtendedInfo holds the extended address summary.
type AddressExtendedInfo struct {
	Address      string
	Amount       []AddressExtendedAmountInfo
	StakeAddress *string
	Type         string
	Script       bool
}

// AddressTotalInfo holds the lifetime totals of an address.
type AddressTotalInfo struct {
	Address     string
	ReceivedSum []AddressAmountInfo
	SentSum     []AddressAmountInfo
	TxCount     int
}

// AssetListItemInfo is one entry of an asset listing.
type AssetListItemInfo struct {
	Asset    string
	Quantity string
}

// AssetHistoryItemInfo is one mint or burn event of an asset. Action is
// "minted" or "burned" and Amount is the unsigned quantity.
type AssetHistoryItemInfo struct {
	TxHash string
	Action string
	Amount string
}

// AssetTransactionInfo is one transaction involving an asset.
type AssetTransactionInfo struct {
	TxHash      string
	TxIndex     uint32
	BlockHeight uint64
	BlockTime   int64
}

// AssetUTXOInfo is one current UTxO holding an asset.
type AssetUTXOInfo struct {
	Address             string
	TxHash              string
	OutputIndex         uint32
	Amount              []AddressAmountInfo
	Block               string
	BlockHeight         uint64
	BlockTime           int64
	DataHash            *string
	InlineDatum         *string
	InlineDatumJSON     json.RawMessage
	ReferenceScriptHash *string
}

// ScriptInfo holds a script's language and serialised size. SerialisedSize
// is nil for a native script.
type ScriptInfo struct {
	Hash           string
	Type           string
	SerialisedSize *int
}

// ScriptRedeemerInfo is one redeemer that executed a script.
type ScriptRedeemerInfo struct {
	TxHash           string
	TxIndex          int
	Purpose          string
	RedeemerDataHash string
	DatumHash        *string
	UnitMem          string
	UnitSteps        string
	Fee              string
}

// ScriptUTXOInfo is one current UTxO carrying a reference script.
type ScriptUTXOInfo struct {
	Address             string
	TxHash              string
	OutputIndex         uint32
	Amount              []AddressAmountInfo
	Block               string
	DataHash            *string
	InlineDatum         *string
	ReferenceScriptHash string
}

// MempoolTransactionInfo is one transaction in the mempool. Tx carries only
// the fields a transaction has before it is in a block.
type MempoolTransactionInfo struct {
	Tx        TransactionInfo
	Inputs    []TransactionInputInfo
	Outputs   []TransactionOutputInfo
	Redeemers []MempoolRedeemerInfo
}

// MempoolRedeemerInfo is one redeemer of a mempool transaction.
type MempoolRedeemerInfo struct {
	TxIndex   int
	Purpose   string
	UnitMem   string
	UnitSteps string
}
