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
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

func decodePolicyID(policyID string) (lcommon.Blake2b224, error) {
	raw, err := hex.DecodeString(policyID)
	if err != nil {
		return lcommon.Blake2b224{}, fmt.Errorf(
			"decode asset policy ID %q: %w",
			policyID,
			err,
		)
	}
	hash, err := lcommon.NewBlake2b224Checked(raw)
	if err != nil {
		return lcommon.Blake2b224{}, fmt.Errorf(
			"asset policy ID %q: %w",
			policyID,
			err,
		)
	}
	return hash, nil
}

// assetKnown reports whether the metadata index has any record of an asset: a
// mint or burn event, or a UTxO that holds or held it. It tells an asset with
// an empty listing apart from one that does not exist.
func (a *NodeAdapter) assetKnown(
	policy lcommon.Blake2b224,
	assetName []byte,
	txn dbtypes.Txn,
) (bool, error) {
	meta := a.ledgerState.Database().Metadata()
	events, err := meta.CountAssetMintBurnHistory(policy, assetName, txn)
	if err != nil {
		return false, fmt.Errorf("count asset events: %w", err)
	}
	if events > 0 {
		return true, nil
	}
	held, err := meta.GetAssetByPolicyAndName(policy, assetName, txn)
	if err != nil {
		return false, fmt.Errorf("get asset: %w", err)
	}
	return held.ID != 0, nil
}

func (a *NodeAdapter) listAssets(
	ctx context.Context,
	policyID []byte,
	params PaginationParams,
) ([]AssetListItemInfo, int, error) {
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	meta := db.Metadata()

	total, err := meta.CountMintedAssets(policyID, txn.Metadata())
	if err != nil {
		return nil, 0, fmt.Errorf("count assets: %w", err)
	}
	offset, ok := paginationOffset(params)
	if !ok || offset >= total {
		return []AssetListItemInfo{}, total, nil
	}
	assets, err := meta.ListMintedAssets(
		policyID,
		params.Count,
		offset,
		params.Order == PaginationOrderDesc,
		txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("list assets: %w", err)
	}
	ret := make([]AssetListItemInfo, 0, len(assets))
	for _, asset := range assets {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		policy, err := lcommon.NewBlake2b224Checked(asset.PolicyId)
		if err != nil {
			return nil, 0, fmt.Errorf("stored asset policy ID: %w", err)
		}
		quantity, err := meta.GetAssetQuantityByPolicyAndName(
			policy,
			asset.Name,
			txn.Metadata(),
		)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"get quantity of asset %x%x: %w",
				asset.PolicyId,
				asset.Name,
				err,
			)
		}
		ret = append(ret, AssetListItemInfo{
			Asset: hex.EncodeToString(asset.PolicyId) +
				hex.EncodeToString(asset.Name),
			Quantity: strconv.FormatUint(quantity, 10),
		})
	}
	return ret, total, nil
}

// Assets returns the assets with a recorded mint or burn, oldest first. The
// quantity of each is its live holdings, as in the single asset lookup.
func (a *NodeAdapter) Assets(
	ctx context.Context,
	params PaginationParams,
) ([]AssetListItemInfo, int, error) {
	return a.listAssets(ctx, nil, params)
}

// AssetsByPolicy returns the assets minted under one policy.
func (a *NodeAdapter) AssetsByPolicy(
	ctx context.Context,
	policyID string,
	params PaginationParams,
) ([]AssetListItemInfo, int, error) {
	policy, err := decodePolicyID(policyID)
	if err != nil {
		return nil, 0, err
	}
	return a.listAssets(ctx, policy[:], params)
}

// AssetHistory returns the mint and burn events of an asset in chain order.
func (a *NodeAdapter) AssetHistory(
	ctx context.Context,
	policyID string,
	assetName []byte,
	params PaginationParams,
) ([]AssetHistoryItemInfo, int, error) {
	policy, err := decodePolicyID(policyID)
	if err != nil {
		return nil, 0, err
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	meta := db.Metadata()

	total, err := meta.CountAssetMintBurnHistory(
		policy, assetName, txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("count asset history: %w", err)
	}
	if total == 0 {
		known, err := a.assetKnown(policy, assetName, txn.Metadata())
		if err != nil {
			return nil, 0, err
		}
		if !known {
			return nil, 0, fmt.Errorf(
				"asset %s%x: %w",
				policyID,
				assetName,
				ErrAssetNotFound,
			)
		}
		return []AssetHistoryItemInfo{}, 0, nil
	}
	offset, ok := paginationOffset(params)
	if !ok || offset >= total {
		return []AssetHistoryItemInfo{}, total, nil
	}
	rows, err := meta.GetAssetMintBurnHistory(
		policy,
		assetName,
		params.Count,
		offset,
		params.Order == PaginationOrderDesc,
		txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get asset history: %w", err)
	}
	ret := make([]AssetHistoryItemInfo, 0, len(rows))
	for _, row := range rows {
		action, amount := "minted", row.Quantity
		if unsigned, burned := strings.CutPrefix(row.Quantity, "-"); burned {
			action, amount = "burned", unsigned
		}
		ret = append(ret, AssetHistoryItemInfo{
			TxHash: hex.EncodeToString(row.TxHash),
			Action: action,
			Amount: amount,
		})
	}
	return ret, total, nil
}

// AssetTransactions returns the transactions involving an asset in chain
// order, within the optional block range.
func (a *NodeAdapter) AssetTransactions(
	ctx context.Context,
	policyID string,
	assetName []byte,
	params TransactionRangeParams,
) ([]AssetTransactionInfo, int, error) {
	policy, err := decodePolicyID(policyID)
	if err != nil {
		return nil, 0, err
	}
	from, fromSatisfiable, err := a.resolveBlockRangeBound(
		ctx, params.From, true,
	)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"resolve asset transactions from range: %w",
			err,
		)
	}
	to, toSatisfiable, err := a.resolveBlockRangeBound(
		ctx, params.To, false,
	)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"resolve asset transactions to range: %w",
			err,
		)
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	meta := db.Metadata()

	total := 0
	if fromSatisfiable && toSatisfiable {
		total, err = meta.CountAssetTransactions(
			policy, assetName, from, to, txn.Metadata(),
		)
		if err != nil {
			return nil, 0, fmt.Errorf("count asset transactions: %w", err)
		}
	}
	if total == 0 {
		known, err := a.assetKnown(policy, assetName, txn.Metadata())
		if err != nil {
			return nil, 0, err
		}
		if !known {
			return nil, 0, fmt.Errorf(
				"asset %s%x: %w",
				policyID,
				assetName,
				ErrAssetNotFound,
			)
		}
		return []AssetTransactionInfo{}, 0, nil
	}
	offset, ok := paginationOffset(params.Pagination)
	if !ok || offset >= total {
		return []AssetTransactionInfo{}, total, nil
	}
	txs, err := meta.GetAssetTransactions(
		policy,
		assetName,
		from,
		to,
		params.Pagination.Count,
		offset,
		params.Pagination.Order == PaginationOrderDesc,
		txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get asset transactions: %w", err)
	}
	infos, err := a.transactionPositionInfos(ctx, txs)
	if err != nil {
		return nil, 0, err
	}
	ret := make([]AssetTransactionInfo, 0, len(infos))
	for _, info := range infos {
		ret = append(ret, AssetTransactionInfo(info))
	}
	return ret, total, nil
}

// positionInRange reports whether a producing transaction position lies
// inside the resolved inclusive range.
func positionInRange(
	slot uint64,
	index uint32,
	from *models.AddressTransactionPosition,
	to *models.AddressTransactionPosition,
) bool {
	if from != nil &&
		(slot < from.Slot || (slot == from.Slot && index < from.TxIndex)) {
		return false
	}
	if to != nil &&
		(slot > to.Slot || (slot == to.Slot && index > to.TxIndex)) {
		return false
	}
	return true
}

// AssetUTXOs returns the current UTxOs holding an asset in producing order.
// Without a block range the page is cut in SQL. A range filters on the
// producing transaction's position, which the SQL page does not express, so
// the matching outputs are listed by reference first and only the requested
// page is loaded in full.
func (a *NodeAdapter) AssetUTXOs(
	ctx context.Context,
	policyID string,
	assetName []byte,
	params TransactionRangeParams,
) ([]AssetUTXOInfo, int, error) {
	policy, err := decodePolicyID(policyID)
	if err != nil {
		return nil, 0, err
	}
	from, fromSatisfiable, err := a.resolveBlockRangeBound(
		ctx, params.From, true,
	)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"resolve asset UTxOs from range: %w",
			err,
		)
	}
	to, toSatisfiable, err := a.resolveBlockRangeBound(
		ctx, params.To, false,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("resolve asset UTxOs to range: %w", err)
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()

	query := models.UtxoWithOrderingQuery{
		MatchAllAddresses: true,
		FilterByAsset:     true,
		AssetPolicyID:     policy[:],
		AssetName:         nonNilAssetName(assetName),
	}
	descending := params.Pagination.Order == PaginationOrderDesc
	var (
		page  []models.UtxoWithOrdering
		total int
	)
	switch {
	case !fromSatisfiable || !toSatisfiable:
	case from == nil && to == nil:
		total, err = db.CountUtxosByAddressWithOrdering(ctx, &query, txn)
		if err != nil {
			return nil, 0, fmt.Errorf("count asset UTxOs: %w", err)
		}
		offset, ok := paginationOffset(params.Pagination)
		if !ok || offset >= total {
			break
		}
		paged := query
		paged.Limit = params.Pagination.Count
		paged.Offset = offset
		paged.Descending = descending
		page, err = db.UtxosByAddressWithOrdering(ctx, &paged, txn)
		if err != nil {
			return nil, 0, fmt.Errorf("get asset UTxOs: %w", err)
		}
	default:
		scan := query
		scan.SkipAssets = true
		all, err := db.UtxosByAddressWithOrdering(ctx, &scan, txn)
		if err != nil {
			return nil, 0, fmt.Errorf("get asset UTxOs: %w", err)
		}
		inRange := make([]models.UtxoWithOrdering, 0, len(all))
		for _, utxo := range all {
			if positionInRange(utxo.TxSlot, utxo.TxBlockIndex, from, to) {
				inRange = append(inRange, utxo)
			}
		}
		total = len(inRange)
		if descending {
			slices.Reverse(inRange)
		}
		offset, ok := paginationOffset(params.Pagination)
		if !ok || offset >= total {
			break
		}
		window := inRange[offset:min(offset+params.Pagination.Count, total)]
		refs := make([]models.UtxoId, 0, len(window))
		slots := make(map[database.UtxoRef]models.UtxoWithOrdering, len(window))
		for _, utxo := range window {
			refs = append(refs, models.UtxoId{Hash: utxo.TxId, Idx: utxo.OutputIdx})
			ref, err := utxoRef(utxo.Utxo)
			if err != nil {
				return nil, 0, err
			}
			slots[ref] = utxo
		}
		page, err = a.orderedUtxosByRefs(ctx, refs, txn)
		if err != nil {
			return nil, 0, fmt.Errorf("get asset UTxOs: %w", err)
		}
		for i := range page {
			ref, err := utxoRef(page[i].Utxo)
			if err != nil {
				return nil, 0, err
			}
			page[i].TxSlot = slots[ref].TxSlot
			page[i].TxBlockIndex = slots[ref].TxBlockIndex
		}
	}
	if total == 0 {
		known, err := a.assetKnown(policy, assetName, txn.Metadata())
		if err != nil {
			return nil, 0, err
		}
		if !known {
			return nil, 0, fmt.Errorf(
				"asset %s%x: %w",
				policyID,
				assetName,
				ErrAssetNotFound,
			)
		}
	}
	infos, err := a.assetUTXOInfos(ctx, page)
	if err != nil {
		return nil, 0, err
	}
	return infos, total, nil
}

func nonNilAssetName(name []byte) []byte {
	if name == nil {
		return []byte{}
	}
	return name
}

// assetUTXOInfos builds the response rows for a page of UTxOs: the output
// address, datum and reference script come from the decoded output CBOR, the
// block from the producing transaction.
func (a *NodeAdapter) assetUTXOInfos(
	ctx context.Context,
	page []models.UtxoWithOrdering,
) ([]AssetUTXOInfo, error) {
	ret := make([]AssetUTXOInfo, 0, len(page))
	if len(page) == 0 {
		return ret, nil
	}
	blockHashes, err := a.addressUtxoBlockHashes(ctx, page)
	if err != nil {
		return nil, fmt.Errorf("get block hashes for asset UTxOs: %w", err)
	}
	outputCbor, err := a.addressUtxoCbor(page)
	if err != nil {
		return nil, fmt.Errorf("resolve CBOR for asset UTxOs: %w", err)
	}
	heights := map[string]uint64{}
	for _, utxo := range page {
		txKey := hex.EncodeToString(utxo.TxId)
		ref, err := utxoRef(utxo.Utxo)
		if err != nil {
			return nil, err
		}
		info := AssetUTXOInfo{
			TxHash:      txKey,
			OutputIndex: utxo.OutputIdx,
			Amount:      addressAmountsFromUtxo(utxo.Utxo),
			Block:       blockHashes[txKey],
			DataHash:    optionalHexString(utxo.DatumHash),
		}
		utxo.Cbor = outputCbor[ref]
		info.Address, err = a.addressFromUtxo(utxo.Utxo)
		if err != nil {
			return nil, fmt.Errorf(
				"resolve address of UTxO %s#%d: %w",
				txKey,
				utxo.OutputIdx,
				err,
			)
		}
		if len(utxo.Cbor) > 0 {
			if output, decodeErr := gledger.NewTransactionOutputFromCbor(
				utxo.Cbor,
			); decodeErr == nil {
				info.InlineDatum, info.ReferenceScriptHash = utxoDatumAndScriptRef(
					output,
				)
				if datum := output.Datum(); datum != nil && len(datum.Cbor()) > 0 {
					info.InlineDatumJSON, err = plutusDataDetailedJSON(datum.Cbor())
					if err != nil {
						return nil, fmt.Errorf(
							"inline datum of UTxO %s#%d: %w",
							txKey,
							utxo.OutputIdx,
							err,
						)
					}
				}
			}
		}
		if info.Block != "" {
			height, ok := heights[info.Block]
			if !ok {
				blockHash, err := hex.DecodeString(info.Block)
				if err != nil {
					return nil, fmt.Errorf("block hash %q: %w", info.Block, err)
				}
				block, err := a.ledgerState.BlockByHash(ctx, blockHash)
				if err != nil {
					return nil, fmt.Errorf("get block %s: %w", info.Block, err)
				}
				height = block.Number
				heights[info.Block] = height
			}
			info.BlockHeight = height
		}
		blockTime, err := a.slotToTime(utxo.TxSlot)
		if err != nil {
			return nil, fmt.Errorf(
				"convert slot %d to block time: %w",
				utxo.TxSlot,
				err,
			)
		}
		info.BlockTime = blockTime.Unix()
		ret = append(ret, info)
	}
	return ret, nil
}
