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

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// AccountUTXOs returns the current UTxOs controlled by the stake
// credential behind stakeAddress. It models NodeAdapter.AddressUTXOs,
// reusing the same CBOR-derived datum/reference-script recovery so the
// fields stay consistent with /addresses/{address}/utxos and
// /txs/{hash}/utxos. Unlike a single-address query, each row's address
// must be recovered per-UTxO from decoded output CBOR, because a stake
// credential can be shared by many distinct payment addresses.
func (a *NodeAdapter) AccountUTXOs(
	stakeAddress string,
	params PaginationParams,
) ([]AccountUTXOInfo, int, error) {
	_, credentialTag, stakeKey, err := parseStakeAddress(stakeAddress)
	if err != nil {
		return nil, 0, err
	}
	// Shared across the existence check, count, and page fetch below so
	// the total and the returned page describe the same snapshot: two
	// separate (nil-txn) calls could otherwise straddle a concurrent
	// commit and return a page inconsistent with the reported total.
	txn := a.ledgerState.Database().Transaction(false)
	defer txn.Release()

	if _, err := a.ledgerState.Database().
		GetAccountByCredential(credentialTag, stakeKey, true, txn); err != nil {
		return nil, 0, err
	}

	// A stake credential's UTxO set is matched entirely by SQL (payment_key
	// is irrelevant; staking_key alone is exact), so it does not need the
	// CBOR-based exact-address filtering that AddressUTXOs does. That makes
	// a cheap SQL COUNT and a LIMIT/OFFSET-bound fetch safe: unlike
	// AddressUTXOs, this query never has to materialize the full UTxO
	// history to page or total a large stake account.
	addressPatterns := []models.UtxoAddressPattern{
		{DelegationPart: stakeKey},
	}
	total, err := a.ledgerState.Database().CountUtxosByAddressWithOrdering(
		&models.UtxoWithOrderingQuery{AddressPatterns: addressPatterns},
		txn,
	)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"count account UTxOs for %q: %w",
			stakeAddress,
			err,
		)
	}
	offset, ok := paginationOffset(params)
	if !ok || offset >= total {
		return []AccountUTXOInfo{}, total, nil
	}

	paged, err := a.ledgerState.Database().UtxosByAddressWithOrdering(
		&models.UtxoWithOrderingQuery{
			AddressPatterns: addressPatterns,
			Limit:           params.Count,
			Offset:          offset,
			Descending:      params.Order == PaginationOrderDesc,
		},
		txn,
	)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get account UTxOs for %q: %w",
			stakeAddress,
			err,
		)
	}

	txBlockHashes, err := a.addressUtxoBlockHashes(paged)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get block hashes for account UTxOs %q: %w",
			stakeAddress,
			err,
		)
	}
	// Inline datum, reference script, and the exact payment address are not
	// persisted in metadata rows, so resolve each paged UTxO's CBOR (hot
	// cache -> block LRU -> cold blob extract) and recover them from the
	// decoded output. Missing entries degrade to zero values rather than
	// failing the whole listing.
	utxoCbor, err := a.addressUtxoCbor(paged)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"resolve CBOR for account UTxOs %q: %w",
			stakeAddress,
			err,
		)
	}

	ret := make([]AccountUTXOInfo, 0, len(paged))
	for _, utxo := range paged {
		txKey := hex.EncodeToString(utxo.TxId)
		address := ""
		var inlineDatum, referenceScriptHash *string
		if cborBytes := utxoCbor[utxoRef(utxo.Utxo)]; len(cborBytes) > 0 {
			if output, decodeErr := gledger.NewTransactionOutputFromCbor(
				cborBytes,
			); decodeErr == nil {
				address = output.Address().String()
				inlineDatum, referenceScriptHash = utxoDatumAndScriptRef(output)
			}
		}
		ret = append(ret, AccountUTXOInfo{
			Address:             address,
			TxHash:              txKey,
			TxIndex:             utxo.OutputIdx,
			OutputIndex:         utxo.OutputIdx,
			Amount:              addressAmountsFromUtxo(utxo.Utxo),
			Block:               txBlockHashes[txKey],
			DataHash:            optionalHexString(utxo.DatumHash),
			InlineDatum:         inlineDatum,
			ReferenceScriptHash: referenceScriptHash,
		})
	}
	return ret, total, nil
}

// AccountWithdrawals returns withdrawal history rows for the stake
// credential behind stakeAddress.
func (a *NodeAdapter) AccountWithdrawals(
	stakeAddress string,
	params PaginationParams,
) ([]AccountWithdrawalInfo, int, error) {
	_, credentialTag, stakeKey, err := parseStakeAddress(stakeAddress)
	if err != nil {
		return nil, 0, err
	}
	if _, err := a.ledgerState.Database().
		GetAccountByCredential(credentialTag, stakeKey, true, nil); err != nil {
		return nil, 0, err
	}

	offset := (params.Page - 1) * params.Count
	total, err := a.ledgerState.Database().
		CountAccountWithdrawalHistoryByCredential(credentialTag, stakeKey, nil)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"count account withdrawal history: %w",
			err,
		)
	}
	if offset >= total {
		return []AccountWithdrawalInfo{}, total, nil
	}
	rows, err := a.ledgerState.Database().
		GetAccountWithdrawalHistoryByCredential(
			credentialTag,
			stakeKey,
			params.Count,
			offset,
			params.Order,
			nil,
		)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get account withdrawal history: %w",
			err,
		)
	}

	blockNumbers := make(map[string]uint64, len(rows))
	ret := make([]AccountWithdrawalInfo, 0, len(rows))
	for _, row := range rows {
		txSlot, blockTime, blockHeight, err := a.accountHistoryBlockInfo(
			row.TxSlot,
			row.BlockHash,
			blockNumbers,
		)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, AccountWithdrawalInfo{
			TxHash:      hex.EncodeToString(row.TxHash),
			Amount:      strconv.FormatUint(row.Amount, 10),
			TxSlot:      txSlot,
			BlockTime:   blockTime,
			BlockHeight: blockHeight,
		})
	}
	return ret, total, nil
}

// AccountTransactions returns transactions associated with addresses
// controlled by the stake credential behind stakeAddress, optionally
// filtered by an inclusive from/to block-range position.
//
// Every step here is bounded by the requested page size, not by the
// credential's full transaction history: the (payment address,
// transaction) association rows are paginated in SQL against
// address_transaction (which already carries slot/tx_index, so the
// from/to range is a SQL predicate, not an in-memory filter), and the
// payment-credential script/key bit and block height/time are then
// resolved only for the <= count rows on the page.
func (a *NodeAdapter) AccountTransactions(
	stakeAddress string,
	params AccountTransactionsParams,
) ([]AccountTransactionInfo, int, error) {
	stakeAddr, credentialTag, stakeKey, err := parseStakeAddress(stakeAddress)
	if err != nil {
		return nil, 0, err
	}
	networkID, err := uintToUint8(
		stakeAddr.NetworkId(),
		"stake address network id",
	)
	if err != nil {
		return nil, 0, err
	}
	if _, err := a.ledgerState.Database().
		GetAccountByCredential(credentialTag, stakeKey, true, nil); err != nil {
		return nil, 0, err
	}

	from, fromSatisfiable, err := a.resolveBlockRangeBound(params.From, true)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"resolve account transactions from range: %w",
			err,
		)
	}
	if !fromSatisfiable {
		return []AccountTransactionInfo{}, 0, nil
	}
	to, _, err := a.resolveBlockRangeBound(params.To, false)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"resolve account transactions to range: %w",
			err,
		)
	}

	offset := (params.Pagination.Page - 1) * params.Pagination.Count
	total, err := a.ledgerState.Database().CountAddressTransactionsByCredential(
		credentialTag,
		stakeKey,
		from,
		to,
		nil,
	)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"count account transactions for %q: %w",
			stakeAddress,
			err,
		)
	}
	if offset >= total {
		return []AccountTransactionInfo{}, total, nil
	}
	rows, err := a.ledgerState.Database().GetAddressTransactionsByCredential(
		credentialTag,
		stakeKey,
		params.Pagination.Count,
		offset,
		params.Pagination.Order,
		from,
		to,
		nil,
	)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get account transactions for %q: %w",
			stakeAddress,
			err,
		)
	}

	// Resolve the payment-credential script/key bit only for the distinct
	// payment keys on this page (<= len(rows)), not the credential's full
	// history.
	paymentKeys := make(map[string][]byte, len(rows))
	for _, row := range rows {
		paymentKeys[hex.EncodeToString(row.PaymentKey)] = row.PaymentKey
	}
	keyList := make([][]byte, 0, len(paymentKeys))
	for _, key := range paymentKeys {
		keyList = append(keyList, key)
	}
	scriptFlags, err := a.ledgerState.Database().
		GetUtxoPaymentScriptByCredential(credentialTag, stakeKey, keyList, nil)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"resolve account transaction payment credential types: %w",
			err,
		)
	}

	blockNumbers := make(map[string]uint64, len(rows))
	ret := make([]AccountTransactionInfo, 0, len(rows))
	for _, row := range rows {
		blockHashKey := hex.EncodeToString(row.BlockHash)
		blockHeight, ok := blockNumbers[blockHashKey]
		if !ok {
			block, err := a.ledgerState.BlockByHash(row.BlockHash)
			if err != nil {
				return nil, 0, fmt.Errorf(
					"get block for transaction %x: %w",
					row.TxHash,
					err,
				)
			}
			blockHeight = block.Number
			blockNumbers[blockHashKey] = blockHeight
		}
		blockTime, err := a.ledgerState.SlotToTime(row.TxSlot)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"get block time for transaction %x: %w",
				row.TxHash,
				err,
			)
		}

		// A payment key with no matching UTxO row (should not happen: every
		// address_transaction row is sourced from a UTxO row) defaults to
		// key-hash, matching the fallback used elsewhere.
		addressType := credentialTag << 1
		if scriptFlags[hex.EncodeToString(row.PaymentKey)] {
			addressType |= 1
		}
		addr, err := lcommon.NewAddressFromParts(
			addressType,
			networkID,
			row.PaymentKey,
			stakeKey,
		)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"build account transaction address: %w",
				err,
			)
		}

		ret = append(ret, AccountTransactionInfo{
			Address:     addr.String(),
			TxHash:      hex.EncodeToString(row.TxHash),
			TxIndex:     row.TxIndex,
			BlockHeight: blockHeight,
			BlockTime:   blockTime.Unix(),
		})
	}
	return ret, total, nil
}

// resolveBlockRangeBound resolves a Blockfrost account-transactions
// from/to block-number position to an inclusive (slot, tx_index) bound
// for the address_transaction SQL filter. A nil pos is unconstrained
// (returns a nil bound, satisfiable=true).
//
// When the exact block number exists, its slot is used directly (the
// common case: callers pass a block number they actually observed). When
// it does not:
//   - for a lower ("from") bound, this falls forward to the next block
//     that does exist, which still correctly captures "at or after this
//     position" even though the literal target block is absent (a sparse
//     import gap). If no block at or after it exists at all (the position
//     is beyond every known block), the range is unsatisfiable and the
//     caller should return an empty result without querying further.
//   - for an upper ("to") bound, there is no equivalent "last existing
//     block at or before" index lookup available, so the bound is instead
//     treated as unconstrained. This can only return more rows than a
//     literal reading of an unresolvable "to" would (never fewer), which
//     is the safe direction for an inclusive range filter.
//
// An explicit ":index" sub-position is honored only when the exact block
// was found; a gap-fallback ignores it and defaults to the start (from)
// or end (to) of the resolved block, since the requested index was
// scoped to a block that does not exist.
func (a *NodeAdapter) resolveBlockRangeBound(
	pos *BlockRangePosition,
	lower bool,
) (*models.AddressTransactionPosition, bool, error) {
	if pos == nil {
		return nil, true, nil
	}
	if pos.Block > math.MaxUint64-database.BlockInitialIndex {
		if lower {
			return nil, false, nil
		}
		return nil, true, nil
	}
	idx := pos.Block + database.BlockInitialIndex

	block, err := a.ledgerState.Database().BlockByIndex(idx, nil)
	switch {
	case err == nil:
		txIndex := uint32(0)
		if !lower {
			txIndex = math.MaxUint32
		}
		if pos.Index != nil {
			txIndex = *pos.Index
		}
		return &models.AddressTransactionPosition{
			Slot:    block.Slot,
			TxIndex: txIndex,
		}, true, nil
	case errors.Is(err, models.ErrBlockNotFound):
		if !lower {
			return nil, true, nil
		}
		next, err := a.ledgerState.Database().BlockAtOrAfterIndex(idx, nil)
		if err == nil {
			return &models.AddressTransactionPosition{
				Slot:    next.Slot,
				TxIndex: 0,
			}, true, nil
		}
		if errors.Is(err, models.ErrBlockNotFound) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf(
			"resolve next block at or after %d: %w",
			pos.Block,
			err,
		)
	default:
		return nil, false, fmt.Errorf(
			"resolve block %d: %w",
			pos.Block,
			err,
		)
	}
}
