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
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strconv"

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
	if _, err := a.ledgerState.Database().
		GetAccountByCredential(credentialTag, stakeKey, true, nil); err != nil {
		return nil, 0, err
	}

	utxos, err := a.ledgerState.UtxosByAddressWithOrdering(
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{
				{DelegationPart: stakeKey},
			},
		},
	)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get account UTxOs for %q: %w",
			stakeAddress,
			err,
		)
	}
	total := len(utxos)
	if params.Order == PaginationOrderDesc {
		for left, right := 0, len(utxos)-1; left < right; left, right = left+1, right-1 {
			utxos[left], utxos[right] = utxos[right], utxos[left]
		}
	}

	paged := paginateUtxos(utxos, params)
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
// Block height is not part of the metadata SQL schema (only block hash
// and slot are persisted on the transaction row), so an explicit
// block-range filter cannot be pushed down into SQL: every transaction
// associated with the credential is fetched in chain order, resolved
// against the block store to recover its height, filtered, and only
// then paged in memory.
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

	txs, err := a.ledgerState.GetTransactionsByAddressKeys(
		nil,
		credentialTag,
		stakeKey,
		0,
		0,
		params.Pagination.Order,
	)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get account transactions for %q: %w",
			stakeAddress,
			err,
		)
	}

	blockNumbers := make(map[string]uint64, len(txs))
	filtered := make([]AccountTransactionInfo, 0, len(txs))
	for _, tx := range txs {
		blockHashKey := hex.EncodeToString(tx.BlockHash)
		blockHeight, ok := blockNumbers[blockHashKey]
		if !ok {
			block, err := a.ledgerState.BlockByHash(tx.BlockHash)
			if err != nil {
				return nil, 0, fmt.Errorf(
					"get block for transaction %x: %w",
					tx.Hash,
					err,
				)
			}
			blockHeight = block.Number
			blockNumbers[blockHashKey] = blockHeight
		}
		if !inBlockRange(blockHeight, tx.BlockIndex, params.From, params.To) {
			continue
		}

		blockTime, err := a.transactionBlockTime(tx)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"get block time for transaction %x: %w",
				tx.Hash,
				err,
			)
		}

		addresses, err := accountTransactionAddresses(
			tx,
			networkID,
			credentialTag,
			stakeKey,
		)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"get addresses for transaction %x: %w",
				tx.Hash,
				err,
			)
		}
		txHash := hex.EncodeToString(tx.Hash)
		for _, address := range addresses {
			filtered = append(filtered, AccountTransactionInfo{
				Address:     address,
				TxHash:      txHash,
				TxIndex:     tx.BlockIndex,
				BlockHeight: blockHeight,
				BlockTime:   blockTime,
			})
		}
	}

	total := len(filtered)
	start, end := paginationRange(total, params.Pagination)
	if start >= end {
		return []AccountTransactionInfo{}, total, nil
	}
	return filtered[start:end], total, nil
}

// accountTransactionAddresses derives the distinct payment addresses
// sharing the given stake credential that participate in tx, across
// every UTxO group that populates the AddressTransaction index (inputs,
// collateral inputs, reference inputs, outputs, and collateral return),
// mirroring collectAddressTransactions at indexing time. The result is
// sorted for deterministic ordering, since a transaction may associate
// more than one address with the same stake credential.
func accountTransactionAddresses(
	tx models.Transaction,
	networkID uint8,
	credentialTag uint8,
	stakeKey []byte,
) ([]string, error) {
	utxos := make(
		[]models.Utxo,
		0,
		len(tx.Inputs)+len(tx.Collateral)+len(tx.ReferenceInputs)+len(tx.Outputs)+1,
	)
	utxos = append(utxos, tx.Inputs...)
	utxos = append(utxos, tx.Collateral...)
	utxos = append(utxos, tx.ReferenceInputs...)
	utxos = append(utxos, tx.Outputs...)
	if tx.CollateralReturn != nil {
		utxos = append(utxos, *tx.CollateralReturn)
	}

	seen := make(map[string]struct{}, len(utxos))
	addresses := make([]string, 0, len(utxos))
	for _, utxo := range utxos {
		if utxo.CredentialTag != credentialTag ||
			!bytes.Equal(utxo.StakingKey, stakeKey) ||
			len(utxo.PaymentKey) == 0 {
			continue
		}
		key := hex.EncodeToString(utxo.PaymentKey)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		addressType := credentialTag << 1
		if utxo.PaymentScript {
			addressType |= 1
		}
		addr, err := lcommon.NewAddressFromParts(
			addressType,
			networkID,
			utxo.PaymentKey,
			stakeKey,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"build account transaction address: %w",
				err,
			)
		}
		addresses = append(addresses, addr.String())
	}
	sort.Strings(addresses)
	return addresses, nil
}

// inBlockRange reports whether a transaction at the given block
// height/index falls within the inclusive [from, to] block-range
// position filter. A nil bound is unconstrained on that side. A bound
// with no explicit index matches the entire block on that side (index 0
// for from, the maximum index for to).
func inBlockRange(
	blockHeight uint64,
	txIndex uint32,
	from *BlockRangePosition,
	to *BlockRangePosition,
) bool {
	if from != nil {
		idx := uint32(0)
		if from.Index != nil {
			idx = *from.Index
		}
		if comparePosition(blockHeight, txIndex, from.Block, idx) < 0 {
			return false
		}
	}
	if to != nil {
		idx := uint32(math.MaxUint32)
		if to.Index != nil {
			idx = *to.Index
		}
		if comparePosition(blockHeight, txIndex, to.Block, idx) > 0 {
			return false
		}
	}
	return true
}

// comparePosition orders (block, index) tuples: -1 if the first
// position sorts before the second, 1 if after, 0 if equal.
func comparePosition(
	block uint64,
	index uint32,
	boundBlock uint64,
	boundIndex uint32,
) int {
	switch {
	case block < boundBlock:
		return -1
	case block > boundBlock:
		return 1
	case index < boundIndex:
		return -1
	case index > boundIndex:
		return 1
	default:
		return 0
	}
}
