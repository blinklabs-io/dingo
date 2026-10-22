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
	"errors"
	"fmt"
	"math/big"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
)

// knownAccount parses a stake address and loads its account, reporting
// models.ErrAccountNotFound when the node has none.
func (a *NodeAdapter) knownAccount(
	ctx context.Context,
	stakeAddress string,
) (uint8, []byte, *models.Account, error) {
	_, tag, key, err := parseStakeAddress(stakeAddress)
	if err != nil {
		return 0, nil, nil, err
	}
	account, err := a.ledgerState.Database().GetAccountByCredential(
		ctx, tag, key, true, nil,
	)
	if err != nil {
		return 0, nil, nil, err
	}
	if account == nil {
		return 0, nil, nil, models.ErrAccountNotFound
	}
	return tag, key, account, nil
}

// assetQuantityUnit renders a native asset as its Blockfrost unit: the policy id
// followed by the asset name, both hex.
func assetQuantityUnit(asset models.AssetQuantity) string {
	return hex.EncodeToString(asset.PolicyID) + hex.EncodeToString(asset.Name)
}

func assetAmounts(assets []models.AssetQuantity) []AddressAmountInfo {
	ret := make([]AddressAmountInfo, 0, len(assets))
	for _, asset := range assets {
		ret = append(ret, AddressAmountInfo{
			Unit:     assetQuantityUnit(asset),
			Quantity: asset.Quantity.String(),
		})
	}
	return ret
}

// accountFirstStakeLabel is the oldest snapshot label that can hold the
// account's stake: the one captured at the boundary that ends the epoch it
// was created in, which carries that epoch's number.
func (a *NodeAdapter) accountFirstStakeLabel(
	account *models.Account,
) (uint64, error) {
	slot := account.CreatedSlot
	if slot == models.AccountCreatedSlotUnset {
		slot = account.AddedSlot
	}
	epoch, err := a.ledgerState.SlotToEpoch(slot)
	if err != nil {
		return 0, fmt.Errorf("get epoch for account slot %d: %w", slot, err)
	}
	return epoch.EpochId, nil
}

// AccountStakeHistory returns the stake an account held in each retained
// epoch-boundary distribution, one entry per active epoch. The snapshot
// labelled E is active in epoch E+1, and only snapshots whose epoch has begun
// are listed.
//
// Per-credential rows are pruned outside API storage mode and before a node
// first retained them, so an account whose history may predate the oldest
// retained row and has no retained row at all is reported as
// ErrHistoryNotRetained rather than as an empty history. A partly retained
// history lists the retained epochs. A node with no retained rows at all
// cannot tell pruning from an account that never held stake, and answers
// empty.
func (a *NodeAdapter) AccountStakeHistory(
	ctx context.Context,
	stakeAddress string,
	params PaginationParams,
) ([]AccountStakeHistoryInfo, int, error) {
	tag, key, account, err := a.knownAccount(ctx, stakeAddress)
	if err != nil {
		return nil, 0, err
	}
	current, err := a.currentEpochNumber()
	if err != nil {
		return nil, 0, err
	}
	offset, ok := paginationOffset(params)
	if !ok {
		return []AccountStakeHistoryInfo{}, 0, nil
	}
	first, err := a.accountFirstStakeLabel(account)
	if err != nil {
		return nil, 0, err
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	meta := db.Metadata()
	// A snapshot captured before the account existed holds none of its
	// stake, so only labels from first on can leave its history partial.
	incomplete, err := a.ledgerState.RewardStakeInputsIncomplete(
		txn, first, current,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("check account stake history: %w", err)
	}
	if incomplete {
		return nil, 0, fmt.Errorf(
			"stake history of %s: %w", stakeAddress, ErrHistoryNotRetained,
		)
	}
	rows, total, err := meta.GetAccountStakeHistory(
		tag, key, current, params.Count, offset, params.Order,
		txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get account stake history: %w", err)
	}
	if total == 0 {
		earliest, retained, err := meta.GetEarliestRewardStakeInputEpoch(
			txn.Metadata(),
		)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"get earliest retained stake epoch: %w",
				err,
			)
		}
		if retained && first < min(earliest, current) {
			return nil, 0, fmt.Errorf(
				"stake history of %s before epoch %d: %w",
				stakeAddress, earliest+1, ErrHistoryNotRetained,
			)
		}
	}
	ret := make([]AccountStakeHistoryInfo, 0, len(rows))
	for _, row := range rows {
		poolID, err := poolIDString(row.PoolKeyHash)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, AccountStakeHistoryInfo{
			ActiveEpoch: row.Epoch + 1,
			Amount:      strconv.FormatUint(row.Stake, 10),
			PoolID:      poolID,
		})
	}
	return ret, total, nil
}

// AccountMIRs returns the Move Instantaneous Rewards paid to an account.
func (a *NodeAdapter) AccountMIRs(
	ctx context.Context,
	stakeAddress string,
	params PaginationParams,
) ([]AccountMIRInfo, int, error) {
	tag, key, _, err := a.knownAccount(ctx, stakeAddress)
	if err != nil {
		return nil, 0, err
	}
	offset, ok := paginationOffset(params)
	if !ok {
		return []AccountMIRInfo{}, 0, nil
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	rows, total, err := db.Metadata().GetAccountMIRHistory(
		tag, key, params.Count, offset, params.Order, txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get account MIRs: %w", err)
	}
	ret := make([]AccountMIRInfo, 0, len(rows))
	for _, row := range rows {
		amount := row.Amount
		if amount == nil {
			amount = new(big.Int)
		}
		ret = append(ret, AccountMIRInfo{
			TxHash: hex.EncodeToString(row.TxHash),
			Amount: amount.String(),
		})
	}
	return ret, total, nil
}

// accountAggregateError wraps a store error, mapping an exhausted read
// budget to ErrAddressSummaryTooLarge.
func accountAggregateError(what string, err error) error {
	if errors.Is(err, models.ErrAccountAggregateLimit) {
		return fmt.Errorf("%s: %w: %w", what, ErrAddressSummaryTooLarge, err)
	}
	return fmt.Errorf("%s: %w", what, err)
}

// AccountAddressAssets returns the native assets held across the addresses
// of an account.
func (a *NodeAdapter) AccountAddressAssets(
	ctx context.Context,
	stakeAddress string,
	params PaginationParams,
) ([]AddressAmountInfo, int, error) {
	tag, key, _, err := a.knownAccount(ctx, stakeAddress)
	if err != nil {
		return nil, 0, err
	}
	offset, ok := paginationOffset(params)
	if !ok {
		return []AddressAmountInfo{}, 0, nil
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	rows, total, err := db.Metadata().GetAccountAssetBalances(
		tag, key, params.Count, offset, params.Order,
		a.summaryLimits.accountLimits(), txn.Metadata(),
	)
	if err != nil {
		return nil, 0, accountAggregateError("get account address assets", err)
	}
	return assetAmounts(rows), total, nil
}

// AccountAddressTotal returns the lifetime received and sent totals across
// the addresses of an account.
func (a *NodeAdapter) AccountAddressTotal(
	ctx context.Context,
	stakeAddress string,
) (AccountAddressTotalInfo, error) {
	tag, key, _, err := a.knownAccount(ctx, stakeAddress)
	if err != nil {
		return AccountAddressTotalInfo{}, err
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	totals, err := db.Metadata().GetAccountAddressTotals(
		tag, key, a.summaryLimits.accountLimits(), txn.Metadata(),
	)
	if err != nil {
		return AccountAddressTotalInfo{}, accountAggregateError(
			"get account address totals", err,
		)
	}
	amounts := func(lovelace *big.Int, assets []models.AssetQuantity) []AddressAmountInfo {
		quantity := "0"
		if lovelace != nil {
			quantity = lovelace.String()
		}
		return append(
			[]AddressAmountInfo{{
				Unit:     "lovelace",
				Quantity: quantity,
			}},
			assetAmounts(assets)...,
		)
	}
	return AccountAddressTotalInfo{
		StakeAddress: stakeAddress,
		ReceivedSum:  amounts(totals.ReceivedLovelace, totals.ReceivedAssets),
		SentSum:      amounts(totals.SentLovelace, totals.SentAssets),
		TxCount:      totals.TxCount,
	}, nil
}
