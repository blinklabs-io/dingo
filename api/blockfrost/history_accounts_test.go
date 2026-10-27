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
	"math"
	"net/http"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hexOf(b []byte) string { return hex.EncodeToString(b) }

func setTip(fx *epochFixture, slot uint64) {
	fx.adapter.ledgerState.SetTipForTesting(ochainsync.Tip{
		Point:       ocommon.Point{Slot: slot, Hash: fill32(0xee)},
		BlockNumber: 9,
	})
}

func TestHandleAccountStakeHistory(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.accountStakeHistory.items = []AccountStakeHistoryInfo{
		{ActiveEpoch: 12, Amount: "700", PoolID: "pool1abc"},
	}
	node.accountStakeHistory.total = 1
	recorder := get(t, node, "/api/v0/accounts/stake1abc/history?count=5&order=desc")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "stake1abc", node.accountStakeHistory.key)
	assert.Equal(t, 5, node.accountStakeHistory.params.Count)
	assert.Equal(t, PaginationOrderDesc, node.accountStakeHistory.params.Order)
	assert.JSONEq(t, `[{"active_epoch":12,"amount":"700","pool_id":"pool1abc"}]`, recorder.Body.String())
	assert.Equal(t, "1", recorder.Header().Get("X-Pagination-Count-Total"))
}

func TestHandleAccountHistoryErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		target string
		set    func(*mockNode, error)
	}{
		{"history", "/api/v0/accounts/stake1abc/history", func(n *mockNode, e error) { n.accountStakeHistory.err = e }},
		{"mirs", "/api/v0/accounts/stake1abc/mirs", func(n *mockNode, e error) { n.accountMIRs.err = e }},
		{"assets", "/api/v0/accounts/stake1abc/addresses/assets", func(n *mockNode, e error) { n.accountAssets.err = e }},
		{"total", "/api/v0/accounts/stake1abc/addresses/total", func(n *mockNode, e error) { n.accountTotal.err = e }},
	} {
		for _, errCase := range []struct {
			err    error
			status int
			text   string
		}{
			{ErrInvalidStakeAddress, http.StatusBadRequest, "Invalid stake address"},
			{models.ErrAccountNotFound, http.StatusNotFound, "has not been found"},
			{ErrHistoryNotRetained, http.StatusNotFound, "not retained by this node"},
			{errors.New("boom"), http.StatusInternalServerError, "failed to retrieve"},
		} {
			t.Run(tc.name+"/"+errCase.text, func(t *testing.T) {
				t.Parallel()
				node := &mockNode{}
				tc.set(node, errCase.err)
				recorder := get(t, node, tc.target)
				assert.Equal(t, errCase.status, recorder.Code)
				assert.Contains(t, recorder.Body.String(), errCase.text)
				assert.NotContains(t, recorder.Body.String(), "boom")
			})
		}
	}
}

func TestHandleAccountMIRsAndAssets(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.accountMIRs.items = []AccountMIRInfo{{TxHash: "aa", Amount: "5"}}
	node.accountMIRs.total = 1
	recorder := get(t, node, "/api/v0/accounts/stake1abc/mirs")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.JSONEq(t, `[{"tx_hash":"aa","amount":"5"}]`, recorder.Body.String())

	node.accountAssets.items = []AddressAmountInfo{{Unit: "bb01", Quantity: "9"}}
	node.accountAssets.total = 1
	recorder = get(t, node, "/api/v0/accounts/stake1abc/addresses/assets")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.JSONEq(t, `[{"unit":"bb01","quantity":"9"}]`, recorder.Body.String())
}

func TestHandleAccountAddressTotal(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.accountTotal.value = AccountAddressTotalInfo{
		StakeAddress: "stake1abc",
		ReceivedSum:  []AddressAmountInfo{{Unit: "lovelace", Quantity: "10"}},
		SentSum:      []AddressAmountInfo{{Unit: "lovelace", Quantity: "0"}},
		TxCount:      3,
	}
	recorder := get(t, node, "/api/v0/accounts/stake1abc/addresses/total")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.JSONEq(t, `{"stake_address":"stake1abc","received_sum":[{"unit":"lovelace","quantity":"10"}],"sent_sum":[{"unit":"lovelace","quantity":"0"}],"tx_count":3}`, recorder.Body.String())
}

// extendToEpochFive adds epochs 3 to 5 to the epoch fixture and moves the
// tip into epoch 5.
func (fx *epochFixture) extendToEpochFive(t *testing.T) {
	t.Helper()
	for epoch := uint64(3); epoch < 6; epoch++ {
		_, err := fx.raw.Exec(
			`INSERT INTO epoch (epoch_id, start_slot, length_in_slots, slot_length, era_id) VALUES (?, ?, 86400, 1000, 6)`,
			epoch, epoch*86400,
		)
		require.NoError(t, err)
	}
	require.NoError(t, fx.adapter.ledgerState.PrepareEpochCacheForStartup(context.Background()))
	setTip(fx, 5*86400+100)
}

func (fx *epochFixture) addAccount(t *testing.T, key byte) string {
	t.Helper()
	require.NoError(t, fx.adapter.ledgerState.Database().CreateAccount(
		context.Background(), nil, &models.Account{
			CredentialTag: 0,
			StakingKey:    bytes32of(28, key),
			Active:        true,
			AddedSlot:     10,
			CreatedSlot:   10,
		},
	))
	return newRewardHistoryStakeAddress(t, bytes32of(28, key))
}

func TestNodeAdapterAccountStakeHistory(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	poolA, err := poolIDString(fx.poolA)
	require.NoError(t, err)
	address := fx.addAccount(t, 0x01)

	rows, total, err := fx.adapter.AccountStakeHistory(ctx, address, PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc})
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, []AccountStakeHistoryInfo{{ActiveEpoch: 1, Amount: "700", PoolID: poolA}}, rows,
		"snapshot label 0 is active in epoch 1")

	rows, total, err = fx.adapter.AccountStakeHistory(ctx, address, PaginationParams{Count: 100, Page: 2, Order: PaginationOrderAsc})
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Empty(t, rows)

	// The unregistered credential never held stake.
	unregistered := fx.addAccount(t, 0x03)
	rows, total, err = fx.adapter.AccountStakeHistory(ctx, unregistered, PaginationParams{Count: 100, Page: 1})
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, rows, "every distribution since creation is retained, so empty is complete")

	_, _, err = fx.adapter.AccountStakeHistory(ctx, newRewardHistoryStakeAddress(t, bytes32of(28, 0x99)), PaginationParams{Count: 1, Page: 1})
	assert.ErrorIs(t, err, models.ErrAccountNotFound)
	_, _, err = fx.adapter.AccountStakeHistory(ctx, "not-an-address", PaginationParams{Count: 1, Page: 1})
	assert.ErrorIs(t, err, ErrInvalidStakeAddress)
}

func TestNodeAdapterAccountStakeHistoryReportsPrunedHistory(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	fx.extendToEpochFive(t)
	ctx := context.Background()
	poolA, err := poolIDString(fx.poolA)
	require.NoError(t, err)
	pruned := fx.addAccount(t, 0x01)
	partial := fx.addAccount(t, 0x05)
	page := PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc}

	// Prune everything below label 3, then retain a row for one account.
	_, err = fx.raw.Exec(`DELETE FROM reward_stake_input WHERE epoch < 3`)
	require.NoError(t, err)
	_, err = fx.raw.Exec(
		`INSERT INTO reward_stake_input (pool_key_hash, staking_key, epoch, credential_tag, stake, owner, registered, captured_slot, boundary_slot) VALUES (?, ?, 3, 0, '42', FALSE, TRUE, 0, 0)`,
		fx.poolA, bytes32of(28, 0x05),
	)
	require.NoError(t, err)

	// The account's earlier history was pruned: an empty list would claim it
	// never held stake.
	_, _, err = fx.adapter.AccountStakeHistory(ctx, pruned, page)
	assert.ErrorIs(t, err, ErrHistoryNotRetained)

	// A partly retained history lists what remains.
	rows, total, err := fx.adapter.AccountStakeHistory(ctx, partial, page)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, []AccountStakeHistoryInfo{{ActiveEpoch: 4, Amount: "42", PoolID: poolA}}, rows)

	// A distribution whose epoch has not begun is not history yet.
	_, err = fx.raw.Exec(
		`INSERT INTO reward_stake_input (pool_key_hash, staking_key, epoch, credential_tag, stake, owner, registered, captured_slot, boundary_slot) VALUES (?, ?, 5, 0, '99', FALSE, TRUE, 0, 0)`,
		fx.poolA, bytes32of(28, 0x05),
	)
	require.NoError(t, err)
	_, total, err = fx.adapter.AccountStakeHistory(ctx, partial, page)
	require.NoError(t, err)
	assert.Equal(t, 1, total, "label 5 is active in epoch 6, which has not begun")

	// With nothing retained anywhere, pruning cannot be told from an account
	// that never held stake.
	_, err = fx.raw.Exec(`DELETE FROM reward_stake_input`)
	require.NoError(t, err)
	rows, total, err = fx.adapter.AccountStakeHistory(ctx, pruned, page)
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, rows)
}

func TestNodeAdapterAccountMIRs(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	address := fx.addAccount(t, 0x01)
	other := fx.addAccount(t, 0x02)
	add := func(txByte byte, slot uint64, key byte, amount string) {
		cert := fx.addCert(t, fill32(txByte), slot, 0, 0)
		result, err := fx.raw.Exec(
			`INSERT INTO move_instantaneous_rewards (pot, certificate_id, added_slot) VALUES (0, ?, ?)`,
			cert, slot,
		)
		require.NoError(t, err)
		mirID, err := result.LastInsertId()
		require.NoError(t, err)
		_, err = fx.raw.Exec(
			`INSERT INTO move_instantaneous_rewards_reward (credential, credential_tag, amount, mir_id) VALUES (?, 0, ?, ?)`,
			bytes32of(28, key), amount, mirID,
		)
		require.NoError(t, err)
	}
	add(0xe1, 100, 0x01, "1000")
	add(0xe2, 200, 0x01, "-250")
	add(0xe3, 300, 0x02, "7")

	rows, total, err := fx.adapter.AccountMIRs(ctx, address, PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc})
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Equal(t, []AccountMIRInfo{
		{TxHash: hexOf(fill32(0xe1)), Amount: "1000"},
		{TxHash: hexOf(fill32(0xe2)), Amount: "-250"},
	}, rows)

	rows, total, err = fx.adapter.AccountMIRs(ctx, address, PaginationParams{Count: 1, Page: 1, Order: PaginationOrderDesc})
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Equal(t, []AccountMIRInfo{{TxHash: hexOf(fill32(0xe2)), Amount: "-250"}}, rows)

	rows, total, err = fx.adapter.AccountMIRs(ctx, other, PaginationParams{Count: 100, Page: 1})
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, "7", rows[0].Amount)
}

// addAccountOutput records a transaction paying a stake credential and
// returns nothing; the output carries the given native assets.
func (fx *epochFixture) addAccountOutput(
	t *testing.T,
	txByte byte,
	key byte,
	amount uint64,
	deletedSlot uint64,
	assets ...models.Asset,
) {
	t.Helper()
	tx := &models.Transaction{
		Hash: fill32(txByte), Slot: 50, Valid: true,
		Outputs: []models.Utxo{{
			TxId: fill32(txByte), StakingKey: bytes32of(28, key),
			Amount: types.Uint64(amount), DeletedSlot: deletedSlot,
			Assets: assets,
		}},
	}
	insertAdapterTransaction(t, fx.raw, tx)
	_, err := fx.raw.Exec(
		`INSERT INTO address_transaction (payment_key, staking_key, credential_tag, transaction_id, slot, tx_index) VALUES (?, ?, 0, ?, 50, 0)`,
		bytes32of(28, 0x10), bytes32of(28, key), tx.ID,
	)
	require.NoError(t, err)
}

func TestNodeAdapterAccountAddressAssetsAndTotal(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	address := fx.addAccount(t, 0x01)
	policy := bytes32of(28, 0xaa)
	nameX, nameY := []byte("X"), []byte("Y")

	// Two live outputs holding asset X, one spent output holding Y.
	fx.addAccountOutput(t, 0xf1, 0x01, 1_000_000, 0, models.Asset{PolicyId: policy, Name: nameX, Amount: 5})
	fx.addAccountOutput(t, 0xf2, 0x01, 2_000_000, 0, models.Asset{PolicyId: policy, Name: nameX, Amount: 7})
	fx.addAccountOutput(t, 0xf3, 0x01, 4_000_000, 60, models.Asset{PolicyId: policy, Name: nameY, Amount: 3})
	fx.addAccountOutput(t, 0xf4, 0x02, 9_000_000, 0, models.Asset{PolicyId: policy, Name: nameX, Amount: 100})

	unitX := hexOf(policy) + hexOf(nameX)
	unitY := hexOf(policy) + hexOf(nameY)

	assets, total, err := fx.adapter.AccountAddressAssets(ctx, address, PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc})
	require.NoError(t, err)
	assert.Equal(t, 1, total, "the spent output no longer holds Y")
	assert.Equal(t, []AddressAmountInfo{{Unit: unitX, Quantity: "12"}}, assets)

	info, err := fx.adapter.AccountAddressTotal(ctx, address)
	require.NoError(t, err)
	assert.Equal(t, AccountAddressTotalInfo{
		StakeAddress: address,
		ReceivedSum: []AddressAmountInfo{
			{Unit: "lovelace", Quantity: "7000000"},
			{Unit: unitX, Quantity: "12"},
			{Unit: unitY, Quantity: "3"},
		},
		SentSum: []AddressAmountInfo{
			{Unit: "lovelace", Quantity: "4000000"},
			{Unit: unitY, Quantity: "3"},
		},
		TxCount: 3,
	}, info)

	empty := fx.addAccount(t, 0x03)
	info, err = fx.adapter.AccountAddressTotal(ctx, empty)
	require.NoError(t, err)
	assert.Equal(t, []AddressAmountInfo{{Unit: "lovelace", Quantity: "0"}}, info.ReceivedSum)
	assert.Equal(t, []AddressAmountInfo{{Unit: "lovelace", Quantity: "0"}}, info.SentSum)
	assert.Equal(t, 0, info.TxCount)
}

// A snapshot whose deferred reward_stake_input write has not finished holds
// only part of its rows until the writer or the next reward round completes
// it. The stake endpoints must not present those rows as the whole
// distribution.
func TestNodeAdapterStakeHistoryRefusesIncompleteSnapshot(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	address := fx.addAccount(t, 0x01)
	page := PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc}
	meta := fx.adapter.ledgerState.Database().Metadata()
	const pendingKey = models.StakeRewardSourcePrefix + "stake-inputs-pending"

	require.NoError(t, meta.SetSyncState(
		pendingKey, `{"entries":[{"epoch":0,"boundary_slot":0}]}`, nil,
	))
	_, _, err := fx.adapter.EpochStakes(ctx, 1, page)
	assert.ErrorIs(t, err, ErrHistoryNotRetained,
		"epoch 1 runs on snapshot label 0, whose rows are incomplete")
	_, _, err = fx.adapter.AccountStakeHistory(ctx, address, page)
	assert.ErrorIs(t, err, ErrHistoryNotRetained,
		"the account's history includes the incomplete label 0")

	// Once the snapshot completes, both answer from its rows.
	require.NoError(t, meta.SetSyncState(pendingKey, `{"entries":[]}`, nil))
	_, total, err := fx.adapter.EpochStakes(ctx, 1, page)
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	_, total, err = fx.adapter.AccountStakeHistory(ctx, address, page)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
}

// Lifetime lovelace totals sum UTxO amounts, which can exceed uint64 together
// even though each fits.
func TestNodeAdapterAccountAddressTotalExceedsUint64(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	address := fx.addAccount(t, 0x01)
	fx.addAccountOutput(t, 0xe1, 0x01, math.MaxUint64, 0)
	fx.addAccountOutput(t, 0xe2, 0x01, math.MaxUint64, 60)

	info, err := fx.adapter.AccountAddressTotal(context.Background(), address)
	require.NoError(t, err)
	assert.Equal(t, AddressAmountInfo{Unit: "lovelace", Quantity: "36893488147419103230"}, info.ReceivedSum[0])
	assert.Equal(t, AddressAmountInfo{Unit: "lovelace", Quantity: "18446744073709551615"}, info.SentSum[0])
}

// An incomplete snapshot captured before the account existed cannot hold any
// of its stake, so it does not make the account's history unavailable.
func TestNodeAdapterStakeHistoryIgnoresIncompleteSnapshotBeforeAccount(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	key := bytes32of(28, 0x09)
	require.NoError(t, fx.adapter.ledgerState.Database().CreateAccount(
		ctx, nil, &models.Account{
			CredentialTag: 0, StakingKey: key, Active: true,
			AddedSlot: 172810, CreatedSlot: 172810,
		},
	))
	address := newRewardHistoryStakeAddress(t, key)
	const pendingKey = models.StakeRewardSourcePrefix + "stake-inputs-pending"
	require.NoError(t, fx.adapter.ledgerState.Database().Metadata().SetSyncState(
		pendingKey, `{"entries":[{"epoch":0,"boundary_slot":0}]}`, nil,
	))

	rows, total, err := fx.adapter.AccountStakeHistory(ctx, address, PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc})
	require.NoError(t, err, "label 0 predates an account created in epoch 2")
	assert.Equal(t, 0, total)
	assert.Empty(t, rows)
}

func TestNodeAdapterAccountAggregateReadBudget(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	address := fx.addAccount(t, 0x01)
	policy := bytes32of(28, 0xaa)
	// Four outputs, one spent, each holding its own asset.
	for i := range 4 {
		var deleted uint64
		if i == 0 {
			deleted = 60
		}
		fx.addAccountOutput(
			t, byte(0xc1+i), 0x01, 1_000_000, deleted,
			models.Asset{PolicyId: policy, Name: []byte{byte(i)}, Amount: 1},
		)
	}
	page := PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc}

	fx.adapter.summaryLimits = addressSummaryLimits{maxTotalOutputs: 3}
	_, err := fx.adapter.AccountAddressTotal(ctx, address)
	require.ErrorIs(t, err, ErrAddressSummaryTooLarge)
	fx.adapter.summaryLimits = addressSummaryLimits{maxTotalOutputs: 4}
	info, err := fx.adapter.AccountAddressTotal(ctx, address)
	require.NoError(t, err)
	assert.Equal(t, "4000000", info.ReceivedSum[0].Quantity)

	// Three live holdings, three distinct assets; the spent one is not read
	// by the balances.
	fx.adapter.summaryLimits = addressSummaryLimits{maxAssetRows: 2}
	_, _, err = fx.adapter.AccountAddressAssets(ctx, address, page)
	require.ErrorIs(t, err, ErrAddressSummaryTooLarge)
	_, err = fx.adapter.AccountAddressTotal(ctx, address)
	require.ErrorIs(t, err, ErrAddressSummaryTooLarge)
	fx.adapter.summaryLimits = addressSummaryLimits{maxAssetRows: 3}
	assets, total, err := fx.adapter.AccountAddressAssets(ctx, address, page)
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Len(t, assets, 3)

	fx.adapter.summaryLimits = addressSummaryLimits{maxAssets: 2}
	_, _, err = fx.adapter.AccountAddressAssets(ctx, address, page)
	require.ErrorIs(t, err, ErrAddressSummaryTooLarge)
	_, err = fx.adapter.AccountAddressTotal(ctx, address)
	require.ErrorIs(t, err, ErrAddressSummaryTooLarge)
	fx.adapter.summaryLimits = addressSummaryLimits{maxAssets: 4}
	_, err = fx.adapter.AccountAddressTotal(ctx, address)
	require.NoError(t, err)
}

func TestNodeAdapterAccountAggregateStopsOnCancelledContext(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	address := fx.addAccount(t, 0x01)
	fx.addAccountOutput(t, 0xc1, 0x01, 1_000_000, 0)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := fx.adapter.AccountAddressTotal(cancelled, address)
	require.Error(t, err)
	_, _, err = fx.adapter.AccountAddressAssets(
		cancelled, address, PaginationParams{Count: 10, Page: 1},
	)
	require.Error(t, err)
}
