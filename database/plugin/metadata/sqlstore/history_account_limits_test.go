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

package sqlstore

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func accountLimitsUtxo(
	t *testing.T,
	txByte byte,
	stakingKey []byte,
) models.Utxo {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x50}, lcommon.AddressHashSize),
		stakingKey,
	)
	require.NoError(t, err)
	addrBytes, err := addr.Bytes()
	require.NoError(t, err)
	return models.Utxo{
		TxId:       bytes.Repeat([]byte{txByte}, 32),
		Address:    addrBytes,
		PaymentKey: addr.PaymentKeyHash().Bytes(),
		StakingKey: stakingKey,
		AddedSlot:  uint64(txByte),
		Amount:     types.Uint64(1_000_000),
	}
}

// The lovelace sums stop at the first row past MaxUtxos. A trailing row with
// an unparseable amount is never reached, which a read that scanned every row
// before counting would report instead.
func TestGetAccountAddressTotalsStopsAtUtxoLimit(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	stakingKey := bytes.Repeat([]byte{0x51}, 28)
	require.NoError(t, store.ImportUtxos([]models.Utxo{
		accountLimitsUtxo(t, 0x01, stakingKey),
		accountLimitsUtxo(t, 0x02, stakingKey),
		accountLimitsUtxo(t, 0x03, stakingKey),
	}, nil))
	_, err := store.writeDB.Exec(
		`INSERT INTO utxo (tx_id, staking_key, credential_tag,
    added_slot, deleted_slot, amount, output_idx)
VALUES (?, ?, 0, 4, 0, 'unparseable', 0)`,
		bytes.Repeat([]byte{0x04}, 32),
		stakingKey,
	)
	require.NoError(t, err)

	_, err = store.GetAccountAddressTotals(
		0, stakingKey, models.AccountAggregateLimits{MaxUtxos: 3}, nil,
	)
	require.ErrorIs(t, err, models.ErrAccountAggregateLimit)

	_, err = store.GetAccountAddressTotals(
		0, stakingKey, models.AccountAggregateLimits{}, nil,
	)
	require.ErrorContains(t, err, "amount")
}

func accountAssetUtxo(
	t *testing.T,
	store *Store,
	stakingKey []byte,
) {
	t.Helper()
	policy := bytes.Repeat([]byte{0x52}, 28)
	held := accountLimitsUtxo(t, 0x05, stakingKey)
	for _, name := range []string{"a", "b", "c", "d"} {
		held.Assets = append(held.Assets, models.Asset{
			PolicyId: policy,
			Name:     []byte(name),
			Amount:   types.Uint64(1),
		})
	}
	require.NoError(t, store.ImportUtxos([]models.Utxo{held}, nil))
	_, err := store.writeDB.Exec(
		`INSERT INTO asset (name, policy_id, utxo_id, amount)
SELECT ?, ?, id, 'unparseable' FROM utxo WHERE tx_id = ?`,
		[]byte("z"),
		policy,
		held.TxId,
	)
	require.NoError(t, err)
}

// The asset scans stop at the first holding past MaxAssetRows and, ordered by
// asset, at the first distinct asset past MaxAssets; the unparseable row that
// sorts last is never reached.
func TestGetAccountAssetBalancesStopsAtLimits(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	stakingKey := bytes.Repeat([]byte{0x53}, 28)
	accountAssetUtxo(t, store, stakingKey)

	for name, limits := range map[string]models.AccountAggregateLimits{
		"rows":   {MaxAssetRows: 4},
		"assets": {MaxAssets: 3},
	} {
		_, _, err := store.GetAccountAssetBalances(
			0, stakingKey, 10, 0, "asc", limits, nil,
		)
		require.ErrorIs(t, err, models.ErrAccountAggregateLimit, name)
	}

	_, _, err := store.GetAccountAssetBalances(
		0, stakingKey, 10, 0, "asc", models.AccountAggregateLimits{}, nil,
	)
	require.ErrorContains(t, err, "asset amount")
}

func TestGetAccountAddressTotalsStopsAtAssetLimits(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	stakingKey := bytes.Repeat([]byte{0x54}, 28)
	accountAssetUtxo(t, store, stakingKey)

	for name, limits := range map[string]models.AccountAggregateLimits{
		"rows":   {MaxAssetRows: 4},
		"assets": {MaxAssets: 3},
	} {
		_, err := store.GetAccountAddressTotals(0, stakingKey, limits, nil)
		require.ErrorIs(t, err, models.ErrAccountAggregateLimit, name)
	}
}

func TestGetAccountAggregatesWithinLimits(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	stakingKey := bytes.Repeat([]byte{0x55}, 28)
	policy := bytes.Repeat([]byte{0x56}, 28)
	held := accountLimitsUtxo(t, 0x06, stakingKey)
	held.Assets = []models.Asset{
		{PolicyId: policy, Name: []byte("a"), Amount: 2},
		{PolicyId: policy, Name: []byte("b"), Amount: 3},
	}
	require.NoError(t, store.ImportUtxos([]models.Utxo{held}, nil))
	limits := models.AccountAggregateLimits{
		MaxUtxos: 1, MaxAssetRows: 2, MaxAssets: 2,
	}

	balances, total, err := store.GetAccountAssetBalances(
		0, stakingKey, 10, 0, "asc", limits, nil,
	)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, balances, 2)

	totals, err := store.GetAccountAddressTotals(0, stakingKey, limits, nil)
	require.NoError(t, err)
	require.Equal(t, "1000000", totals.ReceivedLovelace.String())
	require.Len(t, totals.ReceivedAssets, 2)
}
