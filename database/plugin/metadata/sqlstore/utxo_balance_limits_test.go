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

func balanceLimitsAddress(t *testing.T, paymentByte byte) lcommon.Address {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{paymentByte}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	return addr
}

func balanceLimitsUtxo(
	t *testing.T,
	addr lcommon.Address,
	txByte byte,
) models.Utxo {
	t.Helper()
	addrBytes, err := addr.Bytes()
	require.NoError(t, err)
	return models.Utxo{
		TxId:          bytes.Repeat([]byte{txByte}, 32),
		Address:       addrBytes,
		PaymentKey:    addr.PaymentKeyHash().Bytes(),
		PaymentScript: models.PaymentScriptFromAddress(addr),
		AddedSlot:     uint64(txByte),
		Amount:        types.Uint64(1_000_000),
	}
}

// The read stops at the first output past MaxUtxos. A trailing output whose
// stored amount cannot be parsed is never reached, which a read that scanned
// every output before checking the count would report instead.
func TestGetUtxoBalanceByAddressStopsAtUtxoLimit(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	addr := balanceLimitsAddress(t, 0x31)
	require.NoError(t, store.ImportUtxos([]models.Utxo{
		balanceLimitsUtxo(t, addr, 0x01),
		balanceLimitsUtxo(t, addr, 0x02),
		balanceLimitsUtxo(t, addr, 0x03),
	}, nil))
	_, err := store.writeDB.Exec(
		`INSERT INTO utxo (tx_id, payment_key, payment_script, credential_tag,
    added_slot, deleted_slot, amount, output_idx)
VALUES (?, ?, ?, 0, 4, 0, 'unparseable', 0)`,
		bytes.Repeat([]byte{0x04}, 32),
		addr.PaymentKeyHash().Bytes(),
		models.PaymentScriptFromAddress(addr),
	)
	require.NoError(t, err)

	_, err = store.GetUtxoBalanceByAddress(
		addr,
		models.UtxoAddressMatchPaymentCred,
		models.AddressBalanceLimits{MaxUtxos: 2},
		nil,
	)
	require.ErrorIs(t, err, models.ErrAddressBalanceLimit)

	// Without a limit the whole set is read, so the bad row is reached.
	_, err = store.GetUtxoBalanceByAddress(
		addr,
		models.UtxoAddressMatchPaymentCred,
		models.AddressBalanceLimits{},
		nil,
	)
	require.ErrorContains(t, err, "utxo amount")
}

// The read stops at the first distinct asset past MaxAssets. Assets arrive
// in (policy id, name) order, so an asset row that sorts after the limit and
// cannot be parsed is never reached.
func TestGetUtxoBalanceByAddressStopsAtAssetLimit(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	addr := balanceLimitsAddress(t, 0x41)
	policy := bytes.Repeat([]byte{0x42}, lcommon.AddressHashSize)
	held := balanceLimitsUtxo(t, addr, 0x05)
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

	_, err = store.GetUtxoBalanceByAddress(
		addr,
		models.UtxoAddressMatchPaymentCred,
		models.AddressBalanceLimits{MaxAssets: 3},
		nil,
	)
	require.ErrorIs(t, err, models.ErrAddressBalanceLimit)

	_, err = store.GetUtxoBalanceByAddress(
		addr,
		models.UtxoAddressMatchPaymentCred,
		models.AddressBalanceLimits{},
		nil,
	)
	require.ErrorContains(t, err, "asset amount")
}

func TestGetUtxoBalanceByAddressWithinLimits(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	addr := balanceLimitsAddress(t, 0x51)
	policy := bytes.Repeat([]byte{0x52}, lcommon.AddressHashSize)
	first := balanceLimitsUtxo(t, addr, 0x06)
	first.Assets = []models.Asset{
		{PolicyId: policy, Name: []byte("b"), Amount: types.Uint64(2)},
	}
	second := balanceLimitsUtxo(t, addr, 0x07)
	second.Assets = []models.Asset{
		{PolicyId: policy, Name: []byte("a"), Amount: types.Uint64(1)},
		{PolicyId: policy, Name: []byte("b"), Amount: types.Uint64(3)},
	}
	require.NoError(t, store.ImportUtxos([]models.Utxo{first, second}, nil))

	got, err := store.GetUtxoBalanceByAddress(
		addr,
		models.UtxoAddressMatchPaymentCred,
		models.AddressBalanceLimits{MaxUtxos: 2, MaxAssets: 2},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, models.AddressBalance{
		Lovelace:  2_000_000,
		UtxoCount: 2,
		Assets: []models.AssetBalance{
			{PolicyId: policy, Name: []byte("a"), Amount: 1},
			{PolicyId: policy, Name: []byte("b"), Amount: 5},
		},
	}, got)
}
