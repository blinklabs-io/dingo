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
	"context"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// GetPoolDepositsHeld is checked against registrations written through the
// certificate path, because the carry-forward it reports is decided there.

// A re-registration of a still-registered pool carries the first deposit
// forward, so the latest registration reports the first deposit, not its own
// certificate's charge.
func TestGetPoolDepositsHeldCarriesForwardAcrossReRegistration(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 5)
	carried := depositHeldPoolKey(0xc1)
	single := depositHeldPoolKey(0xc2)
	absent := depositHeldPoolKey(0xc3)

	writeDepositHeldCert(t, store, 100, 0, depositHeldRegistration(carried), 500)
	writeDepositHeldCert(t, store, 1_100, 0, depositHeldRegistration(carried), 700)
	writeDepositHeldCert(t, store, 2_100, 0, depositHeldRegistration(carried), 900)
	writeDepositHeldCert(t, store, 150, 0, depositHeldRegistration(single), 600)

	got, err := store.GetPoolDepositsHeld(
		[]lcommon.PoolKeyHash{carried, single, absent}, nil,
	)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{
		string(carried.Bytes()): 500,
		string(single.Bytes()):  600,
	}, got, "the unregistered pool is absent")

	only, err := store.GetPoolDepositsHeld(
		[]lcommon.PoolKeyHash{single}, nil,
	)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{string(single.Bytes()): 600}, only)

	none, err := store.GetPoolDepositsHeld(nil, nil)
	require.NoError(t, err)
	require.Empty(t, none)
}

// A registration after a reap is a first registration again and holds the
// amount in force at its own slot.
func TestGetPoolDepositsHeldAfterReapReportsNewDeposit(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 5)
	pool := depositHeldPoolKey(0xc4)

	writeDepositHeldCert(t, store, 100, 0, depositHeldRegistration(pool), 500)
	writeDepositHeldCert(t, store, 200, 0, depositHeldRetirement(pool, 1), 0)
	writeDepositHeldCert(t, store, 1_500, 0, depositHeldRegistration(pool), 800)

	got, err := store.GetPoolDepositsHeld([]lcommon.PoolKeyHash{pool}, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(800), got[string(pool.Bytes())])
}

// A row with no deposit_held reports its deposit_amount, and a row with
// neither is left out of the map rather than reported as zero.
func TestGetPoolDepositsHeldFallsBackToDepositAmount(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 5)
	fallback := depositHeldPoolKey(0xc5)
	unknown := depositHeldPoolKey(0xc6)

	writeDepositHeldCert(t, store, 100, 0, depositHeldRegistration(fallback), 500)
	_, err := store.writeDB.Exec(
		"UPDATE pool_registration SET deposit_held = NULL WHERE pool_key_hash = ?",
		fallback.Bytes(),
	)
	require.NoError(t, err)
	writeDepositHeldCertUnknown(t, store, 100, 1, depositHeldRegistration(unknown))

	got, err := store.GetPoolDepositsHeld(
		[]lcommon.PoolKeyHash{fallback, unknown}, nil,
	)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{
		string(fallback.Bytes()): 500,
	}, got, "an unknown deposit is absent, not zero")
}

// A cancelled transaction context stops the read before it walks the chunks.
func TestGetPoolDepositsHeldHonoursContext(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 5)
	pools := make([]lcommon.PoolKeyHash, 0, 2*activeDelegatorChunkSize+1)
	for i := range 2*activeDelegatorChunkSize + 1 {
		pools = append(pools, depositHeldPoolKey(byte(i)))
	}
	writeDepositHeldCert(t, store, 100, 0, depositHeldRegistration(pools[0]), 500)

	ctx, cancel := context.WithCancel(t.Context())
	txn := store.ReadTransaction(ctx)
	defer txn.Rollback() //nolint:errcheck
	cancel()

	_, err := store.GetPoolDepositsHeld(pools, txn)
	require.ErrorIs(t, err, context.Canceled)
}
