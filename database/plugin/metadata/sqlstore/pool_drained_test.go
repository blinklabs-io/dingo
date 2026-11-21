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
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

var drainedPoolTables = []string{
	"pool", "pool_registration", "pool_registration_owner",
	"pool_retirement",
}

func tableCount(t *testing.T, store *Store, table string) int {
	t.Helper()
	var n int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM "+table,
	).Scan(&n))
	return n
}

func writePoolCerts(
	t *testing.T,
	store *Store,
	pool lcommon.PoolKeyHash,
	slot uint64,
) {
	t.Helper()
	writeDepositHeldCert(t, store, slot, 0, depositHeldRegistration(pool), 500)
	writeDepositHeldCert(
		t, store, slot+1, 0, depositHeldRetirement(pool, 3), 0,
	)
}

func requirePoolTables(
	t *testing.T,
	store *Store,
	tables []string,
	positive bool,
) {
	t.Helper()
	for _, table := range tables {
		if positive {
			require.Positive(t, tableCount(t, store, table), table)
		} else {
			require.Zero(t, tableCount(t, store, table), table)
		}
	}
}

// With the pools-history family drained the certificate and genesis writes
// leave the pool tables alone, and the certificate rows themselves stay.
func TestDrainedPoolsSkipCertificateAndGenesisWrites(t *testing.T) {
	t.Parallel()
	for _, drained := range []bool{false, true} {
		store := newDepositHeldStore(t)
		depositHeldEpochs(t, store, 4)
		if drained {
			store.SetDrainedFamilies(
				nodesettings.FamilyMask(0).
					With(nodesettings.FamilyPoolsHistory),
			)
		}
		pool := depositHeldPoolKey(0xd1)
		writePoolCerts(t, store, pool, 100)
		genesis := depositHeldPoolKey(0xd2)
		require.NoError(t, store.SetGenesisStakingWithDeposits(
			map[string]lcommon.PoolRegistrationCertificate{
				hex.EncodeToString(genesis.Bytes()): *depositHeldRegistration(genesis),
			},
			map[string]string{}, 2_000_000, 500, nil, nil,
		))

		requirePoolTables(t, store, drainedPoolTables, !drained)
		require.Equal(
			t,
			2,
			tableCount(t, store, "certs"),
			"drained=%v",
			drained,
		)
	}
}

// With the family drained the rollback deletes and the op-cert counter update
// leave the pool tables alone; the op-cert rows above the rollback slot are
// still deleted, since they belong to another family.
func TestDrainedPoolsSkipRollbackAndOpCertPoolWrites(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 4)
	pool := depositHeldPoolKey(0xd3)
	writePoolCerts(t, store, pool, 100)
	store.SetDrainedFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyPoolsHistory),
	)

	require.NoError(t, store.UpdatePoolOpCertSequence(pool, 7, 150, nil))
	var latest int64
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT latest_op_cert_sequence FROM pool",
	).Scan(&latest))
	require.Equal(
		t, encodeOpCertSequence(0), latest,
		"the pool row keeps no counter once drained",
	)
	require.Equal(t, 1, tableCount(t, store, "pool_opcert_sequence"))

	require.NoError(t, store.RestorePoolStateAtSlot(50, nil))
	require.NoError(t, store.DeleteCertificatesAfterSlot(50, nil))

	requirePoolTables(t, store, drainedPoolTables, true)
	require.Zero(t, tableCount(t, store, "pool_opcert_sequence"))
	require.Zero(t, tableCount(t, store, "certs"))
}
