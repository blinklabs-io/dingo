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

package statedrain_test

import (
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/stretchr/testify/require"
)

func TestTripGuardRefusesReadsOfDrainedTables(t *testing.T) {
	t.Parallel()
	guard := statedrain.NewTripGuard()
	require.NoError(t, guard.CheckStatement("SELECT * FROM block_nonce"))
	guard.Arm(nodesettings.FamilyNonces, []string{"block_nonce"})

	for _, q := range []string{
		"SELECT * FROM block_nonce WHERE slot > ?",
		"select x from `block_nonce` b",
		"SELECT 1 FROM epoch e JOIN \"block_nonce\" n ON n.id = e.id",
		"INSERT INTO other SELECT id FROM block_nonce",
		"DELETE FROM other WHERE id IN (SELECT id FROM block_nonce)",
		"WITH w AS (SELECT * FROM block_nonce) SELECT * FROM w",
	} {
		require.ErrorIs(
			t, guard.CheckStatement(q), statedrain.ErrDrainedRead, q,
		)
	}
	require.Len(t, guard.Trips(), 6)
	require.Equal(t, nodesettings.FamilyNonces, guard.Trips()[0].Family)
	require.Equal(t, "block_nonce", guard.Trips()[0].Table)
}

func TestTripGuardAllowsStatementsThatDoNotReadDrainedTables(t *testing.T) {
	t.Parallel()
	guard := statedrain.NewTripGuard()
	guard.Arm(nodesettings.FamilyNonces, []string{"block_nonce"})
	for _, q := range []string{
		"SELECT * FROM block_nonce_other",
		"SELECT * FROM epoch",
		"INSERT INTO sync_state (key) VALUES (?)",
	} {
		require.NoError(t, guard.CheckStatement(q), q)
	}
	require.Empty(t, guard.Trips())
}

func TestTripGuardRefusesWritesToDrainedTables(t *testing.T) {
	t.Parallel()
	guard := statedrain.NewTripGuard()
	guard.Arm(nodesettings.FamilyNonces, []string{"block_nonce"})
	for _, q := range []string{
		"DELETE FROM block_nonce WHERE slot > ?",
		"DELETE FROM \"block_nonce\"",
		"INSERT INTO block_nonce (slot) VALUES (?)",
		"INSERT OR REPLACE INTO `block_nonce` (slot) VALUES (?)",
		"INSERT INTO \"main\".\"block_nonce\" (slot) VALUES (?) " +
			"ON CONFLICT (slot) DO UPDATE SET nonce = excluded.nonce",
		"UPDATE block_nonce SET is_checkpoint = 1 WHERE slot = ?",
		"update \"block_nonce\" set nonce = ?",
		"TRUNCATE TABLE block_nonce",
		"INSERT INTO other SELECT * FROM public.block_nonce",
	} {
		err := guard.CheckStatement(q)
		require.Error(t, err, q)
	}
	trips := guard.Trips()
	require.Len(t, trips, 9)
	for _, trip := range trips[:8] {
		require.True(t, trip.Write, trip.Query)
	}
	require.False(t, trips[8].Write, "a read feeding another table's insert")
	require.ErrorIs(
		t,
		guard.CheckStatement("UPDATE block_nonce SET nonce = ?"),
		statedrain.ErrDrainedWrite,
	)
	require.NotErrorIs(
		t,
		guard.CheckStatement("UPDATE block_nonce SET nonce = ?"),
		statedrain.ErrDrainedRead,
	)
}

func TestTripGuardAllowsWritesToOtherTablesAndTheDrainDelete(t *testing.T) {
	t.Parallel()
	guard := statedrain.NewTripGuard()
	guard.Arm(nodesettings.FamilyNonces, []string{"block_nonce"})
	for _, q := range []string{
		"DELETE FROM epoch WHERE start_slot > ?",
		"INSERT INTO block_nonce_other (slot) VALUES (?)",
		"UPDATE sync_state SET value = ? WHERE sync_key = ?",
		nodesettings.DrainStatementTag +
			" SELECT \"id\" FROM \"block_nonce\" ORDER BY \"id\" LIMIT 1 OFFSET 4",
		nodesettings.DrainStatementTag + " DELETE FROM \"block_nonce\"",
		nodesettings.DrainStatementTag +
			" DELETE FROM \"block_nonce\" WHERE \"id\" <= ?",
	} {
		require.NoError(t, guard.CheckStatement(q), q)
	}
	require.Empty(t, guard.Trips())
}

func TestTablesNamesTheGraduatedFamilies(t *testing.T) {
	t.Parallel()
	require.Equal(
		t, []string{"block_nonce"},
		statedrain.TableNames(nodesettings.FamilyNonces),
	)
	require.Equal(
		t, []string{"network_donation", "network_state"},
		statedrain.TableNames(nodesettings.FamilyNetworkDonations),
	)
	require.Equal(
		t, []string{"reward_live_stake"},
		statedrain.TableNames(nodesettings.FamilyLiveStake),
	)
	require.Equal(
		t, []string{"pparam_update", "pparams"},
		statedrain.TableNames(nodesettings.FamilyPParamsUpdates),
	)
	require.Equal(
		t, []string{"genesis_delegation"},
		statedrain.TableNames(nodesettings.FamilyMIRGenesis),
	)
	require.Equal(
		t, []string{"epoch"},
		statedrain.TableNames(nodesettings.FamilyEpochs),
	)
	require.Equal(
		t, []string{"epoch_summary"},
		statedrain.TableNames(nodesettings.FamilySummaries),
	)
}

func TestDrainedTableInFindsWholeIdentifiersOfDrainedFamilies(t *testing.T) {
	t.Parallel()
	mask := nodesettings.FamilyMask(0).With(nodesettings.FamilyNonces)
	for _, q := range []string{
		"SELECT * FROM block_nonce",
		"select * from \"BLOCK_NONCE\" b",
		"SELECT 1 FROM epoch e, block_nonce n",
		"SELECT 1 FROM main.block_nonce",
		"SELECT 1 FROM (SELECT * FROM block_nonce)",
	} {
		family, table, ok := statedrain.DrainedTableIn(mask, q)
		require.True(t, ok, q)
		require.Equal(t, nodesettings.FamilyNonces, family)
		require.Equal(t, "block_nonce", table)
	}
	for _, q := range []string{
		"SELECT * FROM block_nonce_other",
		"SELECT block_nonce_id FROM epoch",
		"SELECT * FROM network_state",
	} {
		_, _, ok := statedrain.DrainedTableIn(mask, q)
		require.False(t, ok, q)
	}
	_, _, ok := statedrain.DrainedTableIn(0, "SELECT * FROM block_nonce")
	require.False(t, ok, "nothing drained")
}

func TestTripGuardRefusesTheProtocolParameterStatements(t *testing.T) {
	t.Parallel()
	guard := statedrain.NewTripGuard()
	guard.Arm(
		nodesettings.FamilyPParamsUpdates,
		statedrain.TableNames(nodesettings.FamilyPParamsUpdates),
	)
	for _, q := range []string{
		"SELECT cbor, id FROM pparams WHERE epoch <= ? ORDER BY epoch DESC",
		"SELECT era_id, cbor FROM pparams WHERE added_slot <= ? LIMIT 1",
		"SELECT genesis_hash FROM pparam_update WHERE epoch = ? OR epoch = ?",
		"INSERT INTO pparams (cbor, added_slot, epoch, era_id) VALUES (?, ?, ?, ?)",
		"INSERT INTO pparam_update (genesis_hash, cbor) VALUES (?, ?)",
		"UPDATE pparams SET cbor = ? WHERE id = ?",
		"DELETE FROM pparams WHERE added_slot > ?",
		"DELETE FROM pparam_update WHERE added_slot > ?",
	} {
		require.Error(t, guard.CheckStatement(q), q)
	}
	require.Len(t, guard.Trips(), 8)
	require.NoError(t, guard.CheckStatement("SELECT * FROM pparams_other"))
	require.NoError(t, guard.CheckStatement(
		"SELECT value FROM sync_state WHERE sync_key = ?",
	))
}

// The epoch table shares its name with an epoch column of other tables and a
// prefix with epoch_summary, so only a statement that names the table itself
// trips the guard.
func TestTripGuardRefusesTheEpochTablesAndNothingNamedLikeThem(t *testing.T) {
	t.Parallel()
	guard := statedrain.NewTripGuard()
	for _, family := range []nodesettings.Family{
		nodesettings.FamilyEpochs, nodesettings.FamilySummaries,
	} {
		guard.Arm(family, statedrain.TableNames(family))
	}
	reads := []string{
		"SELECT era_id FROM epoch WHERE start_slot <= ? LIMIT 1",
		"SELECT 1 FROM pool_retirement rt JOIN epoch e ON e.epoch_id = rt.epoch",
		"SELECT total_active_stake FROM epoch_summary WHERE epoch = ?",
		"SELECT 1 FROM pool p LEFT JOIN epoch_summary es ON es.epoch = p.id",
	}
	writes := []string{
		"INSERT INTO epoch (epoch_id) VALUES (?)",
		"DELETE FROM epoch WHERE start_slot > ?",
		"INSERT INTO epoch_summary (epoch) VALUES (?)",
		"DELETE FROM epoch_summary WHERE epoch > ?",
	}
	for _, q := range append(slices.Clone(reads), writes...) {
		require.Error(t, guard.CheckStatement(q), q)
	}
	require.Len(t, guard.Trips(), len(reads)+len(writes))
	for _, q := range []string{
		"SELECT epoch FROM pool_retirement WHERE epoch > ?",
		"SELECT * FROM epoch_other",
	} {
		require.NoError(t, guard.CheckStatement(q), q)
	}
	require.Len(t, guard.Trips(), len(reads)+len(writes))

	_, table, ok := statedrain.DrainedTableIn(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyEpochs),
		"SELECT max(epoch_id) FROM epoch",
	)
	require.True(t, ok)
	require.Equal(t, "epoch", table)
}
