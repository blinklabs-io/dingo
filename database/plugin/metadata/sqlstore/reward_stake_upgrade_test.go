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
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func rewardUpgradeFixture(t *testing.T) (*Store, *sql.Conn) {
	t.Helper()
	db, err := sql.Open(
		"sqlite",
		filepath.Join(
			t.TempDir(),
			"upgrade.sqlite",
		)+"?_pragma=synchronous(OFF)&_pragma=journal_mode(MEMORY)",
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return &Store{dialect: SQLiteDialect(), genesisStakingEmpty: true}, conn
}

func seedRewardUpgrade(t *testing.T, conn *sql.Conn) {
	t.Helper()
	for _, query := range []string{
		`CREATE TABLE pool_stake_snapshot (epoch INTEGER, snapshot_type TEXT, total_stake TEXT, calculation_version INTEGER)`,
		`CREATE TABLE reward_snapshot (epoch INTEGER, snapshot_type TEXT, total_active_stake TEXT, calculation_version INTEGER, protocol_version INTEGER)`,
		`CREATE TABLE epoch (epoch_id INTEGER, era_id INTEGER)`,
		`CREATE TABLE pparams (era_id INTEGER, cbor BLOB)`,
		`CREATE TABLE leios_transaction_context (slot INTEGER)`,
		`CREATE TABLE schema_version (version INTEGER)`,
		`INSERT INTO schema_version VALUES (33)`,
		`INSERT INTO epoch VALUES (658, 6)`,
		`INSERT INTO pool_stake_snapshot VALUES (658, 'mark', '123456789', 2), (658, 'set', '234567890', 2), (658, 'go', '345678901', 2)`,
		`INSERT INTO reward_snapshot VALUES (658, 'mark', '123456789', 2, 10)`,
	} {
		_, err := conn.ExecContext(t.Context(), query)
		require.NoError(t, err)
	}
	params := mockledger.NewMockConwayProtocolParams()
	params.ProtocolVersion.Major = 10
	raw, err := cbor.Encode(&params)
	require.NoError(t, err)
	_, err = conn.ExecContext(
		t.Context(),
		`INSERT INTO pparams VALUES (6, ?)`,
		raw,
	)
	require.NoError(t, err)
}

func TestRewardStakeUpgradeCompatibleSnapshots(t *testing.T) {
	t.Parallel()
	s, conn := rewardUpgradeFixture(t)
	seedRewardUpgrade(t, conn)
	require.NoError(t, s.preflightRewardStakeUpgrade(t.Context(), conn))
	for _, table := range []string{"pool_stake_snapshot", "reward_snapshot"} {
		var stale int
		require.NoError(
			t,
			conn.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+table+" WHERE calculation_version IS NULL OR calculation_version <> 3").
				Scan(&stale),
		)
		require.Zero(t, stale)
	}
	var amounts string
	require.NoError(
		t,
		conn.QueryRowContext(t.Context(), `SELECT group_concat(total_stake, ',') FROM (SELECT total_stake FROM pool_stake_snapshot ORDER BY snapshot_type)`).
			Scan(&amounts),
	)
	require.Equal(t, "345678901,123456789,234567890", amounts)
	var total string
	require.NoError(
		t,
		conn.QueryRowContext(t.Context(), `SELECT total_active_stake FROM reward_snapshot`).
			Scan(&total),
	)
	require.Equal(t, "123456789", total)
	var version int
	require.NoError(
		t,
		conn.QueryRowContext(t.Context(), `SELECT version FROM schema_version`).
			Scan(&version),
	)
	require.Equal(t, 33, version)
	require.NoError(t, s.preflightRewardStakeUpgrade(t.Context(), conn))
}

func TestRewardStakeUpgradeRefusalPreservesDatabase(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name              string
		query             string
		unverifiedGenesis bool
	}{
		{name: "unverified genesis", unverifiedGenesis: true},
		{name: "older provenance", query: `UPDATE reward_snapshot SET calculation_version = 1`},
		{name: "unknown provenance", query: `UPDATE reward_snapshot SET calculation_version = NULL`},
		{name: "unknown pool provenance", query: `UPDATE pool_stake_snapshot SET calculation_version = NULL`},
		{name: "future provenance", query: `UPDATE reward_snapshot SET calculation_version = 4`},
		{name: "Dijkstra epoch", query: `UPDATE epoch SET era_id = 7`},
		{name: "missing epoch", query: `DELETE FROM epoch`},
		{name: "unknown epoch", query: `UPDATE epoch SET era_id = NULL`},
		{name: "missing params", query: `DELETE FROM pparams`},
		{name: "Dijkstra params", query: `UPDATE pparams SET era_id = 7`},
		{name: "malformed earlier params", query: `UPDATE pparams SET era_id = 1, cbor = X'ff'`},
		{name: "missing earlier params", query: `UPDATE pparams SET era_id = 1, cbor = NULL`},
		{name: "malformed Conway params", query: `UPDATE pparams SET cbor = X'ff'`},
		{name: "Dijkstra snapshot", query: `UPDATE reward_snapshot SET protocol_version = 12`},
		{name: "Leios context", query: `INSERT INTO leios_transaction_context VALUES (42)`},
		{name: "failed certification", query: `CREATE TRIGGER refuse_reward_upgrade BEFORE UPDATE ON reward_snapshot BEGIN SELECT RAISE(ABORT, 'reject certification'); END`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, conn := rewardUpgradeFixture(t)
			seedRewardUpgrade(t, conn)
			if tc.query != "" {
				_, err := conn.ExecContext(t.Context(), tc.query)
				require.NoError(t, err)
			}
			before := rewardUpgradeFixtureState(t, conn)
			s.genesisStakingEmpty = !tc.unverifiedGenesis
			require.Error(t, s.preflightRewardStakeUpgrade(t.Context(), conn))
			require.Equal(
				t,
				before,
				rewardUpgradeFixtureState(t, conn),
				"refusal must preserve snapshot values, nullable provenance, protocol inputs and schema version",
			)
		})
	}
}

func TestRewardStakeUpgradeRejectsIncomingDijkstraParameters(t *testing.T) {
	t.Parallel()
	s, conn := rewardUpgradeFixture(t)
	seedRewardUpgrade(t, conn)
	params := mockledger.NewMockConwayProtocolParams()
	params.ProtocolVersion.Major = 12
	raw, err := cbor.Encode(&params)
	require.NoError(t, err)
	_, err = conn.ExecContext(t.Context(), `UPDATE pparams SET cbor = ?`, raw)
	require.NoError(t, err)
	require.ErrorContains(
		t,
		s.preflightRewardStakeUpgrade(t.Context(), conn),
		"protocol version inconsistent with era",
	)
	var version int
	require.NoError(
		t,
		conn.QueryRowContext(t.Context(), `SELECT calculation_version FROM reward_snapshot`).
			Scan(&version),
	)
	require.Equal(t, 2, version)
}

func TestRewardStakeUpgradeFreshDatabase(t *testing.T) {
	t.Parallel()
	s, conn := rewardUpgradeFixture(t)
	s.genesisStakingEmpty = false
	require.NoError(t, s.preflightRewardStakeUpgrade(t.Context(), conn))
}

func TestRewardStakeUpgradeRejectsZeroConwayProtocol(t *testing.T) {
	t.Parallel()
	s, conn := rewardUpgradeFixture(t)
	seedRewardUpgrade(t, conn)
	params := mockledger.NewMockConwayProtocolParams()
	params.ProtocolVersion.Major = 0
	raw, err := cbor.Encode(&params)
	require.NoError(t, err)
	_, err = conn.ExecContext(t.Context(), `UPDATE pparams SET cbor = ?`, raw)
	require.NoError(t, err)
	require.ErrorContains(
		t,
		s.preflightRewardStakeUpgrade(t.Context(), conn),
		"protocol version inconsistent with era",
	)
	var version int
	require.NoError(
		t,
		conn.QueryRowContext(t.Context(), `SELECT calculation_version FROM reward_snapshot`).
			Scan(&version),
	)
	require.Equal(t, 2, version)
}

func TestRewardStakeUpgradeImportedProtocolVersion(t *testing.T) {
	t.Parallel()
	s, conn := rewardUpgradeFixture(t)
	seedRewardUpgrade(t, conn)
	_, err := conn.ExecContext(
		t.Context(),
		`UPDATE reward_snapshot SET protocol_version = 0`,
	)
	require.NoError(t, err)
	require.NoError(t, s.preflightRewardStakeUpgrade(t.Context(), conn))
	var version int
	require.NoError(
		t,
		conn.QueryRowContext(t.Context(), `SELECT calculation_version FROM reward_snapshot`).
			Scan(&version),
	)
	require.Equal(t, 3, version)
}

func TestRewardStakeUpgradeRejectsTrailingParameterCBOR(t *testing.T) {
	t.Parallel()
	s, conn := rewardUpgradeFixture(t)
	seedRewardUpgrade(t, conn)
	params := mockledger.NewMockConwayProtocolParams()
	params.ProtocolVersion.Major = 10
	raw, err := cbor.Encode(&params)
	require.NoError(t, err)
	raw = append(raw, 0x00)
	_, err = conn.ExecContext(t.Context(), `UPDATE pparams SET cbor = ?`, raw)
	require.NoError(t, err)
	require.ErrorContains(
		t,
		s.preflightRewardStakeUpgrade(t.Context(), conn),
		"trailing protocol parameter CBOR",
	)
	var version int
	require.NoError(
		t,
		conn.QueryRowContext(t.Context(), `SELECT calculation_version FROM reward_snapshot`).
			Scan(&version),
	)
	require.Equal(t, 2, version)
}

func rewardUpgradeFixtureState(t *testing.T, conn *sql.Conn) []string {
	t.Helper()
	var result []string
	for _, table := range []string{"pool_stake_snapshot", "reward_snapshot", "epoch", "pparams", "leios_transaction_context", "schema_version"} {
		rows, err := conn.QueryContext(
			t.Context(),
			"SELECT * FROM "+table+" ORDER BY rowid",
		)
		require.NoError(t, err)
		columns, err := rows.Columns()
		require.NoError(t, err)
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			require.NoError(t, rows.Scan(pointers...))
			result = append(result, table+fmt.Sprint(values))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	return result
}
