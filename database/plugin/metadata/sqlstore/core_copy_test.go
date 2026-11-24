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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConsensusCopyCarriesCoreModeState(t *testing.T) {
	t.Parallel()
	source, _ := newManagementTestStoreWithRawDB(t)
	dest, _ := newManagementTestStoreWithRawDB(t)
	testConsensusCopyCarriesCoreModeState(t, source, dest)
}

func testConsensusCopyCarriesCoreModeState(
	t *testing.T,
	source, dest *Store,
) {
	t.Helper()
	raw, target := source.writeDB, dest.writeDB
	ctx := context.Background()
	if _, err := raw.Exec("DELETE FROM sync_state"); err != nil {
		t.Fatal(err)
	}
	_, err := raw.Exec(
		`INSERT INTO node_settings (id,storage_mode,network) VALUES (1,'api','preview')`,
	)
	require.NoError(t, err)
	_, err = raw.Exec(
		source.dialect.Rebind(
			`INSERT INTO tip(id,hash,slot,block_number) VALUES(1,?,1234,55)`,
		),
		[]byte{1, 2, 3},
	)
	require.NoError(t, err)
	_, err = raw.Exec(
		`INSERT INTO commit_timestamp(id,timestamp) VALUES(1,9876)`,
	)
	require.NoError(t, err)
	_, err = raw.Exec(
		source.dialect.Rebind(
			`INSERT INTO datum(id,hash,raw_datum,added_slot) VALUES(1,?,?,12)`,
		),
		[]byte{1},
		[]byte{2},
	)
	require.NoError(t, err)
	// A core-mode epoch boundary reads the MIR certificates of its epoch.
	for i := 1; i <= coreCopyBatchRows+2; i++ {
		_, err = raw.Exec(
			source.dialect.Rebind(
				`INSERT INTO move_instantaneous_rewards(id,pot,certificate_id,added_slot,other_pot) VALUES(?,0,?,?,'0')`,
			),
			i, i, 100+i,
		)
		require.NoError(t, err)
	}
	_, err = raw.Exec(
		source.dialect.Rebind(
			`INSERT INTO move_instantaneous_rewards_reward(id,credential,credential_tag,amount,mir_id) VALUES(1,?,0,'5',2)`,
		),
		[]byte{7},
	)
	require.NoError(t, err)
	_, err = raw.Exec(
		source.dialect.Rebind(
			`INSERT INTO address_transaction(id,payment_key,transaction_id,slot,tx_index) VALUES(1,?,1,12,0)`,
		),
		[]byte{9},
	)
	require.NoError(t, err)
	_, err = raw.Exec(
		`INSERT INTO epoch(id,epoch_id,start_slot,era_id,slot_length,length_in_slots) VALUES(1,4,1000,1,1000,432000)`,
	)
	require.NoError(t, err)
	// A table without a single-column key, larger than one batch.
	for i := 0; i <= coreCopyBatchRows; i++ {
		_, err = raw.Exec(
			source.dialect.Rebind(
				`INSERT INTO account_inactivity_activation(credential_tag,staking_key) VALUES(0,?)`,
			),
			[]byte{byte(i >> 8), byte(i)},
		)
		require.NoError(t, err)
	}
	require.NoError(t, source.SetSyncState("zz:family-key", "x", nil))
	for i := range coreCopyBatchRows + 3 {
		require.NoError(
			t,
			source.SetSyncState(
				fmt.Sprintf("state:%08d", i),
				fmt.Sprintf("value:%d", i),
				nil,
			),
		)
	}
	injected := errors.New("interrupted after durable batch")
	err = source.CopyConsensusStateTo(ctx, dest, func(table string) error {
		if table == "sync_state" {
			return injected
		}
		return nil
	})
	require.ErrorIs(t, err, injected)
	var count int
	require.NoError(
		t,
		target.QueryRow(`SELECT count(*) FROM sync_state WHERE sync_key LIKE 'state:%'`).
			Scan(&count),
	)
	require.Equal(t, coreCopyBatchRows, count)
	require.NoError(t, source.CopyConsensusStateTo(ctx, dest, nil))
	require.NoError(
		t,
		target.QueryRow(`SELECT count(*) FROM sync_state WHERE sync_key LIKE 'state:%'`).
			Scan(&count),
	)
	require.Equal(t, coreCopyBatchRows+3, count)
	for table, want := range map[string]int{
		"datum":                             1,
		"move_instantaneous_rewards":        coreCopyBatchRows + 2,
		"move_instantaneous_rewards_reward": 1,
		"address_transaction":               0,
		"epoch":                             1,
		"account_inactivity_activation":     coreCopyBatchRows + 1,
	} {
		require.NoError(
			t,
			target.QueryRow(`SELECT count(*) FROM `+dest.dialect.QuoteIdentifier(table)).Scan(&count),
		)
		require.Equal(t, want, count, table)
	}
	require.NoError(t, target.QueryRow(
		`SELECT count(*) FROM sync_state WHERE sync_key = 'zz:family-key'`,
	).Scan(&count))
	require.Equal(t, 1, count, "a ledger-state family's sync state is copied")
	effects, err := dest.GetMIRCertsInSlotRange(100, 1000, nil)
	require.NoError(t, err)
	require.Len(t, effects, coreCopyBatchRows+2)
	// The destination's identity sequences continue past the copied keys.
	_, err = target.Exec(
		`INSERT INTO move_instantaneous_rewards(pot,certificate_id,added_slot,other_pot) VALUES(0,0,999,'0')`,
	)
	require.NoError(t, err)
	require.NoError(t, dest.FinishConsensusSettingsCopy(ctx, 4, 1234))
	settings, err := dest.GetNodeSettings(ctx)
	require.NoError(t, err)
	require.Equal(t, "core", settings.StorageMode)
	original, err := source.GetNodeSettings(ctx)
	require.NoError(t, err)
	require.Equal(t, "api", original.StorageMode)
	complete, err := dest.GetSyncState(CoreCopyCompleteKey, nil)
	require.NoError(t, err)
	require.Equal(t, "1", complete)
}

func TestConsensusCopyBatchAndCursorAreAtomic(t *testing.T) {
	t.Parallel()
	source, _ := newManagementTestStoreWithRawDB(t)
	dest, target := newManagementTestStoreWithRawDB(t)
	for _, key := range []string{"a", "b", "c"} {
		require.NoError(t, source.SetSyncState(key, key, nil))
	}
	_, err := target.Exec(
		`CREATE TRIGGER refuse_partial_copy BEFORE INSERT ON sync_state WHEN NEW.sync_key = 'b' BEGIN SELECT RAISE(ABORT,'injected batch failure'); END`,
	)
	require.NoError(t, err)
	require.ErrorContains(
		t,
		source.CopyConsensusStateTo(context.Background(), dest, nil),
		"injected batch failure",
	)
	var count int
	require.NoError(
		t,
		target.QueryRow(`SELECT count(*) FROM sync_state WHERE sync_key IN ('a','b','c')`).
			Scan(&count),
	)
	require.Zero(
		t,
		count,
		"a failed batch must leave no rows or advanced cursor",
	)
	cursor, err := dest.GetSyncState(coreCopyPrefix+"cursor:sync_state", nil)
	require.NoError(t, err)
	require.Empty(t, cursor)
	_, err = target.Exec(`DROP TRIGGER refuse_partial_copy`)
	require.NoError(t, err)
	require.NoError(
		t,
		source.CopyConsensusStateTo(context.Background(), dest, nil),
	)
	require.NoError(
		t,
		target.QueryRow(`SELECT count(*) FROM sync_state WHERE sync_key IN ('a','b','c')`).
			Scan(&count),
	)
	require.Equal(t, 3, count)
}
