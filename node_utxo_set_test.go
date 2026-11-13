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

package dingo

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestUtxoSetOnlyModeIsFixedForADatabase marks a fresh database when it
// starts with the live UTxO set as the only copy, then requires it to refuse
// a start without that mode, and requires a synced database without the mark
// to refuse switching to it.
func TestUtxoSetOnlyModeIsFixedForADatabase(t *testing.T) {
	t.Parallel()
	node := func(db *database.Database, only bool) *Node {
		n := &Node{db: db}
		n.config.ledgerUtxoBadgerOnly = only
		return n
	}

	fresh, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(fresh) })
	require.NoError(t, node(fresh, true).checkUtxoSetOnly())
	require.NoError(t, node(fresh, true).checkUtxoSetOnly(), "already marked")
	require.Error(t, node(fresh, false).checkUtxoSetOnly())

	synced, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(synced) })
	require.NoError(t, synced.SetTip(ochainsync.Tip{
		Point: ocommon.Point{Slot: 10, Hash: bytes.Repeat([]byte{1}, 32)},
	}, nil))
	require.NoError(t, node(synced, false).checkUtxoSetOnly())
	require.Error(t, node(synced, true).checkUtxoSetOnly())
}

// TestUtxoSetOnlyStakeRebuildWaitsForTheSet requires the startup
// reward_live_stake rebuild, with the live UTxO set as the only copy, to wait
// for reconcileUtxoSet and then to read UTxO stake from the set.
func TestUtxoSetOnlyStakeRebuildWaitsForTheSet(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	stakeKey := bytes.Repeat([]byte{0x44}, 28)
	require.NoError(t, db.SetLiveUtxoSource(oneStakeSource{key: stakeKey, lovelace: 42}))
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	// A row from an older calculation makes the startup check rebuild.
	_, err = raw.Exec(`INSERT INTO reward_live_stake (staking_key,
credential_tag, utxo_stake, reward_stake, total_stake, registered,
updated_slot, calculation_version) VALUES (?, 0, '1', '0', '1', 0, 0, 0)`,
		bytes.Repeat([]byte{0x55}, 28))
	require.NoError(t, err)
	rows := func() []string {
		r, err := raw.Query(`SELECT staking_key, utxo_stake FROM reward_live_stake ORDER BY 1`)
		require.NoError(t, err)
		defer r.Close()
		var out []string
		for r.Next() {
			var key []byte
			var stake string
			require.NoError(t, r.Scan(&key, &stake))
			out = append(out, fmt.Sprintf("%x=%s", key, stake))
		}
		require.NoError(t, r.Err())
		return out
	}
	before := rows()

	n := &Node{db: db}
	n.config.ledgerUtxoBadgerOnly = true
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	require.NoError(t, n.backfillRewardLiveStake(t.Context()))
	require.Equal(t, before, rows(), "rebuilt before the set was reconciled")

	require.NoError(t, n.reconcileUtxoSet(t.Context()))
	require.Equal(t, []string{fmt.Sprintf("%x=42", stakeKey)}, rows())
}

// oneStakeSource is a live UTxO set holding lovelace for one credential with
// no account.
type oneStakeSource struct {
	key      []byte
	lovelace uint64
}

func (oneStakeSource) LiveUtxoCbor(types.Txn, []byte, uint32) ([]byte, bool, error) {
	return nil, false, nil
}

func (s oneStakeSource) LiveUtxoStake(_ types.Txn, tag uint8, key []byte) (uint64, error) {
	if tag == 0 && bytes.Equal(key, s.key) {
		return s.lovelace, nil
	}
	return 0, nil
}

func (oneStakeSource) LivePointerStakes(types.Txn) ([]types.PointerStake, error) {
	return nil, nil
}

func (s oneStakeSource) EachLiveUtxoStake(
	_ types.Txn,
	fn func(uint8, []byte, uint64) error,
) error {
	return fn(0, s.key, s.lovelace)
}
