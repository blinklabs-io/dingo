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
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	stakeWALCreds     = 1500
	stakeWALPools     = 30
	stakeWALBlocks    = 30
	stakeWALPerBlock  = 20
	stakeWALBoundary1 = 100
	stakeWALBoundary2 = 101
)

type stakeWALResult struct {
	syncFrames     map[string]int
	boundaryFrames map[string]int
	redelegations  int
	digest         string
	indexes        map[string]bool
}

func runStakeWALWorkload(t *testing.T) stakeWALResult {
	t.Helper()
	store, path := newWALSQLiteStore(t)
	w := newStakeWorkload(t, store, stakeWALCreds, stakeWALPools)
	w.register(50)
	w.epochBoundary(stakeWALBoundary1)

	syncStart := walFrameCount(t, path)
	redelegations := w.syncBlocks(stakeWALBlocks, stakeWALPerBlock)
	syncEnd := walFrameCount(t, path)
	w.epochBoundary(stakeWALBoundary2)
	boundaryEnd := walFrameCount(t, path)

	return stakeWALResult{
		syncFrames: stakeWALProfile(
			t, store.writeDB, path, syncStart, syncEnd,
		),
		boundaryFrames: stakeWALProfile(
			t, store.writeDB, path, syncEnd, boundaryEnd,
		),
		redelegations: redelegations,
		digest:        stakeStateDigest(t, store.writeDB),
		indexes:       indexNames(t, store),
	}
}

func indexNames(t *testing.T, store *Store) map[string]bool {
	t.Helper()
	rows, err := store.writeDB.Query(
		"SELECT name FROM sqlite_master WHERE type = 'index'",
	)
	require.NoError(t, err)
	defer rows.Close()
	names := map[string]bool{}
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names[name] = true
	}
	require.NoError(t, rows.Err())
	return names
}

func formatFrames(label string, frames map[string]int, blocks int) string {
	type kv struct {
		name string
		n    int
	}
	var items []kv
	total := 0
	for name, n := range frames {
		items = append(items, kv{name, n})
		total += n
	}
	sort.Slice(items, func(i, j int) bool { return items[i].n > items[j].n })
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d frames", label, total)
	if blocks > 0 {
		fmt.Fprintf(&b, " (%.1f/block)", float64(total)/float64(blocks))
	}
	b.WriteString("\n")
	for _, it := range items {
		fmt.Fprintf(&b, "  %-52s %6d  %5.1f%%\n", it.name, it.n,
			100*float64(it.n)/float64(total))
	}
	return b.String()
}

// stakeWorkloadDigest is the digest of the stake and reward bookkeeping rows,
// their ids and their autoincrement sequences after the workload above. Every
// table outside the reward journal hashes the same as under the write path
// that predates the index and changed-column upserts, so a match shows the
// stored state, reward outputs and live stake totals are byte-identical.
const stakeWorkloadDigest = "2564462cf9f05c479df380a2b93057a1fd309808f778c5bd294050a7e357e739"

// TestStakeBookkeepingWALFrames drives a Shelley-like workload through
// SetTransaction on a file-backed WAL database, attributes every WAL frame to
// the b-tree that owns its page, and bounds the frames of the indexes this
// write path keeps clean: the pool index, the drep indexes and the dropped
// indexes. Table and other index frames are logged, not bounded; the digest
// pins the stored state those writes produce.
func TestStakeBookkeepingWALFrames(t *testing.T) {
	t.Parallel()
	res := runStakeWALWorkload(t)
	t.Log("\n" + formatFrames("sync", res.syncFrames, stakeWALBlocks))
	t.Log("\n" + formatFrames("boundary", res.boundaryFrames, 0))

	require.Equal(t, stakeWorkloadDigest, res.digest,
		"stored stake and reward state changed")
	require.Positive(t, res.redelegations)

	// A touch that moves stake but not the pool must not rewrite the pool
	// index: only a changed pool (one leaf out, one leaf in) may dirty it.
	require.LessOrEqual(
		t,
		res.syncFrames["idx_reward_live_stake_pool_order"],
		3*res.redelegations,
		"idx_reward_live_stake_pool_order is rewritten by touches that keep the pool",
	)
	// Redelegations change only the pool: drep, drep_type and active are not
	// named in the account upsert, so their indexes stay clean.
	for _, name := range []string{
		"idx_account_drep_active_staking_key",
		"idx_account_drep_type_active_staking_key",
	} {
		require.Zero(
			t,
			res.syncFrames[name],
			"%s is rewritten by delegations that leave drep and active alone",
			name,
		)
	}
	// Indexes no reader needs take no frames, and no longer exist.
	droppedIndexes := []string{
		"idx_reward_live_stake_updated_slot",
		"idx_account_reward_delta_tx_hash",
		"idx_account_reward_delta_withdrawal",
		"idx_account_drep",
	}
	for _, name := range droppedIndexes {
		require.Zero(t, res.syncFrames[name], "%s took sync frames", name)
		require.Zero(
			t,
			res.boundaryFrames[name],
			"%s took boundary frames",
			name,
		)
		require.False(t, res.indexes[name], "%s still exists", name)
	}
}
