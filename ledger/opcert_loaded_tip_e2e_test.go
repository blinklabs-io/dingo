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

package ledger_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// opCertObservation is one block's counter for a pool.
type opCertObservation struct {
	pool           lcommon.PoolKeyHash
	sequence, slot uint64
}

// opCertNode is a ledger over a live UTxO set, the only copy of the op-cert
// counters.
type opCertNode struct {
	scenario  ledger.BoundaryEpochStateScenario
	loadedTip uint64
}

func (n opCertNode) observe(t *testing.T, o opCertObservation) {
	t.Helper()
	ldb := n.scenario.LedgerDB()
	txn := ldb.Begin(t.Context(), true)
	require.NoError(t, ldb.UpdatePoolOpCertSequence(
		t.Context(), o.pool, o.sequence, o.slot, txn,
	))
	require.NoError(t, txn.Commit())
	txn.Release()
}

// reads returns every op-cert read and counter verdict the node gives for
// pools around tip, one line each, errors included.
func (n opCertNode) reads(
	t *testing.T,
	pools []lcommon.PoolKeyHash,
	tip uint64,
) []string {
	t.Helper()
	ctx := t.Context()
	ldb := n.scenario.LedgerDB()
	var out []string
	add := func(name string, v ...any) {
		out = append(out, name+": "+fmt.Sprint(v...))
	}
	for i, p := range pools {
		seq, found, err := ldb.LatestPoolOpCertSequence(ctx, p, nil)
		add(fmt.Sprintf("pool %d latest", i), seq, found, err)
		for _, slot := range []uint64{tip - 1, tip, tip + 1, tip + 2} {
			seq, found, err = ldb.LatestPoolOpCertSequenceAtOrBefore(
				ctx, p, slot, nil,
			)
			add(
				fmt.Sprintf("pool %d at or before %d", i, slot),
				seq, found, err,
			)
		}
		for _, counter := range []uint64{0, 4, 5, 6, 7, 8, 9} {
			add(
				fmt.Sprintf("pool %d next block counter %d", i, counter),
				n.scenario.CheckOpCertCounter(p.Bytes(), n.loadedTip, counter),
			)
		}
	}
	all, err := ldb.LatestPoolOpCertSequences(ctx, nil)
	add("all latest", all, err)
	for _, slot := range []uint64{tip - 1, tip, tip + 1, tip + 2} {
		all, err = ldb.LatestPoolOpCertSequencesAtOrBefore(ctx, slot, nil)
		add(fmt.Sprintf("all at or before %d", slot), all, err)
	}
	return out
}

// A set loaded at a tip holds that tip's counter map, so its op-cert reads
// and its verdict on each pool's next block are those of a set that reached
// the same tip block by block: at the tip, just before it, and after the
// next blocks.
func TestOpCertReadsOfASetLoadedAtATipMatchASyncedSet(t *testing.T) {
	t.Parallel()
	const tip = uint64(5_000)
	rotated := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x71}, 28))
	steady := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x72}, 28))
	absent := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x73}, 28))
	pools := []lcommon.PoolKeyHash{rotated, steady, absent}

	synced := opCertNode{}
	synced.scenario, _, _ = setModeScenario(t)
	for _, o := range []opCertObservation{
		{rotated, 3, tip - 300},
		{steady, 7, tip - 200},
		{rotated, 4, tip - 100},
		{steady, 7, tip - 50},
	} {
		synced.observe(t, o)
	}

	// The loaded node opens over the tip's counter map the way a snapshot
	// import writes it: every counter stamped at the tip.
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	loaded := opCertNode{
		scenario: ledger.NewSeededBoundaryEpochStateScenarioForTest(
			t, set, nil, func(db *database.Database) {
				for _, o := range []opCertObservation{
					{rotated, 4, tip}, {steady, 7, tip},
				} {
					require.NoError(
						t,
						dbadapter.New(db).UpdatePoolOpCertSequence(
							t.Context(), o.pool, o.sequence, o.slot, nil,
						),
					)
				}
			},
		),
		loadedTip: tip,
	}

	require.Equal(
		t, synced.reads(t, pools, tip), loaded.reads(t, pools, tip),
		"at the tip",
	)
	require.NoError(
		t,
		loaded.scenario.CheckOpCertCounter(steady.Bytes(), tip, 7),
		"the block after the tip repeats the counter the tip holds",
	)
	require.NoError(
		t,
		loaded.scenario.CheckOpCertCounter(rotated.Bytes(), tip, 5),
		"the block after the tip rotates the counter the tip holds",
	)
	require.Error(
		t,
		loaded.scenario.CheckOpCertCounter(rotated.Bytes(), tip, 6),
		"a rotation past the next counter is rejected",
	)

	for _, o := range []opCertObservation{
		{steady, 7, tip + 1},
		{rotated, 5, tip + 2},
		{absent, 1, tip + 2},
	} {
		synced.observe(t, o)
		loaded.observe(t, o)
	}
	require.Equal(
		t, synced.reads(t, pools, tip), loaded.reads(t, pools, tip),
		"after the next blocks",
	)
}
