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
	"maps"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// renderOpCerts renders the reads the set answers in every state the
// scenario reaches: slots at or after the first observation it records.
func renderOpCerts(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	pools [][]byte,
	slots []uint64,
) []string {
	t.Helper()
	ctx := t.Context()
	var out []string
	all, err := ldb.LatestPoolOpCertSequences(ctx, nil)
	require.NoError(t, err)
	for _, k := range slices.Sorted(maps.Keys(all)) {
		out = append(out, fmt.Sprintf("latest %x %d", k, all[k]))
	}
	for _, pool := range pools {
		pkh := lcommon.PoolKeyHash(pool)
		seq, found, err := ldb.LatestPoolOpCertSequence(ctx, pkh, nil)
		require.NoError(t, err)
		out = append(out, fmt.Sprintf("pool %x %d %t", pool, seq, found))
		for _, slot := range slots {
			seq, found, err := ldb.LatestPoolOpCertSequenceAtOrBefore(
				ctx,
				pkh,
				slot,
				nil,
			)
			require.NoError(t, err, "pool %x at or before %d", pool, slot)
			out = append(out, fmt.Sprintf(
				"pool %x at or before %d: %d %t", pool, slot, seq, found,
			))
		}
	}
	for _, slot := range slots {
		at, err := ldb.LatestPoolOpCertSequencesAtOrBefore(ctx, slot, nil)
		require.NoError(t, err, "every pool at or before %d", slot)
		for _, k := range slices.Sorted(maps.Keys(at)) {
			out = append(out, fmt.Sprintf("at %d %x %d", slot, k, at[k]))
		}
		seen, err := ldb.PoolOpCertSequencesExistAtSlot(ctx, slot, nil)
		require.NoError(t, err, "observed at %d", slot)
		out = append(out, fmt.Sprintf("observed %d %t", slot, seen))
	}
	return out
}

// With the set failed, the LedgerDB and the database fail every read rather
// than answer from the empty SQL tables.
func TestP3DrainedReadsFailWhenTheSetCannotServe(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	// A failed set serves no read.
	scenario.FailSet()
	ctx := t.Context()
	ldb := scenario.LedgerDB()
	cold := bytes.Repeat([]byte{0xa1}, 28)
	pkh := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x6e}, 28))
	_, err = ldb.GetConstitution(nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetCommitteeQuorum(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetCommitteeMembers(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetCommitteeMembersIncludeDeleted(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetCommitteeMember(ctx, 0, cold, 0, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.IsCommitteeMemberResigned(ctx, 0, cold, 0, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetResignedCommitteeMembers(
		ctx, []models.CommitteeCredential{{Credential: cold}}, nil,
	)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetActiveCommitteeMembers(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetCommitteeAuthorizedCount(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetCommitteeHotAuthorizationsSince(ctx, 0, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)

	_, _, err = ldb.LatestPoolOpCertSequence(ctx, pkh, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, _, err = ldb.LatestPoolOpCertSequenceAfter(ctx, pkh, 1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, _, err = ldb.LatestPoolOpCertSequenceAtOrBefore(ctx, pkh, 1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, _, err = ldb.LatestPoolOpCertSequenceInRange(ctx, pkh, 1, 2, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.LatestPoolOpCertSequences(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.LatestPoolOpCertSequencesAtOrBefore(ctx, 1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.PoolOpCertSequencesExistAtSlot(ctx, 1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)

	// The plain database refuses both reads and writes.
	db := scenario.DB
	_, err = db.GetCommitteeMembers(ctx, nil)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
	_, err = db.GetConstitution(nil)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
	_, _, err = db.LatestPoolOpCertSequence(ctx, pkh, nil)
	require.ErrorIs(t, err, nodesettings.ErrFamilyDrained)
	_, _, err = db.Metadata().CountPoolBlocksInSlotRange(
		[]lcommon.PoolKeyHash{pkh}, 0, 10, nil,
	)
	require.ErrorIs(t, err, nodesettings.ErrFamilyDrained)
}

// A drained op-cert read for a slot the set has no record of fails rather
// than answer from the empty table.
func TestOpCertReadOutsideTheSetsRecordFailsOnceDrained(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	require.NoError(t, set.LoadLedgerState(
		t.Context(), dbadapter.New(scenario.DB),
	))
	slots := scenario.OpCerts()
	// A lower counter after the highest was reached: the set keeps the
	// highest sequence, so only the SQL rows say a lower one came later.
	lower := scenario.LedgerDB().Begin(t.Context(), true)
	require.NoError(t, scenario.LedgerDB().UpdatePoolOpCertSequence(
		t.Context(), lcommon.PoolKeyHash(slots.Pool), 5, slots.Batched+1, lower,
	))
	require.NoError(t, lower.Commit())
	require.NoError(t, scenario.DB.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyOpCerts,
	))
	scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyOpCerts),
	)
	ldb := scenario.LedgerDB()
	pkh := lcommon.PoolKeyHash(slots.Pool)
	seq, found, err := ldb.LatestPoolOpCertSequenceAtOrBefore(
		t.Context(), pkh, slots.Batched, nil,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(6), seq)
	_, _, err = ldb.LatestPoolOpCertSequenceAfter(
		t.Context(), pkh, slots.Batched, nil,
	)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
}

// A pool that keeps its counter after a slot is the common case of a node
// bootstrapped from Mithril, and the drained op-cert rows cannot answer it, so
// the set answers when the pool's latest observation repeats its highest
// counter: the highest after the slot is then that counter. A lower counter
// observed last still reaches the emptied table and fails.
func TestOpCertRepeatedCounterIsAnsweredByTheDrainedSet(t *testing.T) {
	t.Parallel()
	scenario, _, _ := setModeScenario(t)
	require.NoError(t, scenario.DB.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyOpCerts,
	))
	scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyOpCerts),
	)
	ldb := scenario.LedgerDB()
	ctx := t.Context()
	write := func(pool lcommon.PoolKeyHash, sequence, slot uint64) {
		txn := ldb.Begin(ctx, true)
		require.NoError(t, ldb.UpdatePoolOpCertSequence(
			ctx, pool, sequence, slot, txn,
		))
		require.NoError(t, txn.Commit())
		txn.Release()
	}
	base := scenario.OpCerts().Batched
	steady := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x6f}, 28))
	for _, slot := range []uint64{base + 2, base + 3, base + 4} {
		write(steady, 7, slot)
	}
	seq, found, err := ldb.LatestPoolOpCertSequenceAfter(
		ctx,
		steady,
		base+2,
		nil,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(7), seq)
	_, found, err = ldb.LatestPoolOpCertSequenceAfter(ctx, steady, base+4, nil)
	require.NoError(t, err)
	require.False(t, found, "nothing observed after the last row")
	seq, found, err = ldb.LatestPoolOpCertSequenceInRange(
		ctx, steady, base+3, base+4, nil,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(7), seq)

	dropped := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x70}, 28))
	write(dropped, 6, base+2)
	write(dropped, 5, base+3)
	_, _, err = ldb.LatestPoolOpCertSequenceAfter(ctx, dropped, base+2, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, _, err = ldb.LatestPoolOpCertSequenceInRange(
		ctx, dropped, base+3, base+3, nil,
	)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
}
