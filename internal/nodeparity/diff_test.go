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

package nodeparity

import (
	"fmt"
	"math/big"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpccardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// poolID builds a deterministic, distinguishable PoolId for test fixtures
// by repeating a single byte across the whole ID.
func poolID(b byte) lcommon.PoolId {
	var id lcommon.PoolId
	for i := range id {
		id[i] = b
	}
	return id
}

// emptySnapshot returns a valid, empty Snapshot (non-nil maps, a minimal
// protocol-params value) for tests to mutate into whatever divergence they
// want to exercise.
func emptySnapshot() *Snapshot {
	return &Snapshot{
		ProtocolParams:    &utxorpccardano.PParams{MaxTxSize: 16384},
		StakeDistribution: map[lcommon.PoolId]*big.Rat{},
		UTxOEntries:       map[string]string{},
	}
}

// TestDiffSnapshots_Identical covers the baseline case: two snapshots with
// identical protocol params, stake distribution, and UTxO set must diff as
// completely empty, so a real match is never mistaken for a divergence.
func TestDiffSnapshots_Identical(t *testing.T) {
	a := emptySnapshot()
	b := emptySnapshot()
	d := DiffSnapshots(a, b)
	assert.True(
		t,
		d.Empty(),
		"identical snapshots must diff empty, got %v",
		d.Lines(),
	)
}

// TestDiffSnapshots_ProtocolParamsDiffer covers a protocol-parameter-only
// divergence: DiffSnapshots must populate ProtocolParamsDiff and leave the
// other two fields (StakeDistribution, UTxO) untouched, so a caller
// counting divergences by field (e.g. the Prometheus label) attributes it
// correctly.
func TestDiffSnapshots_ProtocolParamsDiffer(t *testing.T) {
	a := emptySnapshot()
	b := emptySnapshot()
	b.ProtocolParams = &utxorpccardano.PParams{MaxTxSize: 32768}

	d := DiffSnapshots(a, b)
	require.False(t, d.Empty())
	assert.NotEmpty(t, d.ProtocolParamsDiff)
	assert.Empty(t, d.StakeDistribution)
	assert.Empty(t, d.UTxO)
}

// TestDiffSnapshots_StakeDistribution covers three ways a stake
// distribution can diverge in one comparison: a pool present on both sides
// with a different fraction, a pool present only in a, and a pool present
// only in b. All three must be reported as separate lines, and neither
// protocol params nor UTxO should be touched by a stake-only divergence.
func TestDiffSnapshots_StakeDistribution(t *testing.T) {
	a := emptySnapshot()
	b := emptySnapshot()

	poolA, poolB, poolC := poolID(0x11), poolID(0x22), poolID(0x33)
	a.StakeDistribution[poolA] = big.NewRat(1, 2)
	a.StakeDistribution[poolB] = big.NewRat(1, 2)
	b.StakeDistribution[poolA] = big.NewRat(1, 3) // differs
	b.StakeDistribution[poolC] = big.NewRat(2, 3) // only in b; poolB only in a

	d := DiffSnapshots(a, b)
	require.False(t, d.Empty())
	assert.Empty(t, d.ProtocolParamsDiff)
	assert.Empty(t, d.UTxO)
	// pool A differs, pool B missing from b, pool C missing from a: 3 lines.
	assert.Len(t, d.StakeDistribution, 3)

	joined := fmt.Sprint(d.StakeDistribution)
	assert.Contains(t, joined, poolA.String())
	assert.Contains(t, joined, poolB.String())
	assert.Contains(t, joined, poolC.String())
}

// TestDiffSnapshots_UTxOSetDiffers is the UTxO-set equivalent of
// TestDiffSnapshots_StakeDistribution: a UTxO present on both sides with a
// different canonical encoding, one present only in a, and one present
// only in b -- three divergence lines, with no truncation since the count
// is well under the cap.
func TestDiffSnapshots_UTxOSetDiffers(t *testing.T) {
	a := emptySnapshot()
	b := emptySnapshot()

	a.UTxOEntries["tx1#0"] = "addr1|1000000"
	a.UTxOEntries["tx2#0"] = "addr2|2000000"
	b.UTxOEntries["tx1#0"] = "addr1|9999999" // differs
	b.UTxOEntries["tx3#0"] = "addr3|3000000" // only in b; tx2 only in a

	d := DiffSnapshots(a, b)
	require.False(t, d.Empty())
	assert.Len(t, d.UTxO, 3)
	assert.Zero(t, d.TruncatedUTxO)
}

// TestDiff_CountIncludesTruncatedEntries covers Count()'s reason for
// existing: Lines() rolls every UTxO divergence beyond maxUTxODiffLines
// into one summary line, so len(Lines()) understates the real number of
// divergences whenever TruncatedUTxO is nonzero. Count() must report the
// true total instead.
func TestDiff_CountIncludesTruncatedEntries(t *testing.T) {
	d := Diff{
		ProtocolParamsDiff: "protocol parameters differ",
		StakeDistribution:  []string{"pool a differs"},
		UTxO:               []string{"utxo a differs", "utxo b differs"},
		TruncatedUTxO:      5,
	}
	// Lines(): 1 (params) + 1 (stake) + 2 (utxo) + 1 (summary) = 5.
	assert.Len(t, d.Lines(), 5)
	// Count(): 1 (params) + 1 (stake) + 2 (utxo) + 5 (truncated) = 9.
	assert.Equal(t, 9, d.Count())
}

// TestDiffSnapshots_OrderIsDeterministic covers a divergence in both stake
// distribution and UTxO across enough entries that live Go map iteration
// order would very likely vary between calls if DiffSnapshots iterated the
// maps directly (Go deliberately randomizes map iteration order per run).
// Running the same comparison repeatedly must produce byte-identical
// output every time, and -- since the UTxO side exceeds maxUTxODiffLines --
// the same entries must be the ones kept vs. truncated each time too, not
// merely the same count: an operator diffing two runs' logs, or the
// capped sample itself, must never see something that looks like it
// changed when the underlying snapshots did not.
func TestDiffSnapshots_OrderIsDeterministic(t *testing.T) {
	a := emptySnapshot()
	b := emptySnapshot()

	for i := range 30 {
		pool := poolID(byte(i))
		a.StakeDistribution[pool] = big.NewRat(1, 2)
		b.StakeDistribution[pool] = big.NewRat(1, 3) // every pool differs
	}
	for i := range maxUTxODiffLines + 10 {
		key := fmt.Sprintf("tx%d#0", i)
		a.UTxOEntries[key] = "addr|1"
		b.UTxOEntries[key] = "addr|2"
	}

	first := DiffSnapshots(a, b).Lines()
	for range 10 {
		got := DiffSnapshots(a, b).Lines()
		require.Equal(
			t, first, got,
			"DiffSnapshots must report the same order (and the same "+
				"truncated-vs-kept entries) every time for the same input",
		)
	}
}

// TestDiffSnapshots_UTxODiffCapped covers a badly-diverged UTxO set:
// DiffSnapshots must cap the concrete UTxO diff lines at maxUTxODiffLines
// (so a caller isn't flooded) while still reporting the true count of
// omitted extras separately (TruncatedUTxO), and Lines() must append
// exactly one human-readable summary line for that overflow -- not one per
// omitted entry, and not silently dropped.
func TestDiffSnapshots_UTxODiffCapped(t *testing.T) {
	a := emptySnapshot()
	b := emptySnapshot()

	const extra = 5
	for i := range maxUTxODiffLines + extra {
		key := fmt.Sprintf("tx%d#0", i)
		a.UTxOEntries[key] = "addr|1"
		b.UTxOEntries[key] = "addr|2"
	}

	d := DiffSnapshots(a, b)
	assert.Len(t, d.UTxO, maxUTxODiffLines)
	assert.Equal(t, extra, d.TruncatedUTxO)
	// The truncation summary line only appears via Lines(), not in the
	// UTxO slice itself, so the field-based caller (Prometheus labeling)
	// still gets exactly maxUTxODiffLines concrete entries plus a count.
	lines := d.Lines()
	assert.Len(t, lines, maxUTxODiffLines+1)
	assert.Contains(t, lines[len(lines)-1], fmt.Sprintf("%d more", extra))
}
