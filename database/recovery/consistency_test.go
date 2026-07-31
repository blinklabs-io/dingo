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

package recovery

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSource is a scriptable StateSource for the checks and for recovery.
type fakeSource struct {
	// timestampsFn, when set, answers CommitTimestamps by call number so a
	// test can script a value that changes between reads.
	timestampsFn    func(call int) (int64, int64, error)
	metadataTipErr  error
	blobTipErr      error
	timestampsErr   error
	recentErr       error
	orphanErr       error
	utxoErr         error
	metadataTip     Point
	blobTip         Point
	recent          []BlockRef
	orphans         []BlockRef
	utxos           UtxoIntegrityResult
	metadataBlockNo uint64
	metadataTS      int64
	blobTS          int64
	orphanAfterSlot uint64
	orphanCalls     int
	timestampCalls  int
}

func (f *fakeSource) MetadataTip() (Point, uint64, error) {
	return f.metadataTip, f.metadataBlockNo, f.metadataTipErr
}

func (f *fakeSource) BlobTip() (Point, error) {
	return f.blobTip, f.blobTipErr
}

func (f *fakeSource) CommitTimestamps() (int64, int64, error) {
	f.timestampCalls++
	if f.timestampsFn != nil {
		return f.timestampsFn(f.timestampCalls)
	}
	return f.metadataTS, f.blobTS, f.timestampsErr
}

func (f *fakeSource) RecentBlocks(limit int) ([]BlockRef, error) {
	if f.recentErr != nil {
		return nil, f.recentErr
	}
	if limit < len(f.recent) {
		return f.recent[:limit], nil
	}
	return f.recent, nil
}

func (f *fakeSource) OrphanBlobs(
	afterSlot uint64,
	limit int,
) ([]BlockRef, error) {
	f.orphanCalls++
	f.orphanAfterSlot = afterSlot
	if f.orphanErr != nil {
		return nil, f.orphanErr
	}
	var out []BlockRef
	for _, ref := range f.orphans {
		if ref.Slot <= afterSlot || len(out) >= limit {
			continue
		}
		out = append(out, ref)
	}
	return out, nil
}

func (f *fakeSource) CheckUtxos(limit int) (UtxoIntegrityResult, error) {
	return f.utxos, f.utxoErr
}

// chainTipSource adds a chain tip to a fake source.
type chainTipSource struct {
	*fakeSource
	err       error
	chainTip  Point
	chainNoBN uint64
}

func (c chainTipSource) ChainTip() (Point, uint64, error) {
	return c.chainTip, c.chainNoBN, c.err
}

// linkedBlocks builds a newest-first run of correctly linked blocks.
func linkedBlocks(count int) []BlockRef {
	blocks := make([]BlockRef, count)
	for i := range count {
		// Index 0 is the newest, so slot and number decrease with i.
		n := uint64(count - i)
		blocks[i] = BlockRef{
			Hash:     []byte{byte(n)},
			PrevHash: []byte{byte(n - 1)},
			Slot:     n * 10,
			Number:   n,
			ID:       n,
		}
	}
	return blocks
}

func runChecks(t *testing.T, source StateSource, mode CheckMode) Report {
	t.Helper()
	checker, err := NewChecker(source, mode, nil)
	require.NoError(t, err)
	return checker.Run()
}

func requireCheck(t *testing.T, report Report, name string) CheckResult {
	t.Helper()
	result, ok := report.Find(name)
	require.True(t, ok, "report has no %s check", name)
	return result
}

func TestCheckerReportsHealthyState(t *testing.T) {
	t.Parallel()
	tip := Point{Slot: 100, Hash: []byte{10}}
	source := &fakeSource{
		metadataTip: tip,
		blobTip:     tip,
		metadataTS:  5,
		blobTS:      5,
		recent:      linkedBlocks(10),
		utxos:       UtxoIntegrityResult{Checked: 10},
	}
	report := runChecks(t, source, CheckModeFast)
	assert.Equal(t, SeverityOK, report.Worst())
	assert.False(t, report.Failed())
}

func TestCheckerModeOffRunsNothing(t *testing.T) {
	t.Parallel()
	report := runChecks(t, &fakeSource{}, CheckModeOff)
	assert.Empty(t, report.Results)
}

func TestCheckerFlagsCommitTimestampMismatch(t *testing.T) {
	t.Parallel()
	source := &fakeSource{metadataTS: 5, blobTS: 6}
	result := requireCheck(
		t,
		runChecks(t, source, CheckModeFast),
		CheckCommitTimestamps,
	)
	// The mismatch is repairable, so it warns rather than failing.
	assert.Equal(t, SeverityWarn, result.Severity)
}

func TestCheckerAcceptsFreshDatabase(t *testing.T) {
	t.Parallel()
	source := &fakeSource{metadataTS: 0, blobTS: 0}
	result := requireCheck(
		t,
		runChecks(t, source, CheckModeFast),
		CheckCommitTimestamps,
	)
	assert.Equal(t, SeverityOK, result.Severity)
}

func TestCheckerBlobAheadWarnsAndBlobBehindFails(t *testing.T) {
	t.Parallel()
	t.Run("blob ahead", func(t *testing.T) {
		t.Parallel()
		source := &fakeSource{
			metadataTip: Point{Slot: 100, Hash: []byte{1}},
			blobTip:     Point{Slot: 110, Hash: []byte{2}},
		}
		result := requireCheck(
			t,
			runChecks(t, source, CheckModeFast),
			CheckTipConsistency,
		)
		assert.Equal(t, SeverityWarn, result.Severity)
	})
	t.Run("blob behind", func(t *testing.T) {
		t.Parallel()
		source := &fakeSource{
			metadataTip: Point{Slot: 110, Hash: []byte{2}},
			blobTip:     Point{Slot: 100, Hash: []byte{1}},
		}
		result := requireCheck(
			t,
			runChecks(t, source, CheckModeFast),
			CheckTipConsistency,
		)
		assert.Equal(t, SeverityFail, result.Severity)
	})
	t.Run("same slot different hash", func(t *testing.T) {
		t.Parallel()
		source := &fakeSource{
			metadataTip: Point{Slot: 100, Hash: []byte{1}},
			blobTip:     Point{Slot: 100, Hash: []byte{2}},
		}
		result := requireCheck(
			t,
			runChecks(t, source, CheckModeFast),
			CheckTipConsistency,
		)
		assert.Equal(t, SeverityFail, result.Severity)
	})
}

func TestCheckerDetectsBrokenBlockLinkage(t *testing.T) {
	t.Parallel()
	blocks := linkedBlocks(6)
	blocks[2].PrevHash = []byte{0xff}
	source := &fakeSource{recent: blocks}
	result := requireCheck(
		t,
		runChecks(t, source, CheckModeFast),
		CheckBlockContinuity,
	)
	assert.Equal(t, SeverityFail, result.Severity)
}

func TestCheckerAcceptsRepeatedBlockNumbers(t *testing.T) {
	t.Parallel()
	// A Byron epoch boundary block carries the same chain difficulty as the
	// block before it, so equal consecutive numbers must not be reported.
	blocks := linkedBlocks(4)
	blocks[0].Number = blocks[1].Number
	source := &fakeSource{recent: blocks}
	result := requireCheck(
		t,
		runChecks(t, source, CheckModeFast),
		CheckBlockContinuity,
	)
	assert.Equal(t, SeverityOK, result.Severity)
}

func TestCheckerWarnsOnBlockNumberGap(t *testing.T) {
	t.Parallel()
	blocks := linkedBlocks(4)
	blocks[0].Number = blocks[1].Number + 5
	source := &fakeSource{recent: blocks}
	result := requireCheck(
		t,
		runChecks(t, source, CheckModeFast),
		CheckBlockContinuity,
	)
	assert.Equal(t, SeverityWarn, result.Severity)
}

func TestCheckerSkipsLinkageWithTooFewBlocks(t *testing.T) {
	t.Parallel()
	source := &fakeSource{recent: linkedBlocks(1)}
	result := requireCheck(
		t,
		runChecks(t, source, CheckModeFast),
		CheckBlockContinuity,
	)
	assert.Equal(t, SeverityOK, result.Severity)
}

func TestCheckerFailsOnUnresolvableUtxos(t *testing.T) {
	t.Parallel()
	source := &fakeSource{
		utxos: UtxoIntegrityResult{
			Checked:      10,
			Unresolvable: []string{"aa#0", "bb#1"},
		},
	}
	result := requireCheck(
		t,
		runChecks(t, source, CheckModeFast),
		CheckUtxoIntegrity,
	)
	assert.Equal(t, SeverityFail, result.Severity)
}

func TestCheckerReportsStoreErrorsAsFailures(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	source := &fakeSource{
		timestampsErr:  boom,
		metadataTipErr: boom,
		blobTipErr:     boom,
		recentErr:      boom,
		utxoErr:        boom,
		orphanErr:      boom,
	}
	report := runChecks(t, source, CheckModeFast)
	assert.True(t, report.Failed())
	for _, name := range []string{
		CheckCommitTimestamps,
		CheckTipConsistency,
		CheckBlockContinuity,
		CheckUtxoIntegrity,
		CheckOrphanedData,
	} {
		assert.Equal(
			t,
			SeverityFail,
			requireCheck(t, report, name).Severity,
			"check %s", name,
		)
	}
}

func TestCheckerChainTipComparisons(t *testing.T) {
	t.Parallel()
	ledgerTip := Point{Slot: 100, Hash: []byte{1}}
	cases := map[string]struct {
		chainTip Point
		want     Severity
	}{
		// The chain running ahead is the normal shape: the ledger
		// replays forward to catch up.
		"chain ahead":  {Point{Slot: 200, Hash: []byte{2}}, SeverityOK},
		"chain equal":  {ledgerTip, SeverityOK},
		"chain behind": {Point{Slot: 50, Hash: []byte{3}}, SeverityWarn},
		"same slot different hash": {
			Point{Slot: 100, Hash: []byte{9}},
			SeverityWarn,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			source := chainTipSource{
				fakeSource: &fakeSource{metadataTip: ledgerTip},
				chainTip:   tc.chainTip,
			}
			result := requireCheck(
				t,
				runChecks(t, source, CheckModeFast),
				CheckChainLedgerTip,
			)
			assert.Equal(t, tc.want, result.Severity)
		})
	}
}

func TestCheckerSkipsChainTipWithoutChainSource(t *testing.T) {
	t.Parallel()
	report := runChecks(t, &fakeSource{}, CheckModeFast)
	_, ok := report.Find(CheckChainLedgerTip)
	assert.False(t, ok)
}

func TestParseCheckMode(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]CheckMode{
		"":      CheckModeFast,
		"off":   CheckModeOff,
		"FAST":  CheckModeFast,
		" full": CheckModeFull,
	} {
		got, err := ParseCheckMode(input)
		require.NoError(t, err, "input %q", input)
		assert.Equal(t, want, got, "input %q", input)
	}
	_, err := ParseCheckMode("sideways")
	assert.Error(t, err)
}

func TestCheckModeWindows(t *testing.T) {
	t.Parallel()
	// Full must look at strictly more than fast, or the mode is pointless.
	assert.Greater(
		t,
		CheckModeFull.continuityDepth(),
		CheckModeFast.continuityDepth(),
	)
	assert.Greater(t, CheckModeFull.utxoSample(), CheckModeFast.utxoSample())
	assert.Greater(
		t,
		CheckModeFull.orphanLimit(),
		CheckModeFast.orphanLimit(),
	)
}

func TestNewCheckerRequiresSource(t *testing.T) {
	t.Parallel()
	_, err := NewChecker(nil, CheckModeFast, nil)
	assert.Error(t, err)
}
