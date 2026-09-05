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

package leader

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/consensus"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/consensus/praos"
)

const (
	sigmaAuditEpoch      = uint64(11)
	sigmaAuditStartSlot  = uint64(950400)
	sigmaAuditSlotCount  = uint64(6)
	sigmaAuditPoolStake  = uint64(59_000_000)
	sigmaAuditTotalStake = uint64(1_000_000_000)
)

// recordingStakeProvider records the snapshot epoch each half of the sigma
// ratio was queried with, so a test can prove both come from the SAME stake
// snapshot generation. A numerator taken from a later generation than the
// denominator is exactly the one-sided sigma drift dingo #2798 reported.
type recordingStakeProvider struct {
	poolStakeEpochs  []uint64
	totalStakeEpochs []uint64
	poolStake        uint64
	totalStake       uint64
}

func (p *recordingStakeProvider) GetPoolStake(
	epoch uint64,
	_ []byte,
) (uint64, error) {
	p.poolStakeEpochs = append(p.poolStakeEpochs, epoch)
	return p.poolStake, nil
}

func (p *recordingStakeProvider) GetTotalActiveStake(
	epoch uint64,
) (uint64, error) {
	p.totalStakeEpochs = append(p.totalStakeEpochs, epoch)
	return p.totalStake, nil
}

// sigmaAuditEpochProvider is a minimal EpochInfoProvider. exactCoeff, when
// non-nil, also makes it an ActiveSlotCoeffRatProvider.
type sigmaAuditEpochProvider struct {
	exactCoeff *big.Rat
	floatCoeff float64
}

func (p *sigmaAuditEpochProvider) CurrentEpoch() uint64 { return sigmaAuditEpoch }

func (p *sigmaAuditEpochProvider) EpochNonce(uint64) []byte {
	return coeffTestNonce
}

func (p *sigmaAuditEpochProvider) NextEpochNonceReadyEpoch() (uint64, bool) {
	return 0, false
}

func (p *sigmaAuditEpochProvider) EpochSlotRange(
	uint64,
) (EpochSlotRange, error) {
	return EpochSlotRange{
		StartSlot: sigmaAuditStartSlot,
		SlotCount: sigmaAuditSlotCount,
	}, nil
}

func (p *sigmaAuditEpochProvider) EpochForSlot(uint64) (uint64, error) {
	return sigmaAuditEpoch, nil
}

func (p *sigmaAuditEpochProvider) ActiveSlotCoeff() float64 {
	return p.floatCoeff
}

func (p *sigmaAuditEpochProvider) ConsensusModeForEpoch(
	uint64,
) consensus.ConsensusMode {
	return consensus.ConsensusModeCPraos
}

// ActiveSlotCoeffRat is only reachable through the optional
// ActiveSlotCoeffRatProvider assertion, and only when exactCoeff is set.
func (p *sigmaAuditEpochProvider) ActiveSlotCoeffRat() *big.Rat {
	return p.exactCoeff
}

func newSigmaAuditElection(
	stake *recordingStakeProvider,
	epochs *sigmaAuditEpochProvider,
	logger *slog.Logger,
) *Election {
	return NewElection(
		coeffTestPoolID,
		coeffTestVRFSeed,
		stake,
		epochs,
		nil,
		logger,
	)
}

// TestComputeScheduleDrawsSigmaInputsFromSameSnapshotEpoch proves the pool
// stake numerator and the total active stake denominator are both read from
// the Praos-selected stake snapshot generation for the scheduled epoch
// (praos.StakeSnapshotEpoch, i.e. mark[E-1] = stake at end of E-2, the
// reference node's "set" snapshot / nesPd). Mixing generations would inflate
// or deflate sigma one-sidedly.
func TestComputeScheduleDrawsSigmaInputsFromSameSnapshotEpoch(t *testing.T) {
	stake := &recordingStakeProvider{
		poolStake:  sigmaAuditPoolStake,
		totalStake: sigmaAuditTotalStake,
	}
	election := newSigmaAuditElection(
		stake,
		&sigmaAuditEpochProvider{floatCoeff: 0.05},
		slog.New(slog.DiscardHandler),
	)

	schedule, err := election.computeSchedule(
		context.Background(), sigmaAuditEpoch,
	)
	require.NoError(t, err)
	require.NotNil(t, schedule)

	wantSnapshotEpoch := praos.StakeSnapshotEpoch(sigmaAuditEpoch)
	require.Equal(t, []uint64{wantSnapshotEpoch}, stake.poolStakeEpochs)
	require.Equal(t, []uint64{wantSnapshotEpoch}, stake.totalStakeEpochs)
	require.Equal(t, sigmaAuditPoolStake, schedule.PoolStake)
	require.Equal(t, sigmaAuditTotalStake, schedule.TotalStake)
}

// TestComputeSchedulePrefersExactGenesisActiveSlotCoeff proves the election
// threads the exact Shelley genesis active slot coefficient into the schedule
// calculation when the epoch provider can supply it, instead of the float64
// value returned by ActiveSlotCoeff().
func TestComputeSchedulePrefersExactGenesisActiveSlotCoeff(t *testing.T) {
	exact := big.NewRat(1, 3)
	election := newSigmaAuditElection(
		&recordingStakeProvider{
			poolStake:  sigmaAuditPoolStake,
			totalStake: sigmaAuditTotalStake,
		},
		&sigmaAuditEpochProvider{
			exactCoeff: exact,
			floatCoeff: 1.0 / 3.0,
		},
		slog.New(slog.DiscardHandler),
	)

	schedule, err := election.computeSchedule(
		context.Background(), sigmaAuditEpoch,
	)
	require.NoError(t, err)
	require.NotNil(t, schedule)
	require.NotNil(t, schedule.Threshold)

	want, err := consensus.CertifiedNatThresholdWithMode(
		sigmaAuditPoolStake,
		sigmaAuditTotalStake,
		exact,
		consensus.ConsensusModeCPraos,
	)
	require.NoError(t, err)
	require.Equal(t, 0, schedule.Threshold.Cmp(want),
		"threshold must come from the exact genesis rational, not float64")
}

// TestComputeScheduleWithoutExactCoeffUsesFloatFallback keeps providers that
// cannot supply an exact rational working unchanged.
func TestComputeScheduleWithoutExactCoeffUsesFloatFallback(t *testing.T) {
	election := newSigmaAuditElection(
		&recordingStakeProvider{
			poolStake:  sigmaAuditPoolStake,
			totalStake: sigmaAuditTotalStake,
		},
		&sigmaAuditEpochProvider{floatCoeff: 0.05},
		slog.New(slog.DiscardHandler),
	)

	schedule, err := election.computeSchedule(
		context.Background(), sigmaAuditEpoch,
	)
	require.NoError(t, err)
	require.NotNil(t, schedule)
	require.NotNil(t, schedule.Threshold)

	want, err := consensus.CertifiedNatThresholdWithMode(
		sigmaAuditPoolStake,
		sigmaAuditTotalStake,
		new(big.Rat).SetFloat64(0.05),
		consensus.ConsensusModeCPraos,
	)
	require.NoError(t, err)
	require.Equal(t, 0, schedule.Threshold.Cmp(want))
}

// TestComputeScheduleLogsAuditableSigmaInputs pins the "leader schedule
// calculated" record as a single, self-contained audit of every input to the
// leader check, so a reported schedule divergence can be diffed against the
// reference node's `query stake-snapshot` / `query protocol-state` without
// re-running the node with extra instrumentation (dingo #2798).
func TestComputeScheduleLogsAuditableSigmaInputs(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	exact := big.NewRat(1, 20)
	election := newSigmaAuditElection(
		&recordingStakeProvider{
			poolStake:  sigmaAuditPoolStake,
			totalStake: sigmaAuditTotalStake,
		},
		&sigmaAuditEpochProvider{exactCoeff: exact, floatCoeff: 0.05},
		logger,
	)

	schedule, err := election.computeSchedule(
		context.Background(), sigmaAuditEpoch,
	)
	require.NoError(t, err)
	require.NotNil(t, schedule)

	record := findLogRecord(t, &buf, "leader schedule calculated")
	require.EqualValues(t, sigmaAuditEpoch, record["epoch"])
	require.EqualValues(
		t,
		praos.StakeSnapshotEpoch(sigmaAuditEpoch),
		record["snapshot_epoch"],
	)
	require.Equal(t, "mark", record["snapshot_type"])
	require.EqualValues(t, sigmaAuditStartSlot, record["epoch_start_slot"])
	require.EqualValues(t, sigmaAuditSlotCount, record["epoch_slot_count"])
	require.EqualValues(t, sigmaAuditPoolStake, record["pool_stake"])
	require.EqualValues(t, sigmaAuditTotalStake, record["total_stake"])
	require.Equal(t, hex.EncodeToString(coeffTestNonce), record["epoch_nonce"])
	require.Equal(t, "1/20", record["active_slot_coeff"])
	require.Equal(t, "cpraos", record["consensus_mode"])
	require.Equal(
		t,
		schedule.Threshold.Text(16),
		record["leader_threshold"],
	)
}

// findLogRecord returns the first JSON log record whose "msg" matches.
func findLogRecord(
	t *testing.T,
	buf *bytes.Buffer,
	msg string,
) map[string]any {
	t.Helper()
	scanner := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			continue
		}
		if record["msg"] == msg {
			return record
		}
	}
	require.NoError(t, scanner.Err())
	t.Fatalf("no log record with msg %q in:\n%s", msg, buf.String())
	return nil
}
