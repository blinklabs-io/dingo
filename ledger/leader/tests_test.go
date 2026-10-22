// Copyright 2026 Blink Labs Software
// SPDX-License-Identifier: Apache-2.0

package leader

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"math/big"
	"strconv"
	"strings"
	"testing"

	nodeconsensus "github.com/blinklabs-io/dingo/consensus"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/consensus"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

type e34ScheduleCase struct {
	ActiveStake   string   `json:"activeStake"`
	AssignedSlots []uint64 `json:"assignedSlots"`
	DDenominator  string   `json:"dDenominator"`
	DNumerator    string   `json:"dNumerator"`
	FDenominator  string   `json:"fDenominator"`
	FNumerator    string   `json:"fNumerator"`
	FirstSlot     uint64   `json:"firstSlot"`
	Mode          string   `json:"mode"`
	NonceHex      string   `json:"nonceHex"`
	PoolStake     string   `json:"poolStake"`
	SlotCount     uint64   `json:"slotCount"`
	VRFSkeyHex    string   `json:"vrfSkeyHex"`
}

func e34ScheduleCases(t *testing.T) []e34ScheduleCase {
	t.Helper()
	embedded := fixtures.EmbeddedFixtures()
	manifest, err := fs.ReadFile(embedded, "upstream/e34/SHA256SUMS")
	require.NoError(t, err)
	rows := strings.Split(strings.TrimSpace(string(manifest)), "\n")
	require.Len(t, rows, 16)
	found := false
	for _, row := range rows {
		fields := strings.Fields(row)
		require.Len(t, fields, 2)
		expected, err := hex.DecodeString(fields[0])
		require.NoError(t, err)
		require.Len(t, expected, sha256.Size)
		data, err := fs.ReadFile(embedded, "upstream/e34/"+fields[1])
		require.NoError(t, err)
		sum := sha256.Sum256(data)
		require.Equal(t, expected, sum[:], fields[1])
		if fields[1] == "jormanager/node-schedules.json" {
			found = true
			require.Equal(
				t,
				"9bf077ddb3cd42ece5b69a2b979918ee9623660d3140c6124f0e5f205526d1b3",
				fields[0],
			)
		}
	}
	require.True(t, found)
	data, err := fs.ReadFile(
		embedded,
		"upstream/e34/jormanager/node-schedules.json",
	)
	require.NoError(t, err)
	var document struct {
		Cases    []e34ScheduleCase `json:"cases"`
		Metadata json.RawMessage   `json:"metadata"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&document))
	require.ErrorIs(t, decoder.Decode(new(any)), io.EOF)
	require.Len(t, document.Cases, 21)
	return document.Cases
}

func e34Uint(t *testing.T, value string) uint64 {
	t.Helper()
	n, err := strconv.ParseUint(value, 10, 64)
	require.NoError(t, err)
	return n
}

func (c e34ScheduleCase) inputs(
	t *testing.T,
) (*big.Rat, uint64, uint64, []byte, []byte, consensus.ConsensusMode) {
	t.Helper()
	require.NotZero(t, e34Uint(t, c.FDenominator))
	f := new(
		big.Rat,
	).SetFrac(new(big.Int).SetUint64(e34Uint(t, c.FNumerator)), new(big.Int).SetUint64(e34Uint(t, c.FDenominator)))
	require.NotZero(t, e34Uint(t, c.DDenominator))
	require.NotZero(t, c.SlotCount)
	nonce, err := hex.DecodeString(c.NonceHex)
	require.NoError(t, err)
	require.Len(t, nonce, 32)
	skey, err := hex.DecodeString(c.VRFSkeyHex)
	require.NoError(t, err)
	require.Len(t, skey, 64)
	mode := consensus.ConsensusModeCPraos
	switch c.Mode {
	case "praos":
	case "tpraos":
		mode = consensus.ConsensusModeTPraos
	default:
		t.Fatalf("unknown schedule mode %q", c.Mode)
	}
	signer, err := consensus.NewSimpleVRFSigner(skey[:32])
	require.NoError(t, err)
	require.Equal(t, skey[32:], signer.PublicKey())
	return f, e34Uint(
			t,
			c.PoolStake,
		), e34Uint(
			t,
			c.ActiveStake,
		), nonce, skey[:32], mode
}

// Overlay filtering is fixture-harness policy, not part of Calculator.
func e34NonOverlay(t *testing.T, c e34ScheduleCase, slots []uint64) []uint64 {
	t.Helper()
	numerator := new(big.Int).SetUint64(e34Uint(t, c.DNumerator))
	denominator := new(big.Int).SetUint64(e34Uint(t, c.DDenominator))
	ceiling := func(offset uint64) *big.Int {
		n := new(big.Int).Mul(new(big.Int).SetUint64(offset), numerator)
		n.Add(n, new(big.Int).Sub(denominator, big.NewInt(1)))
		return n.Quo(n, denominator)
	}
	result := make([]uint64, 0)
	for _, slot := range slots {
		require.GreaterOrEqual(t, slot, c.FirstSlot)
		require.Less(t, slot-c.FirstSlot, c.SlotCount)
		offset := slot - c.FirstSlot
		if ceiling(offset).Cmp(ceiling(offset+1)) == 0 {
			result = append(result, slot)
		}
	}
	return result
}

func TestLeaderEligibilityNodeSchedules(t *testing.T) {
	testutil.SkipUnderRace(
		t,
		"each case is a single-goroutine pass over every slot of an epoch; "+
			"CI's check job replays the same cases un-instrumented",
	)
	for i, c := range e34ScheduleCases(t) {
		t.Run(fmt.Sprintf("%02d_%s", i, c.Mode), func(t *testing.T) {
			t.Parallel()
			f, pool, total, nonce, seed, mode := c.inputs(t)
			calc := NewCalculator(
				math.NaN(),
			) // Exact genesis value must override invalid float.
			calc.ActiveSlotCoeffRat = f
			schedule, err := calc.CalculateSchedule(
				11,
				EpochSlotRange{c.FirstSlot, c.SlotCount},
				coeffTestPoolID,
				seed,
				pool,
				total,
				nonce,
				mode,
			)
			require.NoError(t, err)
			if pool == 0 {
				require.Empty(
					t,
					schedule.LeaderSlotsSnapshot(),
					"Dingo zero-pool policy, including f=1",
				)
				return
			}
			got := append([]uint64{}, schedule.LeaderSlotsSnapshot()...)
			if e34Uint(t, c.DNumerator) != 0 {
				got = e34NonOverlay(t, c, got)
			}
			require.Equal(t, c.AssignedSlots, got)
		})
	}
}

func TestLeaderEligibilityExactDomain(t *testing.T) {
	for _, pool := range []uint64{0, 1} {
		calc := NewCalculator(0.05)
		calc.ActiveSlotCoeffRat = new(
			big.Rat,
		).SetFrac(big.NewInt(1), new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil))
		_, err := calc.CalculateSchedule(
			11,
			EpochSlotRange{100, 2},
			coeffTestPoolID,
			coeffTestVRFSeed,
			pool,
			3,
			coeffTestNonce,
			consensus.ConsensusModeCPraos,
		)
		require.ErrorContains(t, err, "PositiveUnitInterval")
	}
}

// The real refresh/reload path must retire old decisions, persist E34 lists,
// and reuse a valid restart without taking a VRF seed again.
func TestE34ScheduleVersionInvalidation(t *testing.T) {
	cases := e34ScheduleCases(t)
	var reference e34ScheduleCase
	for _, c := range cases {
		if c.Mode == "praos" && c.SlotCount == 120 && c.FNumerator == "1" &&
			c.FDenominator == "20" &&
			c.PoolStake == "1" &&
			c.ActiveStake == "3" &&
			c.DNumerator == "0" {
			reference = c
			break
		}
	}
	require.NotZero(t, reference.SlotCount)
	f, pool, total, nonce, seed, _ := reference.inputs(t)
	for _, mutation := range []string{"version5", "absent_version", "consensus_identity", "nonce", "pool_stake", "total_stake", "out_of_range"} {
		t.Run(mutation, func(t *testing.T) {
			backing := newMockSyncStateStore()
			store := NewSyncStateScheduleStore(backing)
			stale := NewSchedule(11, coeffTestPoolID, pool, total, nonce)
			for _, slot := range reference.AssignedSlots {
				stale.AddLeaderSlot(slot)
			}
			switch mutation {
			case "version5":
				stale.FormatVersion = 5
			case "consensus_identity":
				stale.ConsensusIdentity = "tachys/v1"
			case "nonce":
				stale.EpochNonce = makeDistinctNonce(nonce)
			case "pool_stake":
				stale.PoolStake++
			case "total_stake":
				stale.TotalStake++
			case "out_of_range":
				stale.AddLeaderSlot(reference.FirstSlot + reference.SlotCount)
			}
			require.NoError(t, store.SaveSchedule(stale))
			if mutation == "absent_version" {
				key := syncStateScheduleKey(11, coeffTestPoolID)
				var record map[string]json.RawMessage
				require.NoError(
					t,
					json.Unmarshal([]byte(backing.values[key]), &record),
				)
				delete(record, "format_version")
				data, err := json.Marshal(record)
				require.NoError(t, err)
				backing.values[key] = string(data)
			}
			provider := &e34EpochProvider{
				sigmaAuditEpochProvider: sigmaAuditEpochProvider{
					exactCoeff: f,
					floatCoeff: math.NaN(),
				},
				nonce: nonce,
				slotRange: EpochSlotRange{
					reference.FirstSlot,
					reference.SlotCount,
				},
			}
			calls := 0
			restart := func() *Election {
				election := NewElection(
					coeffTestPoolID,
					func() []byte { calls++; return append([]byte(nil), seed...) },
					&recordingStakeProvider{poolStake: pool, totalStake: total},
					provider,
					nil,
					slog.New(slog.DiscardHandler),
				)
				election.scheduleStore = store
				election.schedules = make(map[uint64]*Schedule)
				election.running = true // Exercise synchronous reload without a background worker.
				return election
			}
			first := restart()
			require.NoError(
				t,
				first.RefreshScheduleForEpoch(context.Background(), 11),
			)
			require.Equal(
				t,
				1,
				calls,
				"invalid persisted schedule must be recomputed",
			)
			saved, err := store.LoadSchedule(11, coeffTestPoolID)
			require.NoError(t, err)
			require.Equal(t, ScheduleFormatVersion, saved.FormatVersion)
			require.Equal(t, nodeconsensus.PraosIdentity, saved.ConsensusIdentity)
			require.Equal(
				t,
				reference.AssignedSlots,
				saved.LeaderSlotsSnapshot(),
			)
			second := restart()
			require.NoError(
				t,
				second.RefreshScheduleForEpoch(context.Background(), 11),
			)
			require.Equal(
				t,
				1,
				calls,
				"valid schedule restart must reuse, not sign",
			)
			require.Equal(
				t,
				reference.AssignedSlots,
				second.schedules[11].LeaderSlotsSnapshot(),
			)
		})
	}
}

type e34EpochProvider struct {
	sigmaAuditEpochProvider
	nonce     []byte
	slotRange EpochSlotRange
}

func (p *e34EpochProvider) EpochNonce(
	context.Context,
	uint64,
) []byte {
	return p.nonce
}

func (p *e34EpochProvider) EpochSlotRange(
	uint64,
) (EpochSlotRange, error) {
	return p.slotRange, nil
}

func TestLeaderEligibilityZeroPoolValidatesInputs(t *testing.T) {
	for _, name := range []string{"key", "nonce", "mode", "range", "slot_width"} {
		t.Run(name, func(t *testing.T) {
			seed, nonce := coeffTestVRFSeed, coeffTestNonce
			mode := consensus.ConsensusModeCPraos
			slotRange := EpochSlotRange{StartSlot: 100, SlotCount: 2}
			switch name {
			case "key":
				seed = nil
			case "nonce":
				nonce = nil
			case "mode":
				mode = consensus.ConsensusMode(255)
			case "range":
				slotRange.SlotCount = 0
			case "slot_width":
				slotRange.StartSlot = math.MaxInt64
			}
			_, err := NewCalculator(1).CalculateSchedule(
				11, slotRange, coeffTestPoolID, seed, 0, 3, nonce, mode,
			)
			require.Error(t, err)
		})
	}
}
