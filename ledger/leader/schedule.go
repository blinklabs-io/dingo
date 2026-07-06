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

// Package leader provides Ouroboros Praos leader election functionality
// for block production. It determines which slots a stake pool is eligible
// to produce blocks based on the stake distribution snapshot.
package leader

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"sync"

	"github.com/blinklabs-io/gouroboros/consensus"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/vrf"
)

// ScheduleFormatVersion identifies the in-memory and on-disk shape of a
// computed Schedule. Bump it whenever a code change alters how schedules
// are computed (e.g. the consensus mode threading added with the
// per-era TPraos/CPraos split): older persisted schedules then fail
// validatePersistedSchedule and are recomputed from scratch instead of
// being silently reused with stale assumptions.
const ScheduleFormatVersion = 1

// Schedule represents the leader schedule for a stake pool in an epoch.
// It contains the slots where the pool is eligible to produce blocks.
type Schedule struct {
	FormatVersion int                 // Snapshot of ScheduleFormatVersion at compute time
	Epoch         uint64              // Epoch this schedule is for
	PoolId        lcommon.PoolKeyHash // Pool key hash
	PoolStake     uint64              // Pool's stake from active snapshot
	TotalStake    uint64              // Total active stake from active snapshot
	EpochNonce    []byte              // Epoch nonce for VRF
	LeaderSlots   []uint64            // Slots where pool is leader, ascending-sorted

	mu sync.RWMutex
}

// LeaderSlots is maintained in ascending-sorted order so that
// IsLeaderForSlot can do an O(log n) binary search instead of an O(n)
// scan — relevant for high-stake pools with many leader slots per epoch.
// The invariant is upheld at every entry point that populates the slice:
//   - CalculateSchedule iterates slots in ascending order, so AddLeaderSlot
//     only ever appends a value greater than the last.
//   - syncStateScheduleStore.LoadSchedule re-sorts after rebuilding from a
//     persisted record, guarding against a tampered or legacy unsorted list.

// NewSchedule creates a new empty schedule for an epoch.
func NewSchedule(
	epoch uint64,
	poolId lcommon.PoolKeyHash,
	poolStake uint64,
	totalStake uint64,
	epochNonce []byte,
) *Schedule {
	return &Schedule{
		FormatVersion: ScheduleFormatVersion,
		Epoch:         epoch,
		PoolId:        poolId,
		PoolStake:     poolStake,
		TotalStake:    totalStake,
		EpochNonce:    epochNonce,
		LeaderSlots:   make([]uint64, 0),
	}
}

// AddLeaderSlot adds a slot where this pool is the leader.
func (s *Schedule) AddLeaderSlot(slot uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LeaderSlots = append(s.LeaderSlots, slot)
}

// IsLeaderForSlot returns true if the pool is leader for the given slot.
// LeaderSlots is kept ascending-sorted (see the type doc), so this is an
// O(log n) binary search rather than an O(n) scan.
func (s *Schedule) IsLeaderForSlot(slot uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, found := slices.BinarySearch(s.LeaderSlots, slot)
	return found
}

// SlotCount returns the number of slots where this pool is leader.
func (s *Schedule) SlotCount() int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.LeaderSlots)
}

// LeaderSlotsSnapshot returns a copy of the leader slot list.
func (s *Schedule) LeaderSlotsSnapshot() []uint64 {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]uint64(nil), s.LeaderSlots...)
}

// StakeRatio returns the pool's stake as a fraction of total stake.
func (s *Schedule) StakeRatio() float64 {
	if s.TotalStake == 0 {
		return 0
	}
	return float64(s.PoolStake) / float64(s.TotalStake)
}

// Calculator computes leader schedules using VRF and stake distribution.
type Calculator struct {
	// ActiveSlotCoeff (f) determines block production rate.
	// For mainnet, f = 0.05 (5% of slots have blocks on average)
	ActiveSlotCoeff float64

	// SlotsPerEpoch is the number of slots in an epoch.
	SlotsPerEpoch uint64
}

// NewCalculator creates a calculator with the given protocol parameters.
func NewCalculator(activeSlotCoeff float64, slotsPerEpoch uint64) *Calculator {
	return &Calculator{
		ActiveSlotCoeff: activeSlotCoeff,
		SlotsPerEpoch:   slotsPerEpoch,
	}
}

// activeSlotCoeffRat converts the float64 ActiveSlotCoeff to a *big.Rat
// for use with the gouroboros consensus package. Returns an error if the
// coefficient is not a finite value in the range (0, 1].
func (c *Calculator) activeSlotCoeffRat() (*big.Rat, error) {
	if math.IsNaN(c.ActiveSlotCoeff) || math.IsInf(c.ActiveSlotCoeff, 0) {
		return nil, fmt.Errorf(
			"active slot coefficient is not finite: %v",
			c.ActiveSlotCoeff,
		)
	}
	if c.ActiveSlotCoeff <= 0 || c.ActiveSlotCoeff > 1 {
		return nil, fmt.Errorf(
			"active slot coefficient must be in (0, 1], got %v",
			c.ActiveSlotCoeff,
		)
	}
	r := new(big.Rat).SetFloat64(c.ActiveSlotCoeff)
	if r == nil {
		return nil, fmt.Errorf(
			"failed to convert active slot coefficient %v to rational",
			c.ActiveSlotCoeff,
		)
	}
	return r, nil
}

// CalculateSchedule computes which slots a pool leads in the given epoch.
// This uses the certified VRF to determine leader eligibility per slot.
//
// The leader check for each slot uses:
//   - Pool's relative stake (sigma = poolStake / totalStake)
//   - Active slot coefficient (f)
//   - Consensus mode (TPraos for Shelley/Allegra/Mary/Alonzo,
//     CPraos for Babbage/Conway)
//
// A pool is leader if: VRF_output(slot) < threshold(sigma).
//
// The consensus mode is era-specific and MUST be passed correctly. TPraos
// and CPraos differ in two independent ways: the VRF input construction
// (TPraos folds the SeedL constant via vrf.MkSeedTPraos; CPraos uses
// vrf.MkInputVrf directly) and the leader-value/threshold derivation
// (TPraos compares the raw 64-byte VRF output against 2^512·T;
// CPraos compares BLAKE2b-256("L"||output) against 2^256·T). The two
// modes select independent leader-slot sets for the same key+nonce, so
// computing the schedule with the wrong mode yields a leader list that
// disagrees with cardano-node's view of the same epoch. Forging at one
// of those wrongly-marked slots produces a header that cardano-node
// rejects with `OverlayFailure (VRFLeaderValueTooBig …)`, which closes
// the chainsync session and prevents the dingo-forged block from
// reaching the canonical chain.
func (c *Calculator) CalculateSchedule(
	epoch uint64,
	poolId lcommon.PoolKeyHash,
	poolVrfSkey []byte,
	poolStake uint64,
	totalStake uint64,
	epochNonce []byte,
	mode consensus.ConsensusMode,
) (*Schedule, error) {
	if totalStake == 0 {
		return nil, errors.New("total stake cannot be zero")
	}

	// Create VRF signer from the pool's VRF secret key seed
	vrfSigner, err := consensus.NewSimpleVRFSigner(poolVrfSkey)
	if err != nil {
		return nil, fmt.Errorf("create VRF signer: %w", err)
	}

	schedule := NewSchedule(epoch, poolId, poolStake, totalStake, epochNonce)

	// Calculate epoch slot range
	epochStartSlot := epoch * c.SlotsPerEpoch
	epochEndSlot := epochStartSlot + c.SlotsPerEpoch

	activeSlotCoeff, err := c.activeSlotCoeffRat()
	if err != nil {
		return nil, fmt.Errorf("invalid active slot coefficient: %w", err)
	}

	// Precompute the leadership threshold once. It depends only on
	// poolStake, totalStake, activeSlotCoeff, and mode — all constant
	// for the epoch — so computing it inside the per-slot loop (which
	// is what IsSlotLeaderWithMode does) wastes two 20-term big.Rat
	// Taylor series and a 2^N multiply per slot.
	threshold := consensus.CertifiedNatThresholdWithMode(
		poolStake,
		totalStake,
		activeSlotCoeff,
		mode,
	)

	for slot := epochStartSlot; slot < epochEndSlot; slot++ {
		vrfInput, err := vrfInputForMode(mode, slot, epochNonce)
		if err != nil {
			return nil, fmt.Errorf("check slot %d: %w", slot, err)
		}
		_, output, err := vrfSigner.Prove(vrfInput)
		if err != nil {
			return nil, fmt.Errorf("check slot %d: %w", slot, err)
		}
		if consensus.IsVRFOutputBelowThresholdWithMode(
			output,
			threshold,
			mode,
		) {
			schedule.AddLeaderSlot(slot)
		}
	}

	return schedule, nil
}

// vrfInputForMode constructs the era-correct VRF input bytes for slot
// leader-eligibility evaluation. TPraos eras (Shelley/Allegra/Mary/
// Alonzo) fold the SeedL constant via vrf.MkSeedTPraos; CPraos eras
// (Babbage/Conway) use vrf.MkInputVrf directly. consensus.ComputeVRFInput
// only covers the CPraos shape, so it would silently produce the wrong
// VRF output (and therefore the wrong leader determination) when used
// for a TPraos epoch.
func vrfInputForMode(
	mode consensus.ConsensusMode,
	slot uint64,
	epochNonce []byte,
) ([]byte, error) {
	if slot > math.MaxInt64 {
		return nil, fmt.Errorf(
			"slot %d exceeds maximum int64 value for VRF input",
			slot,
		)
	}
	switch mode {
	case consensus.ConsensusModeTPraos:
		return vrf.MkSeedTPraos(int64(slot), epochNonce, vrf.SeedL()) //nolint:gosec
	case consensus.ConsensusModeCPraos:
		return vrf.MkInputVrf(int64(slot), epochNonce) //nolint:gosec
	default:
		return nil, fmt.Errorf("unknown consensus mode: %d", mode)
	}
}

// Threshold calculates the leadership threshold for a given stake ratio
// as a float64 approximation. This is provided for backward compatibility;
// the actual leader election uses arbitrary-precision arithmetic via
// consensus.CertifiedNatThreshold.
//
// threshold(sigma) = 1 - (1-f)^sigma
// where f is the active slot coefficient and sigma is the relative stake.
// Returns 0 if any input is invalid (NaN, Inf, out of range).
func (c *Calculator) Threshold(stakeRatio float64) float64 {
	if math.IsNaN(stakeRatio) || math.IsInf(stakeRatio, 0) {
		return 0
	}
	if stakeRatio <= 0 {
		return 0
	}
	f := c.ActiveSlotCoeff
	if math.IsNaN(f) || math.IsInf(f, 0) || f <= 0 || f > 1 {
		return 0
	}
	if stakeRatio >= 1 {
		return f
	}
	// threshold(sigma) = 1 - (1-f)^sigma
	return 1 - math.Pow(1-f, stakeRatio)
}
