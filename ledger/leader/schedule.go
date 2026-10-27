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

	nodeconsensus "github.com/blinklabs-io/dingo/consensus"
	"github.com/blinklabs-io/gouroboros/consensus"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/vrf"
)

// ScheduleFormatVersion identifies the in-memory and on-disk shape of a
// computed Schedule. Bump it whenever a code change alters how schedules
// are computed (e.g. the consensus mode threading added with the
// per-era TPraos/CPraos split, the epoch slot range becoming era-aware, or
// the active slot coefficient switching from a float64 approximation to the
// exact Shelley genesis rational, or finite E34 leader eligibility). Older
// persisted schedules then fail
// validatePersistedSchedule and are recomputed from scratch instead of being
// silently reused with stale assumptions. validatePersistedSchedule re-checks
// the epoch nonce and both sigma inputs but not the active slot coefficient,
// so the version bump is the only thing that retires a schedule computed with
// a different f.
const ScheduleFormatVersion = 7

// EpochSlotRange identifies the absolute slot range covered by an epoch.
// StartSlot is inclusive and SlotCount is the number of slots to evaluate.
type EpochSlotRange struct {
	StartSlot uint64
	SlotCount uint64
}

// Schedule represents the leader schedule for a stake pool in an epoch.
// It contains the slots where the pool is eligible to produce blocks.
type Schedule struct {
	FormatVersion     int                 // Snapshot of ScheduleFormatVersion at compute time
	ConsensusIdentity string              // Consensus algorithm and semantic version
	Epoch             uint64              // Epoch this schedule is for
	PoolId            lcommon.PoolKeyHash // Pool key hash
	PoolStake         uint64              // Pool's stake from active snapshot
	TotalStake        uint64              // Total active stake from active snapshot
	EpochNonce        []byte              // Epoch nonce for VRF
	EpochStartSlot    uint64              // First absolute slot in the epoch
	EpochSlotCount    uint64              // Number of slots in the epoch
	EraID             uint                // Ledger era governing the schedule
	// DistributionCommitment binds public-schedule algorithms to the whole
	// election distribution. It is empty for Praos schedules.
	DistributionCommitment []byte
	LeaderSlots            []uint64 // Slots where pool is leader, ascending-sorted

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
		FormatVersion:     ScheduleFormatVersion,
		ConsensusIdentity: nodeconsensus.PraosIdentity,
		Epoch:             epoch,
		PoolId:            poolId,
		PoolStake:         poolStake,
		TotalStake:        totalStake,
		EpochNonce:        epochNonce,
		LeaderSlots:       make([]uint64, 0),
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
	// ActiveSlotCoeffRat (f) is the exact active slot coefficient as written
	// in the Shelley genesis. When non-nil it is used verbatim and
	// ActiveSlotCoeff is ignored.
	//
	// Prefer this over ActiveSlotCoeff: finite E34 preparation must use the
	// exact genesis rational, not its binary64 approximation. Unsupported
	// positive rationals are rejected by the node-domain validation.
	ActiveSlotCoeffRat *big.Rat

	// ActiveSlotCoeff (f) determines block production rate.
	// For mainnet, f = 0.05 (5% of slots have blocks on average).
	// Used only when ActiveSlotCoeffRat is nil.
	ActiveSlotCoeff float64
}

// NewCalculator creates a calculator with the given protocol parameters.
func NewCalculator(activeSlotCoeff float64) *Calculator {
	return &Calculator{
		ActiveSlotCoeff: activeSlotCoeff,
	}
}

// ratOne is the upper bound of the valid active slot coefficient range.
var ratOne = big.NewRat(1, 1)

// activeSlotCoeffRat resolves the active slot coefficient to a *big.Rat for
// use with the gouroboros consensus package. The exact genesis rational is
// preferred; the float64 field is only a fallback for callers that have no
// rational available. Returns an error if the coefficient is not in the range
// (0, 1].
func (c *Calculator) activeSlotCoeffRat() (*big.Rat, error) {
	if c.ActiveSlotCoeffRat != nil {
		if c.ActiveSlotCoeffRat.Sign() <= 0 ||
			c.ActiveSlotCoeffRat.Cmp(ratOne) > 0 {
			return nil, fmt.Errorf(
				"active slot coefficient must be in (0, 1], got %s",
				c.ActiveSlotCoeffRat.RatString(),
			)
		}
		// Copy so a caller-owned genesis value can never be mutated by the
		// consensus package or by a later assignment through the same pointer.
		return new(big.Rat).Set(c.ActiveSlotCoeffRat), nil
	}
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
// Each slot is checked with one immutable, prepared finite E34 predicate.
//
// The consensus mode is era-specific and MUST be passed correctly. TPraos
// and CPraos differ in two independent ways: the VRF input construction
// (TPraos folds the SeedL constant via vrf.MkSeedTPraos; CPraos uses
// vrf.MkInputVrf directly) and certified-natural derivation (TPraos uses
// the raw 512-bit output; CPraos uses BLAKE2b-256("L"||output), 256 bits).
// The modes select independent leader-slot sets for the same key+nonce, so
// computing the schedule with the wrong mode yields a leader list that
// disagrees with cardano-node's view of the same epoch. Forging at one
// of those wrongly-marked slots produces a header that cardano-node
// rejects with `OverlayFailure (VRFLeaderValueTooBig …)`, which closes
// the chainsync session and prevents the dingo-forged block from
// reaching the canonical chain.
func (c *Calculator) CalculateSchedule(
	epoch uint64,
	epochRange EpochSlotRange,
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
	if epochRange.SlotCount == 0 {
		return nil, errors.New("epoch slot count cannot be zero")
	}
	if epochRange.StartSlot > ^uint64(0)-epochRange.SlotCount {
		return nil, fmt.Errorf(
			"epoch slot range overflows uint64: start=%d count=%d",
			epochRange.StartSlot,
			epochRange.SlotCount,
		)
	}

	// Create VRF signer from the pool's VRF secret key seed
	vrfSigner, err := consensus.NewSimpleVRFSigner(poolVrfSkey)
	if err != nil {
		return nil, fmt.Errorf("create VRF signer: %w", err)
	}

	schedule := NewSchedule(epoch, poolId, poolStake, totalStake, epochNonce)

	// Calculate epoch slot range
	epochStartSlot := epochRange.StartSlot
	epochEndSlot := epochStartSlot + epochRange.SlotCount

	activeSlotCoeff, err := c.activeSlotCoeffRat()
	if err != nil {
		return nil, fmt.Errorf("invalid active slot coefficient: %w", err)
	}

	// Prepare once for the epoch; no logarithm or cache lookup per slot.
	eligibility, err := consensus.NewLeaderEligibility(
		poolStake, totalStake, activeSlotCoeff, mode,
	)
	if err != nil {
		return nil, fmt.Errorf("prepare leader eligibility: %w", err)
	}
	// Zero-pool policy is separate from the primitive (which accepts f=1).
	// Validate nonce and the entire slot interval before returning empty.
	if poolStake == 0 {
		if _, err := vrfInputForMode(mode, epochStartSlot, epochNonce); err != nil {
			return nil, fmt.Errorf("check slot %d: %w", epochStartSlot, err)
		}
		if epochEndSlot-1 > math.MaxInt64 {
			return nil, fmt.Errorf(
				"slot %d exceeds maximum int64 value for VRF input",
				epochEndSlot-1,
			)
		}
		return schedule, nil
	}

	for slot := epochStartSlot; slot < epochEndSlot; slot++ {
		vrfInput, err := vrfInputForMode(mode, slot, epochNonce)
		if err != nil {
			return nil, fmt.Errorf("check slot %d: %w", slot, err)
		}
		_, output, err := vrfSigner.Prove(vrfInput)
		if err != nil {
			return nil, fmt.Errorf("check slot %d: %w", slot, err)
		}
		eligible, err := eligibility.CheckVRFOutput(output)
		if err != nil {
			return nil, fmt.Errorf("check VRF leader eligibility: %w", err)
		}
		if eligible {
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
		return vrf.MkSeedTPraos(
			int64(slot),
			epochNonce,
			vrf.SeedL(),
		) //nolint:gosec
	case consensus.ConsensusModeCPraos:
		return vrf.MkInputVrf(int64(slot), epochNonce) //nolint:gosec
	default:
		return nil, fmt.Errorf("unknown consensus mode: %d", mode)
	}
}
