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

package snapshot

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"math/big"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

var errRewardInputEpochUnavailable = errors.New(
	"reward input epoch metadata unavailable",
)

// errFallbackSupersededByAuthoritative is returned internally by
// saveSnapshotInTxn (never to an outside caller — saveSnapshot translates it
// into saved=false, nil) when a fallback, non-authoritative write's
// in-transaction re-check finds that an authoritative mark snapshot has
// already landed for the target epoch/boundary. See saveSnapshot's
// checkAuthoritativeMark parameter.
var errFallbackSupersededByAuthoritative = errors.New(
	"snapshot: authoritative mark snapshot already captured",
)

// saveSnapshot saves a stake distribution as a snapshot of the given type.
// It returns saved=false with no error when a fallback capture is superseded by
// an authoritative capture that already landed for the target epoch.
//
// resolveAutoVote controls whether the CIP-1694 reward-account auto-vote is
// computed against the live Pool/Account tables and frozen onto the snapshot
// rows.
//
// persistRewardInputs controls whether the live reward aggregate is copied into
// RewardSnapshot/Reward*Input rows for stake reward calculation.
//
// Both gates MUST be true only when the caller knows that live state matches the
// target epoch boundary (e.g. the normal epoch-transition call at the boundary
// moment, or a fresh genesis bootstrap). For historical seed writes — most
// notably the post-Mithril epoch-0 / N-1 / N-2 rows — pass false: those rows
// represent older boundaries than the live state. Resolving auto-votes would
// silently freeze today's delegation map onto a historical snapshot, and
// persisting reward inputs would make the reward calculator consume synthetic
// current-state rows as if they were exact historical inputs. Historical rows
// with unresolved auto-vote are tallied as PoolRewardAccountAutoVoteNone.
//
// checkAuthoritativeMark must be true only for the fallback (event-driven)
// mark-snapshot capture path (captureMarkSnapshot); the authoritative
// CaptureEpochBoundarySnapshot path always passes false. A fallback claims the
// lockable reward_snapshot key before replacing Mark rows and is superseded
// (this method returns errFallbackSupersededByAuthoritative) whenever an
// authoritative row already exists. When reward inputs are available, the
// normal ClaimFallbackRewardSnapshot marker provides that guard. Without
// ended-epoch metadata, ClaimFallbackRewardSnapshotGuard temporarily claims the
// same key and removes only the row it inserted before commit, preserving the
// invariant that no reward_snapshot remains without reward inputs. In both
// cases a concurrent authoritative writer blocks on the same key/row lock
// (MySQL/Postgres) or is serialized by single-writer semantics (SQLite),
// closing the check-then-write race outright rather than merely narrowing it.
func (m *Manager) saveSnapshot(
	ctx context.Context,
	epoch uint64,
	snapshotType string,
	distribution *StakeDistribution,
	evt event.EpochTransitionEvent,
	resolveAutoVote bool,
	persistRewardInputs bool,
	checkAuthoritativeMark bool,
) (bool, error) {
	_ = ctx
	txn := m.db.Transaction(true) // read-write transaction
	defer func() { _ = txn.Rollback() }()

	if err := m.saveSnapshotInTxn(
		epoch,
		snapshotType,
		distribution,
		evt,
		resolveAutoVote,
		persistRewardInputs,
		checkAuthoritativeMark,
		txn,
	); err != nil {
		switch {
		case errors.Is(err, errFallbackSupersededByAuthoritative):
			m.logger.Debug(
				"authoritative mark snapshot landed concurrently; discarding fallback capture",
				"component",
				"snapshot",
				"epoch",
				epoch,
				"snapshot_type",
				snapshotType,
			)
			return false, nil
		}
		return false, err
	}

	if err := txn.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (m *Manager) saveSnapshotInTxn(
	epoch uint64,
	snapshotType string,
	distribution *StakeDistribution,
	evt event.EpochTransitionEvent,
	resolveAutoVote bool,
	persistRewardInputs bool,
	checkAuthoritativeMark bool,
	txn *database.Txn,
) error {
	meta := m.db.Metadata()
	metaTxn := txn.Metadata()

	authoritative := !checkAuthoritativeMark

	// Build the reward-state bundle before any write so the reward_snapshot
	// marker can be the first row written. Writing the marker before the
	// pool-stake snapshots gives the authoritative (epoch-rollover) and fallback
	// (event-driven) capture paths the same lock-acquisition order
	// (reward_snapshot, then pool_stake_snapshot, then the reward input rows),
	// which keeps a concurrent authoritative-vs-fallback capture deadlock-free on
	// MySQL/Postgres. bundle is nil when reward inputs are disabled for this
	// call, or skipped because the ended-epoch metadata is not yet available. In
	// either case there is no durable reward marker or reward-input row to write,
	// so both the authoritative and fallback capture paths fall through and still
	// persist the Mark pool-stake snapshot and epoch summary below — the
	// leader-election data that must be captured on every epoch transition
	// regardless of reward-input availability.
	var bundle *rewardStateBundle
	if persistRewardInputs {
		var err error
		bundle, err = m.buildRewardStateInputs(
			epoch, snapshotType, distribution, evt, meta, metaTxn,
		)
		if err != nil {
			return fmt.Errorf("build reward state inputs: %w", err)
		}
	}

	var temporaryFallbackGuardID uint
	if bundle != nil {
		bundle.snapshot.Authoritative = authoritative
		if authoritative {
			// Authoritative capture: overwrite any provisional row.
			if err := meta.SaveRewardSnapshot(bundle.snapshot, metaTxn); err != nil {
				return fmt.Errorf("save reward snapshot: %w", err)
			}
		} else {
			// Fallback capture: claim the marker atomically and bail out if an
			// authoritative row already occupies it.
			proceed, err := meta.ClaimFallbackRewardSnapshot(
				bundle.snapshot, metaTxn,
			)
			if err != nil {
				return fmt.Errorf("claim fallback reward snapshot: %w", err)
			}
			if !proceed {
				return errFallbackSupersededByAuthoritative
			}
		}
	} else if checkAuthoritativeMark {
		// No reward bundle means there is no durable reward_snapshot marker to
		// claim. Temporarily claim the same unique key so a fallback still
		// serializes against authoritative rollover before replacing Mark rows.
		// The helper leaves an existing provisional row untouched and returns a
		// non-zero ID only when this transaction inserted the temporary row.
		proceed, guardID, err := meta.ClaimFallbackRewardSnapshotGuard(
			epoch,
			snapshotType,
			metaTxn,
		)
		if err != nil {
			return fmt.Errorf("claim fallback reward snapshot guard: %w", err)
		}
		if !proceed {
			return errFallbackSupersededByAuthoritative
		}
		temporaryFallbackGuardID = guardID
	}

	// Save pool stake snapshots
	snapshots := make(
		[]*models.PoolStakeSnapshot,
		0,
		len(distribution.PoolStakes),
	)
	leiosKeys, err := m.snapshotLeiosKeys(
		distribution,
		evt,
		meta,
		metaTxn,
	)
	if err != nil {
		return fmt.Errorf("resolve snapshot Leios keys: %w", err)
	}
	for poolKeyHash, stake := range distribution.PoolStakes {
		delegators := distribution.DelegatorCount[poolKeyHash]
		leiosKey := leiosKeys[string(poolKeyHash[:])]
		var leiosKeyPublic, leiosKeyPossessionProof []byte
		if leiosKey != nil {
			leiosKeyPublic = append([]byte(nil), leiosKey.PublicKey...)
			leiosKeyPossessionProof = append(
				[]byte(nil), leiosKey.PossessionProof...,
			)
		}
		snapshots = append(snapshots, &models.PoolStakeSnapshot{
			Epoch:                   epoch,
			SnapshotType:            snapshotType,
			PoolKeyHash:             poolKeyHash[:], // Convert [28]byte to []byte
			TotalStake:              types.Uint64(stake),
			DelegatorCount:          delegators,
			CapturedSlot:            distribution.Slot,
			LeiosKeyPublic:          leiosKeyPublic,
			LeiosKeyPossessionProof: leiosKeyPossessionProof,
			CalculationVersion:      models.RewardStakeCalculationVersion,
		})
	}

	// Freeze the CIP-1694 SPO reward-account auto-vote per pool at
	// the snapshot boundary so governance ratification at epoch N
	// reads snapshot-era delegation rather than the live, possibly
	// re-delegated state. Skipped (rows left Resolved=false) when
	// the caller is seeding historical epochs where live state does
	// not match the target boundary.
	if resolveAutoVote {
		if err := m.db.ResolvePoolRewardAccountAutoVotes(
			snapshots, txn,
		); err != nil {
			return fmt.Errorf("resolve reward-account auto-votes: %w", err)
		}
	}

	if err := meta.DeletePoolStakeSnapshotsForEpoch(
		epoch, snapshotType, metaTxn,
	); err != nil {
		return fmt.Errorf("replace pool snapshots: delete prior set: %w", err)
	}
	if err := meta.SavePoolStakeSnapshots(snapshots, metaTxn); err != nil {
		return fmt.Errorf("save pool snapshots: %w", err)
	}

	// Save epoch summary
	summary := &models.EpochSummary{
		Epoch:            epoch,
		TotalActiveStake: types.Uint64(distribution.TotalStake),
		TotalPoolCount:   distribution.TotalPools,
		TotalDelegators:  sumDelegators(distribution.DelegatorCount),
		EpochNonce:       evt.EpochNonce,
		BoundarySlot:     evt.BoundarySlot,
		SnapshotReady:    true,
	}

	if err := meta.SaveEpochSummary(summary, metaTxn); err != nil {
		return fmt.Errorf("save epoch summary: %w", err)
	}

	// Finalize the reward input rows. The reward_snapshot marker was already
	// written above (SaveRewardSnapshot / ClaimFallbackRewardSnapshot); here we
	// replace the per-pool and per-credential rows keyed off it.
	if bundle != nil {
		if err := m.saveRewardStateInputRows(
			epoch, bundle, meta, metaTxn,
		); err != nil {
			return fmt.Errorf("save reward state inputs: %w", err)
		}
	}

	// A no-bundle fallback uses a temporary reward_snapshot row only as a
	// lockable serialization key. Delete exactly the row this transaction
	// inserted; the database retains its row/unique-key locks until commit, so a
	// waiting authoritative upsert cannot pass this capture before then.
	if temporaryFallbackGuardID != 0 {
		if err := meta.ReleaseFallbackRewardSnapshotGuard(
			temporaryFallbackGuardID,
			metaTxn,
		); err != nil {
			return fmt.Errorf("release fallback reward snapshot guard: %w", err)
		}
	}

	return nil
}

// snapshotLeiosKeys resolves the pool parameters that were effective during
// the epoch ending at this SNAP boundary. A registration submitted during the
// ended epoch is still a future parameter until POOLREAP, so reading the live
// pool row (or simply the latest registration at distribution.Slot) would
// freeze a key one epoch too early. This is the same historical selection used
// by buildRewardStateInputs for the rest of the snapshotted pool parameters.
//
// Missing legacy epoch metadata leaves the affected seats keyless. It must not
// fall back to current pool state: doing so would make an old snapshot resolve
// differently after a key rotation.
func (m *Manager) snapshotLeiosKeys(
	distribution *StakeDistribution,
	evt event.EpochTransitionEvent,
	meta metadata.MetadataStore,
	metaTxn types.Txn,
) (map[string]*lcommon.LeiosKey, error) {
	ret := make(map[string]*lcommon.LeiosKey)
	if distribution == nil || len(distribution.PoolStakes) == 0 {
		return ret, nil
	}
	endedEpoch, err := meta.GetEpoch(evt.PreviousEpoch, metaTxn)
	if err != nil {
		return nil, fmt.Errorf(
			"get ended epoch %d: %w", evt.PreviousEpoch, err,
		)
	}
	if endedEpoch == nil {
		return ret, nil
	}
	poolKeys := make([]lcommon.PoolKeyHash, 0, len(distribution.PoolStakes))
	for poolKey := range distribution.PoolStakes {
		poolKeys = append(poolKeys, poolKey)
	}
	registrations, err := meta.GetPoolRegistrationsEffectiveForEpoch(
		poolKeys,
		endedEpoch.StartSlot,
		evt.PreviousEpoch,
		distribution.Slot,
		metaTxn,
	)
	if err != nil {
		return nil, err
	}
	for _, registration := range registrations {
		if len(registration.LeiosKeyPublic) == 0 ||
			len(registration.LeiosKeyPossessionProof) == 0 {
			continue
		}
		ret[string(registration.PoolKeyHash)] = &lcommon.LeiosKey{
			PublicKey: append(
				[]byte(nil), registration.LeiosKeyPublic...,
			),
			PossessionProof: append(
				[]byte(nil), registration.LeiosKeyPossessionProof...,
			),
		}
	}
	return ret, nil
}

// rewardStateBundle is the fully computed reward-state capture for one epoch
// boundary, held in memory so saveSnapshotInTxn can write its reward_snapshot
// marker first (for consistent lock ordering) and the input rows afterward.
type rewardStateBundle struct {
	snapshot    *models.RewardSnapshot
	poolInputs  []*models.RewardPoolInput
	stakeInputs []*models.RewardStakeInput
}

// buildRewardStateInputs computes the reward-state bundle without writing any
// row. It returns (nil, nil) when reward inputs must be skipped for this epoch
// because the ended-epoch metadata is not yet available.
//
// rewardInputs hard-errors per pool on missing/legacy/corrupt registration data
// (see rewardInputPoolError). A single pool with stale registration data must
// not wedge epoch-boundary snapshot capture for every other pool, so degraded
// pools are excluded from the reward-input distribution (and only from it —
// PoolStakeSnapshot and EpochSummary still reflect the true observed stake).
// TotalPoolCount and TotalDelegators describe that reduced reward-input set, so
// they still count exactly the rows written to reward_pool_input.
//
// TotalActiveStake does not. It is the reward calculation's sigma_a
// denominator, the Go equivalent of ssTotalActiveStake in cardano-ledger, which
// sums the stake of every registered credential that has a delegation
// (Cardano.Ledger.State.SnapShots.mkSnapShot over
// Cardano.Ledger.State.Stake.resolveInstantStake) independently of which pools
// appear in ssStakePoolsSnapShot. Deriving it from the reduced distribution
// instead shrinks that denominator, which raises sigma_a for every surviving
// pool, lowers its apparent performance, and under-credits every member and
// leader reward on the node — a divergence proportional to the excluded pool's
// share of active stake. It is therefore computed from the full reward-stake
// distribution, and the reward calculation requires the reward_pool_input rows
// to sum to no more than it rather than to exactly it.
func (m *Manager) buildRewardStateInputs(
	epoch uint64,
	snapshotType string,
	distribution *StakeDistribution,
	evt event.EpochTransitionEvent,
	meta metadata.MetadataStore,
	metaTxn types.Txn,
) (*rewardStateBundle, error) {
	rewardDistribution, err := rewardStakeDistribution(distribution)
	if err != nil {
		return nil, err
	}
	poolInputs, stakeInputs, effective, err := m.rewardInputsSkippingDegradedPools(
		epoch,
		rewardDistribution,
		evt,
		meta,
		metaTxn,
	)
	if err != nil {
		if errors.Is(err, errRewardInputEpochUnavailable) {
			m.logger.Warn(
				"skipping reward inputs without ended-epoch metadata",
				"component", "snapshot",
				"epoch", epoch,
			)
			return nil, nil
		}
		return nil, err
	}

	return &rewardStateBundle{
		snapshot: &models.RewardSnapshot{
			Epoch:        epoch,
			SnapshotType: snapshotType,
			TotalActiveStake: types.Uint64(
				sumPoolStakes(rewardDistribution.PoolStakes),
			),
			TotalPoolCount:     uint64(len(effective.PoolStakes)),
			TotalDelegators:    sumDelegators(effective.DelegatorCount),
			CapturedSlot:       distribution.Slot,
			BoundarySlot:       evt.BoundarySlot,
			EpochNonce:         evt.EpochNonce,
			ProtocolVersion:    evt.ProtocolVersion,
			CalculationVersion: models.RewardStakeCalculationVersion,
		},
		poolInputs:  poolInputs,
		stakeInputs: stakeInputs,
	}, nil
}

// rewardStakeDistribution derives reward pool totals solely from the
// RewardLiveStake credential inputs attached to dist. The caller's PoolStakes
// and aggregate fields remain the canonical leader-election distribution.
func rewardStakeDistribution(
	dist *StakeDistribution,
) (*StakeDistribution, error) {
	if dist == nil {
		return nil, errors.New("missing stake distribution")
	}
	for _, input := range dist.StakeInputs {
		if len(input.PoolKeyHash) != len(lcommon.PoolKeyHash{}) {
			return nil, fmt.Errorf(
				"invalid reward stake input pool key length %d",
				len(input.PoolKeyHash),
			)
		}
		if len(input.StakingKey) != len(lcommon.PoolKeyHash{}) {
			return nil, fmt.Errorf(
				"invalid reward stake input credential length %d",
				len(input.StakingKey),
			)
		}
		if input.CredentialTag > 1 {
			return nil, fmt.Errorf(
				"invalid reward stake input credential tag %d",
				input.CredentialTag,
			)
		}
	}
	deduped := dedupeStakeInputs(dist.StakeInputs)
	reward := &StakeDistribution{
		Slot:           dist.Slot,
		StakeInputs:    deduped,
		PoolStakes:     make(map[lcommon.PoolKeyHash]uint64),
		DelegatorCount: make(map[lcommon.PoolKeyHash]uint64),
	}
	for _, input := range reward.StakeInputs {
		if input.Stake == 0 {
			continue
		}
		var poolKey lcommon.PoolKeyHash
		copy(poolKey[:], input.PoolKeyHash)
		current := reward.PoolStakes[poolKey]
		if current > ^uint64(0)-input.Stake {
			return nil, fmt.Errorf(
				"delegated stake overflow for pool %x",
				poolKey[:],
			)
		}
		if reward.TotalStake > ^uint64(0)-input.Stake {
			return nil, errors.New("total active stake overflow")
		}
		reward.PoolStakes[poolKey] = current + input.Stake
		reward.DelegatorCount[poolKey]++
		reward.TotalStake += input.Stake
	}
	reward.TotalPools = uint64(len(reward.PoolStakes))
	return reward, nil
}

// saveRewardStateInputRows replaces the per-pool and per-credential reward input
// rows for an epoch whose reward_snapshot marker has already been written.
func (m *Manager) saveRewardStateInputRows(
	epoch uint64,
	bundle *rewardStateBundle,
	meta metadata.MetadataStore,
	metaTxn types.Txn,
) error {
	if err := meta.DeleteRewardOutputsForEpoch(epoch, metaTxn); err != nil {
		return fmt.Errorf("replace reward outputs: %w", err)
	}
	if err := meta.DeleteRewardInputsForEpoch(epoch, metaTxn); err != nil {
		return fmt.Errorf("replace reward inputs: %w", err)
	}

	if err := meta.SaveRewardPoolInputs(bundle.poolInputs, metaTxn); err != nil {
		return fmt.Errorf("save reward pool inputs: %w", err)
	}
	if err := meta.SaveRewardStakeInputs(bundle.stakeInputs, metaTxn); err != nil {
		return fmt.Errorf("save reward stake inputs: %w", err)
	}
	return nil
}

// rewardInputsSkippingDegradedPools calls rewardInputs and, when it fails
// because exactly one pool has missing or invalid registration data (a
// rewardInputPoolError — unresolvable registration, malformed reward
// account, malformed credential tag, missing margin, or a malformed owner
// key hash), excludes that single pool from a working copy of the stake
// distribution, logs a warning, and retries. Real Cardano reward semantics
// already exclude a pool with no resolvable registration from the active
// reward stake: its delegators simply do not participate in pool reward
// distribution for the epoch: they are not otherwise penalized. Any other
// error (for example a totals mismatch within the stake distribution
// itself, or a missing ended-epoch row) is a genuine data-integrity problem
// unrelated to pool registration quality and is returned unchanged so the
// caller still hard-fails on it.
//
// The returned distribution is the one actually consumed to build
// poolInputs/stakeInputs, so its totals are safe to use for RewardSnapshot.
func (m *Manager) rewardInputsSkippingDegradedPools(
	epoch uint64,
	distribution *StakeDistribution,
	evt event.EpochTransitionEvent,
	meta metadata.MetadataStore,
	metaTxn types.Txn,
) ([]*models.RewardPoolInput, []*models.RewardStakeInput, *StakeDistribution, error) {
	working := cloneStakeDistributionForRewardInputs(distribution)
	for {
		poolInputs, stakeInputs, err := m.rewardInputs(
			epoch,
			working,
			evt,
			meta,
			metaTxn,
		)
		if err == nil {
			return poolInputs, stakeInputs, working, nil
		}
		var poolErr *rewardInputPoolError
		if !errors.As(err, &poolErr) {
			return nil, nil, nil, err
		}
		if !excludeRewardInputPool(working, poolErr.poolKeyHash) {
			// Removing the reported pool made no progress (already
			// absent, or a malformed key length); surface the original
			// error instead of looping forever.
			return nil, nil, nil, err
		}
		m.logger.Warn(
			"skipping pool from reward inputs: missing or invalid pool registration data",
			"component",
			"snapshot",
			"epoch",
			epoch,
			"pool_key_hash",
			hex.EncodeToString(poolErr.poolKeyHash),
			"reason",
			err,
		)
	}
}

// rewardInputPoolError identifies the single pool responsible for a
// rewardInputs failure so a caller can exclude just that pool (and its
// delegators) from reward participation instead of aborting the whole
// epoch-boundary snapshot for every pool. Its Error() text is unchanged
// from a plain error so direct callers (including tests asserting on
// rewardInputs's returned error) see the exact same message as before.
type rewardInputPoolError struct {
	poolKeyHash []byte
	msg         string
}

func (e *rewardInputPoolError) Error() string { return e.msg }

// cloneStakeDistributionForRewardInputs returns a copy of dist whose
// pool/stake maps and slice are independent of the original, so pools can be
// excluded from it without mutating the distribution used for
// PoolStakeSnapshot/EpochSummary, which must keep reflecting the true
// observed stake regardless of pool-registration data quality.
func cloneStakeDistributionForRewardInputs(
	dist *StakeDistribution,
) *StakeDistribution {
	clone := &StakeDistribution{
		Slot: dist.Slot,
		PoolStakes: make(
			map[lcommon.PoolKeyHash]uint64,
			len(dist.PoolStakes),
		),
		DelegatorCount: make(
			map[lcommon.PoolKeyHash]uint64,
			len(dist.DelegatorCount),
		),
		StakeInputs: append([]StakeInput(nil), dist.StakeInputs...),
		TotalStake:  dist.TotalStake,
		TotalPools:  dist.TotalPools,
	}
	maps.Copy(clone.PoolStakes, dist.PoolStakes)
	maps.Copy(clone.DelegatorCount, dist.DelegatorCount)
	return clone
}

// excludeRewardInputPool removes a single pool, and its delegators' stake
// inputs, from a reward-input working copy of a stake distribution. It
// reports whether the pool was present (and so removal made progress); a
// caller can use a false return to avoid looping forever on a pool it can
// no longer locate.
func excludeRewardInputPool(dist *StakeDistribution, poolKeyHash []byte) bool {
	var poolKey lcommon.PoolKeyHash
	if len(poolKeyHash) != len(poolKey) {
		return false
	}
	copy(poolKey[:], poolKeyHash)
	if _, ok := dist.PoolStakes[poolKey]; !ok {
		return false
	}
	delete(dist.PoolStakes, poolKey)
	delete(dist.DelegatorCount, poolKey)
	if dist.TotalPools > 0 {
		dist.TotalPools--
	}
	filtered := make([]StakeInput, 0, len(dist.StakeInputs))
	for _, input := range dist.StakeInputs {
		if bytes.Equal(input.PoolKeyHash, poolKey[:]) {
			continue
		}
		filtered = append(filtered, input)
	}
	dist.StakeInputs = filtered
	dist.TotalStake = sumPoolStakes(dist.PoolStakes)
	return true
}

// sumPoolStakes totals all pool stakes, mirroring sumDelegators. Recomputed
// directly from the map (rather than tracked incrementally) so it cannot
// drift from dist.PoolStakes after pools are excluded.
func sumPoolStakes(stakes map[lcommon.PoolKeyHash]uint64) uint64 {
	var total uint64
	for _, stake := range stakes {
		total += stake
	}
	return total
}

func (m *Manager) rewardInputs(
	epoch uint64,
	distribution *StakeDistribution,
	evt event.EpochTransitionEvent,
	meta metadata.MetadataStore,
	metaTxn types.Txn,
) ([]*models.RewardPoolInput, []*models.RewardStakeInput, error) {
	if err := validateRewardStakeInputTotals(distribution); err != nil {
		return nil, nil, err
	}
	if len(distribution.PoolStakes) == 0 {
		return nil, nil, nil
	}

	poolKeys := make([]lcommon.PoolKeyHash, 0, len(distribution.PoolStakes))
	for poolKey := range distribution.PoolStakes {
		poolKeys = append(poolKeys, poolKey)
	}
	blockCounts, totalBlocks, err := rewardPoolBlockCounts(
		meta,
		metaTxn,
		poolKeys,
		evt,
	)
	if err != nil {
		return nil, nil, err
	}
	endedEpoch, err := meta.GetEpoch(evt.PreviousEpoch, metaTxn)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"get ended epoch %d for reward inputs: %w",
			evt.PreviousEpoch, err,
		)
	}
	if endedEpoch == nil {
		return nil, nil, fmt.Errorf(
			"%w: ended epoch %d not found for reward inputs",
			errRewardInputEpochUnavailable,
			evt.PreviousEpoch,
		)
	}
	// SNAP snapshots the pool params active DURING the ended epoch:
	// re-registrations submitted within it are future params promoted only
	// after SNAP (in POOLREAP), so the latest cert at the snapshot slot
	// would be one epoch early for any pool that changed its params
	// mid-epoch.
	registrations, err := meta.GetPoolRegistrationsEffectiveForEpoch(
		poolKeys,
		endedEpoch.StartSlot,
		evt.PreviousEpoch,
		distribution.Slot,
		metaTxn,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"get reward input pool registrations: %w",
			err,
		)
	}
	registrationByHash := make(
		map[string]models.PoolRegistration,
		len(registrations),
	)
	for _, registration := range registrations {
		registrationByHash[string(registration.PoolKeyHash)] = registration
	}
	ownerSets, err := rewardOwnerSets(registrationByHash)
	if err != nil {
		return nil, nil, err
	}
	stakeInputs, ownerStakeByPool := rewardStakeInputs(
		epoch,
		distribution,
		evt,
		ownerSets,
	)

	inputs := make([]*models.RewardPoolInput, 0, len(distribution.PoolStakes))
	for poolKey, stake := range distribution.PoolStakes {
		poolKeyHash := make([]byte, len(poolKey))
		copy(poolKeyHash, poolKey[:])
		input := &models.RewardPoolInput{
			Epoch:          epoch,
			PoolKeyHash:    poolKeyHash,
			DelegatedStake: types.Uint64(stake),
			OwnerStake:     types.Uint64(ownerStakeByPool[string(poolKey[:])]),
			DelegatorCount: distribution.DelegatorCount[poolKey],
			CapturedSlot:   distribution.Slot,
			BoundarySlot:   evt.BoundarySlot,
		}
		if totalBlocks != nil {
			input.TotalBlocksInEpoch = new(*totalBlocks)
		}
		if blockCounts != nil {
			input.BlocksProduced = new(blockCounts[string(poolKey[:])])
		}
		registration, ok := registrationByHash[string(poolKey[:])]
		if !ok {
			return nil, nil, &rewardInputPoolError{
				poolKeyHash: append([]byte(nil), poolKey[:]...),
				msg: fmt.Sprintf(
					"missing pool registration while saving reward inputs: epoch=%d snapshot_slot=%d pool_key_hash=%s",
					epoch,
					distribution.Slot,
					hex.EncodeToString(poolKey[:]),
				),
			}
		}
		if len(registration.RewardAccount) != len(lcommon.PoolKeyHash{}) {
			return nil, nil, &rewardInputPoolError{
				poolKeyHash: append([]byte(nil), poolKey[:]...),
				msg: fmt.Sprintf(
					"invalid reward account length while saving reward inputs: epoch=%d snapshot_slot=%d pool_key_hash=%s length=%d",
					epoch,
					distribution.Slot,
					hex.EncodeToString(poolKey[:]),
					len(registration.RewardAccount),
				),
			}
		}
		if registration.RewardAccountCredentialTag > 1 {
			return nil, nil, &rewardInputPoolError{
				poolKeyHash: append([]byte(nil), poolKey[:]...),
				msg: fmt.Sprintf(
					"invalid reward account credential tag while saving reward inputs: epoch=%d snapshot_slot=%d pool_key_hash=%s tag=%d",
					epoch,
					distribution.Slot,
					hex.EncodeToString(poolKey[:]),
					registration.RewardAccountCredentialTag,
				),
			}
		}
		if registration.Margin == nil || registration.Margin.Rat == nil {
			return nil, nil, &rewardInputPoolError{
				poolKeyHash: append([]byte(nil), poolKey[:]...),
				msg: fmt.Sprintf(
					"missing pool margin while saving reward inputs: epoch=%d snapshot_slot=%d pool_key_hash=%s",
					epoch,
					distribution.Slot,
					hex.EncodeToString(poolKey[:]),
				),
			}
		}
		input.Pledge = registration.Pledge
		input.Cost = registration.Cost
		input.Margin = cloneRat(registration.Margin)
		input.RewardAccount = append(
			[]byte(nil),
			registration.RewardAccount...,
		)
		input.RewardAccountCredentialTag = registration.RewardAccountCredentialTag
		inputs = append(inputs, input)
	}
	return inputs, stakeInputs, nil
}

func rewardPoolBlockCounts(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	poolKeys []lcommon.PoolKeyHash,
	evt event.EpochTransitionEvent,
) (map[string]uint64, *uint64, error) {
	if evt.BoundarySlot == 0 {
		return nil, nil, nil
	}
	epoch, err := meta.GetEpoch(evt.PreviousEpoch, metaTxn)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"get previous epoch for reward performance: %w",
			err,
		)
	}
	if epoch == nil || epoch.LengthInSlots == 0 {
		return nil, nil, nil
	}

	startSlot := epoch.StartSlot
	endSlot := startSlot + uint64(epoch.LengthInSlots) - 1
	boundaryEndSlot := evt.BoundarySlot - 1
	if boundaryEndSlot < endSlot {
		endSlot = boundaryEndSlot
	}
	if endSlot < startSlot {
		return nil, nil, nil
	}

	counts, total, err := meta.CountPoolBlocksInSlotRange(
		poolKeys,
		startSlot,
		endSlot,
		metaTxn,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"count reward pool blocks in epoch %d: %w",
			evt.PreviousEpoch,
			err,
		)
	}
	return counts, &total, nil
}

func validateRewardStakeInputTotals(distribution *StakeDistribution) error {
	if distribution == nil {
		return errors.New("missing stake distribution")
	}
	stakeByPool := make(
		map[lcommon.PoolKeyHash]uint64,
		len(distribution.PoolStakes),
	)
	for _, input := range distribution.StakeInputs {
		if len(input.PoolKeyHash) != len(lcommon.PoolKeyHash{}) {
			return fmt.Errorf(
				"invalid reward stake input pool key length %d",
				len(input.PoolKeyHash),
			)
		}
		if len(input.StakingKey) != len(lcommon.PoolKeyHash{}) {
			return fmt.Errorf(
				"invalid reward stake input credential length %d",
				len(input.StakingKey),
			)
		}
		if input.CredentialTag > 1 {
			return fmt.Errorf(
				"invalid reward stake input credential tag %d",
				input.CredentialTag,
			)
		}
		if input.Stake == 0 {
			continue
		}
		var poolKey lcommon.PoolKeyHash
		copy(poolKey[:], input.PoolKeyHash)
		current := stakeByPool[poolKey]
		if current > ^uint64(0)-input.Stake {
			return fmt.Errorf(
				"reward stake input total overflow for pool %s",
				hex.EncodeToString(poolKey[:]),
			)
		}
		stakeByPool[poolKey] = current + input.Stake
	}
	for poolKey, expected := range distribution.PoolStakes {
		actual := stakeByPool[poolKey]
		if actual != expected {
			return fmt.Errorf(
				"reward stake input total mismatch for pool %s: inputs=%d pool=%d",
				hex.EncodeToString(poolKey[:]),
				actual,
				expected,
			)
		}
		delete(stakeByPool, poolKey)
	}
	for poolKey, actual := range stakeByPool {
		if actual != 0 {
			return fmt.Errorf(
				"reward stake inputs contain unknown pool %s with stake %d",
				hex.EncodeToString(poolKey[:]),
				actual,
			)
		}
	}
	return nil
}

func rewardOwnerSets(
	registrations map[string]models.PoolRegistration,
) (map[string]map[string]struct{}, error) {
	ret := make(map[string]map[string]struct{}, len(registrations))
	for poolKey, registration := range registrations {
		owners := make(map[string]struct{}, len(registration.Owners))
		for _, owner := range registration.Owners {
			if len(owner.KeyHash) != len(lcommon.PoolKeyHash{}) {
				return nil, &rewardInputPoolError{
					poolKeyHash: append(
						[]byte(nil),
						registration.PoolKeyHash...),
					msg: fmt.Sprintf(
						"invalid pool owner key hash length while saving reward inputs: pool_key_hash=%s length=%d",
						hex.EncodeToString(registration.PoolKeyHash),
						len(owner.KeyHash),
					),
				}
			}
			owners[models.NewStakeCredentialRef(0, owner.KeyHash).MapKey()] = struct{}{}
		}
		ret[poolKey] = owners
	}
	return ret, nil
}

func rewardStakeInputs(
	epoch uint64,
	distribution *StakeDistribution,
	evt event.EpochTransitionEvent,
	ownerSets map[string]map[string]struct{},
) ([]*models.RewardStakeInput, map[string]uint64) {
	ret := make(
		[]*models.RewardStakeInput,
		0,
		len(distribution.StakeInputs),
	)
	ownerStakeByPool := make(map[string]uint64, len(distribution.PoolStakes))
	for _, input := range distribution.StakeInputs {
		row := &models.RewardStakeInput{
			Epoch:         epoch,
			PoolKeyHash:   append([]byte(nil), input.PoolKeyHash...),
			CredentialTag: input.CredentialTag,
			StakingKey:    append([]byte(nil), input.StakingKey...),
			Stake:         types.Uint64(input.Stake),
			Registered:    input.Registered,
			CapturedSlot:  distribution.Slot,
			BoundarySlot:  evt.BoundarySlot,
		}
		owners := ownerSets[string(row.PoolKeyHash)]
		if _, ok := owners[models.NewStakeCredentialRef(
			row.CredentialTag,
			row.StakingKey,
		).MapKey()]; ok {
			row.Owner = true
			ownerStakeByPool[string(row.PoolKeyHash)] += uint64(row.Stake)
		}
		ret = append(ret, row)
	}
	return ret, ownerStakeByPool
}

func cloneRat(r *types.Rat) *types.Rat {
	if r == nil || r.Rat == nil {
		return nil
	}
	return &types.Rat{Rat: new(big.Rat).Set(r.Rat)}
}

// rotateSnapshots promotes Mark→Set→Go for the new epoch.
func (m *Manager) rotateSnapshots(ctx context.Context, newEpoch uint64) {
	_ = ctx
	// In practice, we store snapshots with their epoch and type.
	// The active stake distribution for epoch N is the Mark snapshot
	// captured at the epoch boundary. Historical Set/Go-equivalent data is
	// addressed by older epoch numbers rather than by copying rows between
	// snapshot types.

	goEpoch := uint64(0)
	setEpoch := uint64(0)
	if newEpoch > 1 {
		goEpoch = newEpoch - 2
	}
	if newEpoch > 0 {
		setEpoch = newEpoch - 1
	}

	m.logger.Debug(
		"snapshot rotation conceptual",
		"component", "snapshot",
		"epoch", newEpoch,
		"go_epoch", goEpoch,
		"set_epoch", setEpoch,
	)
}

// poolSnapshotRetentionMaxDepth bounds how many epochs of pool_stake_snapshot
// rows the deferred-header retention pin (issue #3727) may ever hold below the
// current epoch. It is a safety backstop far larger than any legitimate
// deferred-header gap (a header awaiting apply needs a snapshot within a few
// epochs of the apply cursor, and the retention guard additionally evicts
// deferred headers the cursor has already passed), so in normal operation it
// never binds; it exists only so a permanently-stuck deferred header cannot pin
// ~1.3M rows/epoch on mainnet without limit. Retention never drops below the
// default currentEpoch-3 window regardless.
const poolSnapshotRetentionMaxDepth uint64 = 24

// cleanupOldSnapshots removes snapshots older than needed for the rotation and
// delayed reward models. We keep 4 epochs of per-pool and per-credential rows:
// current, current-1, current-2 for Go, and current-3 so reward calculation can
// be replayed after a rollback across the boundary where those rewards were
// applied.
//
// reward_stake_input is pruned to that 4-epoch window in every storage mode:
// it scales with delegator count (~1.3M rows/epoch on mainnet) and is not
// needed once the epoch it snapshots has finished replaying. reward_account_output
// is pruned to the same window in CORE storage mode (types.StorageModeCore),
// matching dingo's original pruning behavior exactly, but retained WITHOUT BOUND
// in API storage mode (types.StorageModeAPI) so the Blockfrost account
// reward-history endpoint (GET /accounts/{stake_address}/rewards, dingo #1875)
// can serve an account's full reward history instead of only the trailing few
// epochs — the same "silently look empty past the window" failure mode #2987
// already identified for epoch_summary. See
// rewardstate.DeleteStateBeforeEpoch (core, prunes both tables) and
// rewardstate.DeleteStakeInputBeforeEpoch (API, prunes only
// reward_stake_input) for the implementation and full rationale.
//
// epoch_summary is deliberately NOT pruned. It is a single small row per epoch
// (aggregate stake/pool/delegator totals plus the epoch nonce and boundary
// slot), so retaining the full history costs one row per epoch — roughly a
// thousand rows over a network's lifetime — while making historical epoch
// aggregates permanently queryable. Pruning it also made a legitimately
// captured boundary indistinguishable from one that was never captured, which
// is what made dingo #2987 read as a missing epoch-2 snapshot on a node that
// had in fact captured it correctly 400 epochs earlier.
func (m *Manager) cleanupOldSnapshots(
	ctx context.Context,
	currentEpoch uint64,
) error {
	_ = ctx
	if currentEpoch < 3 {
		return nil // Not enough history to clean
	}

	deleteBeforeEpoch := currentEpoch - 3

	// Pool-stake snapshots may still be needed below the default window by a
	// queued/deferred header that validates leader eligibility against an
	// older epoch's mark snapshot (issue #3727). Pruning them out from under
	// such a header makes leaderEligibilityStake read the missing rows as a
	// zero-stake "pool absent" answer, which the reference node never
	// intended. So pool-snapshot pruning runs THROUGH the retention guard: the
	// guard holds the deferred-header set stable across both the retention
	// floor selection and this delete+commit, so no header can be admitted
	// between the floor read and the prune and lose its snapshot. Only the
	// pool-snapshot boundary moves; reward-state retention keeps the unchanged
	// currentEpoch-3 window (deferred header validation reads pool stake, never
	// reward rows), and runs in a separate transaction outside the guard to
	// keep the locked section tight.
	//
	// prunePoolSnapshots deletes AND commits below the boundary the guard
	// hands back (its own transaction), so the rows are actually gone before
	// the guard releases the deferred-header lock.
	prunePoolSnapshots := func(before uint64) error {
		if before < deleteBeforeEpoch {
			m.logger.Info(
				"retaining historical pool stake snapshots for deferred header validation",
				"component", "snapshot",
				"current_epoch", currentEpoch,
				"default_before_epoch", deleteBeforeEpoch,
				"pinned_before_epoch", before,
			)
		}
		poolTxn := m.db.Transaction(true)
		defer func() { _ = poolTxn.Rollback() }()
		if err := m.db.Metadata().DeletePoolStakeSnapshotsBeforeEpoch(
			before,
			poolTxn.Metadata(),
		); err != nil {
			return fmt.Errorf("cleanup pool snapshots: %w", err)
		}
		return poolTxn.Commit()
	}
	// minPoolSnapshotDeleteBefore is the hard backstop on how far the retention
	// pin can lower pruning: retain at most poolSnapshotRetentionMaxDepth epochs
	// of pool snapshots so an unresolvable deferred header cannot pin them
	// without bound (issue #3727, finding 5). It never rises above the default
	// window (a header needing a within-window snapshot is unaffected), and the
	// guard clamps the pinned boundary up to it.
	minPoolSnapshotDeleteBefore := uint64(0)
	if currentEpoch > poolSnapshotRetentionMaxDepth {
		minPoolSnapshotDeleteBefore = currentEpoch - poolSnapshotRetentionMaxDepth
	}
	if minPoolSnapshotDeleteBefore > deleteBeforeEpoch {
		minPoolSnapshotDeleteBefore = deleteBeforeEpoch
	}
	if guard := m.poolSnapshotRetentionGuard(); guard != nil {
		if err := guard(
			deleteBeforeEpoch,
			minPoolSnapshotDeleteBefore,
			prunePoolSnapshots,
		); err != nil {
			return err
		}
	} else if err := prunePoolSnapshots(deleteBeforeEpoch); err != nil {
		return err
	}

	// Reward-state pruning: unchanged currentEpoch-3 window, separate
	// transaction (not under the retention guard).
	txn := m.db.Transaction(true) // read-write transaction
	defer func() { _ = txn.Rollback() }()

	meta := m.db.Metadata()
	metaTxn := txn.Metadata()

	if m.db.StorageMode() == types.StorageModeAPI {
		// API storage mode: retain reward_account_output without bound (see
		// doc comment above) and prune only reward_stake_input.
		if err := meta.DeleteRewardStakeInputBeforeEpoch(
			deleteBeforeEpoch,
			metaTxn,
		); err != nil {
			return fmt.Errorf("cleanup reward state: %w", err)
		}
	} else if err := meta.DeleteRewardStateBeforeEpoch(
		deleteBeforeEpoch,
		metaTxn,
	); err != nil {
		return fmt.Errorf("cleanup reward state: %w", err)
	}

	m.logger.Debug(
		"cleaned up old snapshots",
		"component", "snapshot",
		"before_epoch", deleteBeforeEpoch,
	)

	return txn.Commit()
}

// sumDelegators totals all delegator counts.
func sumDelegators(counts map[lcommon.PoolKeyHash]uint64) uint64 {
	var total uint64
	for _, count := range counts {
		total += count
	}
	return total
}
