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

package ledgerstate

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// credentialHashSize is the length of a stake or pool key hash. Rows carrying
// anything else are rejected by the ledger's own reward-input validator.
const credentialHashSize = 28

// rewardInputBundle is one epoch's reward-calculation basis, derived from a
// single imported stake snapshot.
type rewardInputBundle struct {
	epoch       uint64
	snapshot    *models.RewardSnapshot
	poolInputs  []*models.RewardPoolInput
	stakeInputs []*models.RewardStakeInput
}

// deriveRewardInputs turns one imported stake snapshot into the reward basis
// for the epoch it represents.
//
// A node bootstrapped from a Mithril snapshot has no reward basis for the
// epochs preceding the import, so the first reward rounds after it are
// skipped — and a skipped round is never made up, leaving reward balances and
// the leadership stake derived from them permanently short (issue #3165). The
// snapshot itself carries what those rounds need: each of mark, set and go
// holds the per-credential stake, the credential-to-pool delegations, and the
// pool parameters for one epoch, and the three of them line up with exactly
// the epochs the rounds cannot otherwise compute.
//
// Everything here is derived rather than assumed, and the result is only
// worth persisting if it reconciles: see the caller, which puts the ledger's
// own validateRewardCalculatorInputs between this and the database. Rewards
// computed from a subtly wrong basis would be credited at the wrong amount
// rather than visibly not credited, and a silently wrong reward is worse than
// an absent one.
func deriveRewardInputs(
	snap *ParsedSnapShot,
	epoch uint64,
	capturedSlot uint64,
	boundarySlot uint64,
) *rewardInputBundle {
	if snap == nil {
		return nil
	}

	type poolAgg struct {
		delegated  uint64
		ownerStake uint64
		delegators uint64
	}
	aggs := make(map[string]*poolAgg, len(snap.PoolParams))
	owners := make(map[string]map[string]struct{}, len(snap.PoolParams))
	for poolHex, params := range snap.PoolParams {
		if params == nil {
			continue
		}
		aggs[poolHex] = &poolAgg{}
		set := make(map[string]struct{}, len(params.Owners))
		for _, owner := range params.Owners {
			set[hex.EncodeToString(owner)] = struct{}{}
		}
		owners[poolHex] = set
	}

	stakeInputs := make([]*models.RewardStakeInput, 0, len(snap.Stake))
	for credHex, stake := range snap.Stake {
		// The validator rejects a zero-stake input, and a credential
		// contributes nothing to a pool's reward either way.
		if stake == 0 {
			continue
		}
		poolKey, ok := snap.Delegations[credHex]
		if !ok || len(poolKey) == 0 {
			continue
		}
		poolHex := hex.EncodeToString(poolKey)
		agg, ok := aggs[poolHex]
		if !ok {
			// Delegated to a pool the snapshot carries no parameters for.
			// It cannot be paid, and including it would break the
			// pool-count reconciliation.
			continue
		}
		cred, err := hex.DecodeString(credHex)
		if err != nil || len(cred) == 0 {
			continue
		}
		// Snapshot stake credentials are key hashes; the script-credential
		// tag is not represented in this map.
		const credentialTagKeyHash = 0
		isOwner := false
		if set, ok := owners[poolHex]; ok {
			_, isOwner = set[credHex]
		}

		stakeInputs = append(stakeInputs, &models.RewardStakeInput{
			Epoch:         epoch,
			PoolKeyHash:   append([]byte(nil), poolKey...),
			StakingKey:    cred,
			CredentialTag: credentialTagKeyHash,
			Stake:         types.Uint64(stake),
			Owner:         isOwner,
			Registered:    true,
			CapturedSlot:  capturedSlot,
			BoundarySlot:  boundarySlot,
		})

		agg.delegated += stake
		agg.delegators++
		if isOwner {
			agg.ownerStake += stake
		}
	}

	poolInputs := make([]*models.RewardPoolInput, 0, len(aggs))
	var totalStake, totalDelegators uint64
	for poolHex, params := range snap.PoolParams {
		if params == nil {
			continue
		}
		agg := aggs[poolHex]
		den := params.MarginDen
		if den == 0 {
			den = 1
		}
		poolInputs = append(poolInputs, &models.RewardPoolInput{
			Epoch:       epoch,
			PoolKeyHash: append([]byte(nil), params.PoolKeyHash...),
			Margin: &types.Rat{
				Rat: new(big.Rat).SetFrac64(
					int64(params.MarginNum), // #nosec G115 -- CBOR-bounded
					int64(den),              // #nosec G115 -- CBOR-bounded
				),
			},
			Pledge:                     types.Uint64(params.Pledge),
			Cost:                       types.Uint64(params.Cost),
			DelegatedStake:             types.Uint64(agg.delegated),
			OwnerStake:                 types.Uint64(agg.ownerStake),
			DelegatorCount:             agg.delegators,
			RewardAccount:              append([]byte(nil), params.RewardAccount...),
			RewardAccountCredentialTag: params.RewardAccountCredentialTag,
			CapturedSlot:               capturedSlot,
			BoundarySlot:               boundarySlot,
		})
		totalStake += agg.delegated
		totalDelegators += agg.delegators
	}

	return &rewardInputBundle{
		epoch: epoch,
		snapshot: &models.RewardSnapshot{
			Epoch:            epoch,
			SnapshotType:     "mark",
			TotalActiveStake: types.Uint64(totalStake),
			TotalPoolCount:   uint64(len(poolInputs)),
			TotalDelegators:  totalDelegators,
			CapturedSlot:     capturedSlot,
			BoundarySlot:     boundarySlot,
			// Provisional, not authoritative: this basis was reconstructed
			// from an imported snapshot rather than captured at this node's
			// own SNAP point, so a later authoritative capture must be free
			// to supersede it.
			Authoritative: false,
		},
		poolInputs:  poolInputs,
		stakeInputs: stakeInputs,
	}
}

// validate rejects a derived basis the ledger would refuse to use.
//
// This gate is mandatory rather than defensive. The ledger validates these
// same invariants when it reads the basis, and on the path these rows are read
// from, a failure returns an error rather than skipping the round -- which
// fails the whole epoch rollover. Persisting a basis that cannot reconcile
// would therefore convert a missing reward round into a node that cannot
// cross an epoch boundary at all.
//
// The aggregate identities below hold by construction, since the pool totals
// are summed from the very stake inputs they are checked against; they are
// asserted anyway because "by construction" is exactly the kind of claim that
// stops being true when someone edits the derivation. What genuinely can fail
// comes from the snapshot itself: a margin outside [0,1], or a key hash of an
// unexpected length.
//
// A single bad pool fails the whole bundle rather than being dropped. Dropping
// it would orphan its stake inputs, and an orphaned input is itself a
// validation failure -- worse, one discovered later, at the boundary.
func (b *rewardInputBundle) validate() error {
	if b == nil || b.snapshot == nil {
		return errors.New("missing derived reward snapshot")
	}
	if uint64(len(b.poolInputs)) != b.snapshot.TotalPoolCount {
		return fmt.Errorf(
			"derived pool input count %d does not match snapshot pool count %d",
			len(b.poolInputs), b.snapshot.TotalPoolCount,
		)
	}

	poolStake := make(map[string]uint64, len(b.poolInputs))
	var totalStake, totalDelegators uint64
	for _, pool := range b.poolInputs {
		if pool == nil {
			return errors.New("nil derived pool input")
		}
		if len(pool.PoolKeyHash) != credentialHashSize {
			return fmt.Errorf(
				"derived pool input has %d-byte pool key hash",
				len(pool.PoolKeyHash),
			)
		}
		if len(pool.RewardAccount) == 0 {
			return fmt.Errorf(
				"derived pool input for %x has no reward account",
				pool.PoolKeyHash,
			)
		}
		if pool.Margin == nil || pool.Margin.Rat == nil {
			return fmt.Errorf(
				"derived pool input for %x has no margin", pool.PoolKeyHash,
			)
		}
		if pool.Margin.Sign() < 0 ||
			pool.Margin.Cmp(big.NewRat(1, 1)) > 0 {
			return fmt.Errorf(
				"derived pool input for %x has margin outside [0,1]",
				pool.PoolKeyHash,
			)
		}
		if pool.OwnerStake > pool.DelegatedStake {
			return fmt.Errorf(
				"derived pool input for %x has owner stake above delegated",
				pool.PoolKeyHash,
			)
		}
		key := string(pool.PoolKeyHash)
		if _, dup := poolStake[key]; dup {
			return fmt.Errorf(
				"duplicate derived pool input for %x", pool.PoolKeyHash,
			)
		}
		poolStake[key] = uint64(pool.DelegatedStake)
		totalStake += uint64(pool.DelegatedStake)
		totalDelegators += pool.DelegatorCount
	}
	if totalStake != uint64(b.snapshot.TotalActiveStake) {
		return fmt.Errorf(
			"derived pool stake %d does not match snapshot active stake %d",
			totalStake, uint64(b.snapshot.TotalActiveStake),
		)
	}
	if totalDelegators != b.snapshot.TotalDelegators {
		return fmt.Errorf(
			"derived delegator count %d does not match snapshot count %d",
			totalDelegators, b.snapshot.TotalDelegators,
		)
	}

	stakeByPool := make(map[string]uint64, len(poolStake))
	for _, input := range b.stakeInputs {
		if input == nil {
			return errors.New("nil derived stake input")
		}
		if len(input.StakingKey) != credentialHashSize {
			return fmt.Errorf(
				"derived stake input has %d-byte credential",
				len(input.StakingKey),
			)
		}
		if input.Stake == 0 {
			return fmt.Errorf(
				"derived stake input for %x has zero stake",
				input.StakingKey,
			)
		}
		key := string(input.PoolKeyHash)
		if _, known := poolStake[key]; !known {
			return fmt.Errorf(
				"derived stake input references unknown pool %x",
				input.PoolKeyHash,
			)
		}
		stakeByPool[key] += uint64(input.Stake)
	}
	for key, want := range poolStake {
		if stakeByPool[key] != want {
			return fmt.Errorf(
				"derived stake inputs total %d for pool %x, pool input says %d",
				stakeByPool[key], []byte(key), want,
			)
		}
	}
	return nil
}

// rewardInputStore is the slice of the metadata store this seeding needs.
type rewardInputStore interface {
	SaveRewardSnapshot(*models.RewardSnapshot, types.Txn) error
	SaveRewardPoolInputs([]*models.RewardPoolInput, types.Txn) error
	SaveRewardStakeInputs([]*models.RewardStakeInput, types.Txn) error
}

// seedImportedRewardInputs writes the reward basis for the epochs an imported
// snapshot covers.
//
// mark, set and go hold the stake distribution for the snapshot's own epoch
// and the two before it. Those are exactly the epochs whose reward rounds a
// freshly bootstrapped node cannot otherwise compute, which is why a node that
// skips them ends up roughly three epochs of rewards short -- the shortfall
// measured on preview in issue #3165.
//
// Each epoch is gated independently: a basis that does not reconcile is
// dropped with a warning rather than written, leaving that round to be skipped
// and counted the way it is today. That direction is deliberate. A missing
// round leaves reward balances short, which the metric and warning make
// visible; an unusable one would be read back by a path that returns an error
// rather than skipping, failing the epoch rollover outright.
func seedImportedRewardInputs(
	store rewardInputStore,
	txn types.Txn,
	snapshots *ParsedSnapShots,
	epoch uint64,
	capturedSlot uint64,
	logger rewardSeedLogger,
) error {
	if snapshots == nil || store == nil {
		return nil
	}
	type candidate struct {
		snap  *ParsedSnapShot
		epoch uint64
		name  string
	}
	candidates := []candidate{{&snapshots.Mark, epoch, "mark"}}
	if epoch >= 1 {
		candidates = append(
			candidates, candidate{&snapshots.Set, epoch - 1, "set"},
		)
	}
	if epoch >= 2 {
		candidates = append(
			candidates, candidate{&snapshots.Go, epoch - 2, "go"},
		)
	}

	for _, c := range candidates {
		bundle := deriveRewardInputs(c.snap, c.epoch, capturedSlot, 0)
		if bundle == nil || len(bundle.poolInputs) == 0 {
			continue
		}
		if err := bundle.validate(); err != nil {
			if logger != nil {
				logger.Warn(
					"not seeding reward inputs for an imported epoch: the derived basis does not reconcile, so that epoch's reward round will be skipped and its rewards never credited",
					"component", "ledgerstate",
					"epoch", c.epoch,
					"snapshot", c.name,
					"error", err.Error(),
				)
			}
			continue
		}
		if err := store.SaveRewardSnapshot(bundle.snapshot, txn); err != nil {
			return fmt.Errorf(
				"seeding reward snapshot for epoch %d: %w", c.epoch, err,
			)
		}
		if err := store.SaveRewardPoolInputs(
			bundle.poolInputs, txn,
		); err != nil {
			return fmt.Errorf(
				"seeding reward pool inputs for epoch %d: %w", c.epoch, err,
			)
		}
		if err := store.SaveRewardStakeInputs(
			bundle.stakeInputs, txn,
		); err != nil {
			return fmt.Errorf(
				"seeding reward stake inputs for epoch %d: %w", c.epoch, err,
			)
		}
		if logger != nil {
			logger.Info(
				"seeded reward inputs for an imported epoch",
				"component", "ledgerstate",
				"epoch", c.epoch,
				"snapshot", c.name,
				"pools", len(bundle.poolInputs),
				"delegators", len(bundle.stakeInputs),
			)
		}
	}
	return nil
}

// rewardSeedLogger is the logging surface this seeding uses.
type rewardSeedLogger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
}
