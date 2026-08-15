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
	"maps"
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
	// unattributedStake is stake delegated to a pool the parameter map does
	// not describe, and unattributedPools how many distinct such pools were
	// seen. Recorded rather than discarded: dropping the stake leaves a
	// basis that still reconciles against itself -- the totals are summed
	// from what remained -- while understating every surviving pool's share
	// of the reward pot. A pool that retired or re-registered between the
	// snapshot's epoch and the import, or a registration set only partially
	// populated when the seeding runs, produces exactly that.
	unattributedStake uint64
	unattributedPools int
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
	params map[string]*ParsedPool,
	epoch uint64,
	capturedSlot uint64,
	boundarySlot uint64,
) *rewardInputBundle {
	if snap == nil {
		return nil
	}
	// Pool parameters come from the caller rather than from the snapshot.
	// Current (UTxO-HD) snapshots carry pool entries inside SnapShots in the
	// compact PoolDistr shape -- pool key and VRF key only, with no margin,
	// cost, pledge, reward account or owners (see parsePoolDistrEntry). A
	// basis built from those is missing everything a reward round needs, so
	// the parameters are taken from the registrations the import decodes out
	// of cert state, and only the stake and delegations -- which SnapShots
	// does hold exactly, per epoch -- come from the snapshot.
	if len(params) == 0 {
		params = snap.PoolParams
	}

	type poolAgg struct {
		delegated  uint64
		ownerStake uint64
		delegators uint64
	}
	aggs := make(map[string]*poolAgg, len(params))
	owners := make(map[string]map[string]struct{}, len(params))
	for poolHex, pool := range params {
		if pool == nil {
			continue
		}
		aggs[poolHex] = &poolAgg{}
		set := make(map[string]struct{}, len(pool.Owners))
		for _, owner := range pool.Owners {
			set[hex.EncodeToString(owner)] = struct{}{}
		}
		owners[poolHex] = set
	}

	unattributed := make(map[string]struct{})
	var unattributedStake uint64
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
			// Delegated to a pool the parameter map does not describe. It
			// cannot be paid, and it cannot be silently dropped either: see
			// unattributedStake.
			if _, seen := unattributed[poolHex]; !seen {
				unattributed[poolHex] = struct{}{}
			}
			unattributedStake += stake
			continue
		}
		cred, err := hex.DecodeString(credHex)
		if err != nil || len(cred) == 0 {
			continue
		}
		// The credential type travels alongside the stake map, because that
		// map is keyed by hash alone and a script credential can share a hash
		// with a key one. Both parsed shapes carry it, the compact UTxO-HD
		// one included, so the key-hash default below is for an entry that is
		// missing rather than for a shape that omits them.
		credentialTag := uint8(0)
		if snap.StakeTags != nil {
			if tag, ok := snap.StakeTags[credHex]; ok {
				credentialTag = tag
			}
		}
		isOwner := false
		if set, ok := owners[poolHex]; ok {
			_, isOwner = set[credHex]
		}

		stakeInputs = append(stakeInputs, &models.RewardStakeInput{
			Epoch:         epoch,
			PoolKeyHash:   append([]byte(nil), poolKey...),
			StakingKey:    cred,
			CredentialTag: credentialTag,
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
	for poolHex, pool := range params {
		if pool == nil {
			continue
		}
		agg := aggs[poolHex]
		den := pool.MarginDen
		if den == 0 {
			den = 1
		}
		poolInputs = append(poolInputs, &models.RewardPoolInput{
			Epoch:       epoch,
			PoolKeyHash: append([]byte(nil), pool.PoolKeyHash...),
			Margin: &types.Rat{
				Rat: new(big.Rat).SetFrac64(
					int64(pool.MarginNum), // #nosec G115 -- bounded
					int64(den),            // #nosec G115 -- bounded
				),
			},
			Pledge:                     types.Uint64(pool.Pledge),
			Cost:                       types.Uint64(pool.Cost),
			DelegatedStake:             types.Uint64(agg.delegated),
			OwnerStake:                 types.Uint64(agg.ownerStake),
			DelegatorCount:             agg.delegators,
			RewardAccount:              append([]byte(nil), pool.RewardAccount...),
			RewardAccountCredentialTag: pool.RewardAccountCredentialTag,
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
		poolInputs:        poolInputs,
		stakeInputs:       stakeInputs,
		unattributedStake: unattributedStake,
		unattributedPools: len(unattributed),
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
	if b.unattributedPools > 0 {
		return fmt.Errorf(
			"%d pools in this epoch's delegations have no parameters "+
				"(%d lovelace of stake unattributed); seeding the remainder "+
				"would understate every other pool's share",
			b.unattributedPools, b.unattributedStake,
		)
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
	resolveParams rewardPoolParamsResolver,
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
		var params map[string]*ParsedPool
		if resolveParams != nil {
			resolved, err := resolveParams(c.epoch)
			switch {
			case err == nil:
				params = resolved
			case errors.Is(err, errRewardParamsWindowUnknown):
				// Registrations are the fallback for pools the snapshot
				// cannot describe, so losing them is not on its own a reason
				// to skip the epoch: a snapshot that describes every pool it
				// delegates to seeds the round without them. Whether that
				// holds is decided below, by the same gate that decides it
				// for every other epoch. Vetoing here instead would drop a
				// round the snapshot could have seeded, which is the failure
				// this seeding exists to prevent.
				//
				// Still logged, because it means the fallback is gone: an
				// epoch that does need it will be dropped by that gate, and
				// this is the line that explains why.
				if logger != nil {
					logger.Warn(
						"no pool parameter window for an imported epoch, so the registration fallback is unavailable; the epoch is still seeded if its snapshot describes every pool it delegates to",
						"component", "ledgerstate",
						"epoch", c.epoch,
						"snapshot", c.name,
						"error", err.Error(),
					)
				}
			default:
				return fmt.Errorf(
					"resolving pool parameters for epoch %d: %w",
					c.epoch, err,
				)
			}
		}
		bundle := deriveRewardInputs(
			c.snap,
			effectiveRewardPoolParams(c.snap, params),
			c.epoch,
			capturedSlot,
			0,
		)
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

// errRewardParamsWindowUnknown marks an epoch whose parameter lookup window
// could not be placed. It is distinct from a lookup that failed: the database
// answered fine, there is simply no defensible window to ask about, so the
// epoch is skipped rather than seeded from a guess and rather than failing
// the whole import.
var errRewardParamsWindowUnknown = errors.New(
	"pool parameter window cannot be determined",
)

// rewardPoolParamsResolver returns the pool parameters in force during the
// given epoch. It is a function rather than a prepared map because the
// seeding covers three epochs and a pool's parameters are not constant
// across them: margin, cost and pledge changes take effect at a boundary, so
// the epoch being seeded decides which registration applies. A nil resolver
// means no parameters are available, which the derivation handles by falling
// back to whatever the snapshot itself carries.
type rewardPoolParamsResolver func(
	epoch uint64,
) (map[string]*ParsedPool, error)

// rewardSeedLogger is the logging surface this seeding uses.
type rewardSeedLogger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
}

// rewardPoolParamsFromRegistrations turns the pool registrations the import
// decoded out of cert state into the parameter source the derivation needs.
//
// This is where the parameters have to come from. Current snapshots carry
// pool entries inside SnapShots in the compact PoolDistr shape, which holds
// the pool and VRF keys and nothing else -- no margin, cost, pledge, reward
// account or owners. A basis built from those cannot reconcile, and would be
// dropped by the gate, leaving the seeding silently ineffective.
//
// It takes registrations, not the denormalized pool rows, because the pool
// row holds only a pool's current parameters while the registration history
// holds what each epoch actually saw. Which registration applies to which
// epoch is the caller's decision -- see rewardPoolParamsResolver -- and this
// only converts whichever set it is handed.
func rewardPoolParamsFromRegistrations(
	pools []models.PoolRegistration,
) map[string]*ParsedPool {
	params := make(map[string]*ParsedPool, len(pools))
	for i := range pools {
		pool := &pools[i]
		if len(pool.PoolKeyHash) == 0 {
			continue
		}
		num, den := uint64(0), uint64(1)
		if pool.Margin != nil && pool.Margin.Rat != nil {
			if n := pool.Margin.Num(); n != nil && n.IsUint64() {
				num = n.Uint64()
			}
			if d := pool.Margin.Denom(); d != nil && d.IsUint64() &&
				d.Uint64() != 0 {
				den = d.Uint64()
			}
		}
		owners := make([][]byte, 0, len(pool.Owners))
		for _, owner := range pool.Owners {
			if len(owner.KeyHash) == 0 {
				continue
			}
			owners = append(owners, append([]byte(nil), owner.KeyHash...))
		}
		params[hex.EncodeToString(pool.PoolKeyHash)] = &ParsedPool{
			PoolKeyHash:                append([]byte(nil), pool.PoolKeyHash...),
			VrfKeyHash:                 append([]byte(nil), pool.VrfKeyHash...),
			Pledge:                     uint64(pool.Pledge),
			Cost:                       uint64(pool.Cost),
			MarginNum:                  num,
			MarginDen:                  den,
			RewardAccount:              append([]byte(nil), pool.RewardAccount...),
			RewardAccountCredentialTag: pool.RewardAccountCredentialTag,
			Owners:                     owners,
		}
	}
	return params
}

// effectiveRewardPoolParams picks, per pool, the parameters the reward round
// for this epoch should be computed from.
//
// The snapshot's own parameters win wherever it has them. They are the ones
// that were in force during the epoch it captured, which is what the round
// needs, and -- the reason issue #3165 stayed open -- the snapshot describes
// every pool that held stake then, including pools that have since retired.
// A retired pool is gone from cert state and from the current pool
// distribution, so nothing else in an imported database can describe it; its
// delegators' stake could not be attributed, and the gate dropped that whole
// epoch's basis rather than seed a partial one.
//
// Registration parameters remain the fallback, for a snapshot whose pool
// entries are the compact shape carrying only a VRF key. Usability is decided
// on the reward account: it is the field the gate rejects a basis for, so a
// pool without one cannot be seeded from the snapshot no matter what else it
// carries.
func effectiveRewardPoolParams(
	snap *ParsedSnapShot,
	registered map[string]*ParsedPool,
) map[string]*ParsedPool {
	effective := make(
		map[string]*ParsedPool, len(registered)+len(snap.PoolParams),
	)
	maps.Copy(effective, registered)
	for key, pool := range snap.PoolParams {
		if pool == nil || len(pool.RewardAccount) == 0 {
			continue
		}
		effective[key] = pool
	}
	return effective
}
