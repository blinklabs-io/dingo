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

// Package rewards implements the Shelley stake-pool reward calculation.
//
// The formulas mirror cardano-ledger's Cardano.Ledger.Shelley.Rewards module,
// Amaru's summary rewards implementation, and the Cardano Foundation calculator
// rewards calculator. Keep all arithmetic rational until the exact floor points
// used by the ledger specification.
package rewards

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
)

const CredentialHashSize = 28

var (
	ErrInvalidCredentialHash = errors.New("invalid credential hash")
	ErrInvalidPoolHash       = errors.New("invalid pool hash")
	ErrInvalidParameters     = errors.New("invalid reward parameters")
	ErrRewardAmountOverflow  = errors.New("reward amount overflow")
)

// Credential identifies a stake credential. Tag 0 is a key hash and tag 1 is a
// script hash, matching Dingo's metadata schema.
type Credential struct {
	Hash [CredentialHashSize]byte
	Tag  uint8
}

func NewCredential(tag uint8, hash []byte) (Credential, error) {
	if len(hash) != CredentialHashSize {
		return Credential{}, fmt.Errorf(
			"%w: got %d bytes, want %d",
			ErrInvalidCredentialHash,
			len(hash),
			CredentialHashSize,
		)
	}
	if tag > 1 {
		return Credential{}, fmt.Errorf("invalid credential tag %d", tag)
	}
	var ret Credential
	ret.Tag = tag
	copy(ret.Hash[:], hash)
	return ret, nil
}

func (c Credential) Key() string {
	return string([]byte{c.Tag}) + string(c.Hash[:])
}

// PoolID identifies a stake pool by key hash.
type PoolID [CredentialHashSize]byte

func NewPoolID(hash []byte) (PoolID, error) {
	if len(hash) != CredentialHashSize {
		return PoolID{}, fmt.Errorf(
			"%w: got %d bytes, want %d",
			ErrInvalidPoolHash,
			len(hash),
			CredentialHashSize,
		)
	}
	var ret PoolID
	copy(ret[:], hash)
	return ret, nil
}

func (p PoolID) String() string {
	return hex.EncodeToString(p[:])
}

// Parameters contains the protocol and network parameters used for one reward
// calculation.
type Parameters struct {
	// MonetaryExpansion is rho.
	MonetaryExpansion *big.Rat
	// TreasuryExpansion is tau.
	TreasuryExpansion *big.Rat
	// Decentralization is d.
	Decentralization *big.Rat
	// PledgeInfluence is a0.
	PledgeInfluence *big.Rat
	// ActiveSlotsCoeff is f.
	ActiveSlotsCoeff *big.Rat
	// OptimalPoolCount is nOpt/k.
	OptimalPoolCount uint64
	// EpochLength is the network epoch length in slots.
	EpochLength uint64
	// MaxLovelaceSupply is maxLL. It is used with reserves to compute total
	// circulating stake, matching cardano-ledger and Amaru.
	MaxLovelaceSupply uint64
	// ProtocolMajorVersion gates ledger reward behavior that changed across
	// hard forks. In particular, Babbage/Vasil (major > 6) forgoes the early
	// reward prefilter; final unregistered rewards are still routed away from
	// spendable accounts at application time.
	ProtocolMajorVersion uint64
}

// Pots captures the pot values available at the start of reward calculation.
type Pots struct {
	Reserves uint64
	Treasury uint64
	Fees     uint64
}

type Snapshot struct {
	Pools            []Pool
	TotalActiveStake uint64
}

type Pool struct {
	ID                      PoolID
	RewardAccount           Credential
	Margin                  *big.Rat
	Pledge                  uint64
	Cost                    uint64
	DelegatedStake          uint64
	OwnerStake              uint64
	BlocksProduced          uint64
	TotalBlocks             uint64
	RewardAccountRegistered bool
	RewardAccountEligible   bool
	Delegators              []Delegator
	Owners                  map[Credential]struct{}
}

type Delegator struct {
	Credential Credential
	Stake      uint64
	Registered bool
	Eligible   bool
}

type Result struct {
	UpdatedPots      Pots
	PoolRewards      []PoolReward
	AccountRewards   []AccountReward
	Efficiency       *big.Rat
	Incentives       uint64
	TotalRewardPot   uint64
	TreasuryTax      uint64
	AvailableRewards uint64
	EffectiveRewards uint64
	Undistributed    uint64
	Unspendable      uint64
	TotalCirculation uint64
	TotalBlocks      uint64
	ExpectedBlocks   *big.Rat
	pendingRewards   []pendingReward
	poolAccounted    []uint64
}

type PoolReward struct {
	PoolID              PoolID
	ApparentPerformance *big.Rat
	OptimalReward       uint64
	PoolReward          uint64
	LeaderReward        uint64
	MemberRewardTotal   uint64
	OwnerStake          uint64
	Undistributed       uint64
	Unspendable         uint64
}

type AccountReward struct {
	Credential Credential
	PoolID     PoolID
	Amount     uint64
	Type       RewardType
	Spendable  bool
}

type RewardType string

const (
	RewardTypeLeader RewardType = "leader"
	RewardTypeMember RewardType = "member"
)

// Calculate computes rewards for a completed epoch. The returned UpdatedPots
// reflects the reward calculation alone: reserves lose incentives and regain
// rewards not paid or sent to treasury, treasury receives the tax and
// unspendable rewards, and fees are cleared.
func Calculate(pots Pots, snapshot Snapshot, params Parameters) (*Result, error) {
	if err := validateParameters(params); err != nil {
		return nil, err
	}
	if err := validateSnapshot(snapshot); err != nil {
		return nil, err
	}
	if pots.Reserves > params.MaxLovelaceSupply {
		return nil, fmt.Errorf(
			"%w: reserves %d exceed max supply %d",
			ErrInvalidParameters,
			pots.Reserves,
			params.MaxLovelaceSupply,
		)
	}

	totalBlocks, err := totalBlocks(snapshot.Pools)
	if err != nil {
		return nil, err
	}
	expectedBlocks := expectedBlocks(params)
	if expectedBlocks.Sign() == 0 &&
		params.Decentralization.Cmp(new(big.Rat).SetFrac64(4, 5)) < 0 {
		return nil, fmt.Errorf(
			"%w: expected blocks is zero",
			ErrInvalidParameters,
		)
	}
	efficiency := rewardEfficiency(totalBlocks, expectedBlocks, params.Decentralization)
	incentives, err := floorMulChecked(
		minRat(oneRat(), efficiency),
		params.MonetaryExpansion,
		uintRat(pots.Reserves),
	)
	if err != nil {
		return nil, fmt.Errorf("calculate incentives: %w", err)
	}
	totalRewardPot, overflow := addUint64(incentives, pots.Fees)
	if overflow {
		return nil, fmt.Errorf("%w: reward pot overflow", ErrInvalidParameters)
	}
	treasuryTax, err := floorMulChecked(params.TreasuryExpansion, uintRat(totalRewardPot))
	if err != nil {
		return nil, fmt.Errorf("calculate treasury tax: %w", err)
	}
	availableRewards := totalRewardPot - treasuryTax
	treasuryAfterTax, overflow := addUint64(pots.Treasury, treasuryTax)
	if overflow {
		return nil, fmt.Errorf("%w: treasury tax overflow", ErrInvalidParameters)
	}

	totalCirculation := params.MaxLovelaceSupply - pots.Reserves

	result := &Result{
		UpdatedPots: Pots{
			Reserves: pots.Reserves - incentives,
			Treasury: treasuryAfterTax,
			Fees:     0,
		},
		Incentives:       incentives,
		TotalRewardPot:   totalRewardPot,
		TreasuryTax:      treasuryTax,
		AvailableRewards: availableRewards,
		Efficiency:       efficiency,
		TotalCirculation: totalCirculation,
		TotalBlocks:      totalBlocks,
		ExpectedBlocks:   expectedBlocks,
	}

	if availableRewards == 0 ||
		snapshot.TotalActiveStake == 0 ||
		totalCirculation == 0 {
		result.Undistributed = availableRewards
		if err := result.addUndistributedToReserves(); err != nil {
			return nil, err
		}
		return result, nil
	}

	pools := append([]Pool(nil), snapshot.Pools...)
	sort.Slice(pools, func(i, j int) bool {
		return pools[i].ID.String() < pools[j].ID.String()
	})

	for _, pool := range pools {
		poolReward, err := calculatePoolRewards(
			pool,
			availableRewards,
			snapshot.TotalActiveStake,
			totalCirculation,
			totalBlocks,
			params,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"calculate reward for pool %s: %w",
				pool.ID.String(),
				err,
			)
		}
		result.PoolRewards = append(result.PoolRewards, poolReward)
		result.poolAccounted = append(result.poolAccounted, 0)

		if poolReward.LeaderReward > 0 &&
			params.rewardPassesPrefilter(pool.RewardAccountRegistered) {
			reward := AccountReward{
				Credential: pool.RewardAccount,
				PoolID:     pool.ID,
				Amount:     poolReward.LeaderReward,
				Type:       RewardTypeLeader,
				Spendable:  pool.RewardAccountEligible,
			}
			if err := result.addReward(params, reward); err != nil {
				return nil, err
			}
		}

		delegators := append([]Delegator(nil), pool.Delegators...)
		sort.Slice(delegators, func(i, j int) bool {
			return delegators[i].Credential.Key() <
				delegators[j].Credential.Key()
		})
		for _, delegator := range delegators {
			if _, owner := pool.Owners[delegator.Credential]; owner {
				continue
			}
			amount, err := memberRewardChecked(
				poolReward.PoolReward,
				pool.Cost,
				normalizedMargin(pool.Margin),
				delegator.Stake,
				pool.DelegatedStake,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"calculate member reward for pool %s: %w",
					pool.ID.String(),
					err,
				)
			}
			if amount == 0 {
				continue
			}
			if params.rewardPassesPrefilter(delegator.Registered) {
				reward := AccountReward{
					Credential: delegator.Credential,
					PoolID:     pool.ID,
					Amount:     amount,
					Type:       RewardTypeMember,
					Spendable:  delegator.Eligible,
				}
				if err := result.addReward(params, reward); err != nil {
					return nil, err
				}
			}
		}
	}

	if err := result.finalizeRewards(params); err != nil {
		return nil, err
	}

	for i := range result.PoolRewards {
		poolReward := result.PoolRewards[i]
		accounted := result.poolAccounted[i]
		if accounted < poolReward.PoolReward {
			poolReward.Undistributed = poolReward.PoolReward - accounted
		}
		result.PoolRewards[i] = poolReward
	}

	accounted, overflow := addUint64(result.EffectiveRewards, result.Unspendable)
	if overflow || accounted > result.AvailableRewards {
		return nil, fmt.Errorf(
			"%w: rewards exceed available pot",
			ErrInvalidParameters,
		)
	}
	if accounted < result.AvailableRewards {
		result.Undistributed = result.AvailableRewards - accounted
	}
	if err := result.addUndistributedToReserves(); err != nil {
		return nil, err
	}
	if err := result.addUnspendableToTreasury(); err != nil {
		return nil, err
	}
	return result, nil
}

type pendingReward struct {
	reward    AccountReward
	poolIndex int
}

func (r *Result) addReward(params Parameters, reward AccountReward) error {
	poolIndex := len(r.PoolRewards) - 1
	if params.aggregateRewards() {
		return r.applyReward(reward, poolIndex)
	}
	r.pendingRewards = append(r.pendingRewards, pendingReward{
		reward:    reward,
		poolIndex: poolIndex,
	})
	return nil
}

func (r *Result) finalizeRewards(params Parameters) error {
	if params.aggregateRewards() {
		return nil
	}
	if len(r.pendingRewards) == 0 {
		return nil
	}
	selected := make(map[string]pendingReward, len(r.pendingRewards))
	for _, pending := range r.pendingRewards {
		key := pending.reward.Credential.Key()
		if current, ok := selected[key]; !ok ||
			rewardComesBefore(pending.reward, current.reward) {
			selected[key] = pending
		}
	}
	delivered := make([]pendingReward, 0, len(selected))
	for _, pending := range selected {
		delivered = append(delivered, pending)
	}
	sort.Slice(delivered, func(i, j int) bool {
		return pendingRewardComesBefore(delivered[i], delivered[j])
	})
	for _, pending := range delivered {
		if err := r.applyReward(pending.reward, pending.poolIndex); err != nil {
			return err
		}
	}
	r.pendingRewards = nil
	return nil
}

func (r *Result) applyReward(reward AccountReward, poolIndex int) error {
	r.AccountRewards = append(r.AccountRewards, reward)
	if poolIndex >= 0 && poolIndex < len(r.poolAccounted) {
		accounted, overflow := addUint64(
			r.poolAccounted[poolIndex],
			reward.Amount,
		)
		if overflow {
			return fmt.Errorf(
				"%w: pool accounted reward overflow",
				ErrInvalidParameters,
			)
		}
		r.poolAccounted[poolIndex] = accounted
		if reward.Type == RewardTypeMember {
			memberTotal, overflow := addUint64(
				r.PoolRewards[poolIndex].MemberRewardTotal,
				reward.Amount,
			)
			if overflow {
				return fmt.Errorf(
					"%w: pool member reward overflow",
					ErrInvalidParameters,
				)
			}
			r.PoolRewards[poolIndex].MemberRewardTotal = memberTotal
		}
	}
	if reward.Spendable {
		effective, overflow := addUint64(r.EffectiveRewards, reward.Amount)
		if overflow {
			return fmt.Errorf(
				"%w: effective reward overflow",
				ErrInvalidParameters,
			)
		}
		r.EffectiveRewards = effective
		return nil
	}
	unspendable, overflow := addUint64(r.Unspendable, reward.Amount)
	if overflow {
		return fmt.Errorf(
			"%w: unspendable reward overflow",
			ErrInvalidParameters,
		)
	}
	r.Unspendable = unspendable
	if poolIndex >= 0 && poolIndex < len(r.PoolRewards) {
		poolUnspendable, overflow := addUint64(
			r.PoolRewards[poolIndex].Unspendable,
			reward.Amount,
		)
		if overflow {
			return fmt.Errorf(
				"%w: pool unspendable reward overflow",
				ErrInvalidParameters,
			)
		}
		r.PoolRewards[poolIndex].Unspendable = poolUnspendable
	}
	return nil
}

func (r *Result) addUndistributedToReserves() error {
	reserves, overflow := addUint64(r.UpdatedPots.Reserves, r.Undistributed)
	if overflow {
		return fmt.Errorf("%w: reserve refund overflow", ErrInvalidParameters)
	}
	r.UpdatedPots.Reserves = reserves
	return nil
}

func (r *Result) addUnspendableToTreasury() error {
	treasury, overflow := addUint64(r.UpdatedPots.Treasury, r.Unspendable)
	if overflow {
		return fmt.Errorf("%w: unspendable treasury overflow", ErrInvalidParameters)
	}
	r.UpdatedPots.Treasury = treasury
	return nil
}

func pendingRewardComesBefore(a, b pendingReward) bool {
	if rewardComesBefore(a.reward, b.reward) {
		return true
	}
	if rewardComesBefore(b.reward, a.reward) {
		return false
	}
	return a.reward.Credential.Key() < b.reward.Credential.Key()
}

func rewardComesBefore(a, b AccountReward) bool {
	if a.Type != b.Type {
		return rewardTypeOrder(a.Type) < rewardTypeOrder(b.Type)
	}
	return string(a.PoolID[:]) < string(b.PoolID[:])
}

func rewardTypeOrder(t RewardType) int {
	switch t {
	case RewardTypeLeader:
		return 0
	case RewardTypeMember:
		return 1
	default:
		return 2
	}
}

func (params Parameters) forgoRewardPrefilter() bool {
	return params.ProtocolMajorVersion > 6
}

// RequiresRewardPrefilter reports whether this protocol version still uses the
// pre-Babbage reward calculation prefilter.
func (params Parameters) RequiresRewardPrefilter() bool {
	return !params.forgoRewardPrefilter()
}

func (params Parameters) rewardPassesPrefilter(registered bool) bool {
	return params.forgoRewardPrefilter() || registered
}

func (params Parameters) aggregateRewards() bool {
	return params.ProtocolMajorVersion >= 3
}

// Validate checks that the reward parameters are structurally usable before
// they are applied to a reward update.
func (params Parameters) Validate() error {
	return validateParameters(params)
}

func validateParameters(params Parameters) error {
	for _, field := range []struct {
		name     string
		rat      *big.Rat
		unit     bool
		positive bool
	}{
		{name: "monetary expansion", rat: params.MonetaryExpansion, unit: true},
		{name: "treasury expansion", rat: params.TreasuryExpansion, unit: true},
		{name: "decentralization", rat: params.Decentralization, unit: true},
		{name: "pledge influence", rat: params.PledgeInfluence},
		{
			name:     "active slot coeff",
			rat:      params.ActiveSlotsCoeff,
			unit:     true,
			positive: true,
		},
	} {
		if field.rat == nil {
			return fmt.Errorf("%w: missing %s", ErrInvalidParameters, field.name)
		}
		if field.rat.Sign() < 0 {
			return fmt.Errorf("%w: negative %s", ErrInvalidParameters, field.name)
		}
		if field.positive && field.rat.Sign() == 0 {
			return fmt.Errorf(
				"%w: active slot coeff is zero",
				ErrInvalidParameters,
			)
		}
		if field.unit && field.rat.Cmp(oneRat()) > 0 {
			return fmt.Errorf(
				"%w: %s greater than one",
				ErrInvalidParameters,
				field.name,
			)
		}
	}
	if params.OptimalPoolCount == 0 {
		return fmt.Errorf("%w: optimal pool count is zero", ErrInvalidParameters)
	}
	if params.EpochLength == 0 {
		return fmt.Errorf("%w: epoch length is zero", ErrInvalidParameters)
	}
	return nil
}

func validateSnapshot(snapshot Snapshot) error {
	seenPools := make(map[PoolID]struct{}, len(snapshot.Pools))
	seenDelegators := make(map[string]PoolID)
	credentialEligibility := make(map[string]bool)
	var totalDelegated uint64
	for _, pool := range snapshot.Pools {
		if _, ok := seenPools[pool.ID]; ok {
			return fmt.Errorf(
				"%w: duplicate pool %s in reward snapshot",
				ErrInvalidParameters,
				pool.ID.String(),
			)
		}
		seenPools[pool.ID] = struct{}{}
		if err := validateCredential(
			pool.RewardAccount,
			fmt.Sprintf("pool %s reward account", pool.ID.String()),
		); err != nil {
			return err
		}
		if err := validateCredentialEligibility(
			credentialEligibility,
			pool.RewardAccount,
			pool.RewardAccountEligible,
		); err != nil {
			return err
		}
		if pool.Margin != nil {
			if pool.Margin.Sign() < 0 || pool.Margin.Cmp(oneRat()) > 0 {
				return fmt.Errorf(
					"%w: pool %s margin outside [0,1]",
					ErrInvalidParameters,
					pool.ID.String(),
				)
			}
		}
		if pool.OwnerStake > pool.DelegatedStake {
			return fmt.Errorf(
				"%w: pool %s owner stake %d exceeds delegated stake %d",
				ErrInvalidParameters,
				pool.ID.String(),
				pool.OwnerStake,
				pool.DelegatedStake,
			)
		}
		if err := validatePoolDelegators(pool); err != nil {
			return err
		}
		var computedOwnerStake uint64
		for owner := range pool.Owners {
			if owner.Tag != 0 {
				return fmt.Errorf(
					"%w: pool %s owner %x has non-key credential tag %d",
					ErrInvalidParameters,
					pool.ID.String(),
					owner.Hash,
					owner.Tag,
				)
			}
			ownerStake, found := poolDelegatorStake(pool.Delegators, owner)
			if !found {
				return fmt.Errorf(
					"%w: pool %s owner %x is not a delegator",
					ErrInvalidParameters,
					pool.ID.String(),
					owner.Hash,
				)
			}
			var overflow bool
			computedOwnerStake, overflow = addUint64(
				computedOwnerStake,
				ownerStake,
			)
			if overflow {
				return fmt.Errorf(
					"%w: pool %s owner stake overflow",
					ErrInvalidParameters,
					pool.ID.String(),
				)
			}
		}
		if computedOwnerStake != pool.OwnerStake {
			return fmt.Errorf(
				"%w: pool %s computed owner stake %d does not match owner stake %d",
				ErrInvalidParameters,
				pool.ID.String(),
				computedOwnerStake,
				pool.OwnerStake,
			)
		}
		for _, delegator := range pool.Delegators {
			if err := validateCredentialEligibility(
				credentialEligibility,
				delegator.Credential,
				delegator.Eligible,
			); err != nil {
				return err
			}
			key := delegator.Credential.Key()
			if existingPool, ok := seenDelegators[key]; ok {
				return fmt.Errorf(
					"%w: duplicate delegator %x in pools %s and %s",
					ErrInvalidParameters,
					delegator.Credential.Hash,
					existingPool.String(),
					pool.ID.String(),
				)
			}
			seenDelegators[key] = pool.ID
		}
		var overflow bool
		totalDelegated, overflow = addUint64(
			totalDelegated,
			pool.DelegatedStake,
		)
		if overflow {
			return fmt.Errorf(
				"%w: total delegated stake overflow",
				ErrInvalidParameters,
			)
		}
	}
	if totalDelegated != snapshot.TotalActiveStake {
		return fmt.Errorf(
			"%w: total delegated stake %d does not match active stake %d",
			ErrInvalidParameters,
			totalDelegated,
			snapshot.TotalActiveStake,
		)
	}
	return nil
}

func validateCredentialEligibility(
	eligibility map[string]bool,
	credential Credential,
	eligible bool,
) error {
	key := credential.Key()
	if existing, ok := eligibility[key]; ok && existing != eligible {
		return fmt.Errorf(
			"%w: credential %x has conflicting reward eligibility",
			ErrInvalidParameters,
			credential.Hash,
		)
	}
	eligibility[key] = eligible
	return nil
}

func poolDelegatorStake(delegators []Delegator, credential Credential) (uint64, bool) {
	for _, delegator := range delegators {
		if delegator.Credential == credential {
			return delegator.Stake, true
		}
	}
	return 0, false
}

func validateCredential(credential Credential, label string) error {
	if credential.Tag > 1 {
		return fmt.Errorf(
			"%w: %s has invalid credential tag %d",
			ErrInvalidParameters,
			label,
			credential.Tag,
		)
	}
	return nil
}

func validatePoolDelegators(pool Pool) error {
	if len(pool.Delegators) == 0 {
		return nil
	}
	seenDelegators := make(map[string]struct{}, len(pool.Delegators))
	var totalDelegatorStake uint64
	for _, delegator := range pool.Delegators {
		if err := validateCredential(
			delegator.Credential,
			fmt.Sprintf("pool %s delegator", pool.ID.String()),
		); err != nil {
			return err
		}
		key := delegator.Credential.Key()
		if _, ok := seenDelegators[key]; ok {
			return fmt.Errorf(
				"%w: duplicate delegator %x in pool %s",
				ErrInvalidParameters,
				delegator.Credential.Hash,
				pool.ID.String(),
			)
		}
		seenDelegators[key] = struct{}{}
		var overflow bool
		totalDelegatorStake, overflow = addUint64(
			totalDelegatorStake,
			delegator.Stake,
		)
		if overflow {
			return fmt.Errorf(
				"%w: pool %s delegated stake overflow",
				ErrInvalidParameters,
				pool.ID.String(),
			)
		}
	}
	if totalDelegatorStake > pool.DelegatedStake {
		return fmt.Errorf(
			"%w: pool %s delegator stake %d exceeds delegated stake %d",
			ErrInvalidParameters,
			pool.ID.String(),
			totalDelegatorStake,
			pool.DelegatedStake,
		)
	}
	return nil
}

func calculatePoolRewards(
	pool Pool,
	availableRewards uint64,
	totalActiveStake uint64,
	totalCirculation uint64,
	totalBlocks uint64,
	params Parameters,
) (PoolReward, error) {
	ret := PoolReward{
		PoolID:     pool.ID,
		OwnerStake: pool.OwnerStake,
	}
	if pool.DelegatedStake == 0 ||
		pool.Pledge > pool.OwnerStake {
		return ret, nil
	}

	ret.ApparentPerformance = apparentPerformance(
		params.Decentralization,
		pool.DelegatedStake,
		totalActiveStake,
		pool.BlocksProduced,
		totalBlocks,
	)
	optimalReward, err := optimalPoolRewardChecked(
		availableRewards,
		params.OptimalPoolCount,
		params.PledgeInfluence,
		pool.DelegatedStake,
		pool.Pledge,
		totalCirculation,
	)
	if err != nil {
		return PoolReward{}, err
	}
	ret.OptimalReward = optimalReward
	poolReward, err := floorMulChecked(
		ret.ApparentPerformance,
		uintRat(ret.OptimalReward),
	)
	if err != nil {
		return PoolReward{}, err
	}
	ret.PoolReward = poolReward
	leader, err := leaderRewardChecked(
		ret.PoolReward,
		pool.Cost,
		normalizedMargin(pool.Margin),
		pool.OwnerStake,
		pool.DelegatedStake,
	)
	if err != nil {
		return PoolReward{}, err
	}
	ret.LeaderReward = leader
	return ret, nil
}

// CalculatePoolReward re-derives a single pool's reward fields (OptimalReward,
// PoolReward, LeaderReward, ApparentPerformance) from frozen snapshot inputs
// using the same arithmetic Calculate applies per pool. It lets callers
// validate persisted pool reward outputs against the inputs instead of trusting
// the stored values. Member distribution (MemberRewardTotal, Undistributed) is
// not derived here because it depends on per-delegator eligibility, not the
// pool-level reward that leader and member payouts are computed from.
func CalculatePoolReward(
	pool Pool,
	availableRewards uint64,
	totalActiveStake uint64,
	totalCirculation uint64,
	totalBlocks uint64,
	params Parameters,
) (PoolReward, error) {
	return calculatePoolRewards(
		pool,
		availableRewards,
		totalActiveStake,
		totalCirculation,
		totalBlocks,
		params,
	)
}

func expectedBlocks(params Parameters) *big.Rat {
	nonObftSlots := new(big.Rat).Sub(oneRat(), params.Decentralization)
	return new(big.Rat).Mul(
		new(big.Rat).Mul(nonObftSlots, params.ActiveSlotsCoeff),
		uintRat(params.EpochLength),
	)
}

func rewardEfficiency(
	totalBlocks uint64,
	expectedBlocks *big.Rat,
	decentralization *big.Rat,
) *big.Rat {
	if decentralization.Cmp(new(big.Rat).SetFrac64(4, 5)) >= 0 {
		return oneRat()
	}
	if expectedBlocks == nil || expectedBlocks.Sign() == 0 {
		return oneRat()
	}
	return new(big.Rat).Quo(
		uintRat(totalBlocks),
		expectedBlocks,
	)
}

func totalBlocks(pools []Pool) (uint64, error) {
	var ret uint64
	explicit := false
	for _, pool := range pools {
		if pool.TotalBlocks == 0 {
			continue
		}
		if explicit && pool.TotalBlocks != ret {
			return 0, fmt.Errorf(
				"%w: inconsistent total blocks %d and %d",
				ErrInvalidParameters,
				ret,
				pool.TotalBlocks,
			)
		}
		explicit = true
		if pool.TotalBlocks > ret {
			ret = pool.TotalBlocks
		}
	}
	if explicit {
		return ret, nil
	}
	for _, pool := range pools {
		var overflow bool
		ret, overflow = addUint64(ret, pool.BlocksProduced)
		if overflow {
			return 0, fmt.Errorf(
				"%w: total blocks overflow",
				ErrInvalidParameters,
			)
		}
	}
	return ret, nil
}

// apparentPerformance implements mkApparentPerformance from cardano-ledger.
func apparentPerformance(
	d *big.Rat,
	poolStake uint64,
	activeStake uint64,
	blocksProduced uint64,
	totalBlocks uint64,
) *big.Rat {
	if poolStake == 0 || activeStake == 0 {
		return new(big.Rat)
	}
	if d.Cmp(new(big.Rat).SetFrac64(4, 5)) >= 0 {
		return oneRat()
	}
	blocksDenom := totalBlocks
	if blocksDenom == 0 {
		blocksDenom = 1
	}
	beta := new(big.Rat).SetFrac(
		new(big.Int).SetUint64(blocksProduced),
		new(big.Int).SetUint64(blocksDenom),
	)
	sigma := new(big.Rat).SetFrac(
		new(big.Int).SetUint64(poolStake),
		new(big.Int).SetUint64(activeStake),
	)
	return new(big.Rat).Quo(beta, sigma)
}

// optimalPoolRewardChecked implements maxPool' from the Shelley ledger.
func optimalPoolRewardChecked(
	availableRewards uint64,
	optimalPoolCount uint64,
	a0 *big.Rat,
	poolStake uint64,
	pledge uint64,
	totalStake uint64,
) (uint64, error) {
	if totalStake == 0 || optimalPoolCount == 0 {
		return 0, nil
	}
	z0 := new(big.Rat).SetFrac(
		big.NewInt(1),
		new(big.Int).SetUint64(optimalPoolCount),
	)
	sigma := new(big.Rat).SetFrac(
		new(big.Int).SetUint64(poolStake),
		new(big.Int).SetUint64(totalStake),
	)
	pledgeRatio := new(big.Rat).SetFrac(
		new(big.Int).SetUint64(pledge),
		new(big.Int).SetUint64(totalStake),
	)
	s := minRat(sigma, z0)
	p := minRat(pledgeRatio, z0)

	left := new(big.Rat).Quo(
		uintRat(availableRewards),
		new(big.Rat).Add(oneRat(), a0),
	)
	z0MinusS := new(big.Rat).Sub(z0, s)
	pledgeDiscount := new(big.Rat).Quo(
		new(big.Rat).Mul(p, z0MinusS),
		z0,
	)
	inner := new(big.Rat).Sub(s, pledgeDiscount)
	z0Factor := new(big.Rat).Quo(inner, z0)
	right := new(big.Rat).Add(
		s,
		new(big.Rat).Mul(new(big.Rat).Mul(p, a0), z0Factor),
	)
	return floorRatChecked(new(big.Rat).Mul(left, right))
}

func leaderRewardChecked(
	poolReward uint64,
	cost uint64,
	margin *big.Rat,
	ownerStake uint64,
	poolStake uint64,
) (uint64, error) {
	if poolReward <= cost {
		return poolReward, nil
	}
	if poolStake == 0 {
		return poolReward, nil
	}
	ownerStakeRatio := new(big.Rat).SetFrac(
		new(big.Int).SetUint64(ownerStake),
		new(big.Int).SetUint64(poolStake),
	)
	oneMinusMargin := new(big.Rat).Sub(oneRat(), margin)
	factor := new(big.Rat).Add(
		margin,
		new(big.Rat).Mul(oneMinusMargin, ownerStakeRatio),
	)
	variableReward := poolReward - cost
	marginReward, err := floorMulChecked(factor, uintRat(variableReward))
	if err != nil {
		return 0, err
	}
	ret, overflow := addUint64(cost, marginReward)
	if overflow {
		return 0, fmt.Errorf("%w: leader reward overflow", ErrRewardAmountOverflow)
	}
	return ret, nil
}

// MemberReward computes the reward for a single non-owner pool member from the
// pool's total reward, cost, margin, the member's stake, and the pool's total
// delegated stake. It mirrors the member split applied in Calculate (the margin
// is normalized identically) and is exported so the precompute-reuse validator
// can re-derive and verify a persisted member reward amount without recomputing
// the whole epoch. Keeping this the same code path Calculate uses guarantees the
// validator cannot drift from the authoritative calculation.
func MemberReward(
	poolReward uint64,
	cost uint64,
	margin *big.Rat,
	memberStake uint64,
	poolStake uint64,
) (uint64, error) {
	return memberRewardChecked(
		poolReward,
		cost,
		normalizedMargin(margin),
		memberStake,
		poolStake,
	)
}

func memberRewardChecked(
	poolReward uint64,
	cost uint64,
	margin *big.Rat,
	memberStake uint64,
	poolStake uint64,
) (uint64, error) {
	if poolReward <= cost || poolStake == 0 || memberStake == 0 {
		return 0, nil
	}
	memberStakeRatio := new(big.Rat).SetFrac(
		new(big.Int).SetUint64(memberStake),
		new(big.Int).SetUint64(poolStake),
	)
	return floorMulChecked(
		new(big.Rat).Sub(oneRat(), margin),
		uintRat(poolReward-cost),
		memberStakeRatio,
	)
}

func normalizedMargin(margin *big.Rat) *big.Rat {
	if margin == nil {
		return new(big.Rat)
	}
	return new(big.Rat).Set(margin)
}

func floorMulChecked(values ...*big.Rat) (uint64, error) {
	acc := oneRat()
	for _, value := range values {
		if value == nil || value.Sign() <= 0 {
			return 0, nil
		}
		acc.Mul(acc, value)
	}
	return floorRatChecked(acc)
}

func floorRatChecked(value *big.Rat) (uint64, error) {
	if value == nil || value.Sign() <= 0 {
		return 0, nil
	}
	num := new(big.Int).Set(value.Num())
	den := value.Denom()
	q := new(big.Int).Quo(num, den)
	if !q.IsUint64() {
		return 0, fmt.Errorf("%w: floor %s", ErrRewardAmountOverflow, q.String())
	}
	return q.Uint64(), nil
}

func minRat(a, b *big.Rat) *big.Rat {
	if a.Cmp(b) <= 0 {
		return new(big.Rat).Set(a)
	}
	return new(big.Rat).Set(b)
}

func uintRat(v uint64) *big.Rat {
	return new(big.Rat).SetInt(new(big.Int).SetUint64(v))
}

func oneRat() *big.Rat {
	return new(big.Rat).SetInt64(1)
}

func addUint64(a, b uint64) (uint64, bool) {
	if a > math.MaxUint64-b {
		return 0, true
	}
	return a + b, false
}
