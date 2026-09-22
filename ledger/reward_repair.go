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

package ledger

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/rewards"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// PoolStakeCorrection is one pool's own historical-stake reconstruction
// disagreeing with what reward_pool_input persisted for it at original
// calculation time, found by RepairStaleDelegationRewardCorruption.
type PoolStakeCorrection struct {
	PoolKeyHash    []byte
	PersistedStake uint64
	CorrectedStake uint64
}

// CreditCorrection is one delegator's stake-reward credit at the payout
// epoch that RepairStaleDelegationRewardCorruption found to differ from what
// a correct calculation, using the corrected network-wide totalActiveStake,
// would produce.
type CreditCorrection struct {
	PoolKeyHash     []byte
	CredentialTag   uint8
	StakingKey      []byte
	RewardType      rewards.RewardType
	OriginalAmount  uint64
	CorrectedAmount uint64
	Applied         bool
	ApplyErr        error
}

// StaleDelegationRewardRepairReport is the outcome of scanning and,
// optionally, repairing one mark epoch's stake-reward credits for dingo
// #4529 stale-delegation corruption.
type StaleDelegationRewardRepairReport struct {
	MarkEpoch                 uint64
	PayoutEpoch               uint64
	PersistedTotalActiveStake uint64
	CorrectedTotalActiveStake uint64
	PoolCorrections           []PoolStakeCorrection
	PoolScanErrors            map[string]error
	CreditCorrections         []CreditCorrection
}

// nodeShelleyGenesis is the minimal interface RepairStaleDelegationReward
// Corruption needs from a node config, matching rewardParametersFromPParams.
type nodeShelleyGenesisConfig interface {
	ShelleyGenesis() *shelley.ShelleyGenesis
}

// FindStaleDelegationStakeCorrections compares every pool active at
// markEpoch's mark-snapshot boundary against what reward_pool_input
// persisted for it at original calculation time, using the CURRENT (already
// correctly guarded by dc8e29bd/#4528) historical stake reconstruction. A
// pool whose two values differ was miscounted by pre-#4528 code -- see
// historical_stake.go's activeDelegationSQL doc comment for the mechanism
// (dingo #4529). A per-pool query error (for example the unrelated
// historicalRewardsBatch chain-integrity issue tracked separately) is
// recorded rather than aborting the whole scan, since one poisoned
// credential in one pool must not hide a real correction in every other
// pool.
func FindStaleDelegationStakeCorrections(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	markEpoch uint64,
) (
	corrections []PoolStakeCorrection,
	scanErrors map[string]error,
	persistedTotal uint64,
	err error,
) {
	epoch, err := meta.GetEpoch(markEpoch, metaTxn)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("get mark epoch %d: %w", markEpoch, err)
	}
	if epoch == nil || epoch.LengthInSlots == 0 {
		return nil, nil, 0, fmt.Errorf(
			"no epoch data for mark epoch %d",
			markEpoch,
		)
	}
	boundarySlot := epoch.StartSlot
	snapshotSlot := boundarySlot - 1

	poolInputs, err := meta.GetRewardPoolInputs(markEpoch, metaTxn)
	if err != nil {
		return nil, nil, 0, fmt.Errorf(
			"get reward pool inputs for epoch %d: %w", markEpoch, err,
		)
	}
	scanErrors = make(map[string]error)
	for _, input := range poolInputs {
		if input == nil {
			continue
		}
		persistedTotal += uint64(input.DelegatedStake)
	}
	for _, input := range poolInputs {
		if input == nil {
			continue
		}
		fresh, _, err := meta.GetEpochBoundaryStakeByPools(
			[][]byte{
				input.PoolKeyHash,
			},
			snapshotSlot,
			boundarySlot,
			0,
			0,
			metaTxn,
		)
		if err != nil {
			scanErrors[string(input.PoolKeyHash)] = err
			continue
		}
		freshStake := fresh[string(input.PoolKeyHash)]
		if freshStake != uint64(input.DelegatedStake) {
			corrections = append(corrections, PoolStakeCorrection{
				PoolKeyHash:    input.PoolKeyHash,
				PersistedStake: uint64(input.DelegatedStake),
				CorrectedStake: freshStake,
			})
		}
	}
	return corrections, scanErrors, persistedTotal, nil
}

// RepairStaleDelegationRewardCorruption finds every pool whose own
// mark[markEpoch] stake reconstruction disagrees with what was persisted
// (see FindStaleDelegationStakeCorrections), derives the corrected
// network-wide totalActiveStake from those corrections, and re-derives every
// active pool's leader/member reward split at markEpoch using dingo's own
// real reward-calculation machinery (rewardBlockCounts,
// rewardParametersFromPParams, rewards.CalculatePoolReward,
// rewards.MemberRewardWithParameters, rewards.LeaderRewardWithParameters)
// with that one corrected input, comparing the result against each
// delegator's persisted account_reward_delta credit at the payout epoch
// (markEpoch+3). When apply is true, a differing credit is corrected via
// meta.CorrectAccountRewardCredit; when false, the report only records what
// would change.
//
// A pool with no stake correction of its own can still need a credit
// correction: the corrected totalActiveStake is a shared input to every
// pool's apparentPerformance, so a purely downstream floor-rounding
// perturbation (dingo #4529's persistent, diffuse symptom) is repaired the
// same way as the two pools' own direct corruption.
func (ls *LedgerState) RepairStaleDelegationRewardCorruption(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	nodeConfig nodeShelleyGenesisConfig,
	markEpoch uint64,
	apply bool,
) (*StaleDelegationRewardRepairReport, error) {
	poolCorrections, scanErrors, persistedTotal, err := FindStaleDelegationStakeCorrections(
		meta,
		metaTxn,
		markEpoch,
	)
	if err != nil {
		return nil, err
	}
	report := &StaleDelegationRewardRepairReport{
		MarkEpoch:                 markEpoch,
		PayoutEpoch:               markEpoch + 3,
		PersistedTotalActiveStake: persistedTotal,
		PoolScanErrors:            scanErrors,
		PoolCorrections:           poolCorrections,
	}
	correctedTotal := persistedTotal
	for _, c := range poolCorrections {
		correctedTotal = correctedTotal - c.PersistedStake + c.CorrectedStake
	}
	report.CorrectedTotalActiveStake = correctedTotal
	if correctedTotal == persistedTotal {
		// Nothing to repair: every pool's own stake already matches what was
		// persisted, so the shared totalActiveStake denominator is unchanged
		// and every pool's reward split is provably identical to what was
		// already applied.
		return report, nil
	}

	snapshotEpoch, err := meta.GetEpoch(markEpoch, metaTxn)
	if err != nil || snapshotEpoch == nil {
		return nil, fmt.Errorf("get mark epoch %d: %w", markEpoch, err)
	}
	performanceEpoch := markEpoch + 1
	potsEpoch := markEpoch + 2

	protocolMajor, eraId, err := rewardEraAtEpoch(meta, metaTxn, markEpoch)
	if err != nil {
		return nil, err
	}
	eraDesc := eras.GetEraById(eraId)
	if eraDesc == nil || eraDesc.DecodePParamsFunc == nil {
		return nil, fmt.Errorf(
			"no pparams decoder for era %d (protocol major %d)",
			eraId,
			protocolMajor,
		)
	}
	pparamsRows, err := meta.GetPParams(markEpoch, eraId, metaTxn)
	if err != nil {
		return nil, fmt.Errorf(
			"get pparams for epoch %d era %d: %w",
			markEpoch,
			eraId,
			err,
		)
	}
	if len(pparamsRows) == 0 {
		return nil, fmt.Errorf(
			"no pparams rows for epoch %d era %d",
			markEpoch,
			eraId,
		)
	}
	pparams, err := eraDesc.DecodePParamsFunc(pparamsRows[0].Cbor)
	if err != nil {
		return nil, fmt.Errorf(
			"decode pparams for epoch %d: %w",
			markEpoch,
			err,
		)
	}
	genesis := nodeConfig.ShelleyGenesis()
	if genesis == nil {
		return nil, fmt.Errorf("missing Shelley genesis")
	}
	epochLength := uint64(genesis.EpochLength)
	if epochLength == 0 {
		epochLength = uint64(snapshotEpoch.LengthInSlots)
	}
	params, err := rewardParametersFromPParams(pparams, nodeConfig, epochLength)
	if err != nil {
		return nil, fmt.Errorf(
			"build reward parameters for epoch %d: %w",
			markEpoch,
			err,
		)
	}

	poolInputs, err := meta.GetRewardPoolInputs(markEpoch, metaTxn)
	if err != nil {
		return nil, fmt.Errorf(
			"get reward pool inputs for epoch %d: %w",
			markEpoch,
			err,
		)
	}
	blockCounts, totalBlocks, blockCountsKnown, err := ls.rewardBlockCounts(
		meta, metaTxn, performanceEpoch, poolInputs, params.Decentralization,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get block counts for epoch %d: %w",
			performanceEpoch,
			err,
		)
	}
	if !blockCountsKnown {
		return nil, fmt.Errorf(
			"block counts unknown for performance epoch %d; cannot re-derive rewards",
			performanceEpoch,
		)
	}

	potsRows, err := meta.GetRewardAdaPots(potsEpoch, metaTxn)
	if err != nil {
		return nil, fmt.Errorf(
			"get reward ada pots for epoch %d: %w",
			potsEpoch,
			err,
		)
	}
	if potsRows == nil {
		return nil, fmt.Errorf("no reward ada pots for epoch %d", potsEpoch)
	}
	availableRewards, err := rewardAvailableRewards(
		uint64(potsRows.Rewards),
		params,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"compute available rewards for epoch %d: %w",
			potsEpoch,
			err,
		)
	}
	if params.MaxLovelaceSupply < uint64(potsRows.Reserves) {
		return nil, fmt.Errorf(
			"reserves %d exceed max lovelace supply %d at epoch %d",
			uint64(potsRows.Reserves), params.MaxLovelaceSupply, potsEpoch,
		)
	}
	totalCirculation := params.MaxLovelaceSupply - uint64(potsRows.Reserves)

	correctedStakeByPool := make(map[string]uint64, len(poolCorrections))
	for _, c := range poolCorrections {
		correctedStakeByPool[string(c.PoolKeyHash)] = c.CorrectedStake
	}

	accountOutputsByEpoch := make(map[uint64][]*models.RewardAccountOutput)

	for _, input := range poolInputs {
		if input == nil {
			continue
		}
		delegatedStake := uint64(input.DelegatedStake)
		if corrected, ok := correctedStakeByPool[string(input.PoolKeyHash)]; ok {
			delegatedStake = corrected
		}
		pool := rewards.Pool{
			DelegatedStake: delegatedStake,
			OwnerStake:     uint64(input.OwnerStake),
			Cost:           uint64(input.Cost),
			Margin:         input.Margin.Rat,
			BlocksProduced: blockCounts[string(input.PoolKeyHash)],
			Pledge:         uint64(input.Pledge),
		}
		poolReward, err := rewards.CalculatePoolReward(
			pool,
			availableRewards,
			correctedTotal,
			totalCirculation,
			totalBlocks,
			params,
		)
		if err != nil {
			report.PoolScanErrors[string(input.PoolKeyHash)] = fmt.Errorf(
				"recompute pool reward: %w", err,
			)
			continue
		}

		stakeInputs, err := meta.GetEpochBoundaryRewardStakeInputsForPools(
			[][]byte{
				input.PoolKeyHash,
			},
			snapshotEpoch.StartSlot-1,
			snapshotEpoch.StartSlot,
			0,
			0,
			metaTxn,
		)
		if err != nil {
			report.PoolScanErrors[string(input.PoolKeyHash)] = fmt.Errorf(
				"get per-delegator stake: %w", err,
			)
			continue
		}
		for _, si := range stakeInputs {
			var correctedAmount uint64
			var rewardType rewards.RewardType
			isRewardAccount := si.CredentialTag == input.RewardAccountCredentialTag &&
				string(si.StakingKey) == string(input.RewardAccount)
			if isRewardAccount {
				correctedAmount, err = rewards.LeaderRewardWithParameters(
					poolReward.PoolReward, pool.Cost, pool.Margin,
					pool.OwnerStake, pool.DelegatedStake, params,
				)
				rewardType = rewards.RewardTypeLeader
			} else {
				correctedAmount, err = rewards.MemberRewardWithParameters(
					poolReward.PoolReward, pool.Cost, pool.Margin,
					uint64(si.Stake), pool.DelegatedStake, params,
				)
				rewardType = rewards.RewardTypeMember
			}
			if err != nil {
				report.PoolScanErrors[string(input.PoolKeyHash)] = fmt.Errorf(
					"recompute delegator reward: %w", err,
				)
				continue
			}
			existing, ok := accountOutputsByEpoch[markEpoch]
			if !ok {
				existing, err = meta.GetRewardAccountOutputs(markEpoch, metaTxn)
				if err != nil {
					report.PoolScanErrors[string(input.PoolKeyHash)] = fmt.Errorf(
						"get existing reward outputs: %w",
						err,
					)
					continue
				}
				accountOutputsByEpoch[markEpoch] = existing
			}
			var originalAmount uint64
			var found bool
			for _, e := range existing {
				if e == nil {
					continue
				}
				if string(e.PoolKeyHash) == string(input.PoolKeyHash) &&
					e.CredentialTag == si.CredentialTag &&
					string(e.StakingKey) == string(si.StakingKey) &&
					rewards.RewardType(e.RewardType) == rewardType {
					originalAmount = uint64(e.Amount)
					found = true
					break
				}
			}
			if !found || originalAmount == correctedAmount {
				continue
			}
			cc := CreditCorrection{
				PoolKeyHash:     input.PoolKeyHash,
				CredentialTag:   si.CredentialTag,
				StakingKey:      si.StakingKey,
				RewardType:      rewardType,
				OriginalAmount:  originalAmount,
				CorrectedAmount: correctedAmount,
			}
			if apply {
				payoutSlotEpoch, err := meta.GetEpoch(
					report.PayoutEpoch,
					metaTxn,
				)
				if err != nil || payoutSlotEpoch == nil {
					cc.ApplyErr = fmt.Errorf("get payout epoch: %w", err)
				} else {
					poolID, poolIDErr := rewards.NewPoolID(input.PoolKeyHash)
					credential, credErr := rewards.NewCredential(
						si.CredentialTag, si.StakingKey,
					)
					if poolIDErr != nil {
						cc.ApplyErr = fmt.Errorf("pool id: %w", poolIDErr)
					} else if credErr != nil {
						cc.ApplyErr = fmt.Errorf("credential: %w", credErr)
					} else {
						sourceHash := stakeRewardSourceHash(markEpoch, rewards.AccountReward{
							Credential: credential,
							PoolID:     poolID,
							Type:       rewardType,
						})
						cc.ApplyErr = meta.CorrectAccountRewardCredit(
							si.CredentialTag, si.StakingKey,
							payoutSlotEpoch.StartSlot, sourceHash, correctedAmount, metaTxn,
						)
						cc.Applied = cc.ApplyErr == nil
					}
				}
			}
			report.CreditCorrections = append(report.CreditCorrections, cc)
		}
	}
	return report, nil
}

// rewardEraAtEpoch resolves the protocol major version and era id in effect
// for markEpoch's reward calculation, the same way loadPersistedProtocol
// Parameters resolves it for the live path.
func rewardEraAtEpoch(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	markEpoch uint64,
) (protocolMajor uint64, eraId uint, err error) {
	snapshot, err := meta.GetRewardSnapshot(markEpoch, "mark", metaTxn)
	if err != nil {
		return 0, 0, fmt.Errorf(
			"get reward snapshot for epoch %d: %w",
			markEpoch,
			err,
		)
	}
	if snapshot == nil {
		return 0, 0, fmt.Errorf("no mark snapshot for epoch %d", markEpoch)
	}
	protocolMajor = uint64(snapshot.ProtocolVersion)
	for _, e := range eras.Eras {
		if protocolMajor >= uint64(e.MinMajorVersion) &&
			protocolMajor <= uint64(e.MaxMajorVersion) {
			return protocolMajor, e.Id, nil
		}
	}
	return protocolMajor, 0, fmt.Errorf(
		"no era covers protocol major version %d", protocolMajor,
	)
}
