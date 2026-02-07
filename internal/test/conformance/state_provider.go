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

package conformance

import (
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
)

// ErrNotFound is returned when a requested item is not found
var ErrNotFound = errors.New("conformance: not found")

// DingoStateProvider implements conformance.StateProvider by wrapping
// DingoStateManager to satisfy all gouroboros state interfaces.
type DingoStateProvider struct {
	manager *DingoStateManager
}

// NewDingoStateProvider creates a new DingoStateProvider.
func NewDingoStateProvider(manager *DingoStateManager) *DingoStateProvider {
	return &DingoStateProvider{manager: manager}
}

// ========== common.LedgerState ==========

// NetworkId returns the network identifier
func (p *DingoStateProvider) NetworkId() uint {
	// Default to testnet (0) for conformance tests
	return 0
}

// CostModels returns which Plutus language versions have cost models
// defined. CostModel values are empty markers (struct{} upstream).
func (p *DingoStateProvider) CostModels() map[common.PlutusLanguage]common.CostModel {
	if p.manager.protocolParams == nil {
		return make(map[common.PlutusLanguage]common.CostModel)
	}
	return extractCostModels(p.manager.protocolParams)
}

// ========== common.UtxoState ==========

// UtxoById looks up a UTxO by transaction input
func (p *DingoStateProvider) UtxoById(
	id common.TransactionInput,
) (common.Utxo, error) {
	if id == nil {
		return common.Utxo{}, ErrNotFound
	}

	inputId := id.Id()
	inputIdx := id.Index()
	utxoId := fmt.Sprintf("%x#%d", inputId.Bytes(), inputIdx)

	if utxo, ok := p.manager.utxos[utxoId]; ok {
		return utxo, nil
	}
	return common.Utxo{}, ErrNotFound
}

// ========== common.CertState ==========

// StakeRegistration looks up stake registrations by staking key
func (p *DingoStateProvider) StakeRegistration(
	stakingKey []byte,
) ([]common.StakeRegistrationCertificate, error) {
	// For conformance testing, we track registrations by credential hash
	// Return empty slice if not found
	return []common.StakeRegistrationCertificate{}, nil
}

// IsStakeCredentialRegistered checks if a stake credential is currently registered
func (p *DingoStateProvider) IsStakeCredentialRegistered(
	cred common.Credential,
) bool {
	_, exists := p.manager.stakeRegistrations[cred.Credential]
	return exists
}

// ========== common.SlotState ==========

// SlotToTime converts a slot number to a time
func (p *DingoStateProvider) SlotToTime(slot uint64) (time.Time, error) {
	// For conformance testing, use a simple epoch-based calculation
	// assuming slot 0 = Unix epoch and 1 second per slot
	//nolint:gosec // G115: slot values in tests won't overflow int64
	return time.Unix(int64(slot), 0), nil
}

// TimeToSlot converts a time to a slot number
func (p *DingoStateProvider) TimeToSlot(t time.Time) (uint64, error) {
	//nolint:gosec // G115: Unix timestamps won't be negative in tests
	return uint64(t.Unix()), nil
}

// ========== common.PoolState ==========

// PoolCurrentState returns the current state of a pool
func (p *DingoStateProvider) PoolCurrentState(
	poolKeyHash common.PoolKeyHash,
) (*common.PoolRegistrationCertificate, *uint64, error) {
	if p.manager.poolRegistrations[poolKeyHash] {
		// Check if pool has pending retirement
		if retireEpoch, retiring := p.manager.govState.PoolRetirements[poolKeyHash]; retiring {
			return &common.PoolRegistrationCertificate{
				Operator: poolKeyHash,
			}, &retireEpoch, nil
		}
		return &common.PoolRegistrationCertificate{
			Operator: poolKeyHash,
		}, nil, nil
	}
	// Also check if pool is pending retirement
	if retireEpoch, retiring := p.manager.govState.PoolRetirements[poolKeyHash]; retiring {
		return &common.PoolRegistrationCertificate{
			Operator: poolKeyHash,
		}, &retireEpoch, nil
	}
	return nil, nil, nil
}

// IsPoolRegistered checks if a pool is currently registered
func (p *DingoStateProvider) IsPoolRegistered(
	poolKeyHash common.PoolKeyHash,
) bool {
	if p.manager.poolRegistrations[poolKeyHash] {
		return true
	}
	// Also check pending retirements (pool is still registered until retirement)
	_, retiring := p.manager.govState.PoolRetirements[poolKeyHash]
	return retiring
}

// ========== common.RewardState ==========

// CalculateRewards calculates rewards for the given epoch
func (p *DingoStateProvider) CalculateRewards(
	pots common.AdaPots,
	snapshot common.RewardSnapshot,
	params common.RewardParameters,
) (*common.RewardCalculationResult, error) {
	return common.CalculateRewards(pots, snapshot, params)
}

// GetAdaPots returns the current ADA pots
func (p *DingoStateProvider) GetAdaPots() common.AdaPots {
	return common.AdaPots{}
}

// UpdateAdaPots updates the ADA pots
func (p *DingoStateProvider) UpdateAdaPots(pots common.AdaPots) error {
	return nil
}

// GetRewardSnapshot returns the stake snapshot for reward calculation
func (p *DingoStateProvider) GetRewardSnapshot(
	epoch uint64,
) (common.RewardSnapshot, error) {
	return common.RewardSnapshot{}, nil
}

// IsRewardAccountRegistered checks if a reward account is registered
func (p *DingoStateProvider) IsRewardAccountRegistered(
	cred common.Credential,
) bool {
	return p.IsStakeCredentialRegistered(cred)
}

// RewardAccountBalance returns the current reward balance for a stake credential
func (p *DingoStateProvider) RewardAccountBalance(
	cred common.Credential,
) (*uint64, error) {
	balance, exists := p.manager.stakeRegistrations[cred.Credential]
	if !exists {
		return nil, nil
	}
	return &balance, nil
}

// ========== common.GovState ==========

// CommitteeMember looks up a constitutional committee member by credential hash
func (p *DingoStateProvider) CommitteeMember(
	coldKey common.Blake2b224,
) (*common.CommitteeMember, error) {
	// Check current members first
	if expiry, ok := p.manager.committeeMembers[coldKey]; ok {
		member := &common.CommitteeMember{
			ColdKey:     coldKey,
			ExpiryEpoch: expiry,
		}
		// Add hot key if authorized
		if hotKey, hasHot := p.manager.hotKeyAuthorizations[coldKey]; hasHot {
			member.HotKey = &hotKey
		}
		return member, nil
	}

	// Check proposed members from governance state
	if memberInfo := p.manager.govState.GetCommitteeMember(coldKey); memberInfo != nil {
		member := &common.CommitteeMember{
			ColdKey:     coldKey,
			ExpiryEpoch: memberInfo.ExpiryEpoch,
			Resigned:    memberInfo.Resigned,
		}
		if memberInfo.HotKey != nil {
			member.HotKey = memberInfo.HotKey
		}
		return member, nil
	}

	// Check if member is proposed in a pending UpdateCommittee action
	if p.manager.govState.IsProposedCommitteeMember(coldKey) {
		// Get the expiry from the proposal
		for _, proposal := range p.manager.govState.Proposals {
			if proposal.ActionType == common.GovActionTypeUpdateCommittee {
				if expiry, ok := proposal.ProposedMembers[coldKey]; ok {
					return &common.CommitteeMember{
						ColdKey:     coldKey,
						ExpiryEpoch: expiry,
					}, nil
				}
			}
		}
	}

	return nil, nil
}

// CommitteeMembers returns all committee members
func (p *DingoStateProvider) CommitteeMembers() ([]common.CommitteeMember, error) {
	var members []common.CommitteeMember

	// Add current members
	for coldKey, expiry := range p.manager.committeeMembers {
		member := common.CommitteeMember{
			ColdKey:     coldKey,
			ExpiryEpoch: expiry,
		}
		if hotKey, hasHot := p.manager.hotKeyAuthorizations[coldKey]; hasHot {
			member.HotKey = &hotKey
		}
		members = append(members, member)
	}

	// Add members from governance state
	for coldKey, memberInfo := range p.manager.govState.CommitteeMembers {
		// Skip if already added
		found := false
		for _, m := range members {
			if m.ColdKey == coldKey {
				found = true
				break
			}
		}
		if found {
			continue
		}
		member := common.CommitteeMember{
			ColdKey:     coldKey,
			ExpiryEpoch: memberInfo.ExpiryEpoch,
			Resigned:    memberInfo.Resigned,
		}
		if memberInfo.HotKey != nil {
			member.HotKey = memberInfo.HotKey
		}
		members = append(members, member)
	}

	return members, nil
}

// DRepRegistration looks up a DRep registration by credential hash
func (p *DingoStateProvider) DRepRegistration(
	credential common.Blake2b224,
) (*common.DRepRegistration, error) {
	if p.manager.drepRegistrations[credential] {
		return &common.DRepRegistration{
			Credential: credential,
		}, nil
	}
	return nil, nil
}

// DRepRegistrations returns all DRep registrations
func (p *DingoStateProvider) DRepRegistrations() ([]common.DRepRegistration, error) {
	dreps := make([]common.DRepRegistration, 0, len(p.manager.drepRegistrations))
	for cred := range p.manager.drepRegistrations {
		dreps = append(dreps, common.DRepRegistration{
			Credential: cred,
		})
	}
	return dreps, nil
}

// Constitution returns the current constitution
func (p *DingoStateProvider) Constitution() (*common.Constitution, error) {
	return &common.Constitution{}, nil
}

// TreasuryValue returns the current treasury value
func (p *DingoStateProvider) TreasuryValue() (uint64, error) {
	return 0, nil
}

// GovActionById looks up a governance action by its ID
func (p *DingoStateProvider) GovActionById(
	id common.GovActionId,
) (*common.GovActionState, error) {
	key := fmt.Sprintf(
		"%s#%d",
		hex.EncodeToString(id.TransactionId[:]),
		id.GovActionIdx,
	)
	proposal := p.manager.govState.GetProposal(key)
	if proposal == nil {
		return nil, nil
	}
	return &common.GovActionState{
		ActionId:   id,
		ActionType: proposal.ActionType,
		ExpirySlot: proposal.ExpiresAfter * 432000, // Approximate: epoch * slots per epoch
	}, nil
}

// GovActionExists checks if a governance action exists
func (p *DingoStateProvider) GovActionExists(id common.GovActionId) bool {
	state, _ := p.GovActionById(id)
	return state != nil
}

// extractCostModels returns which Plutus language versions are
// present in the protocol parameters.
//
// NOTE: common.CostModel is currently struct{} in gouroboros
// (a placeholder type). The returned map values carry no cost
// parameter data -- callers use map membership to check version
// availability. When gouroboros extends CostModel with real
// fields, this function should populate them from the raw
// []int64 cost parameters.
func extractCostModels(
	pp common.ProtocolParameters,
) map[common.PlutusLanguage]common.CostModel {
	if pp == nil {
		return nil
	}

	// Try to get cost models from the protocol parameters.
	type costModelsProvider interface {
		GetCostModels() map[uint][]int64
	}

	if provider, ok := pp.(costModelsProvider); ok {
		models := provider.GetCostModels()
		if models == nil {
			return nil
		}
		result := make(map[common.PlutusLanguage]common.CostModel)
		for version := range models {
			if version > 2 {
				continue
			}
			//nolint:gosec // G115: version is bounds checked above (0-2)
			plutusLang := common.PlutusLanguage(version + 1)
			// TODO: populate CostModel with models[version]
			// when gouroboros extends the type beyond struct{}.
			result[plutusLang] = common.CostModel{}
		}
		return result
	}

	return nil
}

// Compile-time interface check
var _ conformance.StateProvider = (*DingoStateProvider)(nil)
