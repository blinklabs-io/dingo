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
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
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
	_, exists := p.manager.stakeRegistrations[mockledger.NewRewardAccountKey(cred)]
	return exists
}

// StakeCredentialDeposit returns the deposit recorded when the stake
// credential registered, or nil when the credential is not registered or the
// recorded deposit is unknown.
//
// Without this method the harness does not satisfy
// common.StakeCredentialDepositState, so
// UtxoValidateValueNotConservedUtxo's optional type assertion misses and
// every legacy stake deregistration in the corpus is refunded at the current
// KeyDeposit. The corpus then cannot distinguish a correct recorded refund
// from the fallback, which is the gap #3831 covers.
//
// This mirrors ledger.LedgerView.StakeCredentialDeposit: the account lookup
// gates on the same live registration state as
// IsStakeCredentialRegistered above, the registration history carries the
// deposit actually paid, and the import baseline stands in for a credential
// established by a vector's initial state rather than by a certificate in
// that vector. A nil return is preserved rather than coerced to zero, because
// the rule treats any non-nil value as authoritative.
func (p *DingoStateProvider) StakeCredentialDeposit(
	cred common.Credential,
) (*uint64, error) {
	credentialTag, err := models.CredentialTagFromUint(cred.CredType)
	if err != nil {
		return nil, err
	}
	account, err := withBadConnRetry(func() (*models.Account, error) {
		return p.manager.db.GetAccountByCredential(
			credentialTag, cred.Credential[:], false, nil,
		)
	})
	if errors.Is(err, models.ErrAccountNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup stake credential deposit: %w", err)
	}
	if account == nil || !account.Active {
		return nil, nil
	}
	history, err := withBadConnRetry(
		func() ([]models.AccountRegistrationHistoryRow, error) {
			return p.manager.db.GetAccountRegistrationHistoryByCredential(
				credentialTag, cred.Credential[:], 1, 0, "desc", nil,
			)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("lookup stake registration history: %w", err)
	}
	importRegistration, err := withBadConnRetry(
		func() (*models.AccountImportRegistration, error) {
			return p.manager.db.GetAccountImportRegistrationByCredential(
				credentialTag, cred.Credential[:], nil,
			)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("lookup stake import registration: %w", err)
	}
	// A vector's initial-state registration is seeded as an import baseline,
	// so it wins unless the vector's own certificates registered the
	// credential more recently.
	if importRegistration != nil &&
		(len(history) == 0 ||
			importRegistration.AddedSlot >= history[0].AddedSlot) {
		return importRegistration.Deposit, nil
	}
	if len(history) == 0 || history[0].Action != "registered" {
		return nil, nil
	}
	return history[0].Deposit, nil
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

// IsVrfKeyInUse checks if a VRF key hash is registered by another pool.
// Conformance tests don't currently test VRF key uniqueness.
func (p *DingoStateProvider) IsVrfKeyInUse(
	vrfKeyHash common.Blake2b256,
) (bool, common.PoolKeyHash, error) {
	return false, common.PoolKeyHash{}, nil
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
	balance, exists := p.manager.stakeRegistrations[mockledger.NewRewardAccountKey(cred)]
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

// DRepDelegation returns the DRep a stake credential is vote-delegated to, or
// nil if it is not delegated. Used to validate reward withdrawals on protocol
// versions 10 and 11.
func (p *DingoStateProvider) DRepDelegation(
	cred common.Credential,
) (*common.Drep, error) {
	delegation, ok := p.manager.govState.DRepDelegationsByCredential[mockledger.NewRewardAccountKey(cred)]
	if !ok && len(p.manager.govState.DRepDelegationsByCredential) == 0 {
		delegation, ok = p.manager.govState.DRepDelegations[cred.Credential]
	}
	if !ok {
		return nil, nil
	}
	delegation.Credential = append([]byte(nil), delegation.Credential...)
	return &delegation, nil
}

// DRepRegistrations returns all DRep registrations
func (p *DingoStateProvider) DRepRegistrations() ([]common.DRepRegistration, error) {
	dreps := make(
		[]common.DRepRegistration,
		0,
		len(p.manager.drepRegistrations),
	)
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

// TreasuryValue returns the treasury value from the real backend, in the same
// shape production's ledger.LedgerView.TreasuryValue reports.
//
// It never answers a synthetic zero. The harness does not seed treasury/pot
// accounting (see DingoStateManager.persistEnactment), so an unseeded backend
// has no network-state row at all. Returning 0 for that would make a provider
// that cannot answer look healthy: the upstream current-treasury-value rule
// only queries this method once a transaction body actually carries key 21,
// and it compares for equality, so a synthetic zero silently rejects every
// vector that declares a non-zero value and silently accepts one declaring
// zero. Failing closed reports the missing harness state instead.
func (p *DingoStateProvider) TreasuryValue() (uint64, error) {
	state, err := withBadConnRetry(func() (*models.NetworkState, error) {
		return p.manager.db.Metadata().GetNetworkState(nil)
	})
	if err != nil {
		return 0, fmt.Errorf("lookup treasury network state: %w", err)
	}
	if state == nil {
		return 0, errors.New(
			"treasury network state is unavailable: conformance harness does not seed treasury state",
		)
	}
	return uint64(state.Treasury), nil
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

// conformance.StateProvider does not include DRepDelegationState: the Conway
// reward-withdrawal rule discovers it with a runtime type assertion instead.
// Without this guard the harness would keep compiling after a signature drift
// and stop exercising the protocol-version 10/11 withdrawal rule it exists to
// cover, matching ledger.LedgerView's guard for the production path.
var _ common.DRepDelegationState = (*DingoStateProvider)(nil)

// conformance.StateProvider does not include StakeCredentialDepositState
// either. UtxoValidateValueNotConservedUtxo discovers it with an optional type
// assertion and silently falls back to the current KeyDeposit when it misses,
// so a signature drift here would not fail a vector -- it would quietly stop
// exercising the recorded-deposit refund the corpus is supposed to cover.
// Mirrors ledger.LedgerView's guard for the production path.
var _ common.StakeCredentialDepositState = (*DingoStateProvider)(nil)
