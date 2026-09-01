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
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	dingogov "github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
)

// ErrNotFound is returned when a requested item is not found
var ErrNotFound = errors.New("conformance: not found")

// withBadConnRetry retries fn once when it fails with a transient "bad
// connection" driver error -- observed specifically on the first read
// against a Postgres backend immediately after a fresh migration run (a
// newly pooled connection's first statement can race the just-committed
// DDL and get invalidated by the driver), which exceeds database/sql's
// own built-in bad-connection retry budget under this harness's
// connection-pool sizing and usage pattern (many short-lived, nil-txn
// reads). A single manual retry against a fresh connection resolves it;
// if the second attempt also fails, the error is real and is returned
// as-is, not swallowed.
func withBadConnRetry[T any](fn func() (T, error)) (T, error) {
	v, err := fn()
	if err != nil && isBadConnErr(err) {
		v, err = fn()
	}
	return v, err
}

// isBadConnErr reports whether err is (or wraps, including as a message
// substring surfaced through a non-wrapping fmt.Errorf in an
// intermediate layer) database/sql/driver's transient bad-connection
// signal.
func isBadConnErr(err error) bool {
	return errors.Is(err, driver.ErrBadConn) ||
		strings.Contains(err.Error(), "bad connection")
}

// DingoStateProvider implements conformance.StateProvider by wrapping
// DingoStateManager to satisfy all gouroboros state interfaces. Every read
// method below queries manager.db -- the real, configured backend -- live;
// none of them read from any in-memory mirror of UTxO/certificate/pool/
// DRep/committee state (see state_manager.go's type doc comment for the
// one narrow, documented exception: reward-account balances, which are
// harness-injected synthetic validation input, not application state
// Dingo itself commits).
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

// UtxoById looks up a UTxO by transaction input, reading through the real
// backend (metadata row plus blob-stored output CBOR -- see
// DingoStateManager.createUtxo).
func (p *DingoStateProvider) UtxoById(
	id common.TransactionInput,
) (common.Utxo, error) {
	if id == nil {
		return common.Utxo{}, ErrNotFound
	}

	inputId := id.Id()
	inputIdx := id.Index()

	utxo, err := withBadConnRetry(func() (*models.Utxo, error) {
		return p.manager.db.UtxoByRef(inputId.Bytes(), inputIdx, nil)
	})
	if err != nil {
		if errors.Is(err, database.ErrUtxoNotFound) {
			return common.Utxo{}, ErrNotFound
		}
		return common.Utxo{}, fmt.Errorf("lookup utxo: %w", err)
	}
	output, err := utxo.Decode()
	if err != nil {
		return common.Utxo{}, fmt.Errorf("decode utxo output: %w", err)
	}

	txHash := inputId
	return common.Utxo{
		Id: &dingoTransactionInput{
			txId:  txHash,
			index: inputIdx,
		},
		Output: output,
	}, nil
}

// ========== common.CertState ==========

// StakeRegistration looks up stake registrations by staking key
func (p *DingoStateProvider) StakeRegistration(
	stakingKey []byte,
) ([]common.StakeRegistrationCertificate, error) {
	regs, err := withBadConnRetry(
		func() ([]common.StakeRegistrationCertificate, error) {
			return p.manager.db.Metadata().
				GetStakeRegistrationsByCredential(0, stakingKey, nil)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("lookup stake registrations: %w", err)
	}
	if len(regs) == 0 {
		scriptRegs, err := withBadConnRetry(
			func() ([]common.StakeRegistrationCertificate, error) {
				return p.manager.db.Metadata().
					GetStakeRegistrationsByCredential(1, stakingKey, nil)
			},
		)
		if err != nil {
			return nil, fmt.Errorf("lookup stake registrations: %w", err)
		}
		return scriptRegs, nil
	}
	return regs, nil
}

// IsStakeCredentialRegistered checks if a stake credential is currently registered
func (p *DingoStateProvider) IsStakeCredentialRegistered(
	cred common.Credential,
) bool {
	credentialTag := conformanceCredentialTag(cred)
	account, err := withBadConnRetry(func() (*models.Account, error) {
		return p.manager.db.GetAccountByCredential(
			credentialTag, cred.Credential[:], false, nil,
		)
	})
	if err != nil || account == nil {
		return false
	}
	return account.Active
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

// PoolCurrentState returns the current state of a pool. A pool's
// PoolRetirementCertificate is already persisted (pool + pool_retirement
// rows) at certificate-application time via SetTransactionMetadataOnly in
// ApplyTransaction, so there is nothing further to read at epoch-boundary
// time -- see ProcessEpochBoundary's doc comment. The pending retirement
// epoch, when any, is derived by pendingPoolRetirementEpoch (matching
// ledger.LedgerView.PoolCurrentState); whether the pool is still
// considered actively registered is decided by poolIsActive -- see its
// doc comment.
func (p *DingoStateProvider) PoolCurrentState(
	poolKeyHash common.PoolKeyHash,
) (*common.PoolRegistrationCertificate, *uint64, error) {
	pool, err := withBadConnRetry(func() (*models.Pool, error) {
		return p.manager.db.GetPool(poolKeyHash, true, nil)
	})
	if errors.Is(err, models.ErrPoolNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("lookup pool: %w", err)
	}
	pendingEpoch := pendingPoolRetirementEpoch(pool)
	if !poolIsActive(pool, p.manager.currentEpoch) {
		return nil, pendingEpoch, nil
	}
	return &common.PoolRegistrationCertificate{
		Operator: poolKeyHash,
	}, pendingEpoch, nil
}

// IsPoolRegistered checks if a pool is currently active -- see poolIsActive.
func (p *DingoStateProvider) IsPoolRegistered(
	poolKeyHash common.PoolKeyHash,
) bool {
	pool, err := withBadConnRetry(func() (*models.Pool, error) {
		return p.manager.db.GetPool(poolKeyHash, true, nil)
	})
	if err != nil {
		return false
	}
	return poolIsActive(pool, p.manager.currentEpoch)
}

// poolIsActive mirrors the ordering rule the real node's
// GetActivePoolKeyHashesAtSlot uses
// (database/plugin/metadata/sqlstore/pool.go): a pool is active if it has
// a registration and either (a) that registration was submitted at or
// after its latest retirement certificate -- a later re-registration
// cancels a pending retirement, which the metadata store's certificate
// application never separately deletes -- or (b) the retirement's target
// epoch has not yet arrived. The real node derives this comparison from
// live chain tip/added_slot/cert-index ordering; this conformance harness
// has no real block stream to derive tip/cert-index from, so it compares
// each row's AddedSlot (falling back to CertificateID as an
// insertion-order tiebreak for same-slot certificates) against the
// manager's own authoritative currentEpoch instead.
func poolIsActive(pool *models.Pool, currentEpoch uint64) bool {
	reg := latestPoolRegistrationRow(pool)
	if reg == nil {
		return false
	}
	ret := latestPoolRetirementRow(pool)
	if ret == nil {
		return true
	}
	return registrationSupersedesRetirement(reg, ret) || currentEpoch < ret.Epoch
}

// registrationSupersedesRetirement reports whether reg was added after ret,
// meaning a later re-registration cancels ret as a pending retirement --
// the metadata store's certificate application never separately deletes a
// stale retirement row when a pool re-registers.
func registrationSupersedesRetirement(
	reg *models.PoolRegistration,
	ret *models.PoolRetirement,
) bool {
	return reg.AddedSlot > ret.AddedSlot ||
		(reg.AddedSlot == ret.AddedSlot && reg.CertificateID > ret.CertificateID)
}

// latestPoolRegistrationRow returns the most recently added registration
// row for pool (by AddedSlot, then CertificateID), or nil if it has none.
func latestPoolRegistrationRow(pool *models.Pool) *models.PoolRegistration {
	if len(pool.Registration) == 0 {
		return nil
	}
	latest := &pool.Registration[0]
	for i := 1; i < len(pool.Registration); i++ {
		reg := &pool.Registration[i]
		if reg.AddedSlot > latest.AddedSlot ||
			(reg.AddedSlot == latest.AddedSlot &&
				reg.CertificateID > latest.CertificateID) {
			latest = reg
		}
	}
	return latest
}

// latestPoolRetirementRow returns the most recently added retirement row
// for pool (by AddedSlot, then CertificateID), or nil if it has none.
func latestPoolRetirementRow(pool *models.Pool) *models.PoolRetirement {
	if len(pool.Retirement) == 0 {
		return nil
	}
	latest := &pool.Retirement[0]
	for i := 1; i < len(pool.Retirement); i++ {
		ret := &pool.Retirement[i]
		if ret.AddedSlot > latest.AddedSlot ||
			(ret.AddedSlot == latest.AddedSlot &&
				ret.CertificateID > latest.CertificateID) {
			latest = ret
		}
	}
	return latest
}

// pendingPoolRetirementEpoch returns the target epoch of pool's latest
// retirement certificate (by AddedSlot, then CertificateID) -- not the
// maximum epoch value across every retirement row: a later retirement
// certificate replaces the prior schedule even when it targets an earlier
// epoch. A later pool registration cancels the retirement entirely,
// mirroring poolIsActive's ordering rule above.
func pendingPoolRetirementEpoch(pool *models.Pool) *uint64 {
	ret := latestPoolRetirementRow(pool)
	if ret == nil {
		return nil
	}
	if reg := latestPoolRegistrationRow(pool); reg != nil &&
		registrationSupersedesRetirement(reg, ret) {
		return nil
	}
	epoch := ret.Epoch
	return &epoch
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

// RewardAccountBalance returns the current reward balance for a stake
// credential. Reward balances are harness-injected synthetic validation
// input (see DingoStateManager.SetRewardBalances's doc comment), so this
// reads the govState mirror rather than the real backend.
func (p *DingoStateProvider) RewardAccountBalance(
	cred common.Credential,
) (*uint64, error) {
	balance, exists := p.manager.govState.RewardAccountBalances[mockledger.NewRewardAccountKey(cred)]
	if !exists {
		return nil, nil
	}
	return &balance, nil
}

// ========== common.GovState ==========

// CommitteeMember looks up a constitutional committee member by credential
// hash. Enacted (real, committed) members -- including the vector's initial
// committee, loaded into the backend by LoadInitialState -- are read from
// the backend directly and never fall back to the govState mirror:
// govState.CommitteeMembers holds that same initial/enacted set (see
// LoadFromParsedState and enactProposal), so falling back to it here would
// let a backend that drops or cannot read a committee_member row still
// report the vector as passing. A member proposed by a pending (not yet
// enacted) UpdateCommittee action is the one case with no real
// committee_member row to read yet, so that case resolves the persisted
// proposal directly.
func (p *DingoStateProvider) CommitteeMember(
	coldKey common.Blake2b224,
) (*common.CommitteeMember, error) {
	resolve := func(
		credential common.Credential,
	) (*common.CommitteeMember, error) {
		member, err := p.legacyCommitteeMember(credential)
		if err != nil {
			return nil, err
		}
		if member != nil {
			return member, nil
		}
		return p.proposedCommitteeMember(credential)
	}
	keyMember, err := resolve(common.Credential{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: coldKey,
	})
	if err != nil {
		return nil, err
	}
	scriptMember, err := resolve(common.Credential{
		CredType:   common.CredentialTypeScriptHash,
		Credential: coldKey,
	})
	if err != nil {
		return nil, err
	}
	if keyMember != nil && scriptMember != nil {
		return nil, nil
	}
	if keyMember != nil {
		return keyMember, nil
	}
	return scriptMember, nil
}

func (p *DingoStateProvider) CommitteeStateAvailable() (bool, error) {
	return p != nil && p.manager != nil && p.manager.db != nil, nil
}

func (p *DingoStateProvider) CommitteeCredentialMember(
	coldCredential common.Credential,
) (*common.CommitteeMember, error) {
	member, err := p.realCommitteeMember(coldCredential)
	if err != nil {
		return member, err
	}
	if member != nil && !member.Resigned {
		return member, nil
	}
	resignedMember := member
	member, err = p.proposedCommitteeMember(coldCredential)
	if err != nil {
		return nil, err
	}
	if member == nil {
		return resignedMember, nil
	}
	return member, nil
}

// proposedCommitteeMember resolves a member named by a pending, not yet
// enacted UpdateCommittee proposal.
func (p *DingoStateProvider) proposedCommitteeMember(
	coldCredential common.Credential,
) (*common.CommitteeMember, error) {
	proposals, err := withBadConnRetry(
		func() ([]*models.GovernanceProposal, error) {
			return p.manager.db.GetActiveGovernanceProposals(
				p.manager.currentEpoch,
				nil,
			)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("lookup pending committee proposals: %w", err)
	}
	root, err := p.manager.db.GetLastEnactedGovernanceProposal(
		[]uint8{uint8(common.GovActionTypeUpdateCommittee)}, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("lookup committee proposal root: %w", err)
	}
	member, termStart, err := dingogov.ResolveCommitteeProposal(
		proposals, root, coldCredential, p.manager.protocolParams,
	)
	if err != nil {
		return nil, err
	}
	if member != nil {
		if err := p.populateCommitteeMemberStatus(
			coldCredential,
			termStart,
			member,
			true,
		); err != nil {
			return nil, err
		}
	}
	return member, nil
}

// legacyCommitteeMember mirrors ledger.LedgerView.legacyCommitteeCredentialMember:
// the first seated term for a tagged credential, returned even when resigned,
// with no pending-successor resolution.
func (p *DingoStateProvider) legacyCommitteeMember(
	coldCredential common.Credential,
) (*common.CommitteeMember, error) {
	coldTag, err := models.CredentialTagFromUint(coldCredential.CredType)
	if err != nil {
		return nil, fmt.Errorf("invalid committee cold credential: %w", err)
	}
	members, err := withBadConnRetry(func() ([]*models.CommitteeMember, error) {
		return p.manager.db.GetCommitteeMembers(nil)
	})
	if err != nil {
		return nil, fmt.Errorf("lookup committee members: %w", err)
	}
	for _, member := range members {
		if member.ColdCredentialTag != coldTag ||
			common.NewBlake2b224(member.ColdCredHash) != coldCredential.Credential {
			continue
		}
		result := &common.CommitteeMember{
			ColdKey:     coldCredential.Credential,
			ExpiryEpoch: member.ExpiresEpoch,
		}
		if err := p.populateCommitteeMemberStatus(
			coldCredential,
			member.TermStartSlot,
			result,
			false,
		); err != nil {
			return nil, err
		}
		return result, nil
	}
	return nil, nil
}

// realCommitteeMember reads an enacted committee member's full state
// (expiry epoch, hot-key authorization, resignation) by joining the real
// committee_member and auth_committee_hot rows.
func (p *DingoStateProvider) realCommitteeMember(
	coldCredential common.Credential,
) (*common.CommitteeMember, error) {
	coldTag, err := models.CredentialTagFromUint(coldCredential.CredType)
	if err != nil {
		return nil, fmt.Errorf("invalid committee cold credential: %w", err)
	}
	members, err := withBadConnRetry(func() ([]*models.CommitteeMember, error) {
		return p.manager.db.GetCommitteeMembers(nil)
	})
	if err != nil {
		return nil, fmt.Errorf("lookup committee members: %w", err)
	}
	var expiryEpoch uint64
	var termStartSlot uint64
	var addedSlot uint64
	var memberID uint
	found := false
	for _, member := range members {
		if member.ColdCredentialTag == coldTag &&
			common.NewBlake2b224(member.ColdCredHash) == coldCredential.Credential {
			if found && (member.TermStartSlot < termStartSlot ||
				(member.TermStartSlot == termStartSlot && member.AddedSlot < addedSlot) ||
				(member.TermStartSlot == termStartSlot && member.AddedSlot == addedSlot && member.ID < memberID)) {
				continue
			}
			expiryEpoch = member.ExpiresEpoch
			termStartSlot = member.TermStartSlot
			addedSlot = member.AddedSlot
			memberID = member.ID
			found = true
		}
	}
	if !found {
		return nil, nil
	}

	result := &common.CommitteeMember{
		ColdKey:     coldCredential.Credential,
		ExpiryEpoch: expiryEpoch,
	}
	if err := p.populateCommitteeMemberStatus(
		coldCredential,
		termStartSlot,
		result,
		false,
	); err != nil {
		return nil, err
	}
	return result, nil
}

// populateCommitteeMemberStatus mirrors ledger.LedgerView's helper, including
// its pending-term rule: a pending term's start is the proposal's own added
// slot, so the resignation of the term it replaces sits at or after it. Skip
// the lookup rather than clearing its result afterwards, which would drop the
// hot authorization production keeps.
func (p *DingoStateProvider) populateCommitteeMemberStatus(
	coldCredential common.Credential,
	termStartSlot uint64,
	result *common.CommitteeMember,
	pending bool,
) error {
	coldTag, err := models.CredentialTagFromUint(coldCredential.CredType)
	if err != nil {
		return fmt.Errorf("invalid committee cold credential: %w", err)
	}
	auth, err := withBadConnRetry(func() (*models.AuthCommitteeHot, error) {
		return p.manager.db.GetCommitteeMember(
			coldTag,
			coldCredential.Credential[:],
			termStartSlot,
			nil,
		)
	})
	if err != nil && !errors.Is(err, models.ErrCommitteeMemberNotFound) {
		return fmt.Errorf("lookup committee hot key: %w", err)
	}
	if auth != nil {
		hotKey := common.NewBlake2b224(auth.HotCredential)
		result.HotKey = &hotKey
	}

	if pending {
		return nil
	}
	resigned, err := withBadConnRetry(func() (bool, error) {
		return p.manager.db.IsCommitteeMemberResigned(
			coldTag,
			coldCredential.Credential[:],
			termStartSlot,
			nil,
		)
	})
	if err != nil {
		return fmt.Errorf("lookup committee resignation: %w", err)
	}
	result.Resigned = resigned
	if resigned {
		result.HotKey = nil
	}

	return nil
}

// CommitteeMembers returns every enacted committee member -- including the
// vector's initial committee, loaded into the backend by LoadInitialState --
// read from the backend directly. It never merges in govState.CommitteeMembers:
// that map holds the same initial/enacted set (see LoadFromParsedState and
// enactProposal), so merging it here would let a backend that drops or
// cannot read a committee_member row still report the vector as passing.
// Unlike CommitteeMember, there is no per-credential caller asking about a
// specific pending UpdateCommittee proposal here, so there is no
// commit-free case left to fall back for.
func (p *DingoStateProvider) CommitteeMembers() ([]common.CommitteeMember, error) {
	var members []common.CommitteeMember

	realMembers, err := withBadConnRetry(
		func() ([]*models.CommitteeMember, error) {
			return p.manager.db.GetCommitteeMembers(nil)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("lookup committee members: %w", err)
	}
	// A credential is (tag, hash), and several rows for one credential are its
	// successive terms. Counting hashes alone dropped a re-elected member, and
	// conflated a key credential with a script credential of the same hash.
	// CommitteeCredentialMember already resolves a credential to its latest
	// term, so resolve once per unique credential.
	type credentialKey struct {
		tag  uint8
		hash string
	}
	tagsByHash := make(map[string]map[uint8]struct{}, len(realMembers))
	seen := make(map[credentialKey]struct{}, len(realMembers))
	order := make([]credentialKey, 0, len(realMembers))
	for _, dbMember := range realMembers {
		key := credentialKey{
			tag:  dbMember.ColdCredentialTag,
			hash: string(dbMember.ColdCredHash),
		}
		if tagsByHash[key.hash] == nil {
			tagsByHash[key.hash] = make(map[uint8]struct{}, 1)
		}
		tagsByHash[key.hash][key.tag] = struct{}{}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		order = append(order, key)
	}
	for _, key := range order {
		// The legacy list shape cannot carry a credential tag, so a hash
		// seated under both tags stays ambiguous and is omitted.
		if len(tagsByHash[key.hash]) != 1 {
			continue
		}
		member, err := p.CommitteeCredentialMember(common.Credential{
			CredType:   uint(key.tag),
			Credential: common.NewBlake2b224([]byte(key.hash)),
		})
		if err != nil {
			return nil, err
		}
		if member != nil {
			members = append(members, *member)
		}
	}

	return members, nil
}

func (p *DingoStateProvider) CommitteeHotCredentialMember(
	hotCredential common.Credential,
) (*common.CommitteeMember, error) {
	authorizations, err := withBadConnRetry(
		func() ([]*models.AuthCommitteeHot, error) {
			return p.manager.db.GetActiveCommitteeMembers(nil)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("lookup active committee hot credentials: %w", err)
	}
	for _, authorization := range authorizations {
		hotTag, err := models.CredentialTagFromUint(hotCredential.CredType)
		if err != nil {
			return nil, fmt.Errorf("invalid committee hot credential: %w", err)
		}
		if authorization.HotCredentialTag != hotTag ||
			common.NewBlake2b224(authorization.HotCredential) !=
				hotCredential.Credential {
			continue
		}
		member, err := p.CommitteeCredentialMember(common.Credential{
			CredType:   uint(authorization.ColdCredentialTag),
			Credential: common.NewBlake2b224(authorization.ColdCredential),
		})
		if err != nil {
			return nil, err
		}
		if member == nil || member.Resigned ||
			member.ExpiryEpoch < p.manager.currentEpoch {
			continue
		}
		return member, nil
	}
	return nil, nil
}

// DRepRegistration looks up a DRep registration by credential hash. The
// real store keys DRep rows by (credentialTag, credential); the upstream
// conformance interface only carries the hash, so both credential tags are
// checked, matching the tag-agnostic semantics the harness has always used.
func (p *DingoStateProvider) DRepRegistration(
	credential common.Blake2b224,
) (*common.DRepRegistration, error) {
	for _, tag := range [...]uint8{0, 1} {
		drep, err := withBadConnRetry(func() (*models.Drep, error) {
			return p.manager.db.GetDrepByCredential(
				tag, credential[:], false, nil,
			)
		})
		if err != nil {
			if errors.Is(err, models.ErrDrepNotFound) {
				continue
			}
			return nil, fmt.Errorf("lookup drep registration: %w", err)
		}
		if drep != nil && drep.Active {
			return &common.DRepRegistration{Credential: credential}, nil
		}
	}
	return nil, nil
}

// DRepDelegation returns the DRep a stake credential is vote-delegated to, or
// nil if it is not delegated. Used to validate reward withdrawals on
// protocol versions 10 and 11. Reads the account's real account.drep column
// through the real backend, matching production's
// ledger.LedgerView.DRepDelegation, rather than the govState pre-validation
// mirror: a real backend that never persists or returns account.drep
// correctly would still pass every vector here if this read the mirror
// instead, since ApplyTransaction's certificate processing writes
// delegation through the real SetTransactionMetadataOnly path regardless of
// what this read side consults.
func (p *DingoStateProvider) DRepDelegation(
	cred common.Credential,
) (*common.Drep, error) {
	credentialTag := conformanceCredentialTag(cred)
	account, err := withBadConnRetry(func() (*models.Account, error) {
		return p.manager.db.GetAccountByCredential(
			credentialTag, cred.Credential[:], false, nil,
		)
	})
	if err != nil {
		if errors.Is(err, models.ErrAccountNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get account for drep delegation: %w", err)
	}
	if account == nil {
		return nil, nil
	}
	// No DRep delegation: an empty credential together with the default
	// key-hash type. An always-abstain / always-no-confidence delegation
	// carries no credential but a non-default type, so it is a delegation.
	if len(account.Drep) == 0 &&
		account.DrepType == models.DrepTypeAddrKeyHash {
		return nil, nil
	}
	// DrepType is a small ledger enum (0-3); guard the narrowing conversion
	// so an out-of-range value degrades to "no DRep" rather than wrapping.
	if account.DrepType > uint64(math.MaxInt) {
		return nil, nil
	}
	return &common.Drep{
		Type:       int(account.DrepType),
		Credential: append([]byte(nil), account.Drep...),
	}, nil
}

// DRepRegistrations returns all DRep registrations
func (p *DingoStateProvider) DRepRegistrations() ([]common.DRepRegistration, error) {
	dreps, err := withBadConnRetry(func() ([]*models.Drep, error) {
		return p.manager.db.GetActiveDreps(nil)
	})
	if err != nil {
		return nil, fmt.Errorf("lookup active dreps: %w", err)
	}
	result := make([]common.DRepRegistration, 0, len(dreps))
	for _, drep := range dreps {
		result = append(result, common.DRepRegistration{
			Credential: common.NewBlake2b224(drep.Credential),
		})
	}
	return result, nil
}

// Constitution returns the current constitution
func (p *DingoStateProvider) Constitution() (*common.Constitution, error) {
	return &common.Constitution{}, nil
}

// TreasuryValue returns the current treasury value
func (p *DingoStateProvider) TreasuryValue() (uint64, error) {
	return 0, nil
}

// GovActionById looks up a governance action by its ID against the real
// backend.
func (p *DingoStateProvider) GovActionById(
	id common.GovActionId,
) (*common.GovActionState, error) {
	proposal, err := withBadConnRetry(
		func() (*models.GovernanceProposal, error) {
			return p.manager.db.GetGovernanceProposal(
				id.TransactionId[:], id.GovActionIdx, nil,
			)
		},
	)
	if errors.Is(err, models.ErrGovernanceProposalNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup governance action: %w", err)
	}
	return &common.GovActionState{
		ActionId:   id,
		ActionType: common.GovActionType(proposal.ActionType),
		ExpirySlot: proposal.ExpiresEpoch * conformanceSlotsPerEpoch,
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

// Keep the conformance provider on the same credential-aware committee
// capability as the production LedgerView.
var _ eras.CommitteeCredentialState = (*DingoStateProvider)(nil)

// conformance.StateProvider does not include DRepDelegationState: the Conway
// reward-withdrawal rule discovers it with a runtime type assertion instead.
// Without this guard the harness would keep compiling after a signature drift
// and stop exercising the protocol-version 10/11 withdrawal rule it exists to
// cover, matching ledger.LedgerView's guard for the production path.
var _ common.DRepDelegationState = (*DingoStateProvider)(nil)
