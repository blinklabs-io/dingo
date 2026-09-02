// Copyright 2025 Blink Labs Software
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
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

// ErrNilDecodedOutput is returned when a decoded UTxO output is nil.
var ErrNilDecodedOutput = errors.New("nil decoded output")

// ErrUtxoAlreadyConsumed is returned when a UTxO has been consumed by a pending transaction.
var ErrUtxoAlreadyConsumed = errors.New("UTxO already consumed")

// ErrNotImplemented marks LedgerView stubs that are not implemented yet.
var ErrNotImplemented = errors.New("not implemented")

type LedgerView struct {
	ls  *LedgerState
	txn *database.Txn
	// Committee proposal resolution must use the same immutable consensus
	// publication as the validation that owns this view.
	committeeEpoch       uint64
	committeePParams     lcommon.ProtocolParameters
	committeeStatePinned bool
	// intraBlockUtxos tracks outputs created by earlier transactions in the same block.
	// Key format: hex(txId) + ":" + outputIdx
	intraBlockUtxos map[string]lcommon.Utxo
	// consumedUtxos tracks inputs consumed by pending mempool transactions.
	// Key format: hex(txId) + ":" + outputIdx
	consumedUtxos map[string]struct{}
	// skipPhase2Validation is set for accepted block replay, where
	// the producer's isValid flag is authoritative for Phase-2 results.
	skipPhase2Validation bool
}

func (lv *LedgerView) pinCommitteeState(
	epoch uint64,
	pparams lcommon.ProtocolParameters,
) *LedgerView {
	lv.committeeEpoch = epoch
	lv.committeePParams = pparams
	lv.committeeStatePinned = true
	return lv
}

func (lv *LedgerView) SkipPhase2Validation() bool {
	return lv.skipPhase2Validation
}

// MinPoolMargin forwards the CIP-23 minimum pool margin from the underlying
// ledger state so that a *LedgerView (the value passed to ValidateTx*) satisfies
// the eras.MinPoolMarginProvider interface. Without this, the Dijkstra
// pool-margin-floor certificate rule would never see the configured floor.
func (lv *LedgerView) MinPoolMargin() *big.Rat {
	return lv.ls.MinPoolMargin()
}

// var _ eras.MinPoolMarginProvider = (*LedgerView)(nil) makes any future drift
// in the MinPoolMarginProvider method signature a compile error instead of a
// silent runtime no-op for the CIP-23 pool-margin-floor certificate rule.
var _ eras.MinPoolMarginProvider = (*LedgerView)(nil)

// Compile-time guard for the tag-aware committee validation capability.
var _ eras.CommitteeCredentialState = (*LedgerView)(nil)

// Keep the optional Conway governance capability wired to the concrete view
// used for transaction validation. Without this interface, gouroboros falls
// back to weaker existence-only proposal ancestry checks.
var _ lcommon.GovPurposeRootsState = (*LedgerView)(nil)

// The Conway reward-withdrawal rule discovers this capability with a runtime
// type assertion on the *LedgerView passed to ValidateTx*. On protocol versions
// 10 and 11 a failed assertion rejects every affected withdrawal
// (DRepDelegationStateUnavailableError), so signature drift here is a
// consensus-level break. Make it a compile error instead.
var _ lcommon.DRepDelegationState = (*LedgerView)(nil)

// These methods intentionally compile against the released Gouroboros API as
// ordinary methods. Once the credential-aware committee capability is
// released, this assertion can be enabled without changing Dingo's provider.

// Byron redeem and bootstrap witness verification asserts this capability at
// runtime and fails the transaction when it is absent, so drift in
// ByronProtocolMagic would reject every Byron transaction carrying those
// witnesses rather than fail to build.
var _ eras.ByronProtocolMagicProvider = (*LedgerView)(nil)

func (lv *LedgerView) NetworkId() uint {
	genesis := lv.ls.config.CardanoNodeConfig.ShelleyGenesis()
	if genesis == nil {
		// no config, definitely not mainnet
		return 0
	}
	networkName := genesis.NetworkId
	if networkName == "Mainnet" {
		return 1
	}
	return 0
}

func (lv *LedgerView) ByronProtocolMagic() (uint32, error) {
	return lv.ls.ByronProtocolMagic()
}

func (lv *LedgerView) UtxoById(
	utxoId lcommon.TransactionInput,
) (lcommon.Utxo, error) {
	key := fmt.Sprintf("%s:%d", utxoId.Id().String(), utxoId.Index())
	// Check consumed UTxOs first (spent by pending mempool TX)
	if lv.consumedUtxos != nil {
		if _, ok := lv.consumedUtxos[key]; ok {
			return lcommon.Utxo{}, fmt.Errorf(
				"utxo %s: %w",
				key,
				ErrUtxoAlreadyConsumed,
			)
		}
	}
	// Check intra-block/overlay UTxOs (outputs from earlier txs)
	if lv.intraBlockUtxos != nil {
		if utxo, ok := lv.intraBlockUtxos[key]; ok {
			return utxo, nil
		}
	}
	utxo, err := lv.ls.db.UtxoByRef(
		utxoId.Id().Bytes(),
		utxoId.Index(),
		lv.txn,
	)
	if err != nil {
		return lcommon.Utxo{}, err
	}
	tmpOutput, err := utxo.Decode()
	if err != nil {
		return lcommon.Utxo{}, err
	}
	if tmpOutput == nil {
		return lcommon.Utxo{}, fmt.Errorf(
			"decoded output is nil for utxo %s#%d: %w",
			utxoId.Id().String(),
			utxoId.Index(),
			ErrNilDecodedOutput,
		)
	}
	return lcommon.Utxo{
		Id:     utxoId,
		Output: tmpOutput,
	}, nil
}

func (lv *LedgerView) PoolRegistration(
	pkh lcommon.PoolKeyHash,
) ([]lcommon.PoolRegistrationCertificate, error) {
	return lv.ls.db.GetPoolRegistrations(pkh, lv.txn)
}

func (lv *LedgerView) StakeRegistration(
	stakingKey []byte,
) ([]lcommon.StakeRegistrationCertificate, error) {
	// stakingKey = lcommon.NewBlake2b224(stakingKey)
	return lv.ls.db.GetStakeRegistrationsByCredential(0, stakingKey, lv.txn)
}

// StakeRegistrationByCredential returns stake registration certificates for the
// full stake credential identity, preserving key/script credential separation.
func (lv *LedgerView) StakeRegistrationByCredential(
	cred lcommon.Credential,
) ([]lcommon.StakeRegistrationCertificate, error) {
	credentialTag, err := models.CredentialTagFromUint(cred.CredType)
	if err != nil {
		return nil, err
	}
	return lv.ls.db.GetStakeRegistrationsByCredential(
		credentialTag,
		cred.Credential[:],
		lv.txn,
	)
}

// IsStakeCredentialRegistered checks if a stake credential is currently registered
func (lv *LedgerView) IsStakeCredentialRegistered(
	cred lcommon.Credential,
) bool {
	credentialTag, err := models.CredentialTagFromUint(cred.CredType)
	if err != nil {
		return false
	}
	account, err := lv.ls.db.GetAccountByCredential(
		credentialTag,
		cred.Credential[:],
		false,
		lv.txn,
	)
	if err != nil {
		if !errors.Is(err, models.ErrAccountNotFound) {
			lv.ls.config.Logger.Error(
				"failed to get account for stake credential",
				"component", "ledger",
				"credential", cred.Hash().String(),
				"error", err,
			)
		}
		return false
	}
	return account != nil && account.Active
}

// StakeCredentialDeposit returns the registration deposit currently held for
// a registered stake credential. The account lookup preserves the live
// registration semantics used by IsStakeCredentialRegistered, while the
// registration history carries the deposit actually paid rather than the
// current protocol-parameter value.
func (lv *LedgerView) StakeCredentialDeposit(
	cred lcommon.Credential,
) (*uint64, error) {
	credentialTag, err := models.CredentialTagFromUint(cred.CredType)
	if err != nil {
		return nil, err
	}
	account, err := lv.ls.db.GetAccountByCredential(
		credentialTag,
		cred.Credential[:],
		false,
		lv.txn,
	)
	if errors.Is(err, models.ErrAccountNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if account == nil || !account.Active {
		return nil, nil
	}
	history, err := lv.ls.db.GetAccountRegistrationHistoryByCredential(
		credentialTag,
		cred.Credential[:],
		1,
		0,
		"desc",
		lv.txn,
	)
	if err != nil {
		return nil, err
	}
	importRegistration, err := lv.ls.db.GetAccountImportRegistrationByCredential(
		credentialTag,
		cred.Credential[:],
		lv.txn,
	)
	if err != nil {
		return nil, err
	}
	// The import baseline represents state after the snapshot point. Treat it
	// as the latest registration when no certificate history is newer, without
	// exposing a fabricated transaction through the public history API.
	if importRegistration != nil &&
		(len(history) == 0 || importRegistration.AddedSlot >= history[0].AddedSlot) {
		return importRegistration.Deposit, nil
	}
	if len(history) == 0 || history[0].Action != "registered" {
		return nil, nil
	}
	deposit := history[0].Deposit
	return &deposit, nil
}

// It returns the most recent active pool registration certificate
// and the epoch of any pending retirement for the given pool key hash.
func (lv *LedgerView) PoolCurrentState(
	pkh lcommon.PoolKeyHash,
) (*lcommon.PoolRegistrationCertificate, *uint64, error) {
	pool, err := lv.ls.db.GetPool(pkh, false, lv.txn)
	if err != nil {
		if errors.Is(err, models.ErrPoolNotFound) {
			pool = &models.Pool{}
		} else {
			return nil, nil, err
		}
	}
	var currentReg *lcommon.PoolRegistrationCertificate
	var hasReg bool
	var regLatestSlot uint64
	var regLatestCertID uint
	if len(pool.Registration) > 0 {
		var latestIdx int
		for i, reg := range pool.Registration {
			// Use CertificateID for deterministic disambiguation when slots are equal
			if reg.AddedSlot > regLatestSlot ||
				(reg.AddedSlot == regLatestSlot && reg.CertificateID > regLatestCertID) {
				regLatestSlot = reg.AddedSlot
				regLatestCertID = reg.CertificateID
				latestIdx = i
			}
		}
		hasReg = true
		reg := pool.Registration[latestIdx]
		tmp := lcommon.PoolRegistrationCertificate{
			CertType: uint(lcommon.CertificateTypePoolRegistration),
			Operator: lcommon.PoolKeyHash(
				lcommon.NewBlake2b224(pool.PoolKeyHash),
			),
			VrfKeyHash: lcommon.VrfKeyHash(
				lcommon.NewBlake2b256(pool.VrfKeyHash),
			),
			Pledge: uint64(pool.Pledge),
			Cost:   uint64(pool.Cost),
		}
		if pool.Margin != nil {
			tmp.Margin = cbor.Rat{Rat: pool.Margin.Rat}
		}
		tmp.RewardAccount = lcommon.AddrKeyHash(
			lcommon.NewBlake2b224(pool.RewardAccount),
		)
		for _, owner := range reg.Owners {
			tmp.PoolOwners = append(
				tmp.PoolOwners,
				lcommon.AddrKeyHash(lcommon.NewBlake2b224(owner.KeyHash)),
			)
		}
		for _, relay := range reg.Relays {
			r := lcommon.PoolRelay{}
			if relay.Port != 0 {
				port := uint32(relay.Port) // #nosec G115
				r.Port = &port
			}
			if relay.Hostname != "" {
				r.Type = lcommon.PoolRelayTypeSingleHostName
				hostname := relay.Hostname
				r.Hostname = &hostname
			} else if relay.Ipv4 != nil || relay.Ipv6 != nil {
				r.Type = lcommon.PoolRelayTypeSingleHostAddress
				r.Ipv4 = relay.Ipv4
				r.Ipv6 = relay.Ipv6
			}
			tmp.Relays = append(tmp.Relays, r)
		}
		if reg.MetadataUrl != "" {
			tmp.PoolMetadata = &lcommon.PoolMetadata{
				Url: reg.MetadataUrl,
				Hash: lcommon.PoolMetadataHash(
					lcommon.NewBlake2b256(reg.MetadataHash),
				),
			}
		}
		currentReg = &tmp
	}
	// pendingEpoch reports the target epoch of the pool's latest retirement
	// certificate -- the one most recently added by (AddedSlot,
	// CertificateID), not the maximum epoch value across every retirement
	// row: a later retirement certificate replaces the prior schedule even
	// when it moves the target epoch earlier (retire@10 then retire@5 must
	// report 5, not 10). A later pool registration cancels a pending
	// retirement -- mirroring poolIsActive's ordering rule
	// (internal/test/conformance/state_provider.go) -- so pendingEpoch is
	// nil whenever the latest registration was added after the latest
	// retirement.
	var pendingEpoch *uint64
	if len(pool.Retirement) > 0 {
		var retLatestIdx int
		var retLatestSlot uint64
		var retLatestCertID uint
		var hasRet bool
		for i, r := range pool.Retirement {
			if !hasRet || r.AddedSlot > retLatestSlot ||
				(r.AddedSlot == retLatestSlot && r.CertificateID > retLatestCertID) {
				retLatestSlot = r.AddedSlot
				retLatestCertID = r.CertificateID
				retLatestIdx = i
				hasRet = true
			}
		}
		registrationSupersedesRetirement := hasReg &&
			(regLatestSlot > retLatestSlot ||
				(regLatestSlot == retLatestSlot && regLatestCertID > retLatestCertID))
		if hasRet && !registrationSupersedesRetirement {
			epoch := pool.Retirement[retLatestIdx].Epoch
			pendingEpoch = &epoch
		}
	}
	return currentReg, pendingEpoch, nil
}

// IsPoolRegistered checks if a pool is currently registered
func (lv *LedgerView) IsPoolRegistered(pkh lcommon.PoolKeyHash) bool {
	reg, _, err := lv.PoolCurrentState(pkh)
	if err != nil {
		return false
	}
	return reg != nil
}

// IsVrfKeyInUse checks if a VRF key hash is registered by another pool.
// Returns (inUse, owningPoolId, error).
func (lv *LedgerView) IsVrfKeyInUse(
	vrfKeyHash lcommon.Blake2b256,
) (bool, lcommon.PoolKeyHash, error) {
	pool, err := lv.ls.db.GetPoolByVrfKeyHash(
		vrfKeyHash.Bytes(),
		lv.txn,
	)
	if err != nil {
		return false, lcommon.PoolKeyHash{}, err
	}
	if pool == nil {
		return false, lcommon.PoolKeyHash{}, nil
	}
	return true, lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(pool.PoolKeyHash),
	), nil
}

// SlotToTime returns the current time for a given slot based on known epochs
func (lv *LedgerView) SlotToTime(slot uint64) (time.Time, error) {
	return lv.ls.SlotToTime(slot)
}

// TimeToSlot returns the slot number for a given time based on known epochs
func (lv *LedgerView) TimeToSlot(t time.Time) (uint64, error) {
	return lv.ls.TimeToSlot(t)
}

// CalculateRewards calculates rewards for the given stake keys.
// TODO: implement reward calculation. Requires reward formulas from the
// Cardano Shelley formal specification and integration with stake snapshots.
func (lv *LedgerView) CalculateRewards(
	adaPots lcommon.AdaPots,
	rewardSnapshot lcommon.RewardSnapshot,
	rewardParams lcommon.RewardParameters,
) (*lcommon.RewardCalculationResult, error) {
	return nil, ErrNotImplemented
}

// GetAdaPots returns the current Ada pots.
// TODO: implement Ada pots retrieval. Requires tracking of treasury, reserves,
// fees, and rewards pots which are not yet stored in the database.
func (lv *LedgerView) GetAdaPots() lcommon.AdaPots {
	panic(ErrNotImplemented)
}

// GetAdaPotsWithError returns the current Ada pots.
func (lv *LedgerView) GetAdaPotsWithError() (lcommon.AdaPots, error) {
	return lcommon.AdaPots{}, ErrNotImplemented
}

// GetRewardSnapshot returns the current reward snapshot.
// TODO: implement reward snapshot retrieval. Requires per-stake-credential
// reward tracking which is not yet stored in the database.
func (lv *LedgerView) GetRewardSnapshot(
	epoch uint64,
) (lcommon.RewardSnapshot, error) {
	return lcommon.RewardSnapshot{}, ErrNotImplemented
}

// UpdateAdaPots updates the Ada pots.
// TODO: implement Ada pots update. Requires Ada pots storage in the database.
func (lv *LedgerView) UpdateAdaPots(adaPots lcommon.AdaPots) error {
	return ErrNotImplemented
}

// IsRewardAccountRegistered checks if a reward account is registered
func (lv *LedgerView) IsRewardAccountRegistered(
	cred lcommon.Credential,
) bool {
	credentialTag, err := models.CredentialTagFromUint(cred.CredType)
	if err != nil {
		return false
	}
	account, err := lv.ls.db.GetAccountByCredential(
		credentialTag,
		cred.Credential[:],
		false,
		lv.txn,
	)
	if err != nil {
		if !errors.Is(err, models.ErrAccountNotFound) {
			lv.ls.config.Logger.Error(
				"failed to get account for reward account",
				"component", "ledger",
				"credential", cred.Hash().String(),
				"error", err,
			)
		}
		return false
	}
	return account != nil && account.Active
}

// RewardAccountBalance returns the current reward balance for a stake credential.
// Missing and inactive reward accounts are represented by a nil balance, as
// required by the gouroboros reward-state contract. A registered account with
// a zero balance returns a non-nil pointer to zero.
func (lv *LedgerView) RewardAccountBalance(
	cred lcommon.Credential,
) (*uint64, error) {
	credentialTag, err := models.CredentialTagFromUint(cred.CredType)
	if err != nil {
		return nil, err
	}
	account, err := lv.ls.db.GetAccountByCredential(
		credentialTag,
		cred.Credential[:],
		false,
		lv.txn,
	)
	if errors.Is(err, models.ErrAccountNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, nil
	}
	balance := uint64(account.Reward)
	return &balance, nil
}

// CostModels returns which Plutus language versions have cost
// models defined in the current protocol parameters.
//
// NOTE: lcommon.CostModel is currently struct{} in gouroboros
// (a placeholder type). The returned map values carry no cost
// parameter data -- callers use map membership to check version
// availability. When gouroboros extends CostModel with real
// fields, this function should be updated to populate them
// from the raw []int64 cost parameters.
//
// Map keys use PlutusLanguage encoding: PlutusV1=1, PlutusV2=2,
// PlutusV3=3, corresponding to cost model map keys 0, 1, 2.
func (lv *LedgerView) CostModels() map[lcommon.PlutusLanguage]lcommon.CostModel {
	pp := lv.ls.GetCurrentPParams()
	if pp == nil {
		return map[lcommon.PlutusLanguage]lcommon.CostModel{}
	}
	return extractCostModelsFromPParams(pp)
}

// costModelsProvider is an optional interface implemented by
// era-specific protocol parameter types that expose raw cost
// model data as map[uint][]int64.
type costModelsProvider interface {
	GetCostModels() map[uint][]int64
}

// extractCostModelsFromPParams returns which Plutus language
// versions are present in the protocol parameters.
//
// It first tries the costModelsProvider interface, then falls
// back to type-asserting concrete era types. The raw []int64
// values are not stored in the returned CostModel because the
// upstream type is currently struct{}. When gouroboros adds
// fields to CostModel, populate them here from rawModels.
func extractCostModelsFromPParams(
	pp lcommon.ProtocolParameters,
) map[lcommon.PlutusLanguage]lcommon.CostModel {
	if pp == nil {
		return map[lcommon.PlutusLanguage]lcommon.CostModel{}
	}
	rawModels := extractRawCostModels(pp)
	if rawModels == nil {
		return map[lcommon.PlutusLanguage]lcommon.CostModel{}
	}
	result := make(
		map[lcommon.PlutusLanguage]lcommon.CostModel,
		len(rawModels),
	)
	for version := range rawModels {
		if version > 2 {
			continue
		}
		//nolint:gosec
		plutusLang := lcommon.PlutusLanguage(version + 1)
		// TODO: populate CostModel with rawModels[version]
		// when gouroboros extends the type beyond struct{}.
		result[plutusLang] = lcommon.CostModel{}
	}
	return result
}

// extractRawCostModels retrieves the raw cost model data from
// protocol parameters. It tries the costModelsProvider interface
// first, then falls back to type assertions for known era types.
func extractRawCostModels(
	pp lcommon.ProtocolParameters,
) map[uint][]int64 {
	if pp == nil {
		return nil
	}
	// Prefer the interface if the type implements it.
	if provider, ok := pp.(costModelsProvider); ok {
		return provider.GetCostModels()
	}
	// Fall back to concrete era type assertions.
	switch p := pp.(type) {
	case *alonzo.AlonzoProtocolParameters:
		return p.CostModels
	case *babbage.BabbageProtocolParameters:
		return p.CostModels
	case *conway.ConwayProtocolParameters:
		return p.CostModels
	default:
		return nil
	}
}

// CommitteeStateAvailable reports that this SQL-backed view is authoritative,
// including when no committee members are seated.
func (lv *LedgerView) CommitteeStateAvailable() (bool, error) {
	return lv != nil && lv.ls != nil && lv.ls.db != nil, nil
}

// CommitteeMember preserves the legacy hash-only contract. It returns nil
// when key and script credentials with the same hash are both members rather
// than choosing one by iteration order.
func (lv *LedgerView) CommitteeMember(
	coldKey lcommon.Blake2b224,
) (*lcommon.CommitteeMember, error) {
	keyCredential := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: coldKey,
	}
	keyMember, err := lv.legacyCommitteeCredentialMember(keyCredential)
	if err != nil {
		return nil, err
	}
	if keyMember == nil {
		keyMember, err = lv.proposedCommitteeMember(keyCredential)
		if err != nil {
			return nil, err
		}
	}
	scriptCredential := lcommon.Credential{
		CredType:   lcommon.CredentialTypeScriptHash,
		Credential: coldKey,
	}
	scriptMember, err := lv.legacyCommitteeCredentialMember(scriptCredential)
	if err != nil {
		return nil, err
	}
	if scriptMember == nil {
		scriptMember, err = lv.proposedCommitteeMember(scriptCredential)
		if err != nil {
			return nil, err
		}
	}
	if keyMember != nil && scriptMember != nil {
		return nil, nil
	}
	if keyMember != nil {
		return keyMember, nil
	}
	return scriptMember, nil
}

// legacyCommitteeCredentialMember returns the first seated term for a tagged
// credential. The hash-only CommitteeMember API must preserve this behavior;
// successor resolution belongs only to CommitteeCredentialMember.
func (lv *LedgerView) legacyCommitteeCredentialMember(
	coldCredential lcommon.Credential,
) (*lcommon.CommitteeMember, error) {
	coldTag, err := models.CredentialTagFromUint(coldCredential.CredType)
	if err != nil {
		return nil, fmt.Errorf("invalid committee cold credential: %w", err)
	}
	dbMembers, err := lv.ls.db.GetCommitteeMembers(lv.txn)
	if err != nil {
		return nil, fmt.Errorf("get committee members: %w", err)
	}
	for _, found := range dbMembers {
		if found.ColdCredentialTag != coldTag ||
			!bytes.Equal(found.ColdCredHash, coldCredential.Credential[:]) {
			continue
		}
		member := &lcommon.CommitteeMember{
			ColdKey:     coldCredential.Credential,
			ExpiryEpoch: found.ExpiresEpoch,
		}
		if err := lv.populateCommitteeMemberStatus(
			coldCredential, found.TermStartSlot, member, false,
		); err != nil {
			return nil, err
		}
		return member, nil
	}
	return nil, nil
}

// CommitteeCredentialMember resolves a seated or pending proposed committee
// member by full tagged cold credential identity.
func (lv *LedgerView) CommitteeCredentialMember(
	coldCredential lcommon.Credential,
) (*lcommon.CommitteeMember, error) {
	coldTag, err := models.CredentialTagFromUint(coldCredential.CredType)
	if err != nil {
		return nil, fmt.Errorf("invalid committee cold credential: %w", err)
	}
	dbMembers, err := lv.ls.db.GetCommitteeMembers(lv.txn)
	if err != nil {
		return nil, fmt.Errorf("get committee members: %w", err)
	}
	var found *models.CommitteeMember
	for _, member := range dbMembers {
		if member.ColdCredentialTag == coldTag &&
			bytes.Equal(member.ColdCredHash, coldCredential.Credential[:]) {
			if found == nil || member.TermStartSlot > found.TermStartSlot ||
				(member.TermStartSlot == found.TermStartSlot && member.AddedSlot > found.AddedSlot) ||
				(member.TermStartSlot == found.TermStartSlot && member.AddedSlot == found.AddedSlot && member.ID > found.ID) {
				found = member
			}
		}
	}
	if found == nil {
		return lv.proposedCommitteeMember(coldCredential)
	}
	member := &lcommon.CommitteeMember{
		ColdKey:     coldCredential.Credential,
		ExpiryEpoch: found.ExpiresEpoch,
	}
	if err := lv.populateCommitteeMemberStatus(
		coldCredential,
		found.TermStartSlot,
		member,
		false,
	); err != nil {
		return nil, err
	}
	// A re-election may replace a resigned term before enactment. The old
	// historical row remains authoritative for its term, but must not mask the
	// pending successor when validation asks for this cold credential.
	if member.Resigned {
		proposed, err := lv.proposedCommitteeMember(coldCredential)
		if err != nil {
			return nil, err
		}
		if proposed != nil {
			return proposed, nil
		}
		return member, nil
	}
	return member, nil
}

// populateCommitteeMemberStatus fills in resignation and hot-key authorization
// for a term starting at termStartSlot.
//
// A pending term is one a proposal has not yet enacted. Its termStartSlot is
// the proposal's own added slot, so a resignation recorded during the member's
// previous term sits at or after it and would otherwise be read as a
// resignation from a term that has not begun. A resignation belongs to the term
// it occurred in, so a pending term is never resigned and a re-elected member
// can still authorize a hot credential.
func (lv *LedgerView) populateCommitteeMemberStatus(
	coldCredential lcommon.Credential,
	termStartSlot uint64,
	member *lcommon.CommitteeMember,
	pending bool,
) error {
	coldTag, err := models.CredentialTagFromUint(coldCredential.CredType)
	if err != nil {
		return fmt.Errorf("invalid committee cold credential: %w", err)
	}
	if !pending {
		resigned, err := lv.ls.db.IsCommitteeMemberResigned(
			coldTag,
			coldCredential.Credential[:],
			termStartSlot,
			lv.txn,
		)
		if err != nil {
			return fmt.Errorf(
				"check committee member resignation: %w",
				err,
			)
		}
		member.Resigned = resigned
		if resigned {
			return nil
		}
	}
	authorization, err := lv.ls.db.GetCommitteeMember(
		coldTag,
		coldCredential.Credential[:],
		termStartSlot,
		lv.txn,
	)
	if err != nil && !errors.Is(err, models.ErrCommitteeMemberNotFound) {
		return fmt.Errorf("get committee hot credential: %w", err)
	}
	if authorization != nil {
		hotKey := lcommon.NewBlake2b224(authorization.HotCredential)
		member.HotKey = &hotKey
	}
	return nil
}

func (lv *LedgerView) proposedCommitteeMember(
	coldCredential lcommon.Credential,
) (*lcommon.CommitteeMember, error) {
	epoch, pparams := lv.committeeSnapshot()
	proposals, err := lv.ls.db.GetActiveGovernanceProposals(
		epoch,
		lv.txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get active governance proposals: %w", err)
	}
	root, err := lv.ls.db.GetLastEnactedGovernanceProposal(
		[]uint8{uint8(lcommon.GovActionTypeUpdateCommittee)}, lv.txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get committee proposal root: %w", err)
	}
	member, termStart, err := governance.ResolveCommitteeProposal(
		proposals, root, coldCredential, pparams,
	)
	if err != nil {
		return nil, err
	}
	if member != nil {
		if err := lv.populateCommitteeMemberStatus(
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

func (lv *LedgerView) committeeSnapshot() (
	uint64,
	lcommon.ProtocolParameters,
) {
	if lv.committeeStatePinned {
		return lv.committeeEpoch, lv.committeePParams
	}
	state := lv.ls.loadConsensusSnapshot()
	return state.currentEpoch.EpochId, state.currentPParams
}

// CommitteeHotCredentialMember resolves an active committee authorization by
// exact tagged hot credential identity.
func (lv *LedgerView) CommitteeHotCredentialMember(
	hotCredential lcommon.Credential,
) (*lcommon.CommitteeMember, error) {
	hotTag, err := models.CredentialTagFromUint(hotCredential.CredType)
	if err != nil {
		return nil, fmt.Errorf("invalid committee hot credential: %w", err)
	}
	currentEpoch, _ := lv.committeeSnapshot()
	authorizations, err := lv.ls.db.GetActiveCommitteeMembers(lv.txn)
	if err != nil {
		return nil, fmt.Errorf("get active committee hot credentials: %w", err)
	}
	for _, authorization := range authorizations {
		if authorization.HotCredentialTag != hotTag ||
			!bytes.Equal(authorization.HotCredential, hotCredential.Credential[:]) {
			continue
		}
		member, err := lv.CommitteeCredentialMember(lcommon.Credential{
			CredType:   uint(authorization.ColdCredentialTag),
			Credential: lcommon.NewBlake2b224(authorization.ColdCredential),
		})
		if err != nil {
			return nil, err
		}
		if member == nil || member.Resigned ||
			member.ExpiryEpoch < currentEpoch {
			continue
		}
		return member, nil
	}
	return nil, nil
}

// CommitteeMembers returns all seated committee members.
//
// Resolution runs off the single GetCommitteeMembers load rather than calling
// CommitteeCredentialMember per seat, which would reload the whole set for
// every member. Resignations are fetched for the whole set in one query.
func (lv *LedgerView) CommitteeMembers() ([]lcommon.CommitteeMember, error) {
	dbMembers, err := lv.ls.db.GetCommitteeMembers(lv.txn)
	if err != nil {
		return nil, fmt.Errorf("get committee members: %w", err)
	}
	// A credential is (tag, hash). Several rows for one credential are its
	// successive terms, and only the latest is seated. Counting hashes alone
	// would drop a re-elected member as if it were an alias.
	type credentialKey struct {
		tag  uint8
		hash string
	}
	latest := make(map[credentialKey]*models.CommitteeMember, len(dbMembers))
	order := make([]credentialKey, 0, len(dbMembers))
	tagsByHash := make(map[string]map[uint8]struct{}, len(dbMembers))
	for _, m := range dbMembers {
		key := credentialKey{tag: m.ColdCredentialTag, hash: string(m.ColdCredHash)}
		if tagsByHash[key.hash] == nil {
			tagsByHash[key.hash] = make(map[uint8]struct{}, 1)
		}
		tagsByHash[key.hash][key.tag] = struct{}{}
		found, ok := latest[key]
		if !ok {
			latest[key] = m
			order = append(order, key)
			continue
		}
		if m.TermStartSlot > found.TermStartSlot ||
			(m.TermStartSlot == found.TermStartSlot && m.AddedSlot > found.AddedSlot) ||
			(m.TermStartSlot == found.TermStartSlot && m.AddedSlot == found.AddedSlot && m.ID > found.ID) {
			latest[key] = m
		}
	}

	credentials := make([]models.CommitteeCredential, 0, len(order))
	for _, key := range order {
		found := latest[key]
		credentials = append(credentials, models.CommitteeCredential{
			CredentialTag: found.ColdCredentialTag,
			Credential:    found.ColdCredHash,
			TermStartSlot: found.TermStartSlot,
		})
	}
	resigned, err := lv.ls.db.GetResignedCommitteeMembers(credentials, lv.txn)
	if err != nil {
		return nil, fmt.Errorf("get resigned committee members: %w", err)
	}

	members := make([]lcommon.CommitteeMember, 0, len(order))
	for _, key := range order {
		// The legacy list shape cannot carry a credential tag, so a hash
		// seated under both tags stays ambiguous and is omitted rather than
		// aliasing a key member onto a script member.
		if len(tagsByHash[key.hash]) != 1 {
			continue
		}
		found := latest[key]
		coldCredential := lcommon.Credential{
			CredType:   uint(found.ColdCredentialTag),
			Credential: lcommon.NewBlake2b224(found.ColdCredHash),
		}
		member := &lcommon.CommitteeMember{
			ColdKey:     coldCredential.Credential,
			ExpiryEpoch: found.ExpiresEpoch,
		}
		credentialKey := models.CommitteeCredential{
			CredentialTag: found.ColdCredentialTag,
			Credential:    found.ColdCredHash,
		}.Key()
		member.Resigned = resigned[credentialKey]
		if member.Resigned {
			// A re-election may replace a resigned term before enactment.
			proposed, err := lv.proposedCommitteeMember(coldCredential)
			if err != nil {
				return nil, err
			}
			if proposed != nil {
				members = append(members, *proposed)
				continue
			}
			members = append(members, *member)
			continue
		}
		authorization, err := lv.ls.db.GetCommitteeMember(
			found.ColdCredentialTag,
			found.ColdCredHash,
			found.TermStartSlot,
			lv.txn,
		)
		if err != nil && !errors.Is(err, models.ErrCommitteeMemberNotFound) {
			return nil, fmt.Errorf("get committee hot credential: %w", err)
		}
		if authorization != nil {
			hotKey := lcommon.NewBlake2b224(authorization.HotCredential)
			member.HotKey = &hotKey
		}
		members = append(members, *member)
	}
	return members, nil
}

// DRepRegistration returns a DRep registration by credential.
// Returns nil if the credential is not registered as an active DRep.
func (lv *LedgerView) DRepRegistration(
	credential lcommon.Blake2b224,
) (*lcommon.DRepRegistration, error) {
	drep, err := lv.ls.db.GetDrep(credential[:], false, lv.txn)
	if err != nil {
		if errors.Is(err, models.ErrDrepNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get drep: %w", err)
	}
	reg := &lcommon.DRepRegistration{
		Credential: credential,
	}
	if drep.AnchorURL != "" || len(drep.AnchorHash) > 0 {
		if len(drep.AnchorHash) != 32 {
			return nil, fmt.Errorf(
				"invalid DRep anchor hash length: expected 32, got %d",
				len(drep.AnchorHash),
			)
		}
		var dataHash [32]byte
		copy(dataHash[:], drep.AnchorHash)
		reg.Anchor = &lcommon.GovAnchor{
			Url:      drep.AnchorURL,
			DataHash: dataHash,
		}
	}
	return reg, nil
}

// DRepRegistrations returns all active DRep registrations.
func (lv *LedgerView) DRepRegistrations() ([]lcommon.DRepRegistration, error) {
	dreps, err := lv.ls.db.GetActiveDreps(lv.txn)
	if err != nil {
		return nil, fmt.Errorf("get active dreps: %w", err)
	}
	registrations := make([]lcommon.DRepRegistration, 0, len(dreps))
	for _, drep := range dreps {
		reg := lcommon.DRepRegistration{
			Credential: lcommon.NewBlake2b224(drep.Credential),
		}
		if drep.AnchorURL != "" || len(drep.AnchorHash) > 0 {
			if len(drep.AnchorHash) != 32 {
				return nil, fmt.Errorf(
					"invalid DRep anchor hash length: expected 32, got %d",
					len(drep.AnchorHash),
				)
			}
			var dataHash [32]byte
			copy(dataHash[:], drep.AnchorHash)
			reg.Anchor = &lcommon.GovAnchor{
				Url:      drep.AnchorURL,
				DataHash: dataHash,
			}
		}
		registrations = append(registrations, reg)
	}
	return registrations, nil
}

// DRepDelegation returns the DRep that the given stake credential is
// vote-delegated to, or nil when the credential is not registered or is not
// delegated to any DRep. It satisfies gouroboros' common.DRepDelegationState,
// which the ledger rules use to validate reward withdrawals on protocol
// versions 10 and 11 (a withdrawal from a credential not delegated to a DRep
// is rejected).
func (lv *LedgerView) DRepDelegation(
	cred lcommon.Credential,
) (*lcommon.Drep, error) {
	credentialTag, err := models.CredentialTagFromUint(cred.CredType)
	if err != nil {
		return nil, err
	}
	account, err := lv.ls.db.GetAccountByCredential(
		credentialTag,
		cred.Credential[:],
		false,
		lv.txn,
	)
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
	return &lcommon.Drep{
		Type:       int(account.DrepType),
		Credential: account.Drep,
	}, nil
}

// Constitution returns the enacted constitution: its anchor URL, anchor
// hash, and optional guardrails policy hash.
//
// Constitution state that is missing or malformed fails closed with
// governance.ErrConstitutionUnavailable; a constitution store that cannot
// be read at all returns the wrapped store error. Neither reports an
// empty-but-valid constitution, which gouroboros' guardrails rule would
// read as "no guardrails script required".
func (lv *LedgerView) Constitution() (*lcommon.Constitution, error) {
	constitution, err := lv.ls.db.GetConstitution(lv.txn)
	if err != nil {
		return nil, fmt.Errorf("get constitution: %w", err)
	}
	return governance.ConstitutionFromModel(constitution)
}

// TreasuryValue returns the current treasury value.
// TODO: implement treasury value retrieval. Requires Ada pots tracking
// which is not yet stored in the database. The treasury value is part of
// the Ada pots (reserves, treasury, fees, rewards).
func (lv *LedgerView) TreasuryValue() (uint64, error) {
	return 0, ErrNotImplemented
}

// GovActionById returns a governance action by its ID.
// Returns nil if the governance action does not exist.
func (lv *LedgerView) GovActionById(
	id lcommon.GovActionId,
) (*lcommon.GovActionState, error) {
	txn := lv.txn
	if txn == nil {
		txn = lv.ls.db.MetadataTxn(false)
		defer txn.Release()
	}
	proposal, err := lv.ls.db.GetGovernanceProposal(
		id.TransactionId[:],
		id.GovActionIdx,
		txn,
	)
	if err != nil {
		if errors.Is(err, models.ErrGovernanceProposalNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get governance proposal: %w", err)
	}
	// Expired proposals are no longer members of their purpose tree.
	if proposal.ExpiredEpoch != nil {
		return nil, nil
	}
	// The current enacted root must remain resolvable because content-aware
	// rules compare a new action with its predecessor. Older enacted actions
	// must not be returned: ancestry validation would otherwise mistake them
	// for pending members of the purpose tree.
	if proposal.EnactedEpoch != nil {
		isRoot, err := lv.governanceProposalIsPurposeRoot(proposal, txn)
		if err != nil {
			return nil, err
		}
		if !isRoot {
			return nil, nil
		}
	}
	action, err := governance.DecodeGovActionForPParams(
		proposal.GovActionCbor,
		proposal.ActionType,
		lv.ls.GetCurrentPParams(),
	)
	if err != nil {
		return nil, fmt.Errorf("decode governance proposal action: %w", err)
	}
	var expirySlot uint64
	if proposal.EnactedEpoch == nil {
		expirySlot, err = lv.governanceProposalExpirySlot(proposal, txn)
		if err != nil {
			return nil, err
		}
	}
	return &lcommon.GovActionState{
		ActionId:   id,
		ActionType: lcommon.GovActionType(proposal.ActionType),
		ExpirySlot: expirySlot,
		Action:     action,
	}, nil
}

func (lv *LedgerView) governanceProposalIsPurposeRoot(
	proposal *models.GovernanceProposal,
	txn *database.Txn,
) (bool, error) {
	actionTypes := governancePurposeActionTypes(proposal.ActionType)
	if len(actionTypes) == 0 {
		return false, nil
	}
	root, err := lv.ls.db.GetLastEnactedGovernanceProposal(actionTypes, txn)
	if err != nil {
		return false, fmt.Errorf(
			"get governance proposal purpose root: %w",
			err,
		)
	}
	return root != nil &&
		root.ActionIndex == proposal.ActionIndex &&
		bytes.Equal(root.TxHash, proposal.TxHash), nil
}

func governancePurposeActionTypes(actionType uint8) []uint8 {
	switch lcommon.GovActionType(actionType) {
	case lcommon.GovActionTypeParameterChange:
		return []uint8{uint8(lcommon.GovActionTypeParameterChange)}
	case lcommon.GovActionTypeHardForkInitiation:
		return []uint8{uint8(lcommon.GovActionTypeHardForkInitiation)}
	case lcommon.GovActionTypeNoConfidence,
		lcommon.GovActionTypeUpdateCommittee:
		return []uint8{
			uint8(lcommon.GovActionTypeNoConfidence),
			uint8(lcommon.GovActionTypeUpdateCommittee),
		}
	case lcommon.GovActionTypeNewConstitution:
		return []uint8{uint8(lcommon.GovActionTypeNewConstitution)}
	case lcommon.GovActionTypeTreasuryWithdrawal,
		lcommon.GovActionTypeInfo:
		return nil
	default:
		return nil
	}
}

// GovPurposeRoots returns the latest enacted action for each CIP-1694
// governance purpose. A non-nil result with nil fields means Dingo has
// authoritatively determined that the corresponding purpose has no root.
func (lv *LedgerView) GovPurposeRoots() (*lcommon.GovPurposeRoots, error) {
	txn := lv.txn
	if txn == nil {
		txn = lv.ls.db.MetadataTxn(false)
		defer txn.Release()
	}
	parameterChange, err := lv.governancePurposeRoot(
		governancePurposeActionTypes(
			uint8(lcommon.GovActionTypeParameterChange),
		),
		txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get parameter-change purpose root: %w", err)
	}
	hardFork, err := lv.governancePurposeRoot(
		governancePurposeActionTypes(
			uint8(lcommon.GovActionTypeHardForkInitiation),
		),
		txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get hard-fork purpose root: %w", err)
	}
	committee, err := lv.governancePurposeRoot(
		governancePurposeActionTypes(
			uint8(lcommon.GovActionTypeNoConfidence),
		),
		txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get committee purpose root: %w", err)
	}
	constitution, err := lv.governancePurposeRoot(
		governancePurposeActionTypes(
			uint8(lcommon.GovActionTypeNewConstitution),
		),
		txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get constitution purpose root: %w", err)
	}
	return &lcommon.GovPurposeRoots{
		PParamUpdate: parameterChange,
		HardFork:     hardFork,
		Committee:    committee,
		Constitution: constitution,
	}, nil
}

func (lv *LedgerView) governancePurposeRoot(
	actionTypes []uint8,
	txn *database.Txn,
) (*lcommon.GovActionId, error) {
	proposal, err := lv.ls.db.GetLastEnactedGovernanceProposal(
		actionTypes,
		txn,
	)
	if err != nil {
		return nil, err
	}
	if proposal == nil {
		return nil, nil
	}
	if len(proposal.TxHash) != len(lcommon.Blake2b256{}) {
		return nil, fmt.Errorf(
			"invalid governance proposal transaction hash length: %d",
			len(proposal.TxHash),
		)
	}
	var transactionID lcommon.Blake2b256
	copy(transactionID[:], proposal.TxHash)
	return &lcommon.GovActionId{
		TransactionId: transactionID,
		GovActionIdx:  proposal.ActionIndex,
	}, nil
}

func (lv *LedgerView) governanceProposalExpirySlot(
	proposal *models.GovernanceProposal,
	txn *database.Txn,
) (uint64, error) {
	if proposal.ExpiresEpoch < proposal.ProposedEpoch {
		return 0, fmt.Errorf(
			"governance proposal expiry epoch %d precedes proposed epoch %d",
			proposal.ExpiresEpoch,
			proposal.ProposedEpoch,
		)
	}
	epoch, err := lv.ls.db.GetEpoch(proposal.ProposedEpoch, txn)
	if err != nil {
		return 0, fmt.Errorf(
			"get governance proposal epoch %d: %w",
			proposal.ProposedEpoch,
			err,
		)
	}
	if epoch == nil {
		return 0, fmt.Errorf(
			"governance proposal epoch %d not found",
			proposal.ProposedEpoch,
		)
	}
	if epoch.LengthInSlots == 0 {
		return 0, fmt.Errorf(
			"governance proposal epoch %d has zero length",
			proposal.ProposedEpoch,
		)
	}
	epochDelta := proposal.ExpiresEpoch - proposal.ProposedEpoch
	if epochDelta == math.MaxUint64 {
		return 0, errors.New("governance proposal expiry slot overflows")
	}
	epochCount := epochDelta + 1
	epochLength := uint64(epoch.LengthInSlots)
	if epochCount > math.MaxUint64/epochLength {
		return 0, errors.New("governance proposal expiry slot overflows")
	}
	span := epochCount * epochLength
	if epoch.StartSlot > math.MaxUint64-(span-1) {
		return 0, errors.New("governance proposal expiry slot overflows")
	}
	return epoch.StartSlot + span - 1, nil
}

// GovActionExists returns whether a governance action exists.
func (lv *LedgerView) GovActionExists(id lcommon.GovActionId) bool {
	proposal, err := lv.ls.db.GetGovernanceProposal(
		id.TransactionId[:],
		id.GovActionIdx,
		lv.txn,
	)
	if err != nil {
		return false
	}
	// Voting procedures may target only pending actions. GovActionById also
	// resolves the current enacted purpose root for content-aware predecessor
	// rules, so it cannot be used as the existence predicate here.
	return proposal.EnactedEpoch == nil && proposal.ExpiredEpoch == nil
}

// StakeDistribution represents the stake distribution at an epoch boundary.
// Used for leader election in Ouroboros Praos.
type StakeDistribution struct {
	Epoch      uint64            // Epoch this snapshot is for
	PoolStakes map[string]uint64 // poolKeyHash (hex) -> total stake
	TotalStake uint64            // Sum of all pool stakes
}

// GetStakeDistribution returns the mark stake distribution at the requested
// snapshot epoch. Callers choose the Praos-active epoch before calling.
func (lv *LedgerView) GetStakeDistribution(
	epoch uint64,
) (*StakeDistribution, error) {
	snapshots, err := lv.ls.db.Metadata().GetPoolStakeSnapshotsByEpoch(
		epoch,
		"mark",
		(*lv.txn).Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf("get pool stake snapshots: %w", err)
	}

	dist := &StakeDistribution{
		Epoch:      epoch,
		PoolStakes: make(map[string]uint64),
	}

	for _, s := range snapshots {
		poolKey := hex.EncodeToString(s.PoolKeyHash)
		stake := uint64(s.TotalStake)
		dist.PoolStakes[poolKey] = stake
		dist.TotalStake += stake
	}

	return dist, nil
}

// GetLeiosKeys returns the Dijkstra/Leios BLS key frozen with each named pool's
// Mark stake snapshot for epoch. A pool absent from the result has no captured
// key. The returned keys are raw; callers must verify proof of possession
// before treating a key as usable.
func (lv *LedgerView) GetLeiosKeys(
	epoch uint64,
	poolKeyHashes []lcommon.PoolKeyHash,
) (map[string]*lcommon.LeiosKey, error) {
	out := make(map[string]*lcommon.LeiosKey, len(poolKeyHashes))
	if len(poolKeyHashes) == 0 {
		return out, nil
	}
	rawPoolKeyHashes := make([][]byte, 0, len(poolKeyHashes))
	for _, poolKeyHash := range poolKeyHashes {
		rawPoolKeyHashes = append(
			rawPoolKeyHashes,
			append([]byte(nil), poolKeyHash[:]...),
		)
	}
	snapshots, err := lv.ls.db.Metadata().GetPoolStakeSnapshotsForPools(
		epoch,
		models.PoolStakeSnapshotTypeMark,
		rawPoolKeyHashes,
		(*lv.txn).Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf("get pool stake snapshots: %w", err)
	}
	for _, snapshot := range snapshots {
		if len(snapshot.LeiosKeyPublic) == 0 ||
			len(snapshot.LeiosKeyPossessionProof) == 0 {
			continue
		}
		out[hex.EncodeToString(snapshot.PoolKeyHash)] = &lcommon.LeiosKey{
			PublicKey: append(
				[]byte(nil), snapshot.LeiosKeyPublic...,
			),
			PossessionProof: append(
				[]byte(nil), snapshot.LeiosKeyPossessionProof...,
			),
		}
	}
	return out, nil
}

// GetPoolStake returns the stake for a specific pool from the snapshot.
// Returns 0 if the pool has no stake in the snapshot.
func (lv *LedgerView) GetPoolStake(
	epoch uint64,
	poolKeyHash []byte,
) (uint64, error) {
	snapshot, err := lv.ls.db.Metadata().GetPoolStakeSnapshot(
		epoch,
		"mark",
		poolKeyHash,
		(*lv.txn).Metadata(),
	)
	if err != nil {
		return 0, fmt.Errorf("get pool stake snapshot: %w", err)
	}
	if snapshot == nil {
		return 0, nil
	}
	return uint64(snapshot.TotalStake), nil
}

// GetTotalActiveStake returns the total stake from the requested mark snapshot.
func (lv *LedgerView) GetTotalActiveStake(epoch uint64) (uint64, error) {
	return lv.ls.db.Metadata().GetTotalActiveStake(
		epoch,
		"mark",
		(*lv.txn).Metadata(),
	)
}

// GetDRepVotingPower returns the voting power for a DRep by summing the
// current stake of all delegated accounts, approximated from live UTxO
// balance plus reward-account balance.
//
// TODO: Accept an epoch parameter and use epoch-based stake snapshots
// for accurate voting power. The current implementation approximates
// voting power using current live balances.
func (lv *LedgerView) GetDRepVotingPower(
	credentialTag uint8,
	drepCredential []byte,
) (uint64, error) {
	// expiryEpoch 0: this point-in-time API query is not gated by the
	// CIP-0163 epoch-boundary tally (see ledger/governance for that path).
	power, err := lv.ls.db.GetDRepVotingPower(
		credentialTag,
		drepCredential,
		0,
		lv.txn,
	)
	if err != nil {
		return 0, fmt.Errorf("get drep voting power: %w", err)
	}
	return power, nil
}

// GetExpiredDReps returns all active DReps whose expiry epoch is at or
// before the given epoch.
func (lv *LedgerView) GetExpiredDReps(
	epoch uint64,
) ([]*models.Drep, error) {
	dreps, err := lv.ls.db.GetExpiredDReps(epoch, lv.txn)
	if err != nil {
		return nil, fmt.Errorf("get expired dreps: %w", err)
	}
	return dreps, nil
}

// GetCommitteeActiveCount returns the number of active (non-resigned)
// committee members.
func (lv *LedgerView) GetCommitteeActiveCount() (int, error) {
	count, err := lv.ls.db.GetCommitteeActiveCount(lv.txn)
	if err != nil {
		return 0, fmt.Errorf("get committee active count: %w", err)
	}
	return count, nil
}

// IsCommitteeThresholdMet checks whether a committee vote threshold is met.
// Returns true if yesVotes / totalActiveMembers >= threshold.
//
// Edge cases per CIP-1694:
//   - If yesVotes or totalActiveMembers is negative, returns false
//   - If totalActiveMembers is 0, the threshold is trivially met (no committee
//     means no committee can block)
//   - If threshold numerator is 0, any vote count satisfies it
//   - If threshold denominator is 0, this is treated as an error condition
//     and returns false
func IsCommitteeThresholdMet(
	yesVotes int,
	totalActiveMembers int,
	thresholdNumerator uint64,
	thresholdDenominator uint64,
) bool {
	// Guard against negative inputs
	if yesVotes < 0 || totalActiveMembers < 0 {
		return false
	}

	// No active committee members means the threshold is trivially met.
	// Per CIP-1694, if the committee is in a no-confidence state (empty),
	// committee votes are not required.
	if totalActiveMembers == 0 {
		return true
	}

	// Zero threshold is always satisfied
	if thresholdNumerator == 0 {
		return true
	}

	// Invalid threshold (zero denominator) - treat as not met
	if thresholdDenominator == 0 {
		return false
	}

	// Use cross-multiplication to avoid floating point:
	// yesVotes / totalActiveMembers >= numerator / denominator
	// is equivalent to:
	// yesVotes * denominator >= numerator * totalActiveMembers
	//
	// Use math/big.Int to avoid uint64 overflow on large values.
	lhs := new(big.Int).Mul(
		big.NewInt(int64(yesVotes)),
		new(big.Int).SetUint64(thresholdDenominator),
	)
	rhs := new(big.Int).Mul(
		new(big.Int).SetUint64(thresholdNumerator),
		big.NewInt(int64(totalActiveMembers)),
	)

	return lhs.Cmp(rhs) >= 0
}
