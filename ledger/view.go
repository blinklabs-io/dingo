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
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

// ErrNilDecodedOutput is returned when a decoded UTxO output is nil.
var ErrNilDecodedOutput = errors.New("nil decoded output")

type LedgerView struct {
	ls  *LedgerState
	txn *database.Txn
	// intraBlockUtxos tracks outputs created by earlier transactions in the same block.
	// Key format: hex(txId) + ":" + outputIdx
	intraBlockUtxos map[string]lcommon.Utxo
}

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

func (lv *LedgerView) UtxoById(
	utxoId lcommon.TransactionInput,
) (lcommon.Utxo, error) {
	// Check intra-block UTxOs first (outputs from earlier txs in same block)
	if lv.intraBlockUtxos != nil {
		key := fmt.Sprintf("%s:%d", utxoId.Id().String(), utxoId.Index())
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
	return lv.ls.db.GetStakeRegistrations(stakingKey, lv.txn)
}

// IsStakeCredentialRegistered checks if a stake credential is currently registered
func (lv *LedgerView) IsStakeCredentialRegistered(
	cred lcommon.Credential,
) bool {
	account, err := lv.ls.db.GetAccount(cred.Credential[:], false, lv.txn)
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
	if len(pool.Registration) > 0 {
		var latestIdx int
		var latestSlot uint64
		var latestCertID uint
		for i, reg := range pool.Registration {
			// Use CertificateID for deterministic disambiguation when slots are equal
			if reg.AddedSlot > latestSlot ||
				(reg.AddedSlot == latestSlot && reg.CertificateID > latestCertID) {
				latestSlot = reg.AddedSlot
				latestCertID = reg.CertificateID
				latestIdx = i
			}
		}
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
	var pendingEpoch *uint64
	if len(pool.Retirement) > 0 {
		var latestEpoch uint64
		var found bool
		for _, r := range pool.Retirement {
			if !found || r.Epoch > latestEpoch {
				latestEpoch = r.Epoch
				found = true
			}
		}
		if found {
			pendingEpoch = &latestEpoch
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
	return nil, nil
}

// GetAdaPots returns the current Ada pots.
// TODO: implement Ada pots retrieval. Requires tracking of treasury, reserves,
// fees, and rewards pots which are not yet stored in the database.
func (lv *LedgerView) GetAdaPots() lcommon.AdaPots {
	return lcommon.AdaPots{}
}

// GetRewardSnapshot returns the current reward snapshot.
// TODO: implement reward snapshot retrieval. Requires per-stake-credential
// reward tracking which is not yet stored in the database.
func (lv *LedgerView) GetRewardSnapshot(
	epoch uint64,
) (lcommon.RewardSnapshot, error) {
	return lcommon.RewardSnapshot{}, nil
}

// UpdateAdaPots updates the Ada pots.
// TODO: implement Ada pots update. Requires Ada pots storage in the database.
func (lv *LedgerView) UpdateAdaPots(adaPots lcommon.AdaPots) error {
	return nil
}

// IsRewardAccountRegistered checks if a reward account is registered
func (lv *LedgerView) IsRewardAccountRegistered(
	cred lcommon.Credential,
) bool {
	account, err := lv.ls.db.GetAccount(cred.Credential[:], false, lv.txn)
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
// TODO: implement reward account balance retrieval. Requires per-account reward
// balance tracking which is not yet stored in the database.
func (lv *LedgerView) RewardAccountBalance(
	cred lcommon.Credential,
) (*uint64, error) {
	return nil, nil
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

// CommitteeMember returns a committee member by cold key.
// Returns nil if the cold key is not an authorized committee member.
func (lv *LedgerView) CommitteeMember(
	coldKey lcommon.Blake2b224,
) (*lcommon.CommitteeMember, error) {
	member, err := lv.ls.db.GetCommitteeMember(coldKey[:], lv.txn)
	if err != nil {
		if errors.Is(err, models.ErrCommitteeMemberNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get committee member: %w", err)
	}
	resigned, err := lv.ls.db.IsCommitteeMemberResigned(coldKey[:], lv.txn)
	if err != nil {
		return nil, fmt.Errorf(
			"check committee member resignation: %w",
			err,
		)
	}
	hotKey := lcommon.NewBlake2b224(member.HostCredential)
	return &lcommon.CommitteeMember{
		ColdKey:  coldKey,
		HotKey:   &hotKey,
		Resigned: resigned,
	}, nil
}

// CommitteeMembers returns all active (non-resigned) committee members.
func (lv *LedgerView) CommitteeMembers() ([]lcommon.CommitteeMember, error) {
	dbMembers, err := lv.ls.db.GetActiveCommitteeMembers(lv.txn)
	if err != nil {
		return nil, fmt.Errorf("get active committee members: %w", err)
	}
	members := make([]lcommon.CommitteeMember, 0, len(dbMembers))
	for _, m := range dbMembers {
		coldKey := lcommon.NewBlake2b224(m.ColdCredential)
		hotKey := lcommon.NewBlake2b224(m.HostCredential)
		members = append(members, lcommon.CommitteeMember{
			ColdKey:  coldKey,
			HotKey:   &hotKey,
			Resigned: false, // Active members are not resigned
		})
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

// Constitution returns the current constitution.
// Returns nil if no constitution has been established on-chain.
func (lv *LedgerView) Constitution() (*lcommon.Constitution, error) {
	constitution, err := lv.ls.db.GetConstitution(lv.txn)
	if err != nil {
		return nil, fmt.Errorf("get constitution: %w", err)
	}
	if constitution == nil {
		return nil, nil
	}
	// Constitution in gouroboros is currently an empty placeholder struct.
	// Return a non-nil pointer to indicate a constitution exists.
	return &lcommon.Constitution{}, nil
}

// TreasuryValue returns the current treasury value.
// TODO: implement treasury value retrieval. Requires Ada pots tracking
// which is not yet stored in the database. The treasury value is part of
// the Ada pots (reserves, treasury, fees, rewards).
func (lv *LedgerView) TreasuryValue() (uint64, error) {
	return 0, nil
}

// GovActionById returns a governance action by its ID.
// Returns nil if the governance action does not exist.
func (lv *LedgerView) GovActionById(
	id lcommon.GovActionId,
) (*lcommon.GovActionState, error) {
	proposal, err := lv.ls.db.GetGovernanceProposal(
		id.TransactionId[:],
		id.GovActionIdx,
		lv.txn,
	)
	if err != nil {
		if errors.Is(err, models.ErrGovernanceProposalNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get governance proposal: %w", err)
	}
	return &lcommon.GovActionState{
		ActionId:   id,
		ActionType: lcommon.GovActionType(proposal.ActionType),
	}, nil
}

// GovActionExists returns whether a governance action exists.
func (lv *LedgerView) GovActionExists(id lcommon.GovActionId) bool {
	action, err := lv.GovActionById(id)
	if err != nil {
		return false
	}
	return action != nil
}

// StakeDistribution represents the stake distribution at an epoch boundary.
// Used for leader election in Ouroboros Praos.
type StakeDistribution struct {
	Epoch      uint64            // Epoch this snapshot is for
	PoolStakes map[string]uint64 // poolKeyHash (hex) -> total stake
	TotalStake uint64            // Sum of all pool stakes
}

// GetStakeDistribution returns the stake distribution for leader election.
// Uses the "mark" snapshot at the given epoch. Callers pass currentEpoch-2
// so the epoch offset already accounts for the Mark→Set→Go rotation.
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

// GetPoolStake returns the stake for a specific pool from the snapshot.
// Returns 0 if the pool has no stake in the snapshot. Callers pass
// currentEpoch-2 so the epoch offset accounts for Mark→Set→Go rotation.
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

// GetTotalActiveStake returns the total active stake from the snapshot.
// Callers pass currentEpoch-2 so the epoch offset accounts for rotation.
func (lv *LedgerView) GetTotalActiveStake(epoch uint64) (uint64, error) {
	return lv.ls.db.Metadata().GetTotalActiveStake(
		epoch,
		"mark",
		(*lv.txn).Metadata(),
	)
}

// GetDRepVotingPower returns the voting power for a DRep by summing the
// stake of all accounts delegated to it. Uses the current live UTxO set.
//
// TODO: Accept an epoch parameter and use epoch-based stake snapshots
// for accurate voting power. The current implementation approximates
// voting power using the live UTxO set.
func (lv *LedgerView) GetDRepVotingPower(
	drepCredential []byte,
) (uint64, error) {
	power, err := lv.ls.db.GetDRepVotingPower(drepCredential, lv.txn)
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
