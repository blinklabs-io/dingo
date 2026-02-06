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
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
		for i, reg := range pool.Registration {
			if reg.AddedSlot >= latestSlot {
				latestSlot = reg.AddedSlot
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
		for _, owner := range pool.Owners {
			tmp.PoolOwners = append(
				tmp.PoolOwners,
				lcommon.AddrKeyHash(lcommon.NewBlake2b224(owner.KeyHash)),
			)
		}
		for _, relay := range pool.Relays {
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

// CostModels returns the Plutus cost models.
// TODO: implement cost models retrieval from protocol parameters.
// Currently blocked because lcommon.PlutusLanguage and lcommon.CostModel are
// placeholder empty structs in gouroboros. Once these types are fleshed out with
// real fields, this method should extract cost models from the current protocol
// parameters (lv.ls.currentPParams).
func (lv *LedgerView) CostModels() map[lcommon.PlutusLanguage]lcommon.CostModel {
	return map[lcommon.PlutusLanguage]lcommon.CostModel{}
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
	if drep.AnchorUrl != "" || len(drep.AnchorHash) > 0 {
		if len(drep.AnchorHash) != 32 {
			return nil, fmt.Errorf(
				"invalid DRep anchor hash length: expected 32, got %d",
				len(drep.AnchorHash),
			)
		}
		var dataHash [32]byte
		copy(dataHash[:], drep.AnchorHash)
		reg.Anchor = &lcommon.GovAnchor{
			Url:      drep.AnchorUrl,
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
		if drep.AnchorUrl != "" || len(drep.AnchorHash) > 0 {
			if len(drep.AnchorHash) != 32 {
				return nil, fmt.Errorf(
					"invalid DRep anchor hash length: expected 32, got %d",
					len(drep.AnchorHash),
				)
			}
			var dataHash [32]byte
			copy(dataHash[:], drep.AnchorHash)
			reg.Anchor = &lcommon.GovAnchor{
				Url:      drep.AnchorUrl,
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
// Uses the "go" snapshot which represents stake from 2 epochs ago.
func (lv *LedgerView) GetStakeDistribution(epoch uint64) (*StakeDistribution, error) {
	snapshots, err := lv.ls.db.Metadata().GetPoolStakeSnapshotsByEpoch(
		epoch,
		"go",
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

// GetPoolStake returns the stake for a specific pool from the "go" snapshot.
// Returns 0 if the pool has no stake in the snapshot.
func (lv *LedgerView) GetPoolStake(epoch uint64, poolKeyHash []byte) (uint64, error) {
	snapshot, err := lv.ls.db.Metadata().GetPoolStakeSnapshot(
		epoch,
		"go",
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

// GetTotalActiveStake returns the total active stake from the "go" snapshot.
func (lv *LedgerView) GetTotalActiveStake(epoch uint64) (uint64, error) {
	return lv.ls.db.Metadata().GetTotalActiveStake(
		epoch,
		"go",
		(*lv.txn).Metadata(),
	)
}
