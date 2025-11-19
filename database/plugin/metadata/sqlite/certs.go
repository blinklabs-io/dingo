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

package sqlite

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"gorm.io/gorm"
)

// GetStakeRegistrations returns stake registration certificates
func (d *MetadataStoreSqlite) GetStakeRegistrations(
	stakingKey []byte,
	txn *gorm.DB,
) ([]lcommon.StakeRegistrationCertificate, error) {
	ret := []lcommon.StakeRegistrationCertificate{}
	certs := []models.StakeRegistration{}
	if txn != nil {
		result := txn.Where("staking_key = ?", stakingKey).
			Order("id DESC").
			Find(&certs)
		if result.Error != nil {
			return ret, result.Error
		}
	} else {
		result := d.DB().Where("staking_key = ?", stakingKey).
			Order("id DESC").
			Find(&certs)
		if result.Error != nil {
			return ret, result.Error
		}
	}
	var tmpCert lcommon.StakeRegistrationCertificate
	for _, cert := range certs {
		tmpCert = lcommon.StakeRegistrationCertificate{
			CertType: uint(lcommon.CertificateTypeStakeRegistration),
			StakeCredential: lcommon.Credential{
				Credential: lcommon.CredentialHash(cert.StakingKey),
			},
		}
		ret = append(ret, tmpCert)
	}
	return ret, nil
}

// storeCertificate stores a certificate in the appropriate table and creates a unified certificate record
func (d *MetadataStoreSqlite) storeCertificate(
	cert lcommon.Certificate,
	transactionID uint,
	certIndex uint32,
	point ocommon.Point,
	deposit uint64,
	txn *gorm.DB,
) error {
	certType := models.CertificateType(cert.Type())

	// Create unified certificate record first to get its ID
	unifiedCert := models.Certificate{
		BlockHash:     point.Hash,
		TransactionID: transactionID,
		CertificateID: 0, // Will be updated after specific model creation
		Slot:          point.Slot,
		CertIndex:     uint(certIndex),
		CertType:      certType,
	}
	if err := txn.Create(&unifiedCert).Error; err != nil {
		return fmt.Errorf("create unified certificate record: %w", err)
	}

	// Create the specific certificate model based on type
	var specificModelID uint
	switch c := cert.(type) {
	case *lcommon.StakeRegistrationCertificate:
		model := &models.StakeRegistration{
			StakingKey:    c.StakeCredential.Hash().Bytes(),
			CertificateID: unifiedCert.ID,
			DepositAmount: types.Uint64{Val: deposit},
			AddedSlot:     point.Slot,
		}
		if err := txn.Create(model).Error; err != nil {
			return fmt.Errorf("create stake registration: %w", err)
		}
		specificModelID = model.ID

	case *lcommon.StakeDeregistrationCertificate:
		model := &models.StakeDeregistration{
			StakingKey:    c.StakeCredential.Hash().Bytes(),
			CertificateID: unifiedCert.ID,
			AddedSlot:     point.Slot,
		}
		if err := txn.Create(model).Error; err != nil {
			return fmt.Errorf("create stake deregistration: %w", err)
		}
		specificModelID = model.ID

	// PoolKeyHash handling: All certificate types now define PoolKeyHash as PoolKeyHash type (requiring .Bytes()).
	case *lcommon.StakeDelegationCertificate:
		model := &models.StakeDelegation{
			StakingKey:    c.StakeCredential.Hash().Bytes(),
			PoolKeyHash:   c.PoolKeyHash.Bytes(),
			CertificateID: unifiedCert.ID,
			AddedSlot:     point.Slot,
		}
		if err := txn.Create(model).Error; err != nil {
			return fmt.Errorf("create stake delegation: %w", err)
		}
		specificModelID = model.ID

	case *lcommon.PoolRegistrationCertificate:
		// Find or create pool record
		pool := &models.Pool{}
		result := txn.FirstOrCreate(pool, models.Pool{PoolKeyHash: c.Operator.Bytes()})
		if result.Error != nil {
			return fmt.Errorf("find or create pool: %w", result.Error)
		}

		model := &models.PoolRegistration{
			CertificateID: unifiedCert.ID,
			PoolKeyHash:   c.Operator.Bytes(),
			VrfKeyHash:    c.VrfKeyHash.Bytes(),
			RewardAccount: c.RewardAccount.Bytes(),
			Pledge:        types.Uint64{Val: c.Pledge},
			Cost:          types.Uint64{Val: c.Cost},
			DepositAmount: types.Uint64{Val: deposit},
			PoolID:        pool.ID,
			AddedSlot:     point.Slot,
		}
		// Handle Margin field safely
		if c.Margin.Rat != nil {
			model.Margin = &types.Rat{Rat: c.Margin.Rat}
		}
		// Handle PoolMetadata
		if c.PoolMetadata != nil {
			model.MetadataUrl = c.PoolMetadata.Url
			model.MetadataHash = c.PoolMetadata.Hash.Bytes()
		}
		if err := txn.Create(model).Error; err != nil {
			return fmt.Errorf("create pool registration: %w", err)
		}
		// Create owners
		for _, owner := range c.PoolOwners {
			ownerModel := &models.PoolRegistrationOwner{
				KeyHash:            owner.Bytes(),
				PoolRegistrationID: model.ID,
				PoolID:             pool.ID,
			}
			if err := txn.Create(ownerModel).Error; err != nil {
				return fmt.Errorf("create pool registration owner: %w", err)
			}
		}
		// Create relays
		for _, relay := range c.Relays {
			relayModel := &models.PoolRegistrationRelay{}
			if relay.Hostname != nil {
				relayModel.Hostname = *relay.Hostname
			}
			if relay.Port != nil {
				relayModel.Port = uint(*relay.Port)
			}
			if relay.Ipv4 != nil {
				relayModel.Ipv4 = relay.Ipv4
			}
			if relay.Ipv6 != nil {
				relayModel.Ipv6 = relay.Ipv6
			}
			relayModel.PoolRegistrationID = model.ID
			relayModel.PoolID = pool.ID
			if err := txn.Create(relayModel).Error; err != nil {
				return fmt.Errorf("create pool registration relay: %w", err)
			}
		}
		specificModelID = model.ID

	case *lcommon.PoolRetirementCertificate:
		model := &models.PoolRetirement{
			PoolKeyHash:   c.PoolKeyHash.Bytes(),
			Epoch:         c.Epoch,
			CertificateID: unifiedCert.ID,
			AddedSlot:     point.Slot,
		}
		result := txn.FirstOrCreate(model, models.PoolRetirement{
			PoolKeyHash: c.PoolKeyHash.Bytes(),
			Epoch:       c.Epoch,
		})
		if result.Error != nil {
			return fmt.Errorf("create pool retirement: %w", result.Error)
		}
		// Update fields in case it already existed
		updates := map[string]interface{}{
			"certificate_id": unifiedCert.ID,
			"added_slot":     point.Slot,
		}
		if err := txn.Model(model).Updates(updates).Error; err != nil {
			return fmt.Errorf("update pool retirement: %w", err)
		}
		specificModelID = model.ID

	case *lcommon.RegistrationCertificate:
		model := &models.Registration{
			StakingKey:    c.StakeCredential.Hash().Bytes(),
			CertificateID: unifiedCert.ID,
			DepositAmount: types.Uint64{Val: deposit},
			AddedSlot:     point.Slot,
		}
		if err := txn.Create(model).Error; err != nil {
			return fmt.Errorf("create registration: %w", err)
		}
		specificModelID = model.ID

	case *lcommon.DeregistrationCertificate:
		// Store the certificate's actual amount, not the deposit (which is zero for deregistrations)
		amount := uint64(0)
		if c.Amount >= 0 {
			amount = uint64(c.Amount)
		}
		model := &models.Deregistration{
			StakingKey:    c.StakeCredential.Hash().Bytes(),
			CertificateID: unifiedCert.ID, // Set CertificateID to unified cert ID
			Amount:        types.Uint64{Val: amount},
			AddedSlot:     point.Slot,
		}
		if err := txn.Create(model).Error; err != nil {
			return fmt.Errorf("create deregistration: %w", err)
		}
		specificModelID = model.ID

	case *lcommon.StakeRegistrationDelegationCertificate:
		model := &models.StakeRegistrationDelegation{
			StakingKey:    c.StakeCredential.Hash().Bytes(),
			PoolKeyHash:   c.PoolKeyHash.Bytes(),
			CertificateID: unifiedCert.ID,
			DepositAmount: types.Uint64{Val: deposit},
			AddedSlot:     point.Slot,
		}
		if err := txn.Create(model).Error; err != nil {
			return fmt.Errorf("create stake registration delegation: %w", err)
		}
		specificModelID = model.ID

	case *lcommon.VoteDelegationCertificate:
		model := &models.VoteDelegation{
			StakingKey:    c.StakeCredential.Hash().Bytes(),
			Drep:          c.Drep.Credential,
			CertificateID: unifiedCert.ID,
			AddedSlot:     point.Slot,
		}
		if err := txn.Create(model).Error; err != nil {
			return fmt.Errorf("create vote delegation: %w", err)
		}
		specificModelID = model.ID

	case *lcommon.StakeVoteDelegationCertificate:
		model := &models.StakeVoteDelegation{
			StakingKey:    c.StakeCredential.Hash().Bytes(),
			PoolKeyHash:   c.PoolKeyHash.Bytes(),
			Drep:          c.Drep.Credential,
			CertificateID: unifiedCert.ID,
			AddedSlot:     point.Slot,
		}
		if err := txn.Create(model).Error; err != nil {
			return fmt.Errorf("create stake vote delegation: %w", err)
		}
		specificModelID = model.ID

	case *lcommon.VoteRegistrationDelegationCertificate:
		model := &models.VoteRegistrationDelegation{
			StakingKey:    c.StakeCredential.Hash().Bytes(),
			Drep:          c.Drep.Credential,
			CertificateID: unifiedCert.ID,
			DepositAmount: types.Uint64{Val: deposit},
			AddedSlot:     point.Slot,
		}
		if err := txn.Create(model).Error; err != nil {
			return fmt.Errorf("create vote registration delegation: %w", err)
		}
		specificModelID = model.ID

	case *lcommon.StakeVoteRegistrationDelegationCertificate:
		model := &models.StakeVoteRegistrationDelegation{
			StakingKey:    c.StakeCredential.Hash().Bytes(),
			PoolKeyHash:   c.PoolKeyHash.Bytes(),
			Drep:          c.Drep.Credential,
			CertificateID: unifiedCert.ID,
			DepositAmount: types.Uint64{Val: deposit},
			AddedSlot:     point.Slot,
		}
		if err := txn.Create(model).Error; err != nil {
			return fmt.Errorf("create stake vote registration delegation: %w", err)
		}
		specificModelID = model.ID

	case *lcommon.AuthCommitteeHotCertificate:
		model := &models.AuthCommitteeHot{
			CertificateID:  unifiedCert.ID,
			ColdCredential: c.ColdCredential.Hash().Bytes(),
			HostCredential: c.HotCredential.Hash().Bytes(),
			AddedSlot:      types.Uint64{Val: point.Slot},
		}
		result := txn.FirstOrCreate(model, models.AuthCommitteeHot{
			ColdCredential: c.ColdCredential.Hash().Bytes(),
			HostCredential: c.HotCredential.Hash().Bytes(),
		})
		if result.Error != nil {
			return fmt.Errorf("create auth committee hot: %w", result.Error)
		}
		// Update AddedSlot and CertificateID in case it already existed
		if err := txn.Model(model).Updates(map[string]interface{}{
			"added_slot":     types.Uint64{Val: point.Slot},
			"certificate_id": unifiedCert.ID,
		}).Error; err != nil {
			return fmt.Errorf("update auth committee hot: %w", err)
		}
		specificModelID = model.ID

	case *lcommon.ResignCommitteeColdCertificate:
		model := &models.ResignCommitteeCold{
			CertificateID:  unifiedCert.ID,
			ColdCredential: c.ColdCredential.Hash().Bytes(),
			AddedSlot:      types.Uint64{Val: point.Slot},
		}
		// Handle anchor if present
		if c.Anchor != nil {
			model.AnchorUrl = c.Anchor.Url
			model.AnchorHash = c.Anchor.DataHash[:]
		}
		result := txn.FirstOrCreate(model, models.ResignCommitteeCold{
			ColdCredential: c.ColdCredential.Hash().Bytes(),
		})
		if result.Error != nil {
			return fmt.Errorf("create resign committee cold: %w", result.Error)
		}
		// Update fields in case it already existed
		updates := map[string]interface{}{
			"added_slot":     types.Uint64{Val: point.Slot},
			"certificate_id": unifiedCert.ID,
		}
		if c.Anchor != nil {
			updates["anchor_url"] = c.Anchor.Url
			updates["anchor_hash"] = c.Anchor.DataHash[:]
		}
		if err := txn.Model(model).Updates(updates).Error; err != nil {
			return fmt.Errorf("update resign committee cold: %w", err)
		}
		specificModelID = model.ID

	case *lcommon.RegistrationDrepCertificate:
		// Create or update drep record
		var anchorUrl string
		var anchorHash []byte
		if c.Anchor != nil {
			anchorUrl = c.Anchor.Url
			anchorHash = c.Anchor.DataHash[:]
		}
		if err := d.SetDrep(
			c.DrepCredential.Hash().Bytes(),
			point.Slot,
			anchorUrl,
			anchorHash,
			true, // active
			unifiedCert.ID,
			txn,
		); err != nil {
			return fmt.Errorf("set drep: %w", err)
		}

		model := &models.RegistrationDrep{
			CertificateID:  unifiedCert.ID,
			DrepCredential: c.DrepCredential.Hash().Bytes(),
			DepositAmount:  types.Uint64{Val: deposit},
			AddedSlot:      point.Slot,
		}
		// Handle anchor if present
		if c.Anchor != nil {
			model.AnchorUrl = c.Anchor.Url
			model.AnchorHash = c.Anchor.DataHash[:]
		}
		if err := txn.Create(model).Error; err != nil {
			return fmt.Errorf("create registration drep: %w", err)
		}
		specificModelID = model.ID

	case *lcommon.DeregistrationDrepCertificate:
		// Update drep record to inactive
		if err := d.SetDrep(
			c.DrepCredential.Hash().Bytes(),
			point.Slot,
			"",    // no anchor URL for deregistration
			nil,   // no anchor hash for deregistration
			false, // inactive
			unifiedCert.ID,
			txn,
		); err != nil {
			return fmt.Errorf("set drep inactive: %w", err)
		}

		model := &models.DeregistrationDrep{
			CertificateID:  unifiedCert.ID,
			DrepCredential: c.DrepCredential.Hash().Bytes(),
			DepositAmount:  types.Uint64{Val: deposit},
			AddedSlot:      point.Slot,
		}
		if err := txn.Create(model).Error; err != nil {
			return fmt.Errorf("create deregistration drep: %w", err)
		}
		specificModelID = model.ID

	case *lcommon.UpdateDrepCertificate:
		// Update drep record with new anchor info
		var anchorUrl string
		var anchorHash []byte
		if c.Anchor != nil {
			anchorUrl = c.Anchor.Url
			anchorHash = c.Anchor.DataHash[:]
		}
		if err := d.SetDrep(
			c.DrepCredential.Hash().Bytes(),
			point.Slot,
			anchorUrl,
			anchorHash,
			true, // keep active
			unifiedCert.ID,
			txn,
		); err != nil {
			return fmt.Errorf("update drep: %w", err)
		}

		model := &models.UpdateDrep{
			CertificateID:  unifiedCert.ID,
			DrepCredential: c.DrepCredential.Hash().Bytes(),
			AddedSlot:      point.Slot,
		}
		// Handle anchor if present
		if c.Anchor != nil {
			model.AnchorUrl = c.Anchor.Url
			model.AnchorHash = c.Anchor.DataHash[:]
		}
		if err := txn.Create(model).Error; err != nil {
			return fmt.Errorf("create update drep: %w", err)
		}
		specificModelID = model.ID

	case *lcommon.GenesisKeyDelegationCertificate:
		model := &models.GenesisKeyDelegation{
			CertificateID:       unifiedCert.ID,
			GenesisHash:         c.GenesisHash,
			GenesisDelegateHash: c.GenesisDelegateHash,
			VrfKeyHash:          c.VrfKeyHash.Bytes(),
			AddedSlot:           types.Uint64{Val: point.Slot},
		}
		result := txn.FirstOrCreate(model, models.GenesisKeyDelegation{
			GenesisHash: c.GenesisHash,
			VrfKeyHash:  c.VrfKeyHash.Bytes(),
		})
		if result.Error != nil {
			return fmt.Errorf("create genesis key delegation: %w", result.Error)
		}
		// Update fields in case it already existed
		updates := map[string]interface{}{
			"genesis_delegate_hash": c.GenesisDelegateHash,
			"added_slot":            types.Uint64{Val: point.Slot},
			"certificate_id":        unifiedCert.ID,
		}
		if err := txn.Model(model).Updates(updates).Error; err != nil {
			return fmt.Errorf("update genesis key delegation: %w", err)
		}
		specificModelID = model.ID

	case *lcommon.MoveInstantaneousRewardsCertificate:
		// Encode rewards data as JSON (convert map keys to strings for JSON compatibility)
		rewardsMap := make(map[string]uint64)
		for cred, coin := range c.Reward.Rewards {
			rewardsMap[cred.Hash().String()] = coin
		}
		rewardData, err := json.Marshal(rewardsMap)
		if err != nil {
			return fmt.Errorf("marshal MIR rewards: %w", err)
		}
		model := &models.MoveInstantaneousRewards{
			CertificateID: unifiedCert.ID,
			RewardData:    rewardData,
			Source:        c.Reward.Source,
			OtherPot:      types.Uint64{Val: c.Reward.OtherPot},
			AddedSlot:     types.Uint64{Val: point.Slot},
		}
		if err := txn.Create(model).Error; err != nil {
			return fmt.Errorf("create move instantaneous rewards: %w", err)
		}
		specificModelID = model.ID

	case *lcommon.LeiosEbCertificate:
		// Encode voter and signature data as JSON
		persistentVotersData, err := json.Marshal(c.PersistentVoters)
		if err != nil {
			return fmt.Errorf("marshal persistent voters: %w", err)
		}
		// Convert map keys to strings for JSON compatibility
		nonpersistentVotersMap := make(map[string]any)
		for key, value := range c.NonpersistentVoters {
			nonpersistentVotersMap[key.String()] = value
		}
		nonpersistentVotersData, err := json.Marshal(nonpersistentVotersMap)
		if err != nil {
			return fmt.Errorf("marshal nonpersistent voters: %w", err)
		}
		aggregateEligSigData, err := json.Marshal(c.AggregateEligSig)
		if err != nil {
			return fmt.Errorf("marshal aggregate eligibility sig: %w", err)
		}
		aggregateVoteSigData, err := json.Marshal(c.AggregateVoteSig)
		if err != nil {
			return fmt.Errorf("marshal aggregate vote sig: %w", err)
		}
		model := &models.LeiosEb{
			CertificateID:           unifiedCert.ID,
			ElectionID:              c.ElectionId[:],
			EndorserBlockHash:       c.EndorserBlockHash[:],
			PersistentVotersData:    persistentVotersData,
			NonpersistentVotersData: nonpersistentVotersData,
			AggregateEligSigData:    aggregateEligSigData,
			AggregateVoteSigData:    aggregateVoteSigData,
			AddedSlot:               types.Uint64{Val: point.Slot},
		}
		if err := txn.Create(model).Error; err != nil {
			return fmt.Errorf("create leios eb: %w", err)
		}
		specificModelID = model.ID

	default:
		return fmt.Errorf("unsupported certificate type: %d", certType)
	}

	// Update the unified certificate with the specific model ID
	unifiedCert.CertificateID = specificModelID
	if err := txn.Save(&unifiedCert).Error; err != nil {
		return fmt.Errorf("update unified certificate record: %w", err)
	}

	// Update account state based on certificate type
	if err := d.updateAccountState(cert, point, unifiedCert.ID, txn); err != nil {
		return fmt.Errorf("update account state: %w", err)
	}

	// The specific model already has the correct CertificateID set, so no need to update it

	return nil
}

// updateAccountState updates account state based on certificate operations
func (d *MetadataStoreSqlite) updateAccountState(
	cert lcommon.Certificate,
	point ocommon.Point,
	certificateID uint,
	txn *gorm.DB,
) error {
	switch c := cert.(type) {
	case *lcommon.StakeRegistrationCertificate:
		// Create or update account as active
		return d.SetAccount(
			c.StakeCredential.Hash().Bytes(),
			nil, // pool
			nil, // drep
			point.Slot,
			true, // active
			certificateID,
			txn,
		)

	case *lcommon.StakeDeregistrationCertificate:
		// Mark account as inactive
		return d.SetAccount(
			c.StakeCredential.Hash().Bytes(),
			nil, // pool
			nil, // drep
			point.Slot,
			false, // inactive
			certificateID,
			txn,
		)

	case *lcommon.StakeDelegationCertificate:
		// Update account pool delegation
		return d.SetAccount(
			c.StakeCredential.Hash().Bytes(),
			c.PoolKeyHash.Bytes(), // pool
			nil,                   // drep
			point.Slot,
			true, // active
			certificateID,
			txn,
		)

	case *lcommon.RegistrationDrepCertificate:
		// DRep registration doesn't directly affect account state (no staking key)
		return nil

	case *lcommon.UpdateDrepCertificate:
		// DRep update doesn't directly affect account state (no staking key)
		return nil

	case *lcommon.StakeRegistrationDelegationCertificate:
		// Update account pool delegation and mark as active
		return d.SetAccount(
			c.StakeCredential.Hash().Bytes(),
			c.PoolKeyHash.Bytes(), // pool
			nil,                   // drep
			point.Slot,
			true, // active
			certificateID,
			txn,
		)

	case *lcommon.VoteDelegationCertificate:
		// Update account DRep for voting
		return d.SetAccount(
			c.StakeCredential.Hash().Bytes(),
			nil,               // pool
			c.Drep.Credential, // drep
			point.Slot,
			true, // active
			certificateID,
			txn,
		)

	case *lcommon.StakeVoteDelegationCertificate:
		// Update account pool and DRep
		return d.SetAccount(
			c.StakeCredential.Hash().Bytes(),
			c.PoolKeyHash.Bytes(), // pool
			c.Drep.Credential,     // drep
			point.Slot,
			true, // active
			certificateID,
			txn,
		)

	case *lcommon.StakeVoteRegistrationDelegationCertificate:
		// Update account pool and DRep, mark as active
		return d.SetAccount(
			c.StakeCredential.Hash().Bytes(),
			c.PoolKeyHash.Bytes(), // pool
			c.Drep.Credential,     // drep
			point.Slot,
			true, // active
			certificateID,
			txn,
		)

	case *lcommon.RegistrationCertificate:
		// Create or update account as active (legacy registration)
		return d.SetAccount(
			c.StakeCredential.Hash().Bytes(),
			nil, // pool
			nil, // drep
			point.Slot,
			true, // active
			certificateID,
			txn,
		)

	case *lcommon.DeregistrationCertificate:
		// Mark account as inactive (legacy deregistration)
		return d.SetAccount(
			c.StakeCredential.Hash().Bytes(),
			nil, // pool
			nil, // drep
			point.Slot,
			false, // inactive
			certificateID,
			txn,
		)

	case *lcommon.VoteRegistrationDelegationCertificate:
		// Update account DRep and mark as active
		return d.SetAccount(
			c.StakeCredential.Hash().Bytes(),
			nil,               // pool
			c.Drep.Credential, // drep
			point.Slot,
			true, // active
			certificateID,
			txn,
		)

	// Pool certificates don't affect account state directly
	case *lcommon.PoolRegistrationCertificate,
		*lcommon.PoolRetirementCertificate:
		return nil

	// DRep deregistration doesn't affect account state (account remains active)
	case *lcommon.DeregistrationDrepCertificate:
		return nil

	// Other certificate types don't affect account state
	default:
		return nil
	}
}

// DeleteCertificate performs cascading delete of a certificate and all related records
func (d *MetadataStoreSqlite) DeleteCertificate(
	txID uint,
	certIndex uint,
	txn *gorm.DB,
) error {
	if txn == nil {
		txn = d.DB()
	}

	// Get the certificate to determine its type
	var cert models.Certificate
	if err := txn.Where("transaction_id = ? AND cert_index = ?", txID, certIndex).First(&cert).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil // Certificate doesn't exist, nothing to delete
		}
		return fmt.Errorf("find certificate: %w", err)
	}

	// Delete the specific certificate record based on type
	var deleteErr error
	switch cert.CertType {
	case models.CertificateTypeStakeRegistration:
		deleteErr = txn.Delete(
			&models.StakeRegistration{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypeStakeDeregistration:
		deleteErr = txn.Delete(
			&models.StakeDeregistration{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypeStakeDelegation:
		deleteErr = txn.Delete(
			&models.StakeDelegation{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypePoolRegistration:
		deleteErr = txn.Delete(
			&models.PoolRegistration{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypePoolRetirement:
		deleteErr = txn.Delete(
			&models.PoolRetirement{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypeRegistration:
		deleteErr = txn.Delete(
			&models.Registration{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypeDeregistration:
		deleteErr = txn.Delete(
			&models.Deregistration{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypeStakeRegistrationDelegation:
		deleteErr = txn.Delete(
			&models.StakeRegistrationDelegation{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypeVoteDelegation:
		deleteErr = txn.Delete(
			&models.VoteDelegation{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypeStakeVoteDelegation:
		deleteErr = txn.Delete(
			&models.StakeVoteDelegation{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypeVoteRegistrationDelegation:
		deleteErr = txn.Delete(
			&models.VoteRegistrationDelegation{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypeStakeVoteRegistrationDelegation:
		deleteErr = txn.Delete(
			&models.StakeVoteRegistrationDelegation{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypeAuthCommitteeHot:
		deleteErr = txn.Delete(
			&models.AuthCommitteeHot{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypeResignCommitteeCold:
		deleteErr = txn.Delete(
			&models.ResignCommitteeCold{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypeRegistrationDrep:
		deleteErr = txn.Delete(
			&models.RegistrationDrep{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypeDeregistrationDrep:
		deleteErr = txn.Delete(
			&models.DeregistrationDrep{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypeUpdateDrep:
		deleteErr = txn.Delete(
			&models.UpdateDrep{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypeGenesisKeyDelegation:
		deleteErr = txn.Delete(
			&models.GenesisKeyDelegation{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypeMoveInstantaneousRewards:
		deleteErr = txn.Delete(
			&models.MoveInstantaneousRewards{},
			"certificate_id = ?",
			cert.ID,
		).Error
	case models.CertificateTypeLeiosEb:
		deleteErr = txn.Delete(
			&models.LeiosEb{},
			"certificate_id = ?",
			cert.ID,
		).Error
	default:
		// Unknown certificate type - return specific error for testing exhaustiveness
		deleteErr = fmt.Errorf("unknown certificate type: %d", cert.CertType)
	}

	if deleteErr != nil {
		return fmt.Errorf("delete specific certificate record: %w", deleteErr)
	}

	// Delete the unified certificate record
	if err := txn.Delete(&cert).Error; err != nil {
		return fmt.Errorf("delete unified certificate record: %w", err)
	}

	return nil
}
