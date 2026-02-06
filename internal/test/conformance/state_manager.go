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

// Package conformance provides a DingoStateManager that implements the
// ouroboros-mock conformance.StateManager interface using dingo's database
// and ledger packages with an in-memory SQLite database.
package conformance

import (
	"encoding/hex"
	"fmt"
	"maps"
	"math/big"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/glebarez/sqlite"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// DingoStateManager implements conformance.StateManager using dingo's database
// with an in-memory SQLite backend for testing.
type DingoStateManager struct {
	// db is the GORM database connection
	db *gorm.DB

	// protocolParams holds the current protocol parameters
	protocolParams common.ProtocolParameters

	// govState tracks governance-related state
	govState *conformance.GovernanceState

	// currentEpoch tracks the current epoch
	currentEpoch uint64

	// utxos stores UTxOs by their ID string for fast lookup
	utxos map[string]common.Utxo

	// stakeRegistrations tracks registered stake credentials and their balances
	stakeRegistrations map[common.Blake2b224]uint64

	// poolRegistrations tracks registered pools
	poolRegistrations map[common.Blake2b224]bool

	// drepRegistrations tracks registered DReps
	drepRegistrations map[common.Blake2b224]bool

	// committeeMembers tracks committee members (cold key -> expiry epoch)
	committeeMembers map[common.Blake2b224]uint64

	// hotKeyAuthorizations tracks hot key authorizations (cold key -> hot key)
	hotKeyAuthorizations map[common.Blake2b224]common.Blake2b224
}

// NewDingoStateManager creates a new DingoStateManager with an in-memory SQLite database.
func NewDingoStateManager() (*DingoStateManager, error) {
	// Open in-memory SQLite database
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open in-memory database: %w", err)
	}

	// Run migrations for all models
	if err := db.AutoMigrate(models.MigrateModels...); err != nil {
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}

	return &DingoStateManager{
		db:                   db,
		govState:             conformance.NewGovernanceState(),
		utxos:                make(map[string]common.Utxo),
		stakeRegistrations:   make(map[common.Blake2b224]uint64),
		poolRegistrations:    make(map[common.Blake2b224]bool),
		drepRegistrations:    make(map[common.Blake2b224]bool),
		committeeMembers:     make(map[common.Blake2b224]uint64),
		hotKeyAuthorizations: make(map[common.Blake2b224]common.Blake2b224),
	}, nil
}

// LoadInitialState implements conformance.StateManager.LoadInitialState.
func (m *DingoStateManager) LoadInitialState(
	state *conformance.ParsedInitialState,
	pp common.ProtocolParameters,
) error {
	m.protocolParams = pp
	m.currentEpoch = state.CurrentEpoch

	// Clear existing state
	m.utxos = make(map[string]common.Utxo)
	m.stakeRegistrations = make(map[common.Blake2b224]uint64)
	m.poolRegistrations = make(map[common.Blake2b224]bool)
	m.drepRegistrations = make(map[common.Blake2b224]bool)
	m.committeeMembers = make(map[common.Blake2b224]uint64)
	m.hotKeyAuthorizations = make(map[common.Blake2b224]common.Blake2b224)

	// Clear database tables
	if err := m.clearDatabaseTables(); err != nil {
		return fmt.Errorf("failed to clear database: %w", err)
	}

	// Load stake registrations with reward balances
	for hash, registered := range state.StakeRegistrations {
		if registered {
			balance := state.RewardAccounts[hash]
			m.stakeRegistrations[hash] = balance

			// Insert into database
			account := models.Account{
				StakingKey: hash[:],
				AddedSlot:  0,
				Active:     true,
			}
			if err := m.db.Create(&account).Error; err != nil {
				return fmt.Errorf("failed to insert account: %w", err)
			}
		}
	}

	// Load pool registrations
	for hash, registered := range state.PoolRegistrations {
		if registered {
			m.poolRegistrations[hash] = true

			// Insert into database
			pool := models.Pool{
				PoolKeyHash: hash[:],
			}
			if err := m.db.Create(&pool).Error; err != nil {
				return fmt.Errorf("failed to insert pool: %w", err)
			}
		}
	}

	// Load DRep registrations
	for _, hash := range state.DRepRegistrations {
		m.drepRegistrations[hash] = true

		// Insert into database
		drep := models.Drep{
			Credential: hash[:],
			AddedSlot:  0,
			Active:     true,
		}
		if err := m.db.Create(&drep).Error; err != nil {
			return fmt.Errorf("failed to insert drep: %w", err)
		}
	}

	// Load committee members
	maps.Copy(m.committeeMembers, state.CommitteeMembers)

	// Load hot key authorizations
	maps.Copy(m.hotKeyAuthorizations, state.HotKeyAuthorizations)

	// Insert hot key auths into database
	for coldKey, hotKey := range state.HotKeyAuthorizations {
		auth := models.AuthCommitteeHot{
			ColdCredential: coldKey[:],
			HostCredential: hotKey[:],
			AddedSlot:      0,
		}
		if err := m.db.Create(&auth).Error; err != nil {
			return fmt.Errorf("failed to insert auth_committee_hot: %w", err)
		}
	}

	// Load governance state
	m.govState = conformance.NewGovernanceState()
	m.govState.LoadFromParsedState(state)

	// Insert governance proposals into database
	for id, proposal := range state.Proposals {
		govProposal := m.proposalToModel(id, proposal)
		if err := m.db.Create(&govProposal).Error; err != nil {
			return fmt.Errorf("failed to insert governance proposal: %w", err)
		}
	}

	// Populate UTxOs from parsed state using the fully decoded Output
	for utxoId, parsedUtxo := range state.Utxos {
		// Create a mock transaction input for the UTxO ID
		var txHash common.Blake2b256
		copy(txHash[:], parsedUtxo.TxHash)

		mockInput := &dingoTransactionInput{
			txId:  txHash,
			index: parsedUtxo.Index,
		}

		// Use the decoded Output directly
		m.utxos[utxoId] = common.Utxo{
			Id:     mockInput,
			Output: parsedUtxo.Output,
		}

		// Insert into database
		utxoModel := models.Utxo{
			TxId:      parsedUtxo.TxHash,
			OutputIdx: parsedUtxo.Index,
			AddedSlot: 0,
		}
		if parsedUtxo.Output != nil {
			utxoModel.Cbor = parsedUtxo.Output.Cbor()
		}
		if err := m.db.Create(&utxoModel).Error; err != nil {
			return fmt.Errorf("failed to insert utxo: %w", err)
		}
	}

	return nil
}

// ApplyTransaction implements conformance.StateManager.ApplyTransaction.
func (m *DingoStateManager) ApplyTransaction(
	tx common.Transaction,
	slot uint64,
) error {
	// For phase-2 invalid transactions (IsValid=false), only consume collateral
	// and add collateral return output if present
	if !tx.IsValid() {
		// Consume collateral inputs
		for _, input := range tx.Collateral() {
			inputId := input.Id()
			inputIdx := input.Index()
			utxoId := fmt.Sprintf(
				"%s#%d",
				hex.EncodeToString(inputId.Bytes()),
				inputIdx,
			)
			delete(m.utxos, utxoId)

			// Mark as spent in database
			m.db.Model(&models.Utxo{}).
				Where("tx_id = ? AND output_idx = ?", inputId.Bytes(), inputIdx).
				Update("deleted_slot", slot)
		}

		// Add collateral return output if present
		if collateralReturn := tx.CollateralReturn(); collateralReturn != nil {
			txHash := tx.Hash()
			txHashStr := hex.EncodeToString(txHash.Bytes())
			//nolint:gosec // output count bounded by protocol max tx size
			returnIdx := uint32(len(tx.Outputs()))
			utxoId := fmt.Sprintf("%s#%d", txHashStr, returnIdx)

			mockInput := &dingoTransactionInput{
				txId:  txHash,
				index: returnIdx,
			}
			m.utxos[utxoId] = common.Utxo{
				Id:     mockInput,
				Output: collateralReturn,
			}

			// Insert into database
			utxoModel := models.Utxo{
				TxId:      txHash.Bytes(),
				OutputIdx: returnIdx,
				AddedSlot: slot,
				Cbor:      collateralReturn.Cbor(),
			}
			if err := m.db.Create(&utxoModel).Error; err != nil {
				return fmt.Errorf("failed to insert collateral return utxo: %w", err)
			}
		}

		return nil
	}

	// Get tx hash as string
	txHash := tx.Hash()
	txHashStr := hex.EncodeToString(txHash.Bytes())

	// Process consumed UTxOs (inputs)
	inputs := tx.Inputs()
	for _, input := range inputs {
		inputId := input.Id()
		inputIdx := input.Index()
		utxoId := fmt.Sprintf(
			"%s#%d",
			hex.EncodeToString(inputId.Bytes()),
			inputIdx,
		)
		delete(m.utxos, utxoId)

		// Mark as spent in database
		m.db.Model(&models.Utxo{}).
			Where("tx_id = ? AND output_idx = ?", inputId.Bytes(), inputIdx).
			Update("deleted_slot", slot)
	}

	// Process produced UTxOs (outputs)
	outputs := tx.Outputs()
	for idx, output := range outputs {
		utxoId := fmt.Sprintf("%s#%d", txHashStr, idx)

		// Create a mock transaction input for the UTxO ID
		mockInput := &dingoTransactionInput{
			txId:  txHash,
			index: uint32(idx), //nolint:gosec // idx bounded by tx outputs
		}

		m.utxos[utxoId] = common.Utxo{
			Id:     mockInput,
			Output: output,
		}

		// Insert into database
		utxoModel := models.Utxo{
			TxId:      txHash.Bytes(),
			OutputIdx: uint32(idx), //nolint:gosec // idx bounded by tx outputs
			AddedSlot: slot,
			Cbor:      output.Cbor(),
		}
		if err := m.db.Create(&utxoModel).Error; err != nil {
			return fmt.Errorf("failed to insert utxo: %w", err)
		}
	}

	// Process certificates
	certs := tx.Certificates()
	for _, cert := range certs {
		m.processCertificate(cert, slot)
	}

	// Process governance proposals
	proposals := tx.ProposalProcedures()
	for idx, proposal := range proposals {
		govActionId := fmt.Sprintf("%s#%d", txHashStr, idx)
		action := proposal.GovAction()
		if action != nil {
			// Get govActionLifetime from protocol parameters
			var govActionLifetime uint64 = 6 // Default fallback
			if conwayPP, ok := m.protocolParams.(*conway.ConwayProtocolParameters); ok {
				govActionLifetime = conwayPP.GovActionValidityPeriod
			}

			info := conformance.GovActionInfo{
				ActionType:      getActionType(action),
				ExpiresAfter:    m.currentEpoch + govActionLifetime,
				SubmittedEpoch:  m.currentEpoch,
				ProposedMembers: make(map[common.Blake2b224]uint64),
			}

			// Extract action-specific data including parent action ID
			extractActionSpecificData(action, &info)

			m.govState.AddProposal(govActionId, info)

			// Insert into database
			govProposal := m.proposalToModel(govActionId, info)
			govProposal.AddedSlot = slot
			m.db.Create(&govProposal)
		}
	}

	// Process voting procedures
	votes := tx.VotingProcedures()
	for voter, voteMap := range votes {
		for govActionId, votingProc := range voteMap {
			actionKey := fmt.Sprintf(
				"%s#%d",
				hex.EncodeToString(govActionId.TransactionId[:]),
				govActionId.GovActionIdx,
			)
			proposal := m.govState.GetProposal(actionKey)
			if proposal == nil {
				continue
			}
			if proposal.Votes == nil {
				proposal.Votes = make(map[string]uint8)
			}
			// Store vote as "voterType:credHash" -> vote value
			voterKey := fmt.Sprintf(
				"%d:%s",
				voter.Type,
				hex.EncodeToString(voter.Hash[:]),
			)
			proposal.Votes[voterKey] = votingProc.Vote
		}
	}

	return nil
}

// processCertificate processes a single certificate and updates state.
func (m *DingoStateManager) processCertificate(cert common.Certificate, slot uint64) {
	certType := common.CertificateType(cert.Type())

	//exhaustive:ignore
	switch certType {
	case common.CertificateTypeStakeRegistration:
		if regCert, ok := cert.(*common.StakeRegistrationCertificate); ok {
			credential := regCert.StakeCredential.Credential
			m.stakeRegistrations[credential] = 0
			m.govState.RegisterStake(credential)

			// Insert into database
			account := models.Account{
				StakingKey: credential[:],
				AddedSlot:  slot,
				Active:     true,
			}
			m.db.Create(&account)
		}

	case common.CertificateTypeRegistration:
		if regCert, ok := cert.(*common.RegistrationCertificate); ok {
			credential := regCert.StakeCredential.Credential
			m.stakeRegistrations[credential] = 0
			m.govState.RegisterStake(credential)

			// Insert into database
			account := models.Account{
				StakingKey: credential[:],
				AddedSlot:  slot,
				Active:     true,
			}
			m.db.Create(&account)
		}

	case common.CertificateTypeStakeRegistrationDelegation:
		if regCert, ok := cert.(*common.StakeRegistrationDelegationCertificate); ok {
			credential := regCert.StakeCredential.Credential
			m.stakeRegistrations[credential] = 0
			m.govState.RegisterStake(credential)

			// Insert into database
			account := models.Account{
				StakingKey: credential[:],
				AddedSlot:  slot,
				Active:     true,
			}
			m.db.Create(&account)
		}

	case common.CertificateTypeVoteRegistrationDelegation:
		if regCert, ok := cert.(*common.VoteRegistrationDelegationCertificate); ok {
			credential := regCert.StakeCredential.Credential
			m.stakeRegistrations[credential] = 0
			m.govState.RegisterStake(credential)

			// Insert into database
			account := models.Account{
				StakingKey: credential[:],
				AddedSlot:  slot,
				Active:     true,
			}
			m.db.Create(&account)
		}

	case common.CertificateTypeStakeVoteRegistrationDelegation:
		if regCert, ok := cert.(*common.StakeVoteRegistrationDelegationCertificate); ok {
			credential := regCert.StakeCredential.Credential
			m.stakeRegistrations[credential] = 0
			m.govState.RegisterStake(credential)

			// Insert into database
			account := models.Account{
				StakingKey: credential[:],
				AddedSlot:  slot,
				Active:     true,
			}
			m.db.Create(&account)
		}

	case common.CertificateTypeStakeDeregistration:
		if deregCert, ok := cert.(*common.StakeDeregistrationCertificate); ok {
			credential := deregCert.StakeCredential.Credential
			delete(m.stakeRegistrations, credential)
			m.govState.DeregisterStake(credential)

			// Update database
			m.db.Model(&models.Account{}).
				Where("staking_key = ?", credential[:]).
				Update("active", false)
		}

	case common.CertificateTypeDeregistration:
		if deregCert, ok := cert.(*common.DeregistrationCertificate); ok {
			credential := deregCert.StakeCredential.Credential
			delete(m.stakeRegistrations, credential)
			m.govState.DeregisterStake(credential)

			// Update database
			m.db.Model(&models.Account{}).
				Where("staking_key = ?", credential[:]).
				Update("active", false)
		}

	case common.CertificateTypePoolRegistration:
		if poolCert, ok := cert.(*common.PoolRegistrationCertificate); ok {
			poolId := poolCert.Operator
			m.poolRegistrations[poolId] = true
			m.govState.RegisterPool(poolId)

			// Insert or update database
			var pool models.Pool
			result := m.db.Where("pool_key_hash = ?", poolId[:]).First(&pool)
			if result.Error != nil {
				pool = models.Pool{PoolKeyHash: poolId[:]}
				m.db.Create(&pool)
			}
		}

	case common.CertificateTypePoolRetirement:
		if retireCert, ok := cert.(*common.PoolRetirementCertificate); ok {
			poolId := retireCert.PoolKeyHash
			retireEpoch := retireCert.Epoch
			m.govState.RetirePool(poolId, retireEpoch)

			// Insert retirement record
			var pool models.Pool
			if m.db.Where("pool_key_hash = ?", poolId[:]).First(&pool).Error == nil {
				retirement := models.PoolRetirement{
					PoolKeyHash: poolId[:],
					PoolID:      pool.ID,
					Epoch:       retireEpoch,
					AddedSlot:   slot,
				}
				m.db.Create(&retirement)
			}
		}

	case common.CertificateTypeRegistrationDrep:
		if drepCert, ok := cert.(*common.RegistrationDrepCertificate); ok {
			credential := drepCert.DrepCredential.Credential
			m.drepRegistrations[credential] = true
			m.govState.RegisterDRep(credential)

			// Insert into database
			drep := models.Drep{
				Credential: credential[:],
				AddedSlot:  slot,
				Active:     true,
			}
			m.db.Create(&drep)
		}

	case common.CertificateTypeDeregistrationDrep:
		if drepCert, ok := cert.(*common.DeregistrationDrepCertificate); ok {
			credential := drepCert.DrepCredential.Credential
			delete(m.drepRegistrations, credential)
			m.govState.DeregisterDRep(credential)

			// Update database
			m.db.Model(&models.Drep{}).
				Where("credential = ?", credential[:]).
				Update("active", false)
		}

	case common.CertificateTypeAuthCommitteeHot:
		if authCert, ok := cert.(*common.AuthCommitteeHotCertificate); ok {
			coldKey := authCert.ColdCredential.Credential
			hotKey := authCert.HotCredential.Credential
			m.hotKeyAuthorizations[coldKey] = hotKey
			m.govState.AuthorizeHotKey(coldKey, hotKey)

			// Insert into database
			auth := models.AuthCommitteeHot{
				ColdCredential: coldKey[:],
				HostCredential: hotKey[:],
				AddedSlot:      slot,
			}
			m.db.Create(&auth)
		}

	case common.CertificateTypeResignCommitteeCold:
		if resignCert, ok := cert.(*common.ResignCommitteeColdCertificate); ok {
			coldKey := resignCert.ColdCredential.Credential
			delete(m.hotKeyAuthorizations, coldKey)
			m.govState.ResignCommitteeMember(coldKey)

			// Insert into database
			resign := models.ResignCommitteeCold{
				ColdCredential: coldKey[:],
				AddedSlot:      slot,
			}
			m.db.Create(&resign)
		}

	default:
		// Other certificate types not relevant for state tracking
	}
}

// ProcessEpochBoundary implements conformance.StateManager.ProcessEpochBoundary.
func (m *DingoStateManager) ProcessEpochBoundary(newEpoch uint64) error {
	m.currentEpoch = newEpoch
	m.govState.CurrentEpoch = newEpoch

	// Snapshot pool retirements before processing
	retirementsSnapshot := maps.Clone(m.govState.PoolRetirements)

	// Process pool retirements in governance state
	m.govState.ProcessPoolRetirements(newEpoch)

	// Update local pool registrations using the snapshot
	for poolId, retireEpoch := range retirementsSnapshot {
		if newEpoch >= retireEpoch {
			delete(m.poolRegistrations, poolId)

			// Delete from database
			m.db.Where("pool_key_hash = ?", poolId[:]).Delete(&models.Pool{})
		}
	}

	// Phase 1: Enact proposals that were ratified in previous epochs
	var toEnact []string
	for id, proposal := range m.govState.Proposals {
		if proposal == nil {
			continue
		}
		if proposal.RatifiedEpoch != nil && newEpoch > *proposal.RatifiedEpoch {
			toEnact = append(toEnact, id)
		}
	}

	// Enact collected proposals
	for _, id := range toEnact {
		proposal := m.govState.Proposals[id]
		if proposal == nil {
			continue
		}
		// Info proposals cannot be enacted
		if proposal.ActionType == common.GovActionTypeInfo {
			continue
		}
		m.enactProposal(id, proposal)
	}

	// Phase 2: Ratify proposals that meet threshold requirements
	m.ratifyProposals(newEpoch)

	// Phase 3: Expire old proposals
	for id, proposal := range m.govState.Proposals {
		if proposal == nil {
			continue
		}
		if newEpoch > proposal.ExpiresAfter {
			delete(m.govState.Proposals, id)

			// Update database
			m.db.Model(&models.GovernanceProposal{}).
				Where("tx_hash = ?", parseProposalTxHash(id)).
				Update("deleted_slot", newEpoch*432000) // Approximate slot
		}
	}

	return nil
}

// ratifyProposals performs simplified proposal ratification.
func (m *DingoStateManager) ratifyProposals(currentEpoch uint64) {
	for id, proposal := range m.govState.Proposals {
		// Skip already-ratified proposals
		if proposal.RatifiedEpoch != nil {
			continue
		}

		// Require at least 1 epoch between submission and ratification
		if currentEpoch <= proposal.SubmittedEpoch {
			continue
		}

		// Info proposals are auto-ratified
		if proposal.ActionType == common.GovActionTypeInfo {
			epoch := currentEpoch
			proposal.RatifiedEpoch = &epoch
			m.govState.Proposals[id] = proposal

			// Update database
			m.db.Model(&models.GovernanceProposal{}).
				Where("tx_hash = ?", parseProposalTxHash(id)).
				Update("ratified_epoch", currentEpoch)
			continue
		}

		// Skip proposals that haven't been voted on
		if len(proposal.Votes) == 0 {
			continue
		}

		// Count YES votes by voter type
		voterTypesWithYes := make(map[uint8]bool)
		for voterKey, voteValue := range proposal.Votes {
			if voteValue != 1 {
				continue
			}
			if len(voterKey) > 0 {
				voterType := voterKey[0] - '0'
				voterTypesWithYes[voterType] = true
			}
		}

		// Check if required voter types have voted YES
		hasCC := voterTypesWithYes[0] || voterTypesWithYes[1]
		hasDRep := voterTypesWithYes[2] || voterTypesWithYes[3]
		hasSPO := voterTypesWithYes[4] || voterTypesWithYes[5]

		var meetsRequirements bool
		//exhaustive:ignore
		switch proposal.ActionType {
		case common.GovActionTypeNoConfidence,
			common.GovActionTypeHardForkInitiation:
			meetsRequirements = hasCC && hasDRep && hasSPO
		case common.GovActionTypeUpdateCommittee,
			common.GovActionTypeNewConstitution,
			common.GovActionTypeParameterChange,
			common.GovActionTypeTreasuryWithdrawal:
			meetsRequirements = hasCC && hasDRep
		default:
			meetsRequirements = len(voterTypesWithYes) >= 2
		}

		if !meetsRequirements {
			continue
		}

		// Ratify
		epoch := currentEpoch
		proposal.RatifiedEpoch = &epoch
		m.govState.Proposals[id] = proposal

		// Update database
		m.db.Model(&models.GovernanceProposal{}).
			Where("tx_hash = ?", parseProposalTxHash(id)).
			Update("ratified_epoch", currentEpoch)
	}
}

// enactProposal processes a ratified proposal.
func (m *DingoStateManager) enactProposal(
	id string,
	proposal *conformance.ProposalState,
) {
	//exhaustive:ignore
	switch proposal.ActionType {
	case common.GovActionTypeNewConstitution:
		m.govState.Roots.Constitution = &id
		if m.govState.Constitution == nil {
			m.govState.Constitution = &conformance.ConstitutionInfo{}
		}
		if len(proposal.PolicyHash) > 0 {
			m.govState.Constitution.PolicyHash = make(
				[]byte,
				len(proposal.PolicyHash),
			)
			copy(m.govState.Constitution.PolicyHash, proposal.PolicyHash)
		} else {
			m.govState.Constitution.PolicyHash = nil
		}
	case common.GovActionTypeParameterChange:
		m.govState.Roots.ProtocolParameters = &id
		if proposal.ParameterUpdate != nil {
			if conwayPP, ok := m.protocolParams.(*conway.ConwayProtocolParameters); ok {
				conwayPP.Update(proposal.ParameterUpdate)
			}
		}
	case common.GovActionTypeHardForkInitiation:
		m.govState.Roots.HardFork = &id
	case common.GovActionTypeNoConfidence, common.GovActionTypeUpdateCommittee:
		m.govState.Roots.ConstitutionalCommittee = &id
		if proposal.ActionType == common.GovActionTypeUpdateCommittee {
			for coldKey, expiry := range proposal.ProposedMembers {
				m.govState.CommitteeMembers[coldKey] = &conformance.CommitteeMemberInfo{
					ColdKey:     coldKey,
					ExpiryEpoch: expiry,
				}
				m.committeeMembers[coldKey] = expiry
			}
		}
	}

	// Mark as enacted
	m.govState.EnactedProposals[id] = true
	delete(m.govState.Proposals, id)

	// Update database
	m.db.Model(&models.GovernanceProposal{}).
		Where("tx_hash = ?", parseProposalTxHash(id)).
		Update("enacted_epoch", m.currentEpoch)
}

// GetStateProvider implements conformance.StateManager.GetStateProvider.
func (m *DingoStateManager) GetStateProvider() conformance.StateProvider {
	return NewDingoStateProvider(m)
}

// GetGovernanceState implements conformance.StateManager.GetGovernanceState.
func (m *DingoStateManager) GetGovernanceState() *conformance.GovernanceState {
	return m.govState
}

// SetRewardBalances implements conformance.StateManager.SetRewardBalances.
func (m *DingoStateManager) SetRewardBalances(
	balances map[common.Blake2b224]uint64,
) {
	for cred, balance := range balances {
		if _, exists := m.stakeRegistrations[cred]; exists {
			m.stakeRegistrations[cred] = balance
		}
	}
	if m.govState != nil {
		for cred, balance := range balances {
			if m.govState.StakeRegistrations[cred] {
				m.govState.RewardAccounts[cred] = balance
			}
		}
	}
}

// GetProtocolParameters implements conformance.StateManager.GetProtocolParameters.
func (m *DingoStateManager) GetProtocolParameters() common.ProtocolParameters {
	return m.protocolParams
}

// Reset implements conformance.StateManager.Reset.
func (m *DingoStateManager) Reset() error {
	m.protocolParams = nil
	m.currentEpoch = 0
	m.utxos = make(map[string]common.Utxo)
	m.stakeRegistrations = make(map[common.Blake2b224]uint64)
	m.poolRegistrations = make(map[common.Blake2b224]bool)
	m.drepRegistrations = make(map[common.Blake2b224]bool)
	m.committeeMembers = make(map[common.Blake2b224]uint64)
	m.hotKeyAuthorizations = make(map[common.Blake2b224]common.Blake2b224)
	m.govState = conformance.NewGovernanceState()

	// Clear database tables
	return m.clearDatabaseTables()
}

// clearDatabaseTables clears all relevant database tables.
func (m *DingoStateManager) clearDatabaseTables() error {
	tables := []string{
		"utxo",
		"account",
		"pool",
		"pool_registration",
		"pool_retirement",
		"drep",
		"auth_committee_hot",
		"resign_committee_cold",
		"governance_proposal",
	}
	for _, table := range tables {
		if err := m.db.Exec("DELETE FROM " + table).Error; err != nil {
			// Table might not exist, which is fine
			continue
		}
	}
	return nil
}

// proposalToModel converts a governance proposal to a database model.
func (m *DingoStateManager) proposalToModel(
	id string,
	info conformance.GovActionInfo,
) models.GovernanceProposal {
	txHash := parseProposalTxHash(id)
	actionIdx := parseProposalActionIdx(id)

	proposal := models.GovernanceProposal{
		TxHash:        txHash,
		ActionIndex:   actionIdx,
		ActionType:    uint8(info.ActionType), //nolint:gosec // GovActionType is 0-6
		ProposedEpoch: info.SubmittedEpoch,
		ExpiresEpoch:  info.ExpiresAfter,
		PolicyHash:    info.PolicyHash,
	}

	if info.ParentActionId != nil {
		parentTxHash := parseProposalTxHash(*info.ParentActionId)
		parentIdx := parseProposalActionIdx(*info.ParentActionId)
		proposal.ParentTxHash = parentTxHash
		proposal.ParentActionIdx = &parentIdx
	}

	if info.RatifiedEpoch != nil {
		proposal.RatifiedEpoch = info.RatifiedEpoch
	}

	return proposal
}

// Helper functions

func parseProposalTxHash(id string) []byte {
	if len(id) < 64 {
		return nil
	}
	hashStr := id[:64]
	hashBytes, _ := hex.DecodeString(hashStr)
	return hashBytes
}

func parseProposalActionIdx(id string) uint32 {
	if len(id) < 66 {
		return 0
	}
	idxStr := id[65:]
	var idx uint32
	_, _ = fmt.Sscanf(idxStr, "%d", &idx)
	return idx
}

func getActionType(action common.GovAction) common.GovActionType {
	switch action.(type) {
	case *common.HardForkInitiationGovAction:
		return common.GovActionTypeHardForkInitiation
	case *common.TreasuryWithdrawalGovAction:
		return common.GovActionTypeTreasuryWithdrawal
	case *common.NoConfidenceGovAction:
		return common.GovActionTypeNoConfidence
	case *common.UpdateCommitteeGovAction:
		return common.GovActionTypeUpdateCommittee
	case *common.NewConstitutionGovAction:
		return common.GovActionTypeNewConstitution
	case *common.InfoGovAction:
		return common.GovActionTypeInfo
	default:
		return common.GovActionTypeParameterChange
	}
}

func extractActionSpecificData(action common.GovAction, info *conformance.GovActionInfo) {
	switch ga := action.(type) {
	case *common.UpdateCommitteeGovAction:
		if ga.ActionId != nil {
			key := fmt.Sprintf(
				"%x#%d",
				ga.ActionId.TransactionId[:],
				ga.ActionId.GovActionIdx,
			)
			info.ParentActionId = &key
		}
		for cred, epoch := range ga.CredEpochs {
			if cred != nil {
				info.ProposedMembers[cred.Credential] = uint64(epoch)
			}
		}
	case *common.NoConfidenceGovAction:
		if ga.ActionId != nil {
			key := fmt.Sprintf(
				"%x#%d",
				ga.ActionId.TransactionId[:],
				ga.ActionId.GovActionIdx,
			)
			info.ParentActionId = &key
		}
	case *common.HardForkInitiationGovAction:
		if ga.ActionId != nil {
			key := fmt.Sprintf(
				"%x#%d",
				ga.ActionId.TransactionId[:],
				ga.ActionId.GovActionIdx,
			)
			info.ParentActionId = &key
		}
		info.ProtocolVersion = &conformance.ProtocolVersionInfo{
			Major: ga.ProtocolVersion.Major,
			Minor: ga.ProtocolVersion.Minor,
		}
	case *common.NewConstitutionGovAction:
		if ga.ActionId != nil {
			key := fmt.Sprintf(
				"%x#%d",
				ga.ActionId.TransactionId[:],
				ga.ActionId.GovActionIdx,
			)
			info.ParentActionId = &key
		}
		if len(ga.Constitution.ScriptHash) > 0 {
			info.PolicyHash = make([]byte, len(ga.Constitution.ScriptHash))
			copy(info.PolicyHash, ga.Constitution.ScriptHash)
		}
	case *conway.ConwayParameterChangeGovAction:
		if ga.ActionId != nil {
			key := fmt.Sprintf(
				"%x#%d",
				ga.ActionId.TransactionId[:],
				ga.ActionId.GovActionIdx,
			)
			info.ParentActionId = &key
		}
		info.ParameterUpdate = &ga.ParamUpdate
	}
}

// dingoTransactionInput implements common.TransactionInput for mock UTxOs.
type dingoTransactionInput struct {
	txId  common.Blake2b256
	index uint32
}

func (d *dingoTransactionInput) Id() common.Blake2b256 {
	return d.txId
}

func (d *dingoTransactionInput) Index() uint32 {
	return d.index
}

func (d *dingoTransactionInput) String() string {
	return fmt.Sprintf("%x#%d", d.txId[:], d.index)
}

func (d *dingoTransactionInput) Utxorpc() (*utxorpc.TxInput, error) {
	return &utxorpc.TxInput{
		TxHash:      d.txId[:],
		OutputIndex: d.index,
	}, nil
}

func (d *dingoTransactionInput) ToPlutusData() data.PlutusData {
	return data.NewConstr(0,
		data.NewByteString(d.txId[:]),
		data.NewInteger(big.NewInt(int64(d.index))),
	)
}

// Close closes the database connection.
func (m *DingoStateManager) Close() error {
	sqlDB, err := m.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// Compile-time interface check
var _ conformance.StateManager = (*DingoStateManager)(nil)
