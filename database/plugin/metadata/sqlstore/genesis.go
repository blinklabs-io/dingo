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

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/certutil"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/leioskeyepoch"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

func (s *Store) SetGenesisStaking(
	pools map[string]lcommon.PoolRegistrationCertificate,
	stakeDelegations map[string]string,
	keyDeposit uint64,
	blockHash []byte,
	txn types.Txn,
) error {
	return s.SetGenesisStakingWithDeposits(
		pools,
		stakeDelegations,
		keyDeposit,
		0,
		blockHash,
		txn,
	)
}

func (s *Store) SetGenesisStakingWithDeposits(
	pools map[string]lcommon.PoolRegistrationCertificate,
	stakeDelegations map[string]string,
	keyDeposit uint64,
	poolDeposit uint64,
	_ []byte,
	txn types.Txn,
) error {
	for _, certificate := range pools {
		if s.poolsDrained() {
			break
		}
		rewardTag, rewardHash, err := certutil.PoolRewardAccount(&certificate)
		if err != nil {
			return fmt.Errorf("pool reward account: %w", err)
		}
		pool := &models.Pool{
			PoolKeyHash:                certificate.Operator[:],
			VrfKeyHash:                 certificate.VrfKeyHash[:],
			RewardAccount:              rewardHash,
			RewardAccountCredentialTag: rewardTag,
			Pledge:                     types.Uint64(certificate.Pledge),
			Cost:                       types.Uint64(certificate.Cost),
			Margin:                     &types.Rat{Rat: certificate.Margin.Rat},
		}
		if certificate.LeiosKey != nil {
			pool.LeiosKeyPublic = append(
				[]byte(nil), certificate.LeiosKey.PublicKey...,
			)
			pool.LeiosKeyPossessionProof = append(
				[]byte(nil), certificate.LeiosKey.PossessionProof...,
			)
		}
		reg, err := models.PoolRegistrationFromCertificate(&certificate, 0)
		if err != nil {
			return fmt.Errorf("pool registration: %w", err)
		}
		reg.DepositAmount = types.Uint64(poolDeposit)
		if certificate.LeiosKey != nil {
			// The ledger registers genesis pools with mkStakePoolState at nesEL
			// (cardano-ledger 9df9f98c, Shelley/Transition.hs).
			genesisEpoch := leioskeyepoch.GenesisEpoch
			reg.LeiosKeyRegistrationEpoch = &genesisEpoch
		}
		registration := &reg
		if err := s.ImportPool(pool, registration, txn); err != nil {
			return fmt.Errorf("create genesis pool: %w", err)
		}
	}
	if s.familyDrained(nodesettings.FamilyAccounts) {
		return nil
	}
	refs := []models.StakeCredentialRef{}
	accounts, err := models.GenesisStakeAccounts(stakeDelegations, keyDeposit)
	if err != nil {
		return err
	}
	for i := range accounts {
		if err := s.ImportAccount(&accounts[i], txn); err != nil {
			return fmt.Errorf("create genesis account: %w", err)
		}
		refs = append(
			refs,
			models.NewStakeCredentialRef(0, accounts[i].StakingKey),
		)
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	return s.refreshRewardLiveStakeRefs(ctx, db, refs, 0)
}

func (s *Store) SetGenesisGovernance(
	initialDReps conway.ConwayGenesisInitialDReps,
	delegations conway.ConwayGenesisDelegs,
	_ []byte,
	txn types.Txn,
) error {
	// A drained family's tables are skipped; the delegation and registration
	// certificate history is written either way.
	accounts := s.familyDrained(nodesettings.FamilyAccounts)
	dreps := s.familyDrained(nodesettings.FamilyDReps)
	for credential, state := range initialDReps {
		if credential == nil {
			continue
		}
		drepRow, registrationRow, err := models.GenesisDRep(credential, state)
		if err != nil {
			return err
		}
		drep, registration := &drepRow, &registrationRow
		if dreps {
			// The registration certificate history outlives the drained
			// DRep tables.
			if err := s.importGenesisDrepRegistration(registration, txn); err != nil {
				return fmt.Errorf("create genesis drep registration: %w", err)
			}
			continue
		}
		if err := s.ImportDrep(drep, registration, txn); err != nil {
			return fmt.Errorf("create genesis drep: %w", err)
		}
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	refs := []models.StakeCredentialRef{}
	for credential, delegatee := range delegations {
		if credential == nil {
			continue
		}
		tag, err := models.CredentialTagFromUint(credential.CredType)
		if err != nil {
			return err
		}
		stakeKey := credential.Credential[:]
		effect, err := models.GenesisDelegateeOf(delegatee)
		if err != nil {
			return err
		}
		pool, drepCredential, drepType := effect.Pool, effect.Drep, effect.DrepType
		if !accounts {
			if err := insertGenesisAccount(
				ctx, db, stakeKey, tag, pool, drepCredential, drepType,
			); err != nil {
				return err
			}
		}
		switch delegatee.Type {
		case conway.ConwayGenesisDelegateeTypeStake:
			err = insertGenesisDelegation(
				ctx,
				db,
				"stake_delegation",
				"pool_key_hash",
				stakeKey,
				tag,
				pool,
				0,
			)
		case conway.ConwayGenesisDelegateeTypeVote:
			err = insertGenesisDelegation(
				ctx,
				db,
				"vote_delegation",
				"drep",
				stakeKey,
				tag,
				drepCredential,
				drepType,
			)
		case conway.ConwayGenesisDelegateeTypeStakeVote:
			_, err = db.ExecContext(ctx, `
INSERT INTO stake_vote_delegation (
    staking_key, drep, pool_key_hash, certificate_id, credential_tag,
    drep_type, added_slot
)
SELECT ?, ?, ?, 0, ?, ?, 0
WHERE NOT EXISTS (
    SELECT 1 FROM stake_vote_delegation
    WHERE credential_tag = ? AND staking_key = ? AND added_slot = 0
)`,
				stakeKey,
				nullBytes(drepCredential),
				pool,
				tag,
				drepType,
				tag,
				stakeKey,
			)
		}
		if err != nil {
			return err
		}
		if len(drepCredential) > 0 && !dreps {
			if err := addDrepDelegator(
				ctx,
				db,
				uint8(drepType), //nolint:gosec // GenesisDelegateeOf bounds a credential DRep type to 0 or 1
				drepCredential,
				tag,
				stakeKey,
				0,
			); err != nil {
				return fmt.Errorf("create genesis DRep delegator: %w", err)
			}
		}
		refs = append(refs, models.NewStakeCredentialRef(tag, stakeKey))
	}
	return s.refreshRewardLiveStakeRefs(ctx, db, refs, 0)
}

// insertGenesisAccount writes the account a Conway genesis delegation
// creates or updates, with its slot-0 registration.
func insertGenesisAccount(
	ctx context.Context,
	db queryer,
	stakeKey []byte,
	tag uint8,
	pool, drepCredential []byte,
	drepType uint64,
) error {
	if _, err := db.ExecContext(ctx, `
INSERT INTO account (
    staking_key, pool, drep, reward, active, added_slot,
    credential_tag, drep_type, expiration_epoch, created_slot
) VALUES (?, ?, ?, '0', TRUE, 0, ?, ?, 0, 0)
ON CONFLICT (credential_tag, staking_key) DO UPDATE SET
    pool = excluded.pool, drep = excluded.drep,
    drep_type = excluded.drep_type, active = TRUE`,
		stakeKey,
		nullBytes(pool),
		nullBytes(drepCredential),
		tag,
		drepType,
	); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO registration (
    staking_key, certificate_id, credential_tag, added_slot, deposit_amount
)
SELECT ?, 0, ?, 0, '0'
WHERE NOT EXISTS (
    SELECT 1 FROM registration
    WHERE credential_tag = ? AND staking_key = ? AND added_slot = 0
)`,
		stakeKey,
		tag,
		tag,
		stakeKey,
	); err != nil {
		return err
	}
	return nil
}

// importGenesisDrepRegistration writes the registration certificate of a
// genesis DRep without the drained DRep tables.
func (s *Store) importGenesisDrepRegistration(
	registration *models.RegistrationDrep,
	txn types.Txn,
) error {
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			params, err := drepRegistrationParams(registration)
			if err != nil {
				return err
			}
			_, err = s.operationalQueries(db).ImportDrepRegistration(ctx, params)
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		},
	)
}

func insertGenesisDelegation(
	ctx context.Context,
	db queryer,
	table string,
	valueColumn string,
	stakeKey []byte,
	tag uint8,
	value []byte,
	drepType uint64,
) error {
	columns := "staking_key, " + valueColumn +
		", certificate_id, credential_tag, added_slot"
	values := "?, ?, 0, ?, 0"
	args := []any{stakeKey, nullBytes(value), tag, tag, stakeKey}
	if table == "vote_delegation" {
		columns += ", drep_type"
		values = "?, ?, 0, ?, 0, ?"
		args = []any{
			stakeKey,
			nullBytes(value),
			tag,
			drepType,
			tag,
			stakeKey,
		}
	}
	_, err := db.ExecContext(ctx, `
INSERT INTO `+table+` (`+columns+`)
SELECT `+values+`
WHERE NOT EXISTS (
    SELECT 1 FROM `+table+`
    WHERE credential_tag = ? AND staking_key = ? AND added_slot = 0
)`,
		args...,
	)
	return err
}
