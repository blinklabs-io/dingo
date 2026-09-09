// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package sqlstore

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/certutil"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

func (s *Store) SetGenesisStaking(
	pools map[string]lcommon.PoolRegistrationCertificate,
	stakeDelegations map[string]string,
	_ []byte,
	txn types.Txn,
) error {
	for _, certificate := range pools {
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
		registration := &models.PoolRegistration{
			PoolKeyHash:                certificate.Operator[:],
			VrfKeyHash:                 certificate.VrfKeyHash[:],
			RewardAccount:              rewardHash,
			RewardAccountCredentialTag: rewardTag,
			Pledge:                     types.Uint64(certificate.Pledge),
			Cost:                       types.Uint64(certificate.Cost),
			Margin:                     &types.Rat{Rat: certificate.Margin.Rat},
			AddedSlot:                  0,
		}
		if certificate.PoolMetadata != nil {
			registration.MetadataUrl = certificate.PoolMetadata.Url
			registration.MetadataHash = certificate.PoolMetadata.Hash[:]
		}
		for _, owner := range certificate.PoolOwners {
			registration.Owners = append(
				registration.Owners,
				models.PoolRegistrationOwner{KeyHash: owner[:]},
			)
		}
		for _, relay := range certificate.Relays {
			model := models.PoolRegistrationRelay{
				Ipv4: relay.Ipv4,
				Ipv6: relay.Ipv6,
			}
			if relay.Port != nil {
				model.Port = uint(*relay.Port)
			}
			if relay.Hostname != nil {
				model.Hostname = *relay.Hostname
			}
			registration.Relays = append(registration.Relays, model)
		}
		if err := s.ImportPool(pool, registration, txn); err != nil {
			return fmt.Errorf("create genesis pool: %w", err)
		}
	}
	refs := []models.StakeCredentialRef{}
	for stakerHex, poolHex := range stakeDelegations {
		staker, err := hex.DecodeString(stakerHex)
		if err != nil {
			return fmt.Errorf("decode staker hash %s: %w", stakerHex, err)
		}
		pool, err := hex.DecodeString(poolHex)
		if err != nil {
			return fmt.Errorf("decode pool hash %s: %w", poolHex, err)
		}
		if err := s.ImportAccount(&models.Account{
			StakingKey:    staker,
			CredentialTag: 0,
			Pool:          pool,
			Active:        true,
			AddedSlot:     0,
			CreatedSlot:   0,
		}, txn); err != nil {
			return fmt.Errorf("create genesis account: %w", err)
		}
		refs = append(refs, models.NewStakeCredentialRef(0, staker))
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
	for credential, state := range initialDReps {
		if credential == nil {
			continue
		}
		tag, err := models.CredentialTagFromUint(credential.CredType)
		if err != nil {
			return fmt.Errorf("genesis drep credential type: %w", err)
		}
		drep := &models.Drep{
			CredentialTag: tag,
			Credential:    credential.Credential[:],
			AddedSlot:     0,
			ExpiryEpoch:   state.Expiry,
			Active:        true,
		}
		registration := &models.RegistrationDrep{
			CredentialTag:  tag,
			DrepCredential: credential.Credential[:],
			AddedSlot:      0,
			DepositAmount:  types.Uint64(state.Deposit),
		}
		if state.Anchor != nil {
			drep.AnchorURL = state.Anchor.Url
			drep.AnchorHash = state.Anchor.DataHash[:]
			registration.AnchorURL = state.Anchor.Url
			registration.AnchorHash = state.Anchor.DataHash[:]
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
		drepType, drepErr := models.DrepTypeFromInt(delegatee.DRep.Type)
		if drepErr != nil &&
			delegatee.Type != conway.ConwayGenesisDelegateeTypeStake {
			return fmt.Errorf("genesis delegatee drep type: %w", drepErr)
		}
		var drepCredential []byte
		if delegatee.Type != conway.ConwayGenesisDelegateeTypeStake &&
			drepType != models.DrepTypeAlwaysAbstain &&
			drepType != models.DrepTypeAlwaysNoConfidence {
			drepCredential = delegatee.DRep.Credential
		}
		var pool []byte
		switch delegatee.Type {
		case conway.ConwayGenesisDelegateeTypeStake:
			pool = delegatee.PoolId[:]
		case conway.ConwayGenesisDelegateeTypeVote:
		case conway.ConwayGenesisDelegateeTypeStakeVote:
			pool = delegatee.PoolId[:]
		default:
			return fmt.Errorf(
				"unknown genesis delegatee type: %d",
				delegatee.Type,
			)
		}
		if _, err := db.ExecContext(ctx, `
INSERT INTO account (
    staking_key, pool, drep, reward, id, active, added_slot,
    credential_tag, drep_type, expiration_epoch, created_slot
) VALUES (?, ?, ?, '0', NULL, TRUE, 0, ?, ?, 0, 0)
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
		refs = append(refs, models.NewStakeCredentialRef(tag, stakeKey))
	}
	return s.refreshRewardLiveStakeRefs(ctx, db, refs, 0)
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
