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

//nolint:gosec // SQL INTEGER mappings preserve the existing unsigned domain API.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/certutil"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

var certificateTables = []string{
	"stake_registration",
	"pool_registration",
	"pool_retirement",
	"auth_committee_hot",
	"resign_committee_cold",
	"deregistration",
	"stake_deregistration",
	"stake_delegation",
	"stake_registration_delegation",
	"stake_vote_delegation",
	"stake_vote_registration_delegation",
	"registration",
	"registration_drep",
	"deregistration_drep",
	"update_drep",
	"vote_delegation",
	"vote_registration_delegation",
	"move_instantaneous_rewards",
	"genesis_delegation",
}

type certificateAccountState struct {
	active   bool
	pool     []byte
	drep     []byte
	drepType uint64
}

func (s *Store) applyTransactionCertificates(
	ctx context.Context,
	db queryer,
	transactionID int64,
	certificates []lcommon.Certificate,
	point ocommon.Point,
	blockIndex uint32,
	deposits map[int]uint64,
	allowUnknownDeposits bool,
) ([]models.StakeCredentialRef, error) {
	if len(certificates) == 0 {
		return nil, nil
	}
	if err := deleteSpecializedCertificates(ctx, db, transactionID); err != nil {
		return nil, err
	}
	refs := make(map[string]models.StakeCredentialRef)
	for certIndex, certificate := range certificates {
		certType, err := certificateType(certificate)
		if err != nil {
			return nil, err
		}
		unifiedID, err := queryReturnedID(ctx, db, `
INSERT INTO certs (
    block_hash, transaction_id, certificate_id, slot, cert_index, cert_type
) VALUES (?, ?, 0, ?, ?, ?)
ON CONFLICT (transaction_id, cert_index) DO UPDATE SET
    block_hash = excluded.block_hash,
    certificate_id = 0,
    slot = excluded.slot,
    cert_type = excluded.cert_type
RETURNING id`,
			point.Hash,
			transactionID,
			point.Slot,
			certIndex,
			certType,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"create certificate %d: %w",
				certIndex,
				err,
			)
		}
		deposit, found := deposits[certIndex]
		if certificateRequiresDeposit(certificate) && deposits == nil && !allowUnknownDeposits {
			return nil, fmt.Errorf(
				"missing certDeposits for deposit-bearing certificate at index %d",
				certIndex,
			)
		}
		var depositValue any
		if found {
			depositValue = decimalUint64(types.Uint64(deposit))
		} else if !allowUnknownDeposits {
			depositValue = decimalUint64(types.Uint64(0))
		}
		specializedID, ref, err := s.applySpecializedCertificate(
			ctx,
			db,
			certificate,
			uint(unifiedID),
			point.Slot,
			blockIndex,
			uint(certIndex),
			depositValue,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"process certificate %d (%T): %w",
				certIndex,
				certificate,
				err,
			)
		}
		if _, err := db.ExecContext(ctx, `
UPDATE certs SET certificate_id = ? WHERE id = ?`,
			specializedID,
			unifiedID,
		); err != nil {
			return nil, err
		}
		if ref != nil {
			refs[ref.MapKey()] = *ref
		}
	}
	ret := make([]models.StakeCredentialRef, 0, len(refs))
	for _, ref := range refs {
		ret = append(ret, ref)
	}
	return ret, nil
}

func deleteSpecializedCertificates(
	ctx context.Context,
	db queryer,
	transactionID int64,
) error {
	if _, err := db.ExecContext(ctx, `
DELETE FROM pool_registration_owner
WHERE pool_registration_id IN (
    SELECT pool_registration.id
    FROM pool_registration
    JOIN certs ON certs.id = pool_registration.certificate_id
    WHERE certs.transaction_id = ?
)`,
		transactionID,
	); err != nil {
		return fmt.Errorf("delete existing pool registration owners: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
DELETE FROM pool_registration_relay
WHERE pool_registration_id IN (
    SELECT pool_registration.id
    FROM pool_registration
    JOIN certs ON certs.id = pool_registration.certificate_id
    WHERE certs.transaction_id = ?
)`,
		transactionID,
	); err != nil {
		return fmt.Errorf("delete existing pool registration relays: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
DELETE FROM move_instantaneous_rewards_reward
WHERE mir_id IN (
    SELECT move_instantaneous_rewards.id
    FROM move_instantaneous_rewards
    JOIN certs ON certs.id = move_instantaneous_rewards.certificate_id
    WHERE certs.transaction_id = ?
)`,
		transactionID,
	); err != nil {
		return fmt.Errorf("delete existing MIR rewards: %w", err)
	}
	for _, table := range certificateTables {
		query := "DELETE FROM " + table +
			" WHERE certificate_id IN (" +
			"SELECT id FROM certs WHERE transaction_id = ?)"
		if _, err := db.ExecContext(
			ctx,
			query,
			transactionID,
		); err != nil {
			return fmt.Errorf(
				"delete existing %s records: %w",
				table,
				err,
			)
		}
	}
	return nil
}

func certificateRequiresDeposit(certificate lcommon.Certificate) bool {
	switch certificate.(type) {
	case *lcommon.PoolRegistrationCertificate,
		*lcommon.RegistrationCertificate,
		*lcommon.RegistrationDrepCertificate,
		*lcommon.StakeRegistrationCertificate,
		*lcommon.StakeRegistrationDelegationCertificate,
		*lcommon.StakeVoteRegistrationDelegationCertificate,
		*lcommon.VoteRegistrationDelegationCertificate:
		return true
	default:
		return false
	}
}

func certificateType(certificate lcommon.Certificate) (uint, error) {
	switch certificate.(type) {
	case *lcommon.PoolRegistrationCertificate:
		return uint(lcommon.CertificateTypePoolRegistration), nil
	case *lcommon.StakeRegistrationCertificate:
		return uint(lcommon.CertificateTypeStakeRegistration), nil
	case *lcommon.GenesisKeyDelegationCertificate:
		return uint(lcommon.CertificateTypeGenesisKeyDelegation), nil
	case *lcommon.PoolRetirementCertificate:
		return uint(lcommon.CertificateTypePoolRetirement), nil
	case *lcommon.StakeDeregistrationCertificate:
		return uint(lcommon.CertificateTypeStakeDeregistration), nil
	case *lcommon.DeregistrationCertificate:
		return uint(lcommon.CertificateTypeDeregistration), nil
	case *lcommon.StakeDelegationCertificate:
		return uint(lcommon.CertificateTypeStakeDelegation), nil
	case *lcommon.StakeRegistrationDelegationCertificate:
		return uint(lcommon.CertificateTypeStakeRegistrationDelegation), nil
	case *lcommon.StakeVoteDelegationCertificate:
		return uint(lcommon.CertificateTypeStakeVoteDelegation), nil
	case *lcommon.RegistrationCertificate:
		return uint(lcommon.CertificateTypeRegistration), nil
	case *lcommon.RegistrationDrepCertificate:
		return uint(lcommon.CertificateTypeRegistrationDrep), nil
	case *lcommon.DeregistrationDrepCertificate:
		return uint(lcommon.CertificateTypeDeregistrationDrep), nil
	case *lcommon.UpdateDrepCertificate:
		return uint(lcommon.CertificateTypeUpdateDrep), nil
	case *lcommon.StakeVoteRegistrationDelegationCertificate:
		return uint(lcommon.CertificateTypeStakeVoteRegistrationDelegation), nil
	case *lcommon.VoteRegistrationDelegationCertificate:
		return uint(lcommon.CertificateTypeVoteRegistrationDelegation), nil
	case *lcommon.VoteDelegationCertificate:
		return uint(lcommon.CertificateTypeVoteDelegation), nil
	case *lcommon.AuthCommitteeHotCertificate:
		return uint(lcommon.CertificateTypeAuthCommitteeHot), nil
	case *lcommon.ResignCommitteeColdCertificate:
		return uint(lcommon.CertificateTypeResignCommitteeCold), nil
	case *lcommon.MoveInstantaneousRewardsCertificate:
		return uint(lcommon.CertificateTypeMoveInstantaneousRewards), nil
	default:
		return 0, fmt.Errorf("unsupported certificate type %T", certificate)
	}
}

// applySpecializedCertificate persists the certificate-specific row and
// returns its ID plus the reward-account credential whose live stake needs
// refreshing, if any.
func (s *Store) applySpecializedCertificate(
	ctx context.Context,
	db queryer,
	certificate lcommon.Certificate,
	certificateID uint,
	slot uint64,
	blockIndex uint32,
	certIndex uint,
	deposit any,
) (uint, *models.StakeCredentialRef, error) {
	switch cert := certificate.(type) {
	case *lcommon.PoolRegistrationCertificate:
		id, err := applyPoolRegistrationCertificate(
			ctx,
			db,
			cert,
			certificateID,
			slot,
			deposit,
		)
		return id, nil, err
	case *lcommon.PoolRetirementCertificate:
		id, err := applyPoolRetirementCertificate(
			ctx,
			db,
			cert,
			certificateID,
			slot,
		)
		return id, nil, err
	case *lcommon.GenesisKeyDelegationCertificate:
		id, err := insertCertificateRow(ctx, db, `
INSERT INTO genesis_delegation (
    genesis_hash, genesis_delegate_hash, vrf_key_hash, added_slot,
    block_index, cert_index, certificate_id
) VALUES (?, ?, ?, ?, ?, ?, ?)
RETURNING id`,
			cert.GenesisHash,
			cert.GenesisDelegateHash,
			cert.VrfKeyHash[:],
			slot,
			blockIndex,
			certIndex,
			certificateID,
		)
		return id, nil, err
	case *lcommon.RegistrationDrepCertificate:
		id, err := applyDrepRegistrationCertificate(
			ctx,
			db,
			cert,
			certificateID,
			slot,
			deposit,
		)
		return id, nil, err
	case *lcommon.DeregistrationDrepCertificate:
		id, err := applyDrepDeregistrationCertificate(
			ctx,
			db,
			cert,
			certificateID,
			slot,
			uint64(cert.Amount),
		)
		return id, nil, err
	case *lcommon.UpdateDrepCertificate:
		id, err := applyDrepUpdateCertificate(
			ctx,
			db,
			cert,
			certificateID,
			slot,
		)
		return id, nil, err
	case *lcommon.AuthCommitteeHotCertificate:
		// CredType is decoded from CBOR without a range check. Storing it raw
		// would write a tag the validated uint8 writers can never match, so
		// the member would silently drop out of the active committee.
		coldTag, err := models.CredentialTagFromUint(
			cert.ColdCredential.CredType,
		)
		if err != nil {
			return 0, nil, err
		}
		hotTag, err := models.CredentialTagFromUint(
			cert.HotCredential.CredType,
		)
		if err != nil {
			return 0, nil, err
		}
		id, err := insertCertificateRow(ctx, db, `
INSERT INTO auth_committee_hot (
    cold_credential_tag, cold_credential, hot_credential_tag,
    host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?, ?, ?)
RETURNING id`,
			coldTag,
			cert.ColdCredential.Credential[:],
			hotTag,
			cert.HotCredential.Credential[:],
			certificateID,
			slot,
		)
		return id, nil, err
	case *lcommon.ResignCommitteeColdCertificate:
		var anchorURL string
		var anchorHash []byte
		if cert.Anchor != nil {
			anchorURL = cert.Anchor.Url
			anchorHash = cert.Anchor.DataHash[:]
		}
		coldTag, err := models.CredentialTagFromUint(
			cert.ColdCredential.CredType,
		)
		if err != nil {
			return 0, nil, err
		}
		id, err := insertCertificateRow(ctx, db, `
INSERT INTO resign_committee_cold (
    anchor_url, cold_credential_tag, cold_credential, anchor_hash,
    certificate_id, added_slot
) VALUES (?, ?, ?, ?, ?, ?)
RETURNING id`,
			anchorURL,
			coldTag,
			cert.ColdCredential.Credential[:],
			anchorHash,
			certificateID,
			slot,
		)
		return id, nil, err
	case *lcommon.MoveInstantaneousRewardsCertificate:
		id, err := applyMIRCertificate(
			ctx,
			db,
			cert,
			certificateID,
			slot,
		)
		return id, nil, err
	}
	return applyAccountCertificate(
		ctx,
		db,
		certificate,
		certificateID,
		slot,
		deposit,
	)
}

func applyAccountCertificate(
	ctx context.Context,
	db queryer,
	certificate lcommon.Certificate,
	certificateID uint,
	slot uint64,
	deposit any,
) (uint, *models.StakeCredentialRef, error) {
	var (
		stakeCredential lcommon.Credential
		state           = certificateAccountState{active: true}
		table           string
		columns         string
		values          string
		args            []any
	)
	switch cert := certificate.(type) {
	case *lcommon.StakeRegistrationCertificate:
		stakeCredential = cert.StakeCredential
		table = "stake_registration"
		columns = "staking_key, credential_tag, added_slot, deposit_amount, certificate_id"
		values = "?, ?, ?, ?, ?"
	case *lcommon.StakeDeregistrationCertificate:
		stakeCredential = cert.StakeCredential
		state.active = false
		table = "stake_deregistration"
		columns = "staking_key, credential_tag, added_slot, certificate_id"
		values = "?, ?, ?, ?"
	case *lcommon.DeregistrationCertificate:
		stakeCredential = cert.StakeCredential
		state.active = false
		table = "deregistration"
		columns = "staking_key, credential_tag, added_slot, amount, certificate_id"
		values = "?, ?, ?, ?, ?"
	case *lcommon.StakeDelegationCertificate:
		if cert.StakeCredential == nil {
			return 0, nil, errors.New(
				"stake delegation certificate has nil stake credential",
			)
		}
		stakeCredential = *cert.StakeCredential
		state.pool = cert.PoolKeyHash[:]
		table = "stake_delegation"
		columns = "staking_key, credential_tag, pool_key_hash, added_slot, certificate_id"
		values = "?, ?, ?, ?, ?"
	case *lcommon.StakeRegistrationDelegationCertificate:
		stakeCredential = cert.StakeCredential
		state.pool = cert.PoolKeyHash[:]
		table = "stake_registration_delegation"
		columns = "staking_key, credential_tag, pool_key_hash, added_slot, deposit_amount, certificate_id"
		values = "?, ?, ?, ?, ?, ?"
	case *lcommon.StakeVoteDelegationCertificate:
		stakeCredential = cert.StakeCredential
		state.pool = cert.PoolKeyHash[:]
		drep, drepType, err := certificateDrep(cert.Drep)
		if err != nil {
			return 0, nil, err
		}
		state.drep, state.drepType = drep, drepType
		table = "stake_vote_delegation"
		columns = "staking_key, credential_tag, pool_key_hash, drep, drep_type, added_slot, certificate_id"
		values = "?, ?, ?, ?, ?, ?, ?"
	case *lcommon.RegistrationCertificate:
		stakeCredential = cert.StakeCredential
		table = "registration"
		columns = "staking_key, credential_tag, added_slot, deposit_amount, certificate_id"
		values = "?, ?, ?, ?, ?"
	case *lcommon.StakeVoteRegistrationDelegationCertificate:
		stakeCredential = cert.StakeCredential
		state.pool = cert.PoolKeyHash[:]
		drep, drepType, err := certificateDrep(cert.Drep)
		if err != nil {
			return 0, nil, err
		}
		state.drep, state.drepType = drep, drepType
		table = "stake_vote_registration_delegation"
		columns = "staking_key, credential_tag, pool_key_hash, drep, drep_type, added_slot, deposit_amount, certificate_id"
		values = "?, ?, ?, ?, ?, ?, ?, ?"
	case *lcommon.VoteRegistrationDelegationCertificate:
		stakeCredential = cert.StakeCredential
		drep, drepType, err := certificateDrep(cert.Drep)
		if err != nil {
			return 0, nil, err
		}
		state.drep, state.drepType = drep, drepType
		table = "vote_registration_delegation"
		columns = "staking_key, credential_tag, drep, drep_type, added_slot, deposit_amount, certificate_id"
		values = "?, ?, ?, ?, ?, ?, ?"
	case *lcommon.VoteDelegationCertificate:
		stakeCredential = cert.StakeCredential
		drep, drepType, err := certificateDrep(cert.Drep)
		if err != nil {
			return 0, nil, err
		}
		state.drep, state.drepType = drep, drepType
		table = "vote_delegation"
		columns = "staking_key, credential_tag, drep, drep_type, added_slot, certificate_id"
		values = "?, ?, ?, ?, ?, ?"
	default:
		return 0, nil, fmt.Errorf(
			"unsupported account certificate type %T",
			certificate,
		)
	}
	tag, err := models.CredentialTagFromUint(stakeCredential.CredType)
	if err != nil {
		return 0, nil, err
	}
	key := stakeCredential.Credential[:]
	if err := updateCertificateAccount(ctx, db, tag, key, slot, state); err != nil {
		return 0, nil, err
	}
	switch cert := certificate.(type) {
	case *lcommon.StakeRegistrationCertificate,
		*lcommon.RegistrationCertificate:
		args = []any{key, tag, slot, deposit, certificateID}
	case *lcommon.StakeDeregistrationCertificate:
		args = []any{key, tag, slot, certificateID}
	case *lcommon.DeregistrationCertificate:
		args = []any{key, tag, slot, decimalUint64(types.Uint64(cert.Amount)), certificateID}
	case *lcommon.StakeDelegationCertificate:
		args = []any{key, tag, state.pool, slot, certificateID}
	case *lcommon.StakeRegistrationDelegationCertificate:
		args = []any{key, tag, state.pool, slot, deposit, certificateID}
	case *lcommon.StakeVoteDelegationCertificate:
		args = []any{key, tag, state.pool, state.drep, state.drepType, slot, certificateID}
	case *lcommon.StakeVoteRegistrationDelegationCertificate:
		args = []any{key, tag, state.pool, state.drep, state.drepType, slot, deposit, certificateID}
	case *lcommon.VoteRegistrationDelegationCertificate:
		args = []any{key, tag, state.drep, state.drepType, slot, deposit, certificateID}
	case *lcommon.VoteDelegationCertificate:
		args = []any{key, tag, state.drep, state.drepType, slot, certificateID}
	}
	id, err := insertCertificateRow(
		ctx,
		db,
		"INSERT INTO "+table+" ("+columns+") VALUES ("+values+") RETURNING id",
		args...,
	)
	ref := models.NewStakeCredentialRef(tag, key)
	return id, &ref, err
}

func updateCertificateAccount(
	ctx context.Context,
	db queryer,
	tag uint8,
	key []byte,
	slot uint64,
	next certificateAccountState,
) error {
	var (
		active   bool
		pool     []byte
		drep     []byte
		drepType uint64
	)
	err := db.QueryRowContext(ctx, `
SELECT active, pool, drep, drep_type
FROM account
WHERE credential_tag = ? AND staking_key = ?`,
		tag,
		key,
	).Scan(&active, &pool, &drep, &drepType)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		active = true
		pool = nil
		drep = nil
		drepType = 0
	case err != nil:
		return err
	case !active:
		// Re-registering an inactive credential starts with no delegations.
		pool = nil
		drep = nil
		drepType = 0
	}
	if !next.active {
		pool = nil
		drep = nil
		drepType = 0
	}
	if next.pool != nil {
		pool = next.pool
	}
	if next.drep != nil || next.drepType >= models.DrepTypeAlwaysAbstain {
		drep = next.drep
		drepType = next.drepType
	}
	_, err = db.ExecContext(ctx, `
INSERT INTO account (
    staking_key, credential_tag, pool, drep, added_slot, created_slot,
    reward, drep_type, active, expiration_epoch
) VALUES (?, ?, ?, ?, ?, ?, '0', ?, ?, 0)
ON CONFLICT (credential_tag, staking_key) DO UPDATE SET
    pool = excluded.pool,
    drep = excluded.drep,
    added_slot = excluded.added_slot,
    created_slot = CASE
        WHEN account.created_slot <= excluded.created_slot THEN account.created_slot
        ELSE excluded.created_slot
    END,
    drep_type = excluded.drep_type,
    active = excluded.active`,
		key,
		tag,
		pool,
		drep,
		slot,
		slot,
		drepType,
		next.active,
	)
	return err
}

func certificateDrep(
	drep lcommon.Drep,
) ([]byte, uint64, error) {
	drepType, err := models.DrepTypeFromInt(drep.Type)
	if err != nil {
		return nil, 0, err
	}
	if drepType == models.DrepTypeAlwaysAbstain ||
		drepType == models.DrepTypeAlwaysNoConfidence {
		return nil, drepType, nil
	}
	return drep.Credential[:], drepType, nil
}

func applyPoolRegistrationCertificate(
	ctx context.Context,
	db queryer,
	cert *lcommon.PoolRegistrationCertificate,
	certificateID uint,
	slot uint64,
	deposit any,
) (uint, error) {
	rewardTag, rewardAccount, err := certutil.PoolRewardAccount(cert)
	if err != nil {
		return 0, err
	}
	margin := nullableRat(&types.Rat{Rat: cert.Margin.Rat})
	// LeiosKey's proof of possession is deliberately not checked here: the
	// database layer stores whatever gouroboros decoded (length-validated
	// only), the same way vrf_key_hash is stored without ledger-level proof
	// validation at write time. PoP verification happens at read time in
	// ledger/leios's on-chain key provider, which is allowed to depend on
	// ledger/leios's BLS primitives; this package is not (see
	// internal/architecture/import_boundary_test.go). An invalid-PoP key is
	// therefore excluded there, not here -- both layers still end up
	// treating it as absent, matching upstream.
	var leiosKeyPublic, leiosKeyPoP []byte
	if cert.LeiosKey != nil {
		leiosKeyPublic = cert.LeiosKey.PublicKey
		leiosKeyPoP = cert.LeiosKey.PossessionProof
	}
	poolID, err := queryReturnedID(ctx, db, `
INSERT INTO pool (
    margin, pool_key_hash, vrf_key_hash, reward_account,
    latest_op_cert_sequence, reward_account_credential_tag, pledge, cost,
    leios_key_public, leios_key_possession_proof
) VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?)
ON CONFLICT (pool_key_hash) DO UPDATE SET
    margin = excluded.margin,
    vrf_key_hash = excluded.vrf_key_hash,
    reward_account = excluded.reward_account,
    reward_account_credential_tag = excluded.reward_account_credential_tag,
    pledge = excluded.pledge,
    cost = excluded.cost,
    leios_key_public = excluded.leios_key_public,
    leios_key_possession_proof = excluded.leios_key_possession_proof
RETURNING id`,
		margin,
		cert.Operator[:],
		cert.VrfKeyHash[:],
		rewardAccount,
		rewardTag,
		decimalUint64(types.Uint64(cert.Pledge)),
		decimalUint64(types.Uint64(cert.Cost)),
		nullBytes(leiosKeyPublic),
		nullBytes(leiosKeyPoP),
	)
	if err != nil {
		return 0, err
	}
	var metadataURL string
	var metadataHash []byte
	if cert.PoolMetadata != nil {
		metadataURL = cert.PoolMetadata.Url
		metadataHash = cert.PoolMetadata.Hash[:]
	}
	registrationID, err := insertPoolRegistration(ctx, db, []any{
		margin,
		metadataURL,
		cert.VrfKeyHash[:],
		cert.Operator[:],
		rewardAccount,
		rewardTag,
		metadataHash,
		decimalUint64(types.Uint64(cert.Pledge)),
		decimalUint64(types.Uint64(cert.Cost)),
		certificateID,
		poolID,
		slot,
		deposit,
		nullBytes(leiosKeyPublic),
		nullBytes(leiosKeyPoP),
	}, poolID, slot)
	if err != nil {
		return 0, err
	}
	if _, err := db.ExecContext(ctx, `
DELETE FROM pool_registration_owner WHERE pool_registration_id = ?`,
		registrationID,
	); err != nil {
		return 0, err
	}
	if _, err := db.ExecContext(ctx, `
DELETE FROM pool_registration_relay WHERE pool_registration_id = ?`,
		registrationID,
	); err != nil {
		return 0, err
	}
	for _, owner := range cert.PoolOwners {
		if _, err := db.ExecContext(ctx, `
INSERT INTO pool_registration_owner (
    key_hash, pool_registration_id, pool_id
) VALUES (?, ?, ?)`,
			owner[:],
			registrationID,
			poolID,
		); err != nil {
			return 0, err
		}
	}
	for _, relay := range cert.Relays {
		var port uint
		var hostname string
		if relay.Port != nil {
			port = uint(*relay.Port)
		}
		if relay.Hostname != nil {
			hostname = *relay.Hostname
		}
		if _, err := db.ExecContext(ctx, `
INSERT INTO pool_registration_relay (
    ipv4, ipv6, hostname, pool_registration_id, pool_id, port
) VALUES (?, ?, ?, ?, ?, ?)`,
			netIPValue(relay.Ipv4),
			netIPValue(relay.Ipv6),
			hostname,
			registrationID,
			poolID,
			port,
		); err != nil {
			return 0, err
		}
	}
	return uint(registrationID), nil
}

func applyPoolRetirementCertificate(
	ctx context.Context,
	db queryer,
	cert *lcommon.PoolRetirementCertificate,
	certificateID uint,
	slot uint64,
) (uint, error) {
	poolID, err := queryReturnedID(ctx, db, `
INSERT INTO pool (
    pool_key_hash, latest_op_cert_sequence, pledge, cost,
    reward_account_credential_tag
) VALUES (?, 0, '0', '0', 0)
ON CONFLICT (pool_key_hash) DO UPDATE SET
    pool_key_hash = excluded.pool_key_hash
RETURNING id`,
		cert.PoolKeyHash[:],
	)
	if err != nil {
		return 0, err
	}
	return insertCertificateRow(ctx, db, `
INSERT INTO pool_retirement (
    pool_key_hash, certificate_id, pool_id, epoch, added_slot
) VALUES (?, ?, ?, ?, ?)
RETURNING id`,
		cert.PoolKeyHash[:],
		certificateID,
		poolID,
		cert.Epoch,
		slot,
	)
}

func applyDrepRegistrationCertificate(
	ctx context.Context,
	db queryer,
	cert *lcommon.RegistrationDrepCertificate,
	certificateID uint,
	slot uint64,
	deposit any,
) (uint, error) {
	tag, err := models.CredentialTagFromUint(
		cert.DrepCredential.CredType,
	)
	if err != nil {
		return 0, err
	}
	var anchorURL string
	var anchorHash []byte
	if cert.Anchor != nil {
		anchorURL = cert.Anchor.Url
		anchorHash = cert.Anchor.DataHash[:]
	}
	if err := setDrepCertificateState(
		ctx,
		db,
		tag,
		cert.DrepCredential.Credential[:],
		slot,
		anchorURL,
		anchorHash,
		true,
		false,
	); err != nil {
		return 0, err
	}
	return insertCertificateRow(ctx, db, `
INSERT INTO registration_drep (
    anchor_url, drep_credential, anchor_hash, certificate_id,
    credential_tag, added_slot, deposit_amount
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (credential_tag, drep_credential, added_slot) DO UPDATE SET
    anchor_url = excluded.anchor_url,
    anchor_hash = excluded.anchor_hash,
    certificate_id = excluded.certificate_id,
    deposit_amount = excluded.deposit_amount
RETURNING id`,
		anchorURL,
		cert.DrepCredential.Credential[:],
		anchorHash,
		certificateID,
		tag,
		slot,
		deposit,
	)
}

func applyDrepDeregistrationCertificate(
	ctx context.Context,
	db queryer,
	cert *lcommon.DeregistrationDrepCertificate,
	certificateID uint,
	slot uint64,
	deposit any,
) (uint, error) {
	tag, err := models.CredentialTagFromUint(
		cert.DrepCredential.CredType,
	)
	if err != nil {
		return 0, err
	}
	if err := setDrepCertificateState(
		ctx,
		db,
		tag,
		cert.DrepCredential.Credential[:],
		slot,
		"",
		nil,
		false,
		true,
	); err != nil {
		return 0, err
	}
	return insertCertificateRow(ctx, db, `
INSERT INTO deregistration_drep (
    drep_credential, certificate_id, credential_tag, added_slot,
    deposit_amount
) VALUES (?, ?, ?, ?, ?)
RETURNING id`,
		cert.DrepCredential.Credential[:],
		certificateID,
		tag,
		slot,
		deposit,
	)
}

func applyDrepUpdateCertificate(
	ctx context.Context,
	db queryer,
	cert *lcommon.UpdateDrepCertificate,
	certificateID uint,
	slot uint64,
) (uint, error) {
	tag, err := models.CredentialTagFromUint(
		cert.DrepCredential.CredType,
	)
	if err != nil {
		return 0, err
	}
	var anchorURL string
	var anchorHash []byte
	if cert.Anchor != nil {
		anchorURL = cert.Anchor.Url
		anchorHash = cert.Anchor.DataHash[:]
	}
	if err := setDrepCertificateState(
		ctx,
		db,
		tag,
		cert.DrepCredential.Credential[:],
		slot,
		anchorURL,
		anchorHash,
		true,
		true,
	); err != nil {
		return 0, err
	}
	return insertCertificateRow(ctx, db, `
INSERT INTO update_drep (
    anchor_url, credential, anchor_hash, certificate_id,
    credential_tag, added_slot
) VALUES (?, ?, ?, ?, ?, ?)
RETURNING id`,
		anchorURL,
		cert.DrepCredential.Credential[:],
		anchorHash,
		certificateID,
		tag,
		slot,
	)
}

func setDrepCertificateState(
	ctx context.Context,
	db queryer,
	tag uint8,
	credential []byte,
	slot uint64,
	anchorURL string,
	anchorHash []byte,
	active bool,
	requireExisting bool,
) error {
	var exists bool
	if err := db.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1 FROM drep WHERE credential_tag = ? AND credential = ?
)`,
		tag,
		credential,
	).Scan(&exists); err != nil {
		return err
	}
	if requireExisting && !exists {
		return models.ErrDrepNotFound
	}
	if exists {
		_, err := db.ExecContext(ctx, `
UPDATE drep
SET anchor_url = ?, anchor_hash = ?, added_slot = ?, active = ?
WHERE credential_tag = ? AND credential = ?`,
			anchorURL,
			anchorHash,
			slot,
			active,
			tag,
			credential,
		)
		return err
	}
	_, err := db.ExecContext(ctx, `
INSERT INTO drep (
    anchor_url, credential, anchor_hash, added_slot, credential_tag,
    last_activity_epoch, expiry_epoch, active
) VALUES (?, ?, ?, ?, ?, 0, 0, ?)`,
		anchorURL,
		credential,
		anchorHash,
		slot,
		tag,
		active,
	)
	return err
}

func applyMIRCertificate(
	ctx context.Context,
	db queryer,
	cert *lcommon.MoveInstantaneousRewardsCertificate,
	certificateID uint,
	slot uint64,
) (uint, error) {
	id, err := insertCertificateRow(ctx, db, `
INSERT INTO move_instantaneous_rewards (
    pot, certificate_id, added_slot, other_pot
) VALUES (?, ?, ?, ?)
RETURNING id`,
		cert.Reward.Source,
		certificateID,
		slot,
		decimalUint64(types.Uint64(cert.Reward.OtherPot)),
	)
	if err != nil {
		return 0, err
	}
	for credential, amount := range cert.Reward.Rewards {
		tag, err := models.CredentialTagFromUint(credential.CredType)
		if err != nil {
			return 0, err
		}
		if _, err := db.ExecContext(ctx, `
INSERT INTO move_instantaneous_rewards_reward (
    credential, credential_tag, amount, mir_id
) VALUES (?, ?, ?, ?)`,
			credential.Credential[:],
			tag,
			decimalUint64(types.Uint64(amount)),
			id,
		); err != nil {
			return 0, err
		}
	}
	return id, nil
}

func insertCertificateRow(
	ctx context.Context,
	db queryer,
	query string,
	args ...any,
) (uint, error) {
	id, err := queryReturnedID(ctx, db, query, args...)
	return uint(id), err
}
