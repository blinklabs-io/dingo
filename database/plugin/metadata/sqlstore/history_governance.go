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

//nolint:rowserrcheck,sqlclosecheck // Cursors are explicitly closed and close errors are propagated before dependent queries.
package sqlstore

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// GetGovernanceProposalsPage pages every stored proposal in submission
// order, along with the unpaginated count.
func (s *Store) GetGovernanceProposalsPage(
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]*models.GovernanceProposal, int, error) {
	ret := []*models.GovernanceProposal{}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, 0, err
	}
	var total int
	if err := db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM governance_proposal WHERE deleted_slot IS NULL",
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count governance proposals: %w", err)
	}
	if total == 0 {
		return ret, 0, nil
	}
	submission := governanceProposalSubmissionOrderSQL
	ordering := "added_slot ASC," + submission
	if orderDirection(order) == "DESC" {
		ordering = "added_slot DESC," +
			"CASE WHEN governance_proposal_order.tx_index IS NULL THEN 1 ELSE 0 END ASC," +
			"governance_proposal_order.tx_index DESC, tx_hash DESC, action_index DESC"
	}
	query, args := addLimitOffset(
		"SELECT "+governanceProposalColumns+governanceProposalFromSQL+
			" WHERE deleted_slot IS NULL ORDER BY "+ordering,
		nil, limit, offset,
	)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query governance proposals: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		proposal, err := scanGovernanceProposal(rows)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, proposal)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return ret, total, nil
}

// GetGovernanceVotesPage pages the stored votes matching filter, ordered by
// the slot that cast the current vote value then insertion order, along with
// the unpaginated count. Votes invalidated by a DRep deregistration keep
// their row with a deleted slot.
func (s *Store) GetGovernanceVotesPage(
	filter models.GovernanceVoteFilter,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]models.GovernanceVoteRow, int, error) {
	ret := []models.GovernanceVoteRow{}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, 0, err
	}
	const from = ` FROM governance_vote v
JOIN governance_proposal p ON p.id = v.proposal_id AND p.deleted_slot IS NULL
WHERE 1 = 1`
	where := ""
	var args []any
	if filter.ProposalID != nil {
		where += " AND v.proposal_id = ?"
		args = append(args, *filter.ProposalID)
	}
	if filter.VoterType != nil {
		where += " AND v.voter_type = ?"
		args = append(args, *filter.VoterType)
	}
	if len(filter.VoterCredentials) > 0 {
		predicates := make([]string, 0, len(filter.VoterCredentials))
		for _, credential := range filter.VoterCredentials {
			predicates = append(
				predicates,
				"(v.voter_credential_tag = ? AND v.voter_credential = ?)",
			)
			args = append(args, credential.CredentialTag, credential.Credential)
		}
		where += " AND (" + strings.Join(predicates, " OR ") + ")"
	}
	var total int
	if err := db.QueryRowContext(
		ctx, "SELECT COUNT(*)"+from+where, args...,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count governance votes: %w", err)
	}
	if total == 0 {
		return ret, 0, nil
	}
	dir := orderDirection(order)
	query, queryArgs := addLimitOffset(
		`SELECT v.id, v.proposal_id, v.voter_type, v.voter_credential_tag,
v.voter_credential, v.vote, v.anchor_url, v.anchor_hash, v.added_slot,
v.vote_updated_slot, v.deleted_slot, p.tx_hash, p.action_index, p.action_type,
COALESCE(v.vote_updated_slot, v.added_slot)`+from+where+
			" ORDER BY COALESCE(v.vote_updated_slot, v.added_slot) "+dir+
			", v.id "+dir,
		args, limit, offset,
	)
	rows, err := db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query governance votes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row models.GovernanceVoteRow
		var anchorURL sql.NullString
		if err := rows.Scan(
			&row.Vote.ID, &row.Vote.ProposalID, &row.Vote.VoterType,
			&row.Vote.VoterCredentialTag, &row.Vote.VoterCredential,
			&row.Vote.Vote, &anchorURL, &row.Vote.AnchorHash,
			&row.Vote.AddedSlot, &row.Vote.VoteUpdatedSlot,
			&row.Vote.DeletedSlot, &row.ProposalTxHash,
			&row.ProposalActionIndex, &row.ProposalActionType, &row.CastSlot,
		); err != nil {
			return nil, 0, err
		}
		row.Vote.AnchorURL = anchorURL.String
		ret = append(ret, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return ret, total, nil
}

const drepUpdatesQuery = `
SELECT item.added_slot AS added_slot, tx.block_index AS block_index,
       c.cert_index AS cert_index, tx.hash AS tx_hash, ? AS action,
       item.deposit_amount AS deposit
FROM registration_drep item
JOIN certs c ON c.id = item.certificate_id
JOIN "transaction" tx ON tx.id = c.transaction_id
WHERE item.credential_tag = ? AND item.drep_credential = ?
UNION ALL
SELECT item.added_slot, tx.block_index, c.cert_index, tx.hash, ?,
       item.deposit_amount
FROM deregistration_drep item
JOIN certs c ON c.id = item.certificate_id
JOIN "transaction" tx ON tx.id = c.transaction_id
WHERE item.credential_tag = ? AND item.drep_credential = ?
UNION ALL
SELECT item.added_slot, tx.block_index, c.cert_index, tx.hash, ?, NULL
FROM update_drep item
JOIN certs c ON c.id = item.certificate_id
JOIN "transaction" tx ON tx.id = c.transaction_id
WHERE item.credential_tag = ? AND item.credential = ?`

// GetDRepUpdates returns a DRep's registration, update and deregistration
// certificates in chain order, along with the unpaginated count.
func (s *Store) GetDRepUpdates(
	credentialTag uint8,
	credential []byte,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]models.DRepUpdateRow, int, error) {
	ret := []models.DRepUpdateRow{}
	if len(credential) == 0 {
		return ret, 0, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, 0, err
	}
	args := []any{
		"registered", credentialTag, credential,
		"deregistered", credentialTag, credential,
		"updated", credentialTag, credential,
	}
	var total int
	if err := db.QueryRowContext(
		ctx, "SELECT COUNT(*) FROM ("+drepUpdatesQuery+") events", args...,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count drep updates: %w", err)
	}
	if total == 0 {
		return ret, 0, nil
	}
	dir := orderDirection(order)
	query, queryArgs := addLimitOffset(
		"SELECT tx_hash, cert_index, action, deposit FROM ("+drepUpdatesQuery+
			") events ORDER BY added_slot "+dir+", block_index "+dir+
			", cert_index "+dir,
		args, limit, offset,
	)
	rows, err := db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query drep updates: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row models.DRepUpdateRow
		var deposit sql.NullString
		if err := rows.Scan(
			&row.TxHash, &row.CertIndex, &row.Action, &deposit,
		); err != nil {
			return nil, 0, err
		}
		if deposit.Valid {
			value, err := parseNullUint64("drep deposit", deposit)
			if err != nil {
				return nil, 0, err
			}
			row.Deposit = &value
		}
		ret = append(ret, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return ret, total, nil
}

// GetDRepDelegatorStakes returns the registered credentials delegating to a
// DRep with their live stake, in delegation order, along with the
// unpaginated count. predefined selects an always-abstain or
// always-no-confidence DRep, which has no credential of its own.
func (s *Store) GetDRepDelegatorStakes(
	credentialTag uint8,
	credential []byte,
	predefined *uint64,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]models.PoolDelegatorRow, int, error) {
	ret := []models.PoolDelegatorRow{}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, 0, err
	}
	var from string
	var args []any
	var orderBy string
	switch {
	case predefined != nil:
		from = ` FROM account a
LEFT JOIN reward_live_stake rls ON rls.credential_tag = a.credential_tag
 AND rls.staking_key = a.staking_key AND rls.registered = TRUE
WHERE a.drep_type = ? AND (a.drep IS NULL OR LENGTH(a.drep) = 0)
 AND a.active = TRUE`
		args = []any{*predefined}
		orderBy = "a.added_slot %s, a.credential_tag %[1]s, a.staking_key %[1]s"
	case len(credential) > 0:
		from = ` FROM drep_delegator d
JOIN account a ON a.credential_tag = d.stake_credential_tag
 AND a.staking_key = d.stake_credential AND a.active = TRUE
LEFT JOIN reward_live_stake rls ON rls.credential_tag = a.credential_tag
 AND rls.staking_key = a.staking_key AND rls.registered = TRUE
WHERE d.drep_credential_tag = ? AND d.drep_credential = ?
 AND d.removed_slot IS NULL`
		args = []any{credentialTag, credential}
		orderBy = "d.added_slot %s, d.id %[1]s"
	default:
		return ret, 0, nil
	}
	var total int
	if err := db.QueryRowContext(
		ctx, "SELECT COUNT(*)"+from, args...,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count drep delegators: %w", err)
	}
	if total == 0 {
		return ret, 0, nil
	}
	query, queryArgs := addLimitOffset(
		"SELECT a.credential_tag, a.staking_key, rls.total_stake"+from+
			" ORDER BY "+fmt.Sprintf(orderBy, orderDirection(order)),
		args, limit, offset,
	)
	rows, err := db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query drep delegators: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row models.PoolDelegatorRow
		var stake sql.NullString
		if err := rows.Scan(&row.CredentialTag, &row.StakingKey, &stake); err != nil {
			return nil, 0, err
		}
		if row.Stake, err = parseNullUint64("drep delegator stake", stake); err != nil {
			return nil, 0, err
		}
		ret = append(ret, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return ret, total, nil
}

// GetCommitteeHotCredentials returns every hot credential a cold committee
// credential has authorized, across rotations.
func (s *Store) GetCommitteeHotCredentials(
	coldCredentialTag uint8,
	coldCredential []byte,
	txn types.Txn,
) ([]models.CommitteeCredential, error) {
	ret := []models.CommitteeCredential{}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(
		ctx,
		`SELECT DISTINCT hot_credential_tag, host_credential
FROM auth_committee_hot
WHERE cold_credential_tag = ? AND cold_credential = ?
ORDER BY hot_credential_tag, host_credential`,
		coldCredentialTag, coldCredential,
	)
	if err != nil {
		return nil, fmt.Errorf("query committee hot credentials: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var credential models.CommitteeCredential
		if err := rows.Scan(
			&credential.CredentialTag, &credential.Credential,
		); err != nil {
			return nil, err
		}
		ret = append(ret, credential)
	}
	return ret, rows.Err()
}
