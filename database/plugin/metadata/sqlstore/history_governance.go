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
	"errors"
	"fmt"
	"math"
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
	if orderDirectionFromString(order) == "DESC" {
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

// GetGovernanceVotesPage pages every recorded cast of the votes matching
// filter, ordered by the slot that cast it then recording order, along with
// the unpaginated count. A revote keeps its earlier casts, and votes
// invalidated by a DRep deregistration keep theirs with a deleted slot.
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
	const from = ` FROM governance_vote_history h
JOIN governance_vote v ON v.id = h.vote_id
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
	if filter.AuthorizedBy != nil {
		// A subquery, not a list of the member's hot credentials: a member
		// that rotates on a schedule authorizes thousands, and an OR chain
		// that long exceeds SQLite's expression depth.
		where += ` AND EXISTS (SELECT 1 FROM auth_committee_hot a
WHERE a.cold_credential_tag = ? AND a.cold_credential = ?
  AND a.hot_credential_tag = v.voter_credential_tag
  AND a.host_credential = v.voter_credential)`
		args = append(
			args,
			filter.AuthorizedBy.CredentialTag,
			filter.AuthorizedBy.Credential,
		)
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
	dir := orderDirectionFromString(order)
	query, queryArgs := addLimitOffset(
		`SELECT v.id, v.proposal_id, v.voter_type, v.voter_credential_tag,
v.voter_credential, h.vote, h.anchor_url, h.anchor_hash, v.added_slot,
v.vote_updated_slot, v.deleted_slot, p.tx_hash, p.action_index, p.action_type,
h.transition_slot, h.tx_hash, h.cert_index,
(SELECT COUNT(*) FROM governance_vote_history same
 WHERE same.vote_id = h.vote_id AND same.transition_slot = h.transition_slot
   AND same.id < h.id),
(SELECT COUNT(*) FROM governance_vote_history same
 WHERE same.vote_id = h.vote_id AND same.transition_slot = h.transition_slot),
CASE WHEN EXISTS (
    SELECT 1 FROM governance_vote_history earlier
    WHERE earlier.vote_id = h.vote_id
      AND earlier.transition_slot < h.transition_slot
) THEN 0 ELSE 1 END,
CASE WHEN v.deleted_slot IS NULL AND NOT EXISTS (
    SELECT 1 FROM governance_vote_history later
    WHERE later.vote_id = h.vote_id
      AND (later.transition_slot > h.transition_slot
        OR (later.transition_slot = h.transition_slot AND later.id > h.id))
) THEN 1 ELSE 0 END`+from+where+
			" ORDER BY h.transition_slot "+dir+", h.id "+dir,
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
		var certIndex sql.NullInt64
		var counted int64
		var earliest int64
		if err := rows.Scan(
			&row.Vote.ID, &row.Vote.ProposalID, &row.Vote.VoterType,
			&row.Vote.VoterCredentialTag, &row.Vote.VoterCredential,
			&row.Vote.Vote, &anchorURL, &row.Vote.AnchorHash,
			&row.Vote.AddedSlot, &row.Vote.VoteUpdatedSlot,
			&row.Vote.DeletedSlot, &row.ProposalTxHash,
			&row.ProposalActionIndex, &row.ProposalActionType, &row.CastSlot,
			&row.CastTxHash, &certIndex, &row.CastSlotRank,
			&row.CastSlotCount, &earliest, &counted,
		); err != nil {
			return nil, 0, err
		}
		row.Vote.AnchorURL = anchorURL.String
		if certIndex.Valid {
			if certIndex.Int64 < 0 || certIndex.Int64 > math.MaxUint32 {
				return nil, 0, fmt.Errorf(
					"vote cast index %d out of range", certIndex.Int64,
				)
			}
			row.CastCertIndex = uint32(certIndex.Int64)
		}
		row.Counted = counted != 0
		row.CastSlotEarliest = earliest != 0
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

func (s *Store) HasUnattributedCommitteeVotes(
	txn types.Txn,
) (bool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return false, err
	}
	var found int
	err = db.QueryRowContext(ctx, `SELECT 1 FROM governance_vote v
WHERE v.voter_type = ?
  AND NOT EXISTS (SELECT 1 FROM auth_committee_hot a
    WHERE a.hot_credential_tag = v.voter_credential_tag
      AND a.host_credential = v.voter_credential)
LIMIT 1`, models.VoterTypeCC).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("find unattributed committee votes: %w", err)
	}
	return true, nil
}

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
	dir := orderDirectionFromString(order)
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
			" ORDER BY "+fmt.Sprintf(orderBy, orderDirectionFromString(order)),
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
