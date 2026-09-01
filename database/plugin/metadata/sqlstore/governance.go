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
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"strings"
)

const governanceProposalColumns = `
id, tx_hash, action_index, action_type, proposed_epoch, expires_epoch,
parent_tx_hash, parent_action_idx, enacted_epoch, enacted_slot,
ratified_epoch, ratified_slot, policy_hash, anchor_url, anchor_hash, deposit,
return_address, gov_action_cbor, expired_epoch, expired_slot, added_slot,
deleted_slot`

const governanceProposalOrderSQL = `
proposed_epoch ASC, added_slot ASC, tx_hash ASC, action_index ASC`

const ratifiedGovernanceProposalOrderSQL = `
ratified_epoch ASC, ratified_slot ASC, proposed_epoch ASC, added_slot ASC,
tx_hash ASC, action_index ASC`

func (s *Store) GetGovernanceProposal(
	txHash []byte,
	actionIndex uint32,
	txn types.Txn,
) (*models.GovernanceProposal, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	proposal, err := scanGovernanceProposal(db.QueryRowContext(
		ctx,
		"SELECT "+governanceProposalColumns+`
 FROM governance_proposal
 WHERE tx_hash = ? AND action_index = ? AND deleted_slot IS NULL
 LIMIT 1`,
		txHash,
		actionIndex,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return proposal, err
}

func (s *Store) GetActiveGovernanceProposals(
	epoch uint64,
	txn types.Txn,
) ([]*models.GovernanceProposal, error) {
	return s.queryGovernanceProposals(
		txn,
		"expires_epoch >= ? AND enacted_epoch IS NULL "+
			"AND expired_epoch IS NULL AND deleted_slot IS NULL",
		governanceProposalOrderSQL,
		epoch,
	)
}

func (s *Store) GetExpiringGovernanceProposals(
	epoch uint64,
	txn types.Txn,
) ([]*models.GovernanceProposal, error) {
	return s.queryGovernanceProposals(
		txn,
		"expires_epoch < ? AND enacted_epoch IS NULL "+
			"AND expired_epoch IS NULL AND deleted_slot IS NULL",
		governanceProposalOrderSQL,
		epoch,
	)
}

func (s *Store) GetExpiredGovernanceProposalsAt(
	epoch uint64,
	slot uint64,
	txn types.Txn,
) ([]*models.GovernanceProposal, error) {
	return s.queryGovernanceProposals(
		txn,
		"expired_epoch = ? AND expired_slot = ? "+
			"AND enacted_epoch IS NULL AND deleted_slot IS NULL",
		governanceProposalOrderSQL,
		epoch,
		slot,
	)
}

func (s *Store) GetRatifiedGovernanceProposals(
	txn types.Txn,
) ([]*models.GovernanceProposal, error) {
	return s.queryGovernanceProposals(
		txn,
		"ratified_epoch IS NOT NULL AND enacted_epoch IS NULL "+
			"AND deleted_slot IS NULL",
		ratifiedGovernanceProposalOrderSQL,
	)
}

func (s *Store) GetEnactedGovernanceProposalsAt(
	epoch uint64,
	slot uint64,
	txn types.Txn,
) ([]*models.GovernanceProposal, error) {
	return s.queryGovernanceProposals(
		txn,
		"ratified_epoch IS NOT NULL AND enacted_epoch = ? "+
			"AND enacted_slot = ? AND deleted_slot IS NULL",
		ratifiedGovernanceProposalOrderSQL,
		epoch,
		slot,
	)
}

func (s *Store) GetChildGovernanceProposals(
	parentTxHash []byte,
	parentActionIndex uint32,
	txn types.Txn,
) ([]*models.GovernanceProposal, error) {
	return s.queryGovernanceProposals(
		txn,
		"parent_tx_hash = ? AND parent_action_idx = ? "+
			"AND enacted_epoch IS NULL AND expired_epoch IS NULL "+
			"AND deleted_slot IS NULL",
		governanceProposalOrderSQL,
		parentTxHash,
		parentActionIndex,
	)
}

func (s *Store) GetLastEnactedGovernanceProposal(
	actionTypes []uint8,
	txn types.Txn,
) (*models.GovernanceProposal, error) {
	if len(actionTypes) == 0 {
		return nil, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	args := make([]any, len(actionTypes))
	for i, actionType := range actionTypes {
		args[i] = actionType
	}
	proposal, err := scanGovernanceProposal(db.QueryRowContext(
		ctx,
		"SELECT "+governanceProposalColumns+`
 FROM governance_proposal
 WHERE action_type IN (`+bindPlaceholders(len(args))+`)
   AND enacted_epoch IS NOT NULL AND deleted_slot IS NULL
 ORDER BY enacted_epoch DESC, enacted_slot DESC, id DESC
 LIMIT 1`,
		args...,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return proposal, err
}

func (s *Store) SetGovernanceProposal(
	proposal *models.GovernanceProposal,
	txn types.Txn,
) error {
	if proposal == nil {
		return errors.New("set governance proposal: nil proposal")
	}
	if (proposal.RatifiedEpoch == nil) != (proposal.RatifiedSlot == nil) {
		return errors.New(
			"set governance proposal: ratified epoch and slot must both be set or both be nil",
		)
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			var previousEpoch, previousSlot sql.NullInt64
			previousErr := db.QueryRowContext(ctx, `
SELECT ratified_epoch, ratified_slot
FROM governance_proposal
WHERE tx_hash = ? AND action_index = ?`,
				proposal.TxHash,
				proposal.ActionIndex,
			).Scan(&previousEpoch, &previousSlot)
			if previousErr != nil && !errors.Is(previousErr, sql.ErrNoRows) {
				return previousErr
			}

			var id uint
			err := db.QueryRowContext(ctx, `
INSERT INTO governance_proposal (
    tx_hash, action_index, action_type, proposed_epoch, expires_epoch,
    parent_tx_hash, parent_action_idx, enacted_epoch, enacted_slot,
    ratified_epoch, ratified_slot, policy_hash, anchor_url, anchor_hash,
    deposit, return_address, gov_action_cbor, expired_epoch, expired_slot,
    added_slot, deleted_slot
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (tx_hash, action_index) DO UPDATE SET
    action_type = excluded.action_type,
    proposed_epoch = excluded.proposed_epoch,
    expires_epoch = excluded.expires_epoch,
    parent_tx_hash = excluded.parent_tx_hash,
    parent_action_idx = excluded.parent_action_idx,
    policy_hash = excluded.policy_hash,
    anchor_url = excluded.anchor_url,
    anchor_hash = excluded.anchor_hash,
    deposit = excluded.deposit,
    return_address = excluded.return_address,
    gov_action_cbor = CASE
        WHEN excluded.gov_action_cbor IS NOT NULL
         AND length(excluded.gov_action_cbor) > 0
        THEN excluded.gov_action_cbor
        ELSE governance_proposal.gov_action_cbor
    END,
    enacted_epoch = COALESCE(excluded.enacted_epoch,
                             governance_proposal.enacted_epoch),
    enacted_slot = COALESCE(excluded.enacted_slot,
                            governance_proposal.enacted_slot),
    ratified_epoch = COALESCE(excluded.ratified_epoch,
                              governance_proposal.ratified_epoch),
    ratified_slot = COALESCE(excluded.ratified_slot,
                             governance_proposal.ratified_slot),
    expired_epoch = COALESCE(excluded.expired_epoch,
                             governance_proposal.expired_epoch),
    expired_slot = COALESCE(excluded.expired_slot,
                            governance_proposal.expired_slot),
    deleted_slot = COALESCE(excluded.deleted_slot,
                            governance_proposal.deleted_slot)
RETURNING id`,
				proposal.TxHash,
				proposal.ActionIndex,
				proposal.ActionType,
				proposal.ProposedEpoch,
				proposal.ExpiresEpoch,
				proposal.ParentTxHash,
				proposal.ParentActionIdx,
				proposal.EnactedEpoch,
				proposal.EnactedSlot,
				proposal.RatifiedEpoch,
				proposal.RatifiedSlot,
				proposal.PolicyHash,
				proposal.AnchorURL,
				proposal.AnchorHash,
				decimalUint64(types.Uint64(proposal.Deposit)),
				proposal.ReturnAddress,
				proposal.GovActionCbor,
				proposal.ExpiredEpoch,
				proposal.ExpiredSlot,
				proposal.AddedSlot,
				proposal.DeletedSlot,
			).Scan(&id)
			if err != nil {
				return err
			}
			proposal.ID = id

			if proposal.RatifiedEpoch == nil {
				return nil
			}
			if previousErr == nil && previousEpoch.Valid && previousSlot.Valid &&
				previousEpoch.Int64 >= 0 && previousSlot.Int64 >= 0 &&
				uint64(previousEpoch.Int64) == *proposal.RatifiedEpoch &&
				uint64(previousSlot.Int64) == *proposal.RatifiedSlot {
				return nil
			}
			_, err = db.ExecContext(ctx, `
INSERT INTO governance_proposal_ratification_history (
    proposal_id, transition_slot, ratified_epoch, ratified_slot
) VALUES (?, ?, ?, ?)`,
				id,
				*proposal.RatifiedSlot,
				*proposal.RatifiedEpoch,
				*proposal.RatifiedSlot,
			)
			return err
		},
	)
}

func (s *Store) ClearGovernanceProposalRatification(
	txHash []byte,
	actionIndex uint32,
	transitionSlot uint64,
	txn types.Txn,
) error {
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			var (
				id            uint
				ratifiedEpoch sql.NullInt64
				ratifiedSlot  sql.NullInt64
			)
			if err := db.QueryRowContext(ctx, `
SELECT id, ratified_epoch, ratified_slot
FROM governance_proposal
WHERE tx_hash = ? AND action_index = ?`,
				txHash,
				actionIndex,
			).Scan(&id, &ratifiedEpoch, &ratifiedSlot); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return errors.New(
						"clear proposal ratification: expected 1 row, found 0",
					)
				}
				return err
			}
			if !ratifiedEpoch.Valid && !ratifiedSlot.Valid {
				return nil
			}
			if ratifiedEpoch.Valid != ratifiedSlot.Valid {
				return errors.New(
					"clear proposal ratification: inconsistent ratification marker",
				)
			}
			result, err := db.ExecContext(ctx, `
UPDATE governance_proposal
SET ratified_epoch = NULL, ratified_slot = NULL
WHERE id = ?`, id)
			if err != nil {
				return err
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if rows != 1 {
				return fmt.Errorf(
					"clear proposal ratification: expected 1 row, updated %d",
					rows,
				)
			}
			_, err = db.ExecContext(ctx, `
INSERT INTO governance_proposal_ratification_history (
    proposal_id, transition_slot, ratified_epoch, ratified_slot
) VALUES (?, ?, NULL, NULL)`, id, transitionSlot)
			return err
		},
	)
}

func (s *Store) GetGovernanceVotes(
	proposalID uint,
	txn types.Txn,
) ([]*models.GovernanceVote, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `
SELECT id, proposal_id, voter_type, voter_credential_tag, voter_credential,
       vote, anchor_url, anchor_hash, added_slot, vote_updated_slot,
       deleted_slot
FROM governance_vote
WHERE proposal_id = ? AND deleted_slot IS NULL`,
		proposalID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := []*models.GovernanceVote{}
	for rows.Next() {
		vote, err := scanGovernanceVote(rows)
		if err != nil {
			return nil, err
		}
		ret = append(ret, vote)
	}
	return ret, rows.Err()
}

func (s *Store) SetGovernanceVote(
	vote *models.GovernanceVote,
	txn types.Txn,
) error {
	if vote == nil {
		return errors.New("set governance vote: nil vote")
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			var id uint
			err := db.QueryRowContext(ctx, `
INSERT INTO governance_vote (
    proposal_id, voter_type, voter_credential_tag, voter_credential, vote,
    anchor_url, anchor_hash, added_slot, vote_updated_slot, deleted_slot
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (
    proposal_id, voter_type, voter_credential_tag, voter_credential
) DO UPDATE SET
    vote = excluded.vote,
    anchor_url = excluded.anchor_url,
    anchor_hash = excluded.anchor_hash,
    vote_updated_slot = excluded.vote_updated_slot,
    deleted_slot = excluded.deleted_slot
RETURNING id`,
				vote.ProposalID,
				vote.VoterType,
				vote.VoterCredentialTag,
				vote.VoterCredential,
				vote.Vote,
				vote.AnchorURL,
				vote.AnchorHash,
				vote.AddedSlot,
				vote.VoteUpdatedSlot,
				vote.DeletedSlot,
			).Scan(&id)
			if err == nil {
				vote.ID = id
			}
			return err
		},
	)
}

func (s *Store) DeleteGovernanceProposalsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			queries := []struct {
				query string
				args  []any
			}{
				{
					query: "DELETE FROM governance_proposal WHERE added_slot > ?",
					args:  []any{slot},
				},
				{
					query: `UPDATE governance_proposal SET deleted_slot = NULL
				 WHERE deleted_slot > ?`,
					args: []any{slot},
				},
				{
					query: `DELETE FROM governance_proposal_ratification_history
				 WHERE transition_slot > ?`,
					args: []any{slot},
				},
				{
					query: `UPDATE governance_proposal
				 SET ratified_epoch = (
				     SELECT history.ratified_epoch
				     FROM governance_proposal_ratification_history AS history
				     WHERE history.proposal_id = governance_proposal.id
				     ORDER BY history.transition_slot DESC, history.id DESC
				     LIMIT 1
				 ), ratified_slot = (
				     SELECT history.ratified_slot
				     FROM governance_proposal_ratification_history AS history
				     WHERE history.proposal_id = governance_proposal.id
				     ORDER BY history.transition_slot DESC, history.id DESC
				     LIMIT 1
				 )`,
				},
				{
					query: `UPDATE governance_proposal
				 SET enacted_epoch = NULL, enacted_slot = NULL
				 WHERE enacted_slot > ?`,
					args: []any{slot},
				},
				{
					query: `UPDATE governance_proposal
				 SET expired_epoch = NULL, expired_slot = NULL
				 WHERE expired_slot > ?`,
					args: []any{slot},
				},
			}
			for _, query := range queries {
				if _, err := db.ExecContext(
					ctx,
					query.query,
					query.args...,
				); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func (s *Store) DeleteGovernanceVotesAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			if _, err := db.ExecContext(ctx, `
DELETE FROM governance_vote
WHERE added_slot > ? OR vote_updated_slot > ?`,
				slot,
				slot,
			); err != nil {
				return err
			}
			_, err := db.ExecContext(ctx, `
UPDATE governance_vote SET deleted_slot = NULL
WHERE deleted_slot > ?`,
				slot,
			)
			return err
		},
	)
}

func (s *Store) queryGovernanceProposals(
	txn types.Txn,
	predicate string,
	order string,
	args ...any,
) ([]*models.GovernanceProposal, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(
		ctx,
		"SELECT "+governanceProposalColumns+
			" FROM governance_proposal WHERE "+predicate+" ORDER BY "+order,
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := []*models.GovernanceProposal{}
	for rows.Next() {
		proposal, err := scanGovernanceProposal(rows)
		if err != nil {
			return nil, err
		}
		ret = append(ret, proposal)
	}
	return ret, rows.Err()
}

func scanGovernanceProposal(
	row rowScanner,
) (*models.GovernanceProposal, error) {
	var proposal models.GovernanceProposal
	var deposit sql.NullString
	err := row.Scan(
		&proposal.ID,
		&proposal.TxHash,
		&proposal.ActionIndex,
		&proposal.ActionType,
		&proposal.ProposedEpoch,
		&proposal.ExpiresEpoch,
		&proposal.ParentTxHash,
		&proposal.ParentActionIdx,
		&proposal.EnactedEpoch,
		&proposal.EnactedSlot,
		&proposal.RatifiedEpoch,
		&proposal.RatifiedSlot,
		&proposal.PolicyHash,
		&proposal.AnchorURL,
		&proposal.AnchorHash,
		&deposit,
		&proposal.ReturnAddress,
		&proposal.GovActionCbor,
		&proposal.ExpiredEpoch,
		&proposal.ExpiredSlot,
		&proposal.AddedSlot,
		&proposal.DeletedSlot,
	)
	if err != nil {
		return nil, err
	}
	proposal.Deposit, err = parseNullUint64(
		"governance proposal deposit",
		deposit,
	)
	return &proposal, err
}

func scanGovernanceVote(row rowScanner) (*models.GovernanceVote, error) {
	var vote models.GovernanceVote
	err := row.Scan(
		&vote.ID,
		&vote.ProposalID,
		&vote.VoterType,
		&vote.VoterCredentialTag,
		&vote.VoterCredential,
		&vote.Vote,
		&vote.AnchorURL,
		&vote.AnchorHash,
		&vote.AddedSlot,
		&vote.VoteUpdatedSlot,
		&vote.DeletedSlot,
	)
	return &vote, err
}

func (s *Store) GetCommitteeMember(
	coldCredentialTag uint8,
	coldKey []byte,
	termStartSlot uint64,
	txn types.Txn,
) (*models.AuthCommitteeHot, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	var member models.AuthCommitteeHot
	err = db.QueryRowContext(ctx, `
SELECT cold_credential_tag, cold_credential, hot_credential_tag,
       host_credential, id, certificate_id, added_slot
FROM auth_committee_hot
WHERE cold_credential_tag = ? AND cold_credential = ? AND added_slot >= ?
ORDER BY added_slot DESC, certificate_id DESC
LIMIT 1`,
		coldCredentialTag,
		coldKey,
		termStartSlot,
	).Scan(
		&member.ColdCredentialTag,
		&member.ColdCredential,
		&member.HotCredentialTag,
		&member.HotCredential,
		&member.ID,
		&member.CertificateID,
		&member.AddedSlot,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &member, err
}

func (s *Store) GetActiveCommitteeMembers(
	txn types.Txn,
) ([]*models.AuthCommitteeHot, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `
SELECT auth.cold_credential_tag, auth.cold_credential,
       auth.hot_credential_tag, auth.host_credential, auth.id,
       auth.certificate_id, auth.added_slot
FROM committee_member committee
JOIN (
    SELECT cold_credential_tag, cold_credential, hot_credential_tag,
           host_credential, id, certificate_id, added_slot,
           ROW_NUMBER() OVER (
               PARTITION BY cold_credential_tag, cold_credential
               ORDER BY added_slot DESC, certificate_id DESC
           ) rn
    FROM auth_committee_hot
) auth ON auth.cold_credential_tag = committee.cold_credential_tag
      AND auth.cold_credential = committee.cold_cred_hash
WHERE committee.deleted_slot IS NULL
  AND auth.rn = 1
  AND auth.added_slot >= committee.term_start_slot
  AND NOT EXISTS (
      SELECT 1 FROM resign_committee_cold resign
      WHERE resign.cold_credential_tag = auth.cold_credential_tag
        AND resign.cold_credential = auth.cold_credential
        AND resign.added_slot >= committee.term_start_slot
  )`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := []*models.AuthCommitteeHot{}
	for rows.Next() {
		var member models.AuthCommitteeHot
		if err := rows.Scan(
			&member.ColdCredentialTag,
			&member.ColdCredential,
			&member.HotCredentialTag,
			&member.HotCredential,
			&member.ID,
			&member.CertificateID,
			&member.AddedSlot,
		); err != nil {
			return nil, err
		}
		ret = append(ret, &member)
	}
	return ret, rows.Err()
}

func (s *Store) IsCommitteeMemberResigned(
	coldCredentialTag uint8,
	coldKey []byte,
	termStartSlot uint64,
	txn types.Txn,
) (bool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return false, err
	}
	var resigned bool
	err = db.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1 FROM resign_committee_cold
    WHERE cold_credential_tag = ? AND cold_credential = ? AND added_slot >= ?
)`,
		coldCredentialTag,
		coldKey,
		termStartSlot,
	).Scan(&resigned)
	return resigned, err
}

func (s *Store) GetResignedCommitteeMembers(
	coldCredentials []models.CommitteeCredential,
	txn types.Txn,
) (map[string]bool, error) {
	ret := make(map[string]bool)
	if len(coldCredentials) == 0 {
		return ret, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	// One round trip per member turns an epoch voting-state load into a query
	// per committee seat, so fetch the latest resignation slot for the whole
	// set at once. EXISTS(added_slot >= termStart) is equivalent to
	// MAX(added_slot) >= termStart, so the term comparison still happens per
	// credential, just in Go.
	latest := make(map[string]uint64, len(coldCredentials))
	chunkSize := s.dialect.ParameterLimit() / 2
	if chunkSize < 1 {
		chunkSize = 1
	}
	for start := 0; start < len(coldCredentials); start += chunkSize {
		end := min(start+chunkSize, len(coldCredentials))
		chunk := coldCredentials[start:end]
		predicates := make([]string, 0, len(chunk))
		args := make([]any, 0, len(chunk)*2)
		for _, credential := range chunk {
			predicates = append(
				predicates,
				"(cold_credential_tag = ? AND cold_credential = ?)",
			)
			args = append(args, credential.CredentialTag, credential.Credential)
		}
		query := `
SELECT cold_credential_tag, cold_credential, MAX(added_slot)
FROM resign_committee_cold
WHERE (` + strings.Join(predicates, " OR ") + `)
GROUP BY cold_credential_tag, cold_credential`
		rows, err := db.QueryContext(ctx, s.dialect.Rebind(query), args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var tag uint8
			var credential []byte
			var addedSlot uint64
			if err := rows.Scan(&tag, &credential, &addedSlot); err != nil {
				rows.Close()
				return nil, err
			}
			key := models.CommitteeCredential{
				CredentialTag: tag,
				Credential:    credential,
			}.Key()
			if existing, ok := latest[key]; !ok || addedSlot > existing {
				latest[key] = addedSlot
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	for _, credential := range coldCredentials {
		key := credential.Key()
		if addedSlot, ok := latest[key]; ok &&
			addedSlot >= credential.TermStartSlot {
			ret[key] = true
		}
	}
	return ret, nil
}

func (s *Store) GetCommitteeActiveCount(
	txn types.Txn,
) (int, error) {
	members, err := s.GetActiveCommitteeMembers(txn)
	return len(members), err
}
