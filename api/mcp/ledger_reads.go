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

package mcp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ledgerStateAPI is the part of the ledger the Cardano tools use. LedgerDB is
// the LedgerDB the ledger reads through: the decorator that answers ledger
// state from the live UTxO set in set mode, so a tool reading it sees what
// consensus sees.
type ledgerStateAPI interface {
	txEvaluator
	Tip() ochainsync.Tip
	CurrentSlot() (uint64, error)
	GetCurrentPParamsForReporting() lcommon.ProtocolParameters
	LedgerDB() ledgerdb.LedgerDB
	IterateLiveUtxos(
		ctx context.Context,
		fn func(*models.Utxo) error,
	) error
}

// ledgerStateOf returns ls as a ledgerStateAPI, or nil for a nil ls, which a
// typed nil pointer in the interface would not be.
func ledgerStateOf(ls *ledger.LedgerState) ledgerStateAPI {
	if ls == nil {
		return nil
	}
	return ls
}

// stateDBOf returns the LedgerDB the ledger state tools read through, or nil
// when there is no ledger state and the tools read the SQLite database.
func stateDBOf(ls ledgerStateAPI) ledgerdb.LedgerDB {
	if ls == nil {
		return nil
	}
	return ls.LedgerDB()
}

// epochSummaryRead is what get_epoch_summary reports: the epoch, its nonce,
// and how many blocks recorded a nonce in it.
type epochSummaryRead struct {
	epoch      int
	nonce      []byte
	blockCount int
	// startSlot and length are the epoch's boundaries, which an IndexerDB
	// count reads its blocks within.
	startSlot uint64
	length    uint64
}

// epochBlockCounter counts the blocks in the inclusive slot range
// [start, end].
type epochBlockCounter func(ctx context.Context, start, end uint64) (int, error)

// indexerReads are the reads the Cardano tools take from the IndexerDB of a
// node that has one. Each is nil on a node without an IndexerDB.
type indexerReads struct {
	addressLookup utxoAddressLookup
	// epochBlocks counts an epoch's blocks over all the history the
	// IndexerDB holds, where the ledger keeps only a retained window.
	epochBlocks epochBlockCounter
}

// errNoEpochs is what the SQLite path reports when no epoch row exists, the
// maximum of no rows being NULL.
var errNoEpochs = errors.New(
	"sql: Scan error on column index 0, name \"max(epoch_id)\": converting NULL to int is unsupported",
)

// epochSummaryFromLedger reads the epoch summary through ldb. An epoch the
// summary row names takes its nonce from there, else from the epoch row.
func epochSummaryFromLedger(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	requested *int,
) (epochSummaryRead, error) {
	var out epochSummaryRead
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	if requested != nil {
		out.epoch = *requested
		if out.epoch < 0 {
			return out, errors.New("epoch must be nonnegative")
		}
	} else {
		epochs, err := ldb.GetEpochs(txn)
		if err != nil {
			return out, fmt.Errorf("read latest epoch: %w", err)
		}
		if len(epochs) == 0 {
			return out, fmt.Errorf("read latest epoch: %w", errNoEpochs)
		}
		latest := slices.MaxFunc(epochs, func(a, b models.Epoch) int {
			switch {
			case a.EpochId < b.EpochId:
				return -1
			case a.EpochId > b.EpochId:
				return 1
			}
			return 0
		})
		out.epoch = int(latest.EpochId) //nolint:gosec // an epoch number
	}
	epochID := uint64(out.epoch) //nolint:gosec // nonnegative
	row, err := ldb.GetEpoch(epochID, txn)
	if err != nil {
		return out, fmt.Errorf("read epoch boundaries: %w", err)
	}
	if row == nil {
		return out, fmt.Errorf("read epoch boundaries: %w", sql.ErrNoRows)
	}
	out.nonce = row.Nonce
	summary, err := ldb.GetEpochSummary(epochID, txn)
	if err != nil {
		return out, fmt.Errorf("read epoch summary: %w", err)
	}
	if summary != nil && len(summary.EpochNonce) > 0 {
		out.nonce = summary.EpochNonce
	}
	length := row.LengthInSlots
	if length == 0 || row.StartSlot > ^uint64(0)-uint64(length) {
		return out, errors.New("invalid epoch boundaries")
	}
	nonces, err := ldb.GetBlockNoncesInSlotRange(
		row.StartSlot, row.StartSlot+uint64(length), txn,
	)
	if err != nil {
		return out, fmt.Errorf("count recorded epoch blocks: %w", err)
	}
	out.blockCount = len(nonces)
	out.startSlot, out.length = row.StartSlot, uint64(length)
	return out, nil
}

// governanceStateRead is what get_governance_state reports.
type governanceStateRead struct {
	constitution *models.Constitution
	// quorum is the latest committee quorum's text, empty when none is in
	// effect.
	quorum  string
	members []*models.CommitteeMember
	active  int
	// expiryRecorded counts the active DReps with an expiry epoch.
	expiryRecorded int
}

// governanceStateFromLedger reads the governance state through ldb.
func governanceStateFromLedger(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) (governanceStateRead, error) {
	var out governanceStateRead
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	constitution, err := ldb.GetConstitution(txn)
	if err != nil {
		return out, fmt.Errorf("read constitution: %w", err)
	}
	out.constitution = constitution
	quorum, err := ldb.LatestCommitteeQuorum(ctx, txn)
	if err != nil {
		return out, fmt.Errorf("read committee quorum: %w", err)
	}
	if quorum != nil && quorum.Quorum != nil && quorum.Quorum.Rat != nil {
		out.quorum = quorum.Quorum.String()
	}
	members, err := ldb.GetCommitteeMembers(ctx, txn)
	if err != nil {
		return out, fmt.Errorf("query committee members: %w", err)
	}
	out.members = slices.Clone(members)
	slices.SortStableFunc(
		out.members,
		func(a, b *models.CommitteeMember) int {
			switch {
			case a.ExpiresEpoch < b.ExpiresEpoch:
				return -1
			case a.ExpiresEpoch > b.ExpiresEpoch:
				return 1
			}
			return 0
		},
	)
	dreps, err := ldb.GetActiveDreps(ctx, txn)
	if err != nil {
		return out, fmt.Errorf("count active DReps: %w", err)
	}
	for _, d := range dreps {
		out.active++
		if d.ExpiryEpoch > 0 {
			out.expiryRecorded++
		}
	}
	return out, nil
}

// drepFromLedger reads the DRep of (tag, cred), active or not, through ldb,
// or nil when there is none.
func drepFromLedger(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	tag uint8,
	cred []byte,
) (*models.Drep, error) {
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	d, err := ldb.GetDrepByCredential(ctx, tag, cred, true, txn)
	if errors.Is(err, models.ErrDrepNotFound) {
		return nil, nil
	}
	return d, err
}

// proposalLifecycleMatches reports whether p is in the lifecycle state the
// get_governance_proposal status filter names. A soft-deleted proposal never
// is.
func proposalLifecycleMatches(
	p *models.GovernanceProposal,
	status string,
) bool {
	if p.DeletedSlot != nil {
		return false
	}
	switch status {
	case "ratified":
		return p.RatifiedEpoch != nil
	case "enacted":
		return p.EnactedEpoch != nil
	case "expired":
		return p.ExpiredEpoch != nil
	case "all":
		return true
	default:
		return p.EnactedEpoch == nil && p.RatifiedEpoch == nil &&
			p.ExpiredEpoch == nil
	}
}

// proposalTally counts a proposal's live votes by voter role and choice, keyed
// as get_governance_proposal prints them.
func proposalTallyFromVotes(
	votes []*models.GovernanceVote,
) (map[string]map[string]int, error) {
	tally := map[string]map[string]int{
		"CC":   {"Yes": 0, "No": 0, "Abstain": 0},
		"DRep": {"Yes": 0, "No": 0, "Abstain": 0},
		"SPO":  {"Yes": 0, "No": 0, "Abstain": 0},
	}
	for _, v := range votes {
		var role, choice string
		switch v.VoterType {
		case models.VoterTypeCC:
			role = "CC"
		case models.VoterTypeDRep:
			role = "DRep"
		case models.VoterTypeSPO:
			role = "SPO"
		default:
			return nil, fmt.Errorf("invalid voter type %d", v.VoterType)
		}
		switch v.Vote {
		case models.VoteNo:
			choice = "No"
		case models.VoteYes:
			choice = "Yes"
		case models.VoteAbstain:
			choice = "Abstain"
		default:
			return nil, fmt.Errorf("invalid vote %d", v.Vote)
		}
		tally[role][choice]++
	}
	return tally, nil
}

// governanceStateFromSQL reads the governance state from the SQLite
// database, for a server with no ledger state to read through.
func governanceStateFromSQL(
	ctx context.Context,
	db *sql.DB,
) (governanceStateRead, error) {
	var out governanceStateRead
	var constURL sql.NullString
	var constHash, constPolicyHash []byte
	var constSlot sql.NullInt64
	err := db.QueryRowContext(
		ctx,
		"SELECT anchor_url, anchor_hash, policy_hash, added_slot FROM constitution WHERE deleted_slot IS NULL OR deleted_slot = 0 ORDER BY added_slot DESC LIMIT 1",
	).Scan(&constURL, &constHash, &constPolicyHash, &constSlot)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return out, fmt.Errorf("read constitution: %w", err)
	}
	if err == nil && constURL.Valid {
		out.constitution = &models.Constitution{
			AnchorURL:  constURL.String,
			AnchorHash: constHash,
			PolicyHash: constPolicyHash,
			AddedSlot:  uint64(max(constSlot.Int64, 0)),
		}
	}

	var quorumStr sql.NullString
	var quorumSlot sql.NullInt64
	err = db.QueryRowContext(
		ctx,
		"SELECT quorum, added_slot FROM committee_quorum ORDER BY added_slot DESC LIMIT 1",
	).Scan(&quorumStr, &quorumSlot)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return out, fmt.Errorf("read committee quorum: %w", err)
	}
	if quorumStr.Valid {
		out.quorum = quorumStr.String
	}

	commRows, err := db.QueryContext(
		ctx,
		"SELECT cold_cred_hash, expires_epoch, term_start_slot FROM committee_member WHERE deleted_slot IS NULL OR deleted_slot = 0 ORDER BY expires_epoch ASC",
	)
	if err != nil {
		return out, fmt.Errorf("query committee members: %w", err)
	}
	defer commRows.Close()
	for commRows.Next() {
		var credHash []byte
		var expEpoch, termSlot int64
		if err := commRows.Scan(&credHash, &expEpoch, &termSlot); err != nil {
			return out, fmt.Errorf("read committee member: %w", err)
		}
		out.members = append(out.members, &models.CommitteeMember{
			ColdCredHash:  credHash,
			ExpiresEpoch:  uint64(max(expEpoch, 0)),
			TermStartSlot: uint64(max(termSlot, 0)),
		})
	}
	if err := commRows.Err(); err != nil {
		return out, fmt.Errorf("iterate committee members: %w", err)
	}

	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM drep WHERE active = TRUE").
		Scan(&out.active)
	if err != nil {
		return out, fmt.Errorf("count active DReps: %w", err)
	}
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM drep WHERE active = TRUE AND expiry_epoch > 0").
		Scan(&out.expiryRecorded)
	if err != nil {
		return out, fmt.Errorf("count DRep expiries: %w", err)
	}
	return out, nil
}

// drepRead is a DRep as get_governance_state prints it. anchorValid is
// whether the anchor URL is recorded, which the SQLite column can leave NULL.
type drepRead struct {
	models.Drep
	anchorValid bool
}

// drepDetailFromLedger reads a DRep through ldb, or nil when there is none.
func drepDetailFromLedger(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	tag uint8,
	cred []byte,
) (*drepRead, error) {
	d, err := drepFromLedger(ctx, ldb, tag, cred)
	if err != nil || d == nil {
		return nil, err
	}
	return &drepRead{Drep: *d, anchorValid: true}, nil
}

// drepDetailFromSQL reads a DRep from the SQLite database, or nil when there
// is none.
func drepDetailFromSQL(
	ctx context.Context,
	db *sql.DB,
	tag uint8,
	cred []byte,
) (*drepRead, error) {
	var anchorURL sql.NullString
	var anchorHash []byte
	var addedSlot, lastAct, expiry int64
	var active bool
	err := db.QueryRowContext(
		ctx,
		"SELECT anchor_url, anchor_hash, added_slot, last_activity_epoch, expiry_epoch, active FROM drep WHERE credential = ? AND credential_tag = ? LIMIT 1",
		cred,
		tag,
	).Scan(&anchorURL, &anchorHash, &addedSlot, &lastAct, &expiry, &active)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &drepRead{
		Drep: models.Drep{
			AnchorURL:         anchorURL.String,
			AnchorHash:        anchorHash,
			AddedSlot:         uint64(max(addedSlot, 0)),
			LastActivityEpoch: uint64(max(lastAct, 0)),
			ExpiryEpoch:       uint64(max(expiry, 0)),
			Active:            active,
		},
		anchorValid: anchorURL.Valid,
	}, nil
}

// proposalRead is a governance proposal as get_governance_proposal prints
// it. tally is set for a single proposal.
type proposalRead struct {
	txHash                      []byte
	actionIndex, actionType     int
	proposedEpoch, expiresEpoch int
	enacted, ratified, expired  *int64
	deposit                     uint64
	anchorURL                   string
	anchorURLValid              bool
	anchorHash, returnAddress   []byte
	tally                       map[string]map[string]int
}

func epochPtr(p *uint64) *int64 {
	if p == nil {
		return nil
	}
	v := int64(*p) //nolint:gosec // an epoch number
	return &v
}

func proposalReadOf(p *models.GovernanceProposal) *proposalRead {
	return &proposalRead{
		txHash:         p.TxHash,
		actionIndex:    int(p.ActionIndex),
		actionType:     int(p.ActionType),
		proposedEpoch:  int(p.ProposedEpoch), //nolint:gosec // an epoch number
		expiresEpoch:   int(p.ExpiresEpoch),  //nolint:gosec // an epoch number
		enacted:        epochPtr(p.RecordedEnactedEpoch()),
		ratified:       epochPtr(p.RatifiedEpoch),
		expired:        epochPtr(p.ExpiredEpoch),
		deposit:        uint64(p.Deposit),
		anchorURL:      p.AnchorURL,
		anchorURLValid: true,
		anchorHash:     p.AnchorHash,
		returnAddress:  p.ReturnAddress,
	}
}

// proposalDetailFromLedger reads the proposal txHash#actionIndex through ldb
// and its live votes, or nil when it is not in the lifecycle state status
// names.
func proposalDetailFromLedger(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	txHash []byte,
	actionIndex uint32,
	status string,
) (*proposalRead, error) {
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	p, err := ldb.GetGovernanceProposal(ctx, txHash, actionIndex, txn)
	if errors.Is(err, models.ErrGovernanceProposalNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query governance proposal: %w", err)
	}
	if p == nil || !proposalLifecycleMatches(p, status) {
		return nil, nil
	}
	votes, err := ldb.GetGovernanceVotes(ctx, p.ID, txn)
	if err != nil {
		return nil, fmt.Errorf("query vote tallies: %w", err)
	}
	out := proposalReadOf(p)
	out.tally, err = proposalTallyFromVotes(votes)
	return out, err
}

// proposalListFromLedger reads up to limit proposals in the lifecycle state
// status names through ldb, newest row first.
func proposalListFromLedger(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	status string,
	limit int,
) ([]*proposalRead, error) {
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	all, err := ldb.GetAllGovernanceProposals(ctx, txn)
	if err != nil {
		return nil, fmt.Errorf("list governance proposals: %w", err)
	}
	keep := make([]*models.GovernanceProposal, 0, len(all))
	for _, p := range all {
		if proposalLifecycleMatches(p, status) {
			keep = append(keep, p)
		}
	}
	slices.SortStableFunc(keep, func(a, b *models.GovernanceProposal) int {
		switch {
		case a.ID > b.ID:
			return -1
		case a.ID < b.ID:
			return 1
		}
		return 0
	})
	if len(keep) > limit {
		keep = keep[:limit]
	}
	out := make([]*proposalRead, 0, len(keep))
	for _, p := range keep {
		out = append(out, proposalReadOf(p))
	}
	return out, nil
}

// proposalDetailFromSQL is proposalDetailFromLedger over the SQLite
// database, with whereClause the lifecycle predicate.
func proposalDetailFromSQL(
	ctx context.Context,
	db *sql.DB,
	txHash []byte,
	actionIndex uint32,
	whereClause string,
) (*proposalRead, error) {
	//nolint:gosec // whereClause consists only of fixed lifecycle predicates.
	row := db.QueryRowContext(ctx, `
				SELECT id, tx_hash, action_index, action_type, proposed_epoch, expires_epoch,
				       enacted_epoch, ratified_epoch, expired_epoch, anchor_url, anchor_hash, deposit, return_address
				FROM governance_proposal
				WHERE tx_hash = ? AND action_index = ? AND deleted_slot IS NULL AND (`+whereClause+`)
				LIMIT 1`,
		txHash, actionIndex,
	)
	var (
		id                                                   int64
		out                                                  proposalRead
		enactedEpoch, ratifiedEpoch, expiredEpoch            sql.NullInt64
		anchorURL                                            sql.NullString
		actionIndexCol, actionType, proposedEpoch, expiresEp int
	)
	err := row.Scan(
		&id,
		&out.txHash,
		&actionIndexCol,
		&actionType,
		&proposedEpoch,
		&expiresEp,
		&enactedEpoch,
		&ratifiedEpoch,
		&expiredEpoch,
		&anchorURL,
		&out.anchorHash,
		&out.deposit,
		&out.returnAddress,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query governance proposal: %w", err)
	}
	out.actionIndex, out.actionType = actionIndexCol, actionType
	out.proposedEpoch, out.expiresEpoch = proposedEpoch, expiresEp
	out.enacted, out.ratified, out.expired = nullEpoch(enactedEpoch),
		nullEpoch(ratifiedEpoch), nullEpoch(expiredEpoch)
	out.anchorURL, out.anchorURLValid = anchorURL.String, anchorURL.Valid

	voteRows, err := db.QueryContext(ctx, `
				SELECT voter_type, vote, count(*)
				FROM governance_vote
				WHERE proposal_id = ? AND deleted_slot IS NULL
				GROUP BY voter_type, vote`,
		id,
	)
	tally := map[string]map[string]int{
		"CC":   {"Yes": 0, "No": 0, "Abstain": 0},
		"DRep": {"Yes": 0, "No": 0, "Abstain": 0},
		"SPO":  {"Yes": 0, "No": 0, "Abstain": 0},
	}
	if err != nil {
		return nil, fmt.Errorf("query vote tallies: %w", err)
	}
	defer voteRows.Close()
	for voteRows.Next() {
		var voterType, vote, count int
		if err := voteRows.Scan(&voterType, &vote, &count); err != nil {
			return nil, fmt.Errorf("scan vote tally: %w", err)
		}
		var role, choice string
		switch voterType {
		case models.VoterTypeCC:
			role = "CC"
		case models.VoterTypeDRep:
			role = "DRep"
		case models.VoterTypeSPO:
			role = "SPO"
		default:
			return nil, fmt.Errorf("invalid voter type %d", voterType)
		}
		switch vote {
		case models.VoteNo:
			choice = "No"
		case models.VoteYes:
			choice = "Yes"
		case models.VoteAbstain:
			choice = "Abstain"
		default:
			return nil, fmt.Errorf("invalid vote %d", vote)
		}
		byChoice := tally[role]
		if byChoice == nil {
			byChoice = make(map[string]int)
			tally[role] = byChoice
		}
		byChoice[choice] = count
	}
	if err := voteRows.Err(); err != nil {
		return nil, fmt.Errorf("read vote tallies: %w", err)
	}
	out.tally = tally
	return &out, nil
}

func nullEpoch(n sql.NullInt64) *int64 {
	if !n.Valid {
		return nil
	}
	v := n.Int64
	return &v
}

// proposalListFromSQL is proposalListFromLedger over the SQLite database.
func proposalListFromSQL(
	ctx context.Context,
	db *sql.DB,
	whereClause string,
	limit int,
) ([]*proposalRead, error) {
	//nolint:gosec // whereClause contains only fixed predicates selected above.
	query := fmt.Sprintf(`
			SELECT id, tx_hash, action_index, action_type, proposed_epoch, expires_epoch,
			       enacted_epoch, ratified_epoch, expired_epoch, deposit, anchor_url
			FROM governance_proposal
			WHERE deleted_slot IS NULL AND (%s)
			ORDER BY id DESC
			LIMIT ?`, whereClause)
	rows, err := db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("list governance proposals: %w", err)
	}
	defer rows.Close()
	var out []*proposalRead
	for rows.Next() {
		var (
			id                                        int64
			p                                         proposalRead
			enactedEpoch, ratifiedEpoch, expiredEpoch sql.NullInt64
			anchorURL                                 sql.NullString
		)
		if scanErr := rows.Scan(
			&id, &p.txHash, &p.actionIndex, &p.actionType, &p.proposedEpoch,
			&p.expiresEpoch, &enactedEpoch, &ratifiedEpoch, &expiredEpoch,
			&p.deposit, &anchorURL,
		); scanErr != nil {
			return nil, fmt.Errorf("scan governance proposal: %w", scanErr)
		}
		p.enacted, p.ratified = nullEpoch(
			enactedEpoch,
		), nullEpoch(
			ratifiedEpoch,
		)
		p.expired = nullEpoch(expiredEpoch)
		out = append(out, &p)
	}
	if rErr := rows.Err(); rErr != nil {
		return nil, fmt.Errorf("read governance proposals: %w", rErr)
	}
	return out, nil
}

// proposalStatusName is the lifecycle filter get_governance_proposal applies
// for the normalized status argument.
func proposalStatusName(statusFilter string) string {
	if statusFilter == "" {
		return "active"
	}
	return statusFilter
}

func proposalStatus(p *proposalRead, withEpoch bool) string {
	suffix := func(e *int64) string {
		if withEpoch {
			return fmt.Sprintf(" (epoch %d)", *e)
		}
		return ""
	}
	switch {
	case p.enacted != nil:
		return "Enacted" + suffix(p.enacted)
	case p.ratified != nil:
		return "Ratified" + suffix(p.ratified)
	case p.expired != nil:
		return "Expired" + suffix(p.expired)
	}
	return "Active"
}

// renderProposalDetail prints one proposal and its vote tally.
func renderProposalDetail(cleanHash string, p *proposalRead) string {
	urlStr := "none"
	if p.anchorURLValid && p.anchorURL != "" {
		urlStr = formatUntrustedInline(p.anchorURL)
	}
	anchorHashHex := "none"
	if len(p.anchorHash) > 0 {
		anchorHashHex = hex.EncodeToString(p.anchorHash)
	}
	var sb strings.Builder
	fmt.Fprintf(
		&sb,
		"### Governance Action Proposal `%s#%d`\n\n",
		cleanHash,
		p.actionIndex,
	)
	sb.WriteString("| Property | Value |\n")
	sb.WriteString("| :--- | :--- |\n")
	fmt.Fprintf(
		&sb,
		"| **Action Type** | %s (ID %d) |\n",
		govActionTypeName(p.actionType),
		p.actionType,
	)
	fmt.Fprintf(&sb, "| **Lifecycle Status** | %s |\n", proposalStatus(p, true))
	fmt.Fprintf(&sb, "| **Proposed Epoch** | %d |\n", p.proposedEpoch)
	fmt.Fprintf(&sb, "| **Expires Epoch** | %d |\n", p.expiresEpoch)
	fmt.Fprintf(
		&sb,
		"| **Proposal Deposit** | %d Lovelace (%.6f ADA) |\n",
		p.deposit,
		float64(p.deposit)/1000000.0,
	)
	returnAddress := "none"
	if len(p.returnAddress) > 0 {
		address, err := lcommon.NewAddressFromBytes(p.returnAddress)
		if err != nil {
			returnAddress = "0x" + hex.EncodeToString(p.returnAddress)
		} else {
			returnAddress = address.String()
		}
	}
	fmt.Fprintf(&sb, "| **Deposit Return Address** | `%s` |\n", returnAddress)
	fmt.Fprintf(&sb, "| **Metadata Anchor URL** | %s |\n", urlStr)
	fmt.Fprintf(
		&sb,
		"| **Metadata Anchor Hash** | `%s` |\n\n",
		anchorHashHex,
	)

	sb.WriteString("#### CIP-1694 Voting Procedure Breakdown\n\n")
	sb.WriteString("| Voter Role | Yes | No | Abstain |\n")
	sb.WriteString("| :--- | :--- | :--- | :--- |\n")
	fmt.Fprintf(
		&sb,
		"| **Constitutional Committee (CC)** | %d | %d | %d |\n",
		p.tally["CC"]["Yes"], p.tally["CC"]["No"], p.tally["CC"]["Abstain"],
	)
	fmt.Fprintf(
		&sb,
		"| **Delegated Representatives (DReps)** | %d | %d | %d |\n",
		p.tally["DRep"]["Yes"],
		p.tally["DRep"]["No"],
		p.tally["DRep"]["Abstain"],
	)
	fmt.Fprintf(
		&sb,
		"| **Stake Pool Operators (SPOs)** | %d | %d | %d |\n",
		p.tally["SPO"]["Yes"], p.tally["SPO"]["No"], p.tally["SPO"]["Abstain"],
	)
	return sb.String()
}

// renderProposalList prints the proposals as a table.
func renderProposalList(proposals []*proposalRead) string {
	var sb strings.Builder
	sb.WriteString("### Conway CIP-1694 Governance Action Proposals\n\n")
	sb.WriteString(
		"| Proposal Tx#Index | Action Type | Status | Proposed | Expires | Deposit (ADA) |\n",
	)
	sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- |\n")
	for _, p := range proposals {
		fmt.Fprintf(
			&sb,
			"| `%s#%d` | %s | %s | Epoch %d | Epoch %d | %.2f |\n",
			hex.EncodeToString(p.txHash),
			p.actionIndex,
			govActionTypeName(p.actionType),
			proposalStatus(p, false),
			p.proposedEpoch,
			p.expiresEpoch,
			float64(p.deposit)/1000000.0,
		)
	}
	if len(proposals) == 0 {
		sb.WriteString(
			"\n*No governance action proposals found matching the selected filter criteria.*\n",
		)
	}
	return sb.String()
}

// needsIndexerDB is how a tool says that what it was asked for is an API
// index, which only an IndexerDB holds.
const needsIndexerDB = "needs an IndexerDB"

// apiIndexesUnavailable is what a tool that reads only API indexes says on a
// server with no IndexerDB to read them from.
const apiIndexesUnavailable = "this tool reads API indexes and " +
	needsIndexerDB + ", which this node does not have"

// accountFromLedger reads the account (tag, cred) through ldb, inactive or
// not, or nil when the ledger holds none.
func accountFromLedger(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	tag uint8,
	cred []byte,
) (*models.Account, error) {
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	a, err := ldb.GetAccountByCredential(ctx, tag, cred, true, txn)
	if errors.Is(err, models.ErrAccountNotFound) {
		return nil, nil
	}
	return a, err
}

// renderAccount prints a as get_account prints a row of the account table.
// id and certificate_id are IndexerDB row references: rowRefs holds them
// when the server reads an IndexerDB, and nil says they need one.
func renderAccount(cred string, a *models.Account, rowRefs []string) string {
	hexOrNull := func(b []byte) string {
		if b == nil {
			return "null"
		}
		return hex.EncodeToString(b)
	}
	cols := []string{
		"staking_key", "credential_tag", "pool", "drep", "drep_type",
		"active", "reward", "added_slot", "created_slot", "expiration_epoch",
		"id", "certificate_id",
	}
	row := []string{
		hexOrNull(a.StakingKey),
		strconv.FormatUint(uint64(a.CredentialTag), 10),
		hexOrNull(a.Pool),
		hexOrNull(a.Drep),
		strconv.FormatUint(a.DrepType, 10),
		strconv.FormatBool(a.Active),
		strconv.FormatUint(uint64(a.Reward), 10),
		strconv.FormatUint(a.AddedSlot, 10),
		strconv.FormatUint(a.CreatedSlot, 10),
		strconv.FormatUint(a.ExpirationEpoch, 10),
	}
	note := ""
	if rowRefs != nil {
		row = append(row, rowRefs...)
	} else {
		row = append(row, needsIndexerDB, needsIndexerDB)
		note = "\n*`id` and `certificate_id` are IndexerDB row references; " +
			"this node has no IndexerDB to read them from.*\n"
	}
	return fmt.Sprintf(
		"### Stake Account: `%s`\n\n%s%s",
		cred, FormatMarkdownTable(cols, [][]string{row}), note,
	)
}

// accountRowRefs reads the IndexerDB row id and certificate id of the account
// (tag, cred), "null" for a column the row leaves NULL and for an account
// the IndexerDB has no row of.
func accountRowRefs(
	ctx context.Context,
	db *sql.DB,
	tag uint8,
	cred []byte,
) ([]string, error) {
	var id, certID sql.NullInt64
	err := db.QueryRowContext(
		ctx,
		"SELECT id, certificate_id FROM account WHERE staking_key = ? AND credential_tag = ? LIMIT 1",
		cred, tag,
	).Scan(&id, &certID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	out := []string{"null", "null"}
	if id.Valid {
		out[0] = strconv.FormatInt(id.Int64, 10)
	}
	if certID.Valid {
		out[1] = strconv.FormatInt(certID.Int64, 10)
	}
	return out, nil
}

// poolPerformanceFromLedger reads up to limit epochs of the pool's reward
// outputs through ldb, newest first, each with the pool's reward inputs of
// that epoch. A requested epoch reads that epoch alone.
func poolPerformanceFromLedger(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	pool []byte,
	requested *int,
	limit int,
) ([][]string, error) {
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	var epochs []uint64
	if requested != nil {
		if *requested < 0 {
			return nil, errors.New("epoch must be nonnegative")
		}
		epochs = []uint64{uint64(*requested)}
	} else {
		all, err := ldb.GetEpochs(txn)
		if err != nil {
			return nil, fmt.Errorf("read epochs: %w", err)
		}
		for _, e := range all {
			epochs = append(epochs, e.EpochId)
		}
		slices.Sort(epochs)
		slices.Reverse(epochs)
	}
	var rows [][]string
	for _, epoch := range epochs {
		if len(rows) >= limit {
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		outputs, err := ldb.GetRewardPoolOutputs(epoch, txn)
		if err != nil {
			return nil, fmt.Errorf("read reward outputs of epoch %d: %w", epoch, err)
		}
		i := slices.IndexFunc(outputs, func(o *models.RewardPoolOutput) bool {
			return bytes.Equal(o.PoolKeyHash, pool)
		})
		if i < 0 {
			continue
		}
		o := outputs[i]
		inputs, err := ldb.GetRewardPoolInputs(epoch, txn)
		if err != nil {
			return nil, fmt.Errorf("read reward inputs of epoch %d: %w", epoch, err)
		}
		var in *models.RewardPoolInput
		if j := slices.IndexFunc(inputs, func(p *models.RewardPoolInput) bool {
			return bytes.Equal(p.PoolKeyHash, pool)
		}); j >= 0 {
			in = inputs[j]
		}
		rows = append(rows, poolPerformanceRow(o, in))
	}
	return rows, nil
}

// poolPerformanceRow is the get_pool_performance row of one epoch: "-" for
// an input the epoch has no row of, as the SQL join leaves it NULL.
func poolPerformanceRow(
	o *models.RewardPoolOutput,
	in *models.RewardPoolInput,
) []string {
	ratOr := func(r *types.Rat, none string) string {
		if r == nil || r.Rat == nil {
			return none
		}
		return r.String()
	}
	u := func(v types.Uint64) string { return strconv.FormatUint(uint64(v), 10) }
	blocks, delegated, pledge, cost, margin := "0", "-", "-", "-", "-"
	if in != nil {
		if in.BlocksProduced != nil {
			blocks = strconv.FormatUint(*in.BlocksProduced, 10)
		}
		delegated, pledge, cost = u(in.DelegatedStake), u(in.Pledge), u(in.Cost)
		margin = ratOr(in.Margin, "-")
	}
	return []string{
		strconv.FormatUint(o.Epoch, 10),
		blocks,
		delegated,
		pledge,
		cost,
		margin,
		ratOr(o.ApparentPerformance, "-"),
		u(o.TotalReward),
		u(o.LeaderReward),
		u(o.MemberRewardTotal),
	}
}

// liveUtxosFromLedger returns the rows of the live outputs whose address
// match accepts, skipping the first offset matches and stopping after limit,
// in the order the ledger's live UTxO set yields them.
func liveUtxosFromLedger(
	ctx context.Context,
	ls ledgerStateAPI,
	match func(lcommon.Address) bool,
	offset, limit int,
) ([][]string, error) {
	errDone := errors.New("page filled")
	var rows [][]string
	skipped := 0
	err := ls.IterateLiveUtxos(ctx, func(u *models.Utxo) error {
		out, err := u.Decode()
		if err != nil {
			return fmt.Errorf("decode utxo %x#%d: %w", u.TxId, u.OutputIdx, err)
		}
		if !match(out.Address()) {
			return nil
		}
		if skipped < offset {
			skipped++
			return nil
		}
		assets := 0
		if ma := out.Assets(); ma != nil {
			for _, policy := range ma.Policies() {
				assets += len(ma.Assets(policy))
			}
		}
		datumHash := "null"
		if h := out.DatumHash(); h != nil {
			datumHash = h.String()
		}
		rows = append(rows, []string{
			hex.EncodeToString(u.TxId),
			strconv.FormatUint(uint64(u.OutputIdx), 10),
			out.Address().String(),
			out.Amount().String(),
			strconv.Itoa(assets),
			datumHash,
		})
		if len(rows) >= limit {
			return errDone
		}
		return nil
	})
	if err != nil && !errors.Is(err, errDone) {
		return nil, err
	}
	return rows, nil
}

// liveUtxoColumns are the columns liveUtxosFromLedger fills.
var liveUtxoColumns = []string{
	"tx_hash", "output_index", "address", "lovelace", "assets", "datum_hash",
}

// liveUtxoPage answers get_utxos from the ledger's live UTxO set, for a
// server with no IndexerDB: a full address matches its outputs exactly, a
// credential the outputs whose address carries it. Without an IndexerDB's
// address index the set is read in full, in its own order, so pages are by
// offset and there is no cursor.
func liveUtxoPage(
	ctx context.Context,
	ls ledgerStateAPI,
	limits *readLimits,
	addr, cursor string,
	limit, offset int,
) (*mcp.CallToolResult, any, error) {
	if cursor != "" {
		return nil, nil, errors.New(
			"cursor pagination " + needsIndexerDB + "; page by offset",
		)
	}
	if limits.maxOffset > 0 && offset > limits.maxOffset {
		return nil, nil, errOffsetTooLarge
	}
	var match func(lcommon.Address) bool
	if address, err := lcommon.NewAddress(addr); err == nil {
		want, err := address.Bytes()
		if err != nil {
			return nil, nil, err
		}
		match = func(a lcommon.Address) bool {
			raw, err := a.Bytes()
			return err == nil && bytes.Equal(raw, want)
		}
	} else {
		pk, sk, err := parseAddressOrCredential(addr)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Invalid address or payment credential: " + err.Error(),
					},
				},
			}, nil, nil
		}
		match = func(a lcommon.Address) bool {
			payment, stake := addressCredentialHashes(a)
			return (len(pk) == 0 || bytes.Equal(payment, pk)) &&
				(len(sk) == 0 || bytes.Equal(stake, sk))
		}
	}
	if err := limits.admit(); err != nil {
		return nil, nil, err
	}
	defer limits.release()
	rows, err := liveUtxosFromLedger(ctx, ls, match, offset, limit)
	if err != nil {
		return nil, nil, fmt.Errorf("read the live UTxO set: %w", err)
	}
	header := fmt.Sprintf(
		"### UTxOs for `%s`\n\n*(Showing %d results, offset %d, from the "+
			"ledger's live UTxO set. Cursor pages and the stored utxo row "+
			"columns %s.)*\n\n%s",
		addr, len(rows), offset, needsIndexerDB,
		offsetLimitNote(offset, limit, len(rows), limits.maxOffset),
	)
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{
				Text: header + FormatMarkdownTable(liveUtxoColumns, rows),
			},
		},
	}, nil, nil
}

// poolPerformanceColumns are the columns of a get_pool_performance row.
var poolPerformanceColumns = []string{
	"Epoch",
	"Blocks",
	"Delegated Stake (lovelace)",
	"Pledge (lovelace)",
	"Fixed Cost (lovelace)",
	"Margin",
	"Apparent Performance",
	"Total Reward",
	"Leader Reward",
	"Member Rewards",
}

// renderPoolPerformance prints the pool's rows, with source naming where
// they were read from when that limits them.
func renderPoolPerformance(
	poolID string,
	pool []byte,
	rows [][]string,
	source string,
) string {
	if len(rows) == 0 {
		return fmt.Sprintf(
			"No historical reward performance records found for pool `%s` (hash `%s`).\n\n"+
				"**Possible reasons:**\n"+
				"1. The pool did not produce blocks in recent epochs.\n"+
				"2. Reward snapshot calculations for this epoch have not finalized yet.\n"+
				"3. The pool ID is valid but not currently registered on this network.%s",
			poolID,
			hex.EncodeToString(pool),
			source,
		)
	}
	return fmt.Sprintf(
		"### Performance History for Pool `%s`\n\n*(Showing %d epoch snapshots)*\n\n%s%s",
		poolID,
		len(rows),
		FormatMarkdownTable(poolPerformanceColumns, rows),
		source,
	)
}
