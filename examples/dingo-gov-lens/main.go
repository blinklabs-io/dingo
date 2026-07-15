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

package main

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed static
var staticFiles embed.FS

type app struct {
	db         *sql.DB
	govtoolURL string
}

type statusResponse struct {
	Network             string          `json:"network"`
	StorageMode         string          `json:"storageMode"`
	Tip                 *tip            `json:"tip,omitempty"`
	LatestEpoch         *epoch          `json:"latestEpoch,omitempty"`
	ProposalCount       int64           `json:"proposalCount"`
	GovernanceVoteCount int64           `json:"governanceVoteCount"`
	ActiveDrepCount     int64           `json:"activeDrepCount"`
	MinLiveProposalSlot uint64          `json:"minLiveProposalSlot,omitempty"`
	Backfill            *backfillStatus `json:"backfill,omitempty"`
	VoteBackfillPending bool            `json:"voteBackfillPending"`
	LastMetadataWrite   *time.Time      `json:"lastMetadataWrite,omitempty"`
}

type tip struct {
	Slot        uint64 `json:"slot"`
	BlockNumber uint64 `json:"blockNumber"`
	Hash        string `json:"hash"`
}

type epoch struct {
	EpochID      uint64 `json:"epochId"`
	StartSlot    uint64 `json:"startSlot"`
	EraID        uint64 `json:"eraId"`
	LengthSlots  uint64 `json:"lengthSlots"`
	SlotLengthMs uint64 `json:"slotLengthMs"`
}

type backfillStatus struct {
	LastSlot   uint64     `json:"lastSlot"`
	TotalSlots uint64     `json:"totalSlots,omitempty"`
	Completed  bool       `json:"completed"`
	UpdatedAt  *time.Time `json:"updatedAt,omitempty"`
}

type proposal struct {
	ID             int64     `json:"id"`
	TxHash         string    `json:"txHash"`
	ActionIndex    uint64    `json:"actionIndex"`
	ActionType     int64     `json:"actionType"`
	ActionTypeName string    `json:"actionTypeName"`
	ProposedEpoch  uint64    `json:"proposedEpoch"`
	ExpiresEpoch   uint64    `json:"expiresEpoch"`
	AddedSlot      uint64    `json:"addedSlot"`
	Lifecycle      string    `json:"lifecycle"`
	AnchorURL      string    `json:"anchorUrl,omitempty"`
	AnchorHash     string    `json:"anchorHash,omitempty"`
	Deposit        string    `json:"deposit,omitempty"`
	GovToolURL     string    `json:"govtoolUrl"`
	Votes          voteStats `json:"votes"`
}

type proposalDetail struct {
	Proposal proposal   `json:"proposal"`
	Votes    []voteRow  `json:"votes"`
	Summary  voteStats  `json:"summary"`
	Parent   *actionRef `json:"parent,omitempty"`
}

type actionRef struct {
	TxHash      string `json:"txHash"`
	ActionIndex uint64 `json:"actionIndex"`
}

type voteStats struct {
	Committee choiceStats `json:"committee"`
	DRep      choiceStats `json:"drep"`
	SPO       choiceStats `json:"spo"`
	Total     choiceStats `json:"total"`
}

type choiceStats struct {
	No      int64 `json:"no"`
	Yes     int64 `json:"yes"`
	Abstain int64 `json:"abstain"`
}

type voteRow struct {
	VoterType       int64  `json:"voterType"`
	VoterTypeName   string `json:"voterTypeName"`
	VoterCredential string `json:"voterCredential"`
	Vote            int64  `json:"vote"`
	VoteName        string `json:"voteName"`
	AddedSlot       uint64 `json:"addedSlot"`
	UpdatedSlot     uint64 `json:"updatedSlot,omitempty"`
	AnchorURL       string `json:"anchorUrl,omitempty"`
	AnchorHash      string `json:"anchorHash,omitempty"`
}

type drep struct {
	Credential        string `json:"credential"`
	CredentialTag     uint8  `json:"credentialTag"`
	AnchorURL         string `json:"anchorUrl,omitempty"`
	AnchorHash        string `json:"anchorHash,omitempty"`
	AddedSlot         uint64 `json:"addedSlot"`
	LastActivityEpoch uint64 `json:"lastActivityEpoch"`
	ExpiryEpoch       uint64 `json:"expiryEpoch"`
	Active            bool   `json:"active"`
	DelegatorCount    int64  `json:"delegatorCount"`
	VoteCount         int64  `json:"voteCount"`
}

type drepDetail struct {
	DRep        drep              `json:"drep"`
	RecentVotes []drepVote        `json:"recentVotes"`
	Delegations []accountSummary  `json:"delegations"`
	History     []drepHistoryItem `json:"history"`
}

type drepVote struct {
	ProposalTxHash  string `json:"proposalTxHash"`
	ActionIndex     uint64 `json:"actionIndex"`
	ActionTypeName  string `json:"actionTypeName"`
	Vote            int64  `json:"vote"`
	VoteName        string `json:"voteName"`
	AddedSlot       uint64 `json:"addedSlot"`
	UpdatedSlot     uint64 `json:"updatedSlot,omitempty"`
	ProposalGovTool string `json:"proposalGovtoolUrl"`
}

type accountSummary struct {
	StakingKey    string `json:"stakingKey"`
	CredentialTag uint8  `json:"credentialTag"`
	CreatedSlot   uint64 `json:"createdSlot"`
	Reward        string `json:"reward"`
	Active        bool   `json:"active"`
}

type drepHistoryItem struct {
	Kind       string `json:"kind"`
	Slot       uint64 `json:"slot"`
	TxHash     string `json:"txHash,omitempty"`
	AnchorURL  string `json:"anchorUrl,omitempty"`
	AnchorHash string `json:"anchorHash,omitempty"`
	Deposit    string `json:"deposit,omitempty"`
}

type stakeLookup struct {
	StakingKey    string `json:"stakingKey"`
	CredentialTag uint8  `json:"credentialTag"`
	CreatedSlot   uint64 `json:"createdSlot"`
	Pool          string `json:"pool,omitempty"`
	DRep          string `json:"drep,omitempty"`
	DRepType      int64  `json:"drepType"`
	Reward        string `json:"reward"`
	Active        bool   `json:"active"`
}

func main() {
	addr := envOrDefault("ADDR", ":8088")
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(envInt("DB_MAX_OPEN_CONNS", 6))
	db.SetMaxIdleConns(envInt("DB_MAX_IDLE_CONNS", 3))
	db.SetConnMaxLifetime(15 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("ping database: %v", err)
	}

	a := &app{
		db:         db,
		govtoolURL: strings.TrimRight(envOrDefault("GOVTOOL_BASE_URL", "https://preview.gov.tools"), "/"),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/status", a.handleStatus)
	mux.HandleFunc("GET /api/proposals", a.handleProposals)
	mux.HandleFunc("GET /api/proposals/{txHash}/{index}", a.handleProposalDetail)
	mux.HandleFunc("GET /api/dreps", a.handleDreps)
	mux.HandleFunc("GET /api/dreps/{credential}", a.handleDrepDetail)
	mux.HandleFunc("GET /api/stake/{credential}", a.handleStakeLookup)

	static, err := fs.Sub(staticFiles, "static")
	if err != nil {
		log.Fatalf("load static files: %v", err)
	}
	mux.Handle("/", http.FileServer(http.FS(static)))

	server := &http.Server{
		Addr:              addr,
		Handler:           withSecurityHeaders(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	log.Printf("Dingo Gov Lens listening on %s", addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen: %v", err)
	}
}

func (a *app) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ret := statusResponse{}

	// node_settings may not be populated yet during early sync, so a missing
	// row is acceptable; any other error means the backend is unhealthy.
	if err := a.db.QueryRowContext(ctx, `
		SELECT COALESCE(network, ''), COALESCE(storage_mode, '')
		FROM node_settings
		ORDER BY id ASC
		LIMIT 1
	`).Scan(&ret.Network, &ret.StorageMode); err != nil &&
		!errors.Is(err, sql.ErrNoRows) {
		serverError(w, r, "query node settings", err)
		return
	}

	var t tip
	switch err := a.db.QueryRowContext(ctx, `
		SELECT slot, block_number, encode(hash, 'hex')
		FROM tip
		ORDER BY id ASC
		LIMIT 1
	`).Scan(&t.Slot, &t.BlockNumber, &t.Hash); {
	case err == nil:
		ret.Tip = &t
	case errors.Is(err, sql.ErrNoRows):
	default:
		serverError(w, r, "query tip", err)
		return
	}

	var e epoch
	switch err := a.db.QueryRowContext(ctx, `
		SELECT epoch_id, start_slot, era_id, length_in_slots, slot_length
		FROM epoch
		ORDER BY epoch_id DESC
		LIMIT 1
	`).Scan(&e.EpochID, &e.StartSlot, &e.EraID, &e.LengthSlots, &e.SlotLengthMs); {
	case err == nil:
		ret.LatestEpoch = &e
	case errors.Is(err, sql.ErrNoRows):
	default:
		serverError(w, r, "query epoch", err)
		return
	}

	// COUNT/MIN aggregates always return exactly one row, so any error here is
	// a genuine backend failure rather than missing data.
	if err := a.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM governance_proposal
		WHERE deleted_slot IS NULL
	`).Scan(&ret.ProposalCount); err != nil {
		serverError(w, r, "count proposals", err)
		return
	}
	if err := a.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM governance_vote
		WHERE deleted_slot IS NULL
	`).Scan(&ret.GovernanceVoteCount); err != nil {
		serverError(w, r, "count votes", err)
		return
	}
	var minLiveProposalSlot sql.NullInt64
	if err := a.db.QueryRowContext(ctx, `
		SELECT MIN(added_slot) FILTER (WHERE added_slot > 0)
		FROM governance_proposal
		WHERE deleted_slot IS NULL
	`).Scan(&minLiveProposalSlot); err != nil {
		serverError(w, r, "query min proposal slot", err)
		return
	}
	if minLiveProposalSlot.Valid && minLiveProposalSlot.Int64 > 0 {
		ret.MinLiveProposalSlot = uint64(minLiveProposalSlot.Int64)
	}
	if err := a.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM drep
		WHERE active = true
	`).Scan(&ret.ActiveDrepCount); err != nil {
		serverError(w, r, "count dreps", err)
		return
	}
	var bf backfillStatus
	var bfUpdatedAt sql.NullTime
	switch err := a.db.QueryRowContext(ctx, `
		SELECT last_slot, total_slots, completed, updated_at
		FROM backfill_checkpoint
		WHERE phase = 'metadata'
	`).Scan(
		&bf.LastSlot,
		&bf.TotalSlots,
		&bf.Completed,
		&bfUpdatedAt,
	); {
	case err == nil:
		if bfUpdatedAt.Valid {
			bf.UpdatedAt = &bfUpdatedAt.Time
		}
		ret.Backfill = &bf
	case errors.Is(err, sql.ErrNoRows):
	default:
		serverError(w, r, "query backfill checkpoint", err)
		return
	}
	ret.VoteBackfillPending = voteBackfillPending(
		ret.GovernanceVoteCount,
		ret.MinLiveProposalSlot,
		ret.Backfill,
	)
	var ts int64
	switch err := a.db.QueryRowContext(ctx, `
		SELECT timestamp
		FROM commit_timestamp
		WHERE id = 1
	`).Scan(&ts); {
	case err == nil:
		if ts > 0 {
			v := time.UnixMilli(ts)
			ret.LastMetadataWrite = &v
		}
	case errors.Is(err, sql.ErrNoRows):
	default:
		serverError(w, r, "query commit timestamp", err)
		return
	}
	writeJSON(w, http.StatusOK, ret)
}

func voteBackfillPending(
	voteCount int64,
	minLiveProposalSlot uint64,
	backfill *backfillStatus,
) bool {
	return voteCount == 0 &&
		minLiveProposalSlot > 0 &&
		backfill != nil &&
		!backfill.Completed &&
		backfill.LastSlot < minLiveProposalSlot
}

func (a *app) handleProposals(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	lifecycle := r.URL.Query().Get("lifecycle")
	actionType := r.URL.Query().Get("action_type")
	limit := boundedLimit(r.URL.Query().Get("limit"), 100, 250)

	args := []any{}
	where := []string{"gp.deleted_slot IS NULL"}
	switch lifecycle {
	case "active":
		where = append(
			where,
			"gp.enacted_epoch IS NULL",
			"gp.expired_epoch IS NULL",
			"gp.ratified_epoch IS NULL",
		)
	case "ratified":
		where = append(
			where,
			"gp.ratified_epoch IS NOT NULL",
			"gp.enacted_epoch IS NULL",
			"gp.expired_epoch IS NULL",
		)
	case "enacted":
		where = append(where, "gp.enacted_epoch IS NOT NULL")
	case "expired":
		where = append(where, "gp.expired_epoch IS NOT NULL")
	case "":
	default:
		http.Error(w, "invalid lifecycle", http.StatusBadRequest)
		return
	}
	if actionType != "" {
		n, err := strconv.ParseInt(actionType, 10, 64)
		if err != nil {
			http.Error(w, "invalid action_type", http.StatusBadRequest)
			return
		}
		args = append(args, n)
		where = append(where, fmt.Sprintf("gp.action_type = $%d", len(args)))
	}
	args = append(args, limit)

	rows, err := a.db.QueryContext(ctx, `
		SELECT
			gp.id,
			encode(gp.tx_hash, 'hex') AS tx_hash,
			gp.action_index,
			gp.action_type,
			gp.proposed_epoch,
			gp.expires_epoch,
			gp.added_slot,
			COALESCE(gp.anchor_url, ''),
			COALESCE(encode(gp.anchor_hash, 'hex'), ''),
			COALESCE(gp.deposit::text, ''),
			CASE
				WHEN gp.enacted_epoch IS NOT NULL THEN 'enacted'
				WHEN gp.expired_epoch IS NOT NULL THEN 'expired'
				WHEN gp.ratified_epoch IS NOT NULL THEN 'ratified'
				ELSE 'active'
			END AS lifecycle,
			COUNT(*) FILTER (WHERE gv.voter_type = 0 AND gv.vote = 0 AND gv.deleted_slot IS NULL) AS cc_no,
			COUNT(*) FILTER (WHERE gv.voter_type = 0 AND gv.vote = 1 AND gv.deleted_slot IS NULL) AS cc_yes,
			COUNT(*) FILTER (WHERE gv.voter_type = 0 AND gv.vote = 2 AND gv.deleted_slot IS NULL) AS cc_abstain,
			COUNT(*) FILTER (WHERE gv.voter_type = 1 AND gv.vote = 0 AND gv.deleted_slot IS NULL) AS drep_no,
			COUNT(*) FILTER (WHERE gv.voter_type = 1 AND gv.vote = 1 AND gv.deleted_slot IS NULL) AS drep_yes,
			COUNT(*) FILTER (WHERE gv.voter_type = 1 AND gv.vote = 2 AND gv.deleted_slot IS NULL) AS drep_abstain,
			COUNT(*) FILTER (WHERE gv.voter_type = 2 AND gv.vote = 0 AND gv.deleted_slot IS NULL) AS spo_no,
			COUNT(*) FILTER (WHERE gv.voter_type = 2 AND gv.vote = 1 AND gv.deleted_slot IS NULL) AS spo_yes,
			COUNT(*) FILTER (WHERE gv.voter_type = 2 AND gv.vote = 2 AND gv.deleted_slot IS NULL) AS spo_abstain
		FROM governance_proposal gp
		LEFT JOIN governance_vote gv ON gv.proposal_id = gp.id
		WHERE `+strings.Join(where, " AND ")+`
		GROUP BY gp.id
		ORDER BY gp.proposed_epoch DESC, gp.added_slot DESC, gp.tx_hash DESC, gp.action_index ASC
		LIMIT $`+strconv.Itoa(len(args)),
		args...,
	)
	if err != nil {
		serverError(w, r, "query proposals", err)
		return
	}
	defer rows.Close()

	items := []proposal{}
	for rows.Next() {
		var p proposal
		err := rows.Scan(
			&p.ID,
			&p.TxHash,
			&p.ActionIndex,
			&p.ActionType,
			&p.ProposedEpoch,
			&p.ExpiresEpoch,
			&p.AddedSlot,
			&p.AnchorURL,
			&p.AnchorHash,
			&p.Deposit,
			&p.Lifecycle,
			&p.Votes.Committee.No,
			&p.Votes.Committee.Yes,
			&p.Votes.Committee.Abstain,
			&p.Votes.DRep.No,
			&p.Votes.DRep.Yes,
			&p.Votes.DRep.Abstain,
			&p.Votes.SPO.No,
			&p.Votes.SPO.Yes,
			&p.Votes.SPO.Abstain,
		)
		if err != nil {
			serverError(w, r, "scan proposal", err)
			return
		}
		enrichProposal(&p, a.govtoolURL)
		items = append(items, p)
	}
	if err := rows.Err(); err != nil {
		serverError(w, r, "read proposals", err)
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (a *app) handleProposalDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	txHash := strings.ToLower(r.PathValue("txHash"))
	actionIndex, err := strconv.ParseUint(r.PathValue("index"), 10, 64)
	if err != nil || !isHex(txHash, 64) {
		http.Error(w, "invalid proposal id", http.StatusBadRequest)
		return
	}

	var ret proposalDetail
	var parentTx sql.NullString
	var parentIdx sql.NullInt64
	err = a.db.QueryRowContext(ctx, `
		SELECT
			gp.id,
			encode(gp.tx_hash, 'hex') AS tx_hash,
			gp.action_index,
			gp.action_type,
			gp.proposed_epoch,
			gp.expires_epoch,
			gp.added_slot,
			COALESCE(gp.anchor_url, ''),
			COALESCE(encode(gp.anchor_hash, 'hex'), ''),
			COALESCE(gp.deposit::text, ''),
			CASE
				WHEN gp.enacted_epoch IS NOT NULL THEN 'enacted'
				WHEN gp.expired_epoch IS NOT NULL THEN 'expired'
				WHEN gp.ratified_epoch IS NOT NULL THEN 'ratified'
				ELSE 'active'
			END AS lifecycle,
			encode(gp.parent_tx_hash, 'hex'),
			gp.parent_action_idx
		FROM governance_proposal gp
		WHERE gp.tx_hash = decode($1, 'hex')
			AND gp.action_index = $2
			AND gp.deleted_slot IS NULL
	`, txHash, actionIndex).Scan(
		&ret.Proposal.ID,
		&ret.Proposal.TxHash,
		&ret.Proposal.ActionIndex,
		&ret.Proposal.ActionType,
		&ret.Proposal.ProposedEpoch,
		&ret.Proposal.ExpiresEpoch,
		&ret.Proposal.AddedSlot,
		&ret.Proposal.AnchorURL,
		&ret.Proposal.AnchorHash,
		&ret.Proposal.Deposit,
		&ret.Proposal.Lifecycle,
		&parentTx,
		&parentIdx,
	)
	if errors.Is(err, sql.ErrNoRows) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		serverError(w, r, "query proposal", err)
		return
	}
	enrichProposal(&ret.Proposal, a.govtoolURL)
	if parentTx.Valid && parentIdx.Valid {
		ret.Parent = &actionRef{
			TxHash:      parentTx.String,
			ActionIndex: uint64(parentIdx.Int64),
		}
	}

	votes, summary, err := a.proposalVotes(ctx, ret.Proposal.ID)
	if err != nil {
		serverError(w, r, "query votes", err)
		return
	}
	ret.Votes = votes
	ret.Summary = summary
	ret.Proposal.Votes = summary
	writeJSON(w, http.StatusOK, ret)
}

func (a *app) handleDreps(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	active := r.URL.Query().Get("active")
	limit := boundedLimit(r.URL.Query().Get("limit"), 100, 250)
	args := []any{}
	where := []string{"1 = 1"}
	switch active {
	case "true":
		where = append(where, "d.active = true")
	case "false":
		where = append(where, "d.active = false")
	case "":
	default:
		http.Error(w, "invalid active filter", http.StatusBadRequest)
		return
	}
	args = append(args, limit)
	rows, err := a.db.QueryContext(ctx, `
		SELECT
			encode(d.credential, 'hex'),
			d.credential_tag,
			COALESCE(d.anchor_url, ''),
			COALESCE(encode(d.anchor_hash, 'hex'), ''),
			d.added_slot,
			d.last_activity_epoch,
			d.expiry_epoch,
			d.active,
			COUNT(DISTINCT a.id) FILTER (WHERE a.active = true) AS delegator_count,
			COUNT(DISTINCT gv.id) FILTER (WHERE gv.deleted_slot IS NULL) AS vote_count
		FROM drep d
		LEFT JOIN account a ON a.drep = d.credential
			AND a.drep_type = d.credential_tag
		LEFT JOIN governance_vote gv ON gv.voter_type = 1
			AND gv.voter_credential = d.credential
			AND gv.voter_credential_tag = d.credential_tag
		WHERE `+strings.Join(where, " AND ")+`
		GROUP BY d.id
		ORDER BY d.active DESC, d.last_activity_epoch DESC, d.added_slot DESC
		LIMIT $`+strconv.Itoa(len(args)),
		args...,
	)
	if err != nil {
		serverError(w, r, "query dreps", err)
		return
	}
	defer rows.Close()
	items := []drep{}
	for rows.Next() {
		var item drep
		if err := rows.Scan(
			&item.Credential,
			&item.CredentialTag,
			&item.AnchorURL,
			&item.AnchorHash,
			&item.AddedSlot,
			&item.LastActivityEpoch,
			&item.ExpiryEpoch,
			&item.Active,
			&item.DelegatorCount,
			&item.VoteCount,
		); err != nil {
			serverError(w, r, "scan drep", err)
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		serverError(w, r, "read dreps", err)
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (a *app) handleDrepDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	credential := strings.ToLower(r.PathValue("credential"))
	if !isHex(credential, 56) && !isHex(credential, 58) && !isHex(credential, 64) {
		http.Error(w, "invalid drep credential", http.StatusBadRequest)
		return
	}
	credentialTag, ok := parseCredentialTagParam(r)
	if !ok {
		http.Error(w, "invalid or missing credential_tag", http.StatusBadRequest)
		return
	}
	var ret drepDetail
	err := a.db.QueryRowContext(ctx, `
		SELECT
			encode(d.credential, 'hex'),
			d.credential_tag,
			COALESCE(d.anchor_url, ''),
			COALESCE(encode(d.anchor_hash, 'hex'), ''),
			d.added_slot,
			d.last_activity_epoch,
			d.expiry_epoch,
			d.active,
			COUNT(DISTINCT a.id) FILTER (WHERE a.active = true) AS delegator_count,
			COUNT(DISTINCT gv.id) FILTER (WHERE gv.deleted_slot IS NULL) AS vote_count
		FROM drep d
		LEFT JOIN account a ON a.drep = d.credential
			AND a.drep_type = d.credential_tag
		LEFT JOIN governance_vote gv ON gv.voter_type = 1
			AND gv.voter_credential = d.credential
			AND gv.voter_credential_tag = d.credential_tag
		WHERE d.credential = decode($1, 'hex')
			AND d.credential_tag = $2
		GROUP BY d.id
	`, credential, credentialTag).Scan(
		&ret.DRep.Credential,
		&ret.DRep.CredentialTag,
		&ret.DRep.AnchorURL,
		&ret.DRep.AnchorHash,
		&ret.DRep.AddedSlot,
		&ret.DRep.LastActivityEpoch,
		&ret.DRep.ExpiryEpoch,
		&ret.DRep.Active,
		&ret.DRep.DelegatorCount,
		&ret.DRep.VoteCount,
	)
	if errors.Is(err, sql.ErrNoRows) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		serverError(w, r, "query drep", err)
		return
	}
	ret.RecentVotes, err = a.drepVotes(ctx, credential, credentialTag)
	if err != nil {
		serverError(w, r, "query drep votes", err)
		return
	}
	ret.Delegations, err = a.drepDelegations(ctx, credential, credentialTag)
	if err != nil {
		serverError(w, r, "query drep delegations", err)
		return
	}
	ret.History, err = a.drepHistory(ctx, credential, credentialTag)
	if err != nil {
		serverError(w, r, "query drep history", err)
		return
	}
	writeJSON(w, http.StatusOK, ret)
}

func (a *app) handleStakeLookup(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	credential := strings.ToLower(r.PathValue("credential"))
	if !isHex(credential, 56) && !isHex(credential, 58) && !isHex(credential, 64) {
		http.Error(w, "invalid stake credential", http.StatusBadRequest)
		return
	}
	credentialTag, ok := parseCredentialTagParam(r)
	if !ok {
		http.Error(w, "invalid or missing credential_tag", http.StatusBadRequest)
		return
	}
	var ret stakeLookup
	err := a.db.QueryRowContext(ctx, `
		SELECT
			encode(staking_key, 'hex'),
			credential_tag,
			created_slot,
			COALESCE(encode(pool, 'hex'), ''),
			COALESCE(encode(drep, 'hex'), ''),
			drep_type,
			reward::text,
			active
		FROM account
		WHERE staking_key = decode($1, 'hex')
			AND credential_tag = $2
	`, credential, credentialTag).Scan(
		&ret.StakingKey,
		&ret.CredentialTag,
		&ret.CreatedSlot,
		&ret.Pool,
		&ret.DRep,
		&ret.DRepType,
		&ret.Reward,
		&ret.Active,
	)
	if errors.Is(err, sql.ErrNoRows) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		serverError(w, r, "query account", err)
		return
	}
	writeJSON(w, http.StatusOK, ret)
}

func (a *app) proposalVotes(ctx context.Context, proposalID int64) ([]voteRow, voteStats, error) {
	rows, err := a.db.QueryContext(ctx, `
		SELECT
			voter_type,
			encode(voter_credential, 'hex'),
			vote,
			added_slot,
			COALESCE(vote_updated_slot, 0),
			COALESCE(anchor_url, ''),
			COALESCE(encode(anchor_hash, 'hex'), '')
		FROM governance_vote
		WHERE proposal_id = $1
			AND deleted_slot IS NULL
		ORDER BY voter_type ASC, added_slot DESC, voter_credential ASC
	`, proposalID)
	if err != nil {
		return nil, voteStats{}, err
	}
	defer rows.Close()
	ret := []voteRow{}
	var stats voteStats
	for rows.Next() {
		var v voteRow
		if err := rows.Scan(
			&v.VoterType,
			&v.VoterCredential,
			&v.Vote,
			&v.AddedSlot,
			&v.UpdatedSlot,
			&v.AnchorURL,
			&v.AnchorHash,
		); err != nil {
			return nil, voteStats{}, err
		}
		v.VoterTypeName = voterTypeName(v.VoterType)
		v.VoteName = voteName(v.Vote)
		addVote(&stats, v.VoterType, v.Vote)
		ret = append(ret, v)
	}
	return ret, stats, rows.Err()
}

func (a *app) drepVotes(
	ctx context.Context,
	credential string,
	credentialTag uint8,
) ([]drepVote, error) {
	rows, err := a.db.QueryContext(ctx, `
		SELECT
			encode(gp.tx_hash, 'hex'),
			gp.action_index,
			gp.action_type,
			gv.vote,
			gv.added_slot,
			COALESCE(gv.vote_updated_slot, 0)
		FROM governance_vote gv
		JOIN governance_proposal gp ON gp.id = gv.proposal_id
		WHERE gv.voter_type = 1
			AND gv.voter_credential = decode($1, 'hex')
			AND gv.voter_credential_tag = $2
			AND gv.deleted_slot IS NULL
			AND gp.deleted_slot IS NULL
		ORDER BY gv.added_slot DESC
		LIMIT 50
	`, credential, credentialTag)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := []drepVote{}
	for rows.Next() {
		var item drepVote
		var actionType int64
		if err := rows.Scan(
			&item.ProposalTxHash,
			&item.ActionIndex,
			&actionType,
			&item.Vote,
			&item.AddedSlot,
			&item.UpdatedSlot,
		); err != nil {
			return nil, err
		}
		item.ActionTypeName = actionTypeName(actionType)
		item.VoteName = voteName(item.Vote)
		item.ProposalGovTool = govtoolActionURL(a.govtoolURL, item.ProposalTxHash, item.ActionIndex)
		ret = append(ret, item)
	}
	return ret, rows.Err()
}

func (a *app) drepDelegations(
	ctx context.Context,
	credential string,
	credentialTag uint8,
) ([]accountSummary, error) {
	rows, err := a.db.QueryContext(ctx, `
		SELECT encode(staking_key, 'hex'), credential_tag, created_slot, reward::text, active
		FROM account
		WHERE drep = decode($1, 'hex')
			AND drep_type = $2
			AND active = true
		ORDER BY reward DESC, staking_key ASC
		LIMIT 50
	`, credential, credentialTag)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := []accountSummary{}
	for rows.Next() {
		var item accountSummary
		if err := rows.Scan(
			&item.StakingKey,
			&item.CredentialTag,
			&item.CreatedSlot,
			&item.Reward,
			&item.Active,
		); err != nil {
			return nil, err
		}
		ret = append(ret, item)
	}
	return ret, rows.Err()
}

func (a *app) drepHistory(
	ctx context.Context,
	credential string,
	credentialTag uint8,
) ([]drepHistoryItem, error) {
	rows, err := a.db.QueryContext(ctx, `
		SELECT kind, added_slot, tx_hash, anchor_url, anchor_hash, deposit
		FROM (
			SELECT
				'registration' AS kind,
				rd.added_slot,
				COALESCE(encode(tx.hash, 'hex'), '') AS tx_hash,
				COALESCE(rd.anchor_url, '') AS anchor_url,
				COALESCE(encode(rd.anchor_hash, 'hex'), '') AS anchor_hash,
				COALESCE(rd.deposit_amount::text, '') AS deposit
			FROM registration_drep rd
			LEFT JOIN certs c ON c.id = rd.certificate_id
			LEFT JOIN "transaction" tx ON tx.id = c.transaction_id
			WHERE rd.drep_credential = decode($1, 'hex')
				AND rd.credential_tag = $2

			UNION ALL
			SELECT
				'update' AS kind,
				ud.added_slot,
				COALESCE(encode(tx.hash, 'hex'), '') AS tx_hash,
				COALESCE(ud.anchor_url, '') AS anchor_url,
				COALESCE(encode(ud.anchor_hash, 'hex'), '') AS anchor_hash,
				'' AS deposit
			FROM update_drep ud
			LEFT JOIN certs c ON c.id = ud.certificate_id
			LEFT JOIN "transaction" tx ON tx.id = c.transaction_id
			WHERE ud.credential = decode($1, 'hex')
				AND ud.credential_tag = $2

			UNION ALL
			SELECT
				'deregistration' AS kind,
				dd.added_slot,
				COALESCE(encode(tx.hash, 'hex'), '') AS tx_hash,
				'' AS anchor_url,
				'' AS anchor_hash,
				COALESCE(dd.deposit_amount::text, '') AS deposit
			FROM deregistration_drep dd
			LEFT JOIN certs c ON c.id = dd.certificate_id
			LEFT JOIN "transaction" tx ON tx.id = c.transaction_id
			WHERE dd.drep_credential = decode($1, 'hex')
				AND dd.credential_tag = $2
		) h
		ORDER BY added_slot DESC
		LIMIT 50
	`, credential, credentialTag)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := []drepHistoryItem{}
	for rows.Next() {
		var item drepHistoryItem
		if err := rows.Scan(
			&item.Kind,
			&item.Slot,
			&item.TxHash,
			&item.AnchorURL,
			&item.AnchorHash,
			&item.Deposit,
		); err != nil {
			return nil, err
		}
		ret = append(ret, item)
	}
	return ret, rows.Err()
}

func enrichProposal(p *proposal, govtoolURL string) {
	p.ActionTypeName = actionTypeName(p.ActionType)
	p.GovToolURL = govtoolActionURL(govtoolURL, p.TxHash, p.ActionIndex)
	p.Votes.Total.No = p.Votes.Committee.No + p.Votes.DRep.No + p.Votes.SPO.No
	p.Votes.Total.Yes = p.Votes.Committee.Yes + p.Votes.DRep.Yes + p.Votes.SPO.Yes
	p.Votes.Total.Abstain = p.Votes.Committee.Abstain + p.Votes.DRep.Abstain + p.Votes.SPO.Abstain
}

func govtoolActionURL(base, txHash string, actionIndex uint64) string {
	if base == "" {
		return ""
	}
	return fmt.Sprintf("%s/governance_actions/%s#%d", base, txHash, actionIndex)
}

func addVote(stats *voteStats, voterType, vote int64) {
	add := func(c *choiceStats) {
		switch vote {
		case 0:
			c.No++
		case 1:
			c.Yes++
		case 2:
			c.Abstain++
		}
	}
	switch voterType {
	case 0:
		add(&stats.Committee)
	case 1:
		add(&stats.DRep)
	case 2:
		add(&stats.SPO)
	}
	add(&stats.Total)
}

func actionTypeName(v int64) string {
	switch v {
	case 0:
		return "Parameter Change"
	case 1:
		return "Hard Fork Initiation"
	case 2:
		return "Treasury Withdrawal"
	case 3:
		return "No Confidence"
	case 4:
		return "Update Committee"
	case 5:
		return "New Constitution"
	case 6:
		return "Info"
	default:
		return fmt.Sprintf("Type %d", v)
	}
}

func voterTypeName(v int64) string {
	switch v {
	case 0:
		return "Committee"
	case 1:
		return "DRep"
	case 2:
		return "SPO"
	default:
		return fmt.Sprintf("Type %d", v)
	}
}

func voteName(v int64) string {
	switch v {
	case 0:
		return "No"
	case 1:
		return "Yes"
	case 2:
		return "Abstain"
	default:
		return fmt.Sprintf("Vote %d", v)
	}
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func serverError(w http.ResponseWriter, r *http.Request, operation string, err error) {
	log.Printf("%s %s: %s: %v", r.Method, r.URL.Path, operation, err)
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

func boundedLimit(raw string, def, max int) int {
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

func parseCredentialTagParam(r *http.Request) (uint8, bool) {
	raw := strings.TrimSpace(r.URL.Query().Get("credential_tag"))
	if raw == "" {
		return 0, false
	}
	n, err := strconv.ParseUint(raw, 10, 8)
	if err != nil || n > 1 {
		return 0, false
	}
	return uint8(n), true
}

func isHex(s string, length int) bool {
	if length > 0 && len(s) != length {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func envOrDefault(key, def string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return def
}

func envInt(key string, def int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return def
	}
	return value
}

func withSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		next.ServeHTTP(w, r)
	})
}
