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
	"math"
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
	Network             string             `json:"network"`
	StorageMode         string             `json:"storageMode"`
	Tip                 *tip               `json:"tip,omitempty"`
	LatestEpoch         *epoch             `json:"latestEpoch,omitempty"`
	ProposalCount       int64              `json:"proposalCount"`
	GovernanceVoteCount int64              `json:"governanceVoteCount"`
	ActiveDrepCount     int64              `json:"activeDrepCount"`
	ExpiredDrepCount    int64              `json:"expiredDrepCount"`
	MinLiveProposalSlot uint64             `json:"minLiveProposalSlot,omitempty"`
	LatestRewardEpoch   *uint64            `json:"latestRewardEpoch,omitempty"`
	AccountInactivity   *accountInactivity `json:"accountInactivity,omitempty"`
	Backfill            *backfillStatus    `json:"backfill,omitempty"`
	VoteBackfillPending bool               `json:"voteBackfillPending"`
	LastMetadataWrite   *time.Time         `json:"lastMetadataWrite,omitempty"`
}

// accountInactivity reports whether the node has run the one-time CIP-0163
// reward-account inactivity activation. Without it, every
// account.expiration_epoch is 0 (unset) and reward-account expiry is not being
// enforced, so the stake lookup must not present the column as meaningful.
type accountInactivity struct {
	Activated       bool   `json:"activated"`
	ActivationEpoch uint64 `json:"activationEpoch"`
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
	// ExpiryStatus is the inactivity state the Conway tally applies:
	// "expired" when expiry_epoch has been reached, "active" when it has
	// not, "unknown" when no activity has ever been recorded (expiry_epoch
	// 0) or the latest epoch is not known yet. Independent of Active, which
	// only tracks registration.
	ExpiryStatus string `json:"expiryStatus"`
	// EpochsUntilExpiry is expiry_epoch minus the latest epoch, so a
	// non-positive value means the DRep is already expired. Nil when
	// ExpiryStatus is "unknown".
	EpochsUntilExpiry *int64 `json:"epochsUntilExpiry,omitempty"`
	Active            bool   `json:"active"`
	DelegatorCount    int64  `json:"delegatorCount"`
	VoteCount         int64  `json:"voteCount"`
	// FirstSeenSlot and LastRegistrationSlot come from certificate history
	// and are only populated by the DRep detail endpoint.
	FirstSeenSlot        uint64 `json:"firstSeenSlot,omitempty"`
	LastRegistrationSlot uint64 `json:"lastRegistrationSlot,omitempty"`
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
	// ExpirationEpoch is the CIP-0163 reward-account inactivity expiry. 0
	// means unset, which is every account on a node running without
	// delegator inactivity enabled.
	ExpirationEpoch uint64 `json:"expirationEpoch"`
	// InactivityActivated reports membership in the one-time CIP-0163
	// activation stamp (account_inactivity_activation).
	InactivityActivated bool                `json:"inactivityActivated"`
	Rewards             []accountReward     `json:"rewards"`
	Withdrawals         []withdrawalWitness `json:"withdrawals"`
}

// accountReward is one persisted per-epoch reward-calculation output row for a
// stake credential. Only the retained snapshot window (the current epoch and
// the three before it) is queryable; older per-credential rows are pruned.
type accountReward struct {
	Epoch        uint64 `json:"epoch"`
	PoolKeyHash  string `json:"poolKeyHash,omitempty"`
	RewardType   string `json:"rewardType"`
	Amount       string `json:"amount"`
	Spendable    bool   `json:"spendable"`
	BoundarySlot uint64 `json:"boundarySlot"`
}

// withdrawalWitness is one CIP-0163 reward-withdrawal witness, or (when the
// delegator-inactivity gate is off) a plain non-zero withdrawal reconstructed
// from account_reward_delta. See accountWithdrawals for which source a given
// row came from and what that implies about ZeroAmount coverage.
type withdrawalWitness struct {
	TxHash     string `json:"txHash"`
	AddedSlot  uint64 `json:"addedSlot"`
	Amount     string `json:"amount,omitempty"`
	ZeroAmount bool   `json:"zeroAmount"`
}

// epochRow is one epoch of retained epoch and reward state. Every table joined
// here is retained for the life of the database, so the history reaches back to
// the first boundary the node captured.
type epochRow struct {
	EpochID          uint64 `json:"epochId"`
	StartSlot        uint64 `json:"startSlot"`
	EraID            uint64 `json:"eraId"`
	Treasury         string `json:"treasury,omitempty"`
	Reserves         string `json:"reserves,omitempty"`
	Fees             string `json:"fees,omitempty"`
	Rewards          string `json:"rewards,omitempty"`
	PotsCapturedSlot uint64 `json:"potsCapturedSlot,omitempty"`
	ActiveStake      string `json:"activeStake,omitempty"`
	PoolCount        uint64 `json:"poolCount"`
	DelegatorCount   uint64 `json:"delegatorCount"`
	SnapshotReady    bool   `json:"snapshotReady"`
	// RewardBasisStake is the reward-side stake total, which differs from
	// ActiveStake because the reward basis excludes pools with degraded
	// registration data.
	RewardBasisStake string `json:"rewardBasisStake,omitempty"`
	Authoritative    bool   `json:"authoritative"`
	PoolOutputCount  int64  `json:"poolOutputCount"`
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
		db: db,
		govtoolURL: strings.TrimRight(
			envOrDefault("GOVTOOL_BASE_URL", "https://preview.gov.tools"),
			"/",
		),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/status", a.handleStatus)
	mux.HandleFunc("GET /api/proposals", a.handleProposals)
	mux.HandleFunc(
		"GET /api/proposals/{txHash}/{index}",
		a.handleProposalDetail,
	)
	mux.HandleFunc("GET /api/dreps", a.handleDreps)
	mux.HandleFunc("GET /api/dreps/{credential}", a.handleDrepDetail)
	mux.HandleFunc("GET /api/stake/{credential}", a.handleStakeLookup)
	mux.HandleFunc("GET /api/epochs", a.handleEpochs)

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
	if err := server.ListenAndServe(); err != nil &&
		!errors.Is(err, http.ErrServerClosed) {
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
	// Registered DReps whose inactivity expiry has passed. They still read
	// active (that flag only clears on deregistration) but the Conway tally
	// drops them from the voting-power denominator.
	if err := a.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM drep d
		WHERE d.active = true
			AND `+drepExpiredPredicate).Scan(&ret.ExpiredDrepCount); err != nil {
		serverError(w, r, "count expired dreps", err)
		return
	}
	var latestRewardEpoch sql.NullInt64
	if err := a.db.QueryRowContext(ctx, `
		SELECT MAX(epoch)
		FROM reward_ada_pots
	`).Scan(&latestRewardEpoch); err != nil {
		serverError(w, r, "query latest reward epoch", err)
		return
	}
	if latestRewardEpoch.Valid && latestRewardEpoch.Int64 >= 0 {
		v := uint64(latestRewardEpoch.Int64)
		ret.LatestRewardEpoch = &v
	}
	inactivity, err := a.accountInactivityStatus(ctx)
	if err != nil {
		serverError(w, r, "query account inactivity", err)
		return
	}
	ret.AccountInactivity = inactivity
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

// accountInactivityStatus reads the CIP-0163 activation state. The marker row
// carries the activation epoch, but it lives in sync_state, which a completed
// sync/load run clears wholesale, so the recorded activation membership is used
// as the fallback signal that activation has already happened.
func (a *app) accountInactivityStatus(
	ctx context.Context,
) (*accountInactivity, error) {
	ret := &accountInactivity{}
	var marker string
	switch err := a.db.QueryRowContext(ctx, `
		SELECT value
		FROM sync_state
		WHERE sync_key = 'delegator_inactivity_activated'
	`).Scan(&marker); {
	case err == nil:
		marker = strings.TrimSpace(marker)
		if marker != "" {
			ret.Activated = true
			if epoch, parseErr := strconv.ParseUint(marker, 10, 64); parseErr == nil {
				ret.ActivationEpoch = epoch
			}
		}
	case errors.Is(err, sql.ErrNoRows):
	default:
		return nil, err
	}
	if ret.Activated {
		return ret, nil
	}
	if err := a.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM account_inactivity_activation
		)
	`).Scan(&ret.Activated); err != nil {
		return nil, err
	}
	return ret, nil
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
	expiryClause, ok := drepExpiryPredicate(r.URL.Query().Get("expiry"))
	if !ok {
		http.Error(w, "invalid expiry filter", http.StatusBadRequest)
		return
	}
	if expiryClause != "" {
		where = append(where, expiryClause)
	}
	latestEpoch, err := a.latestEpochID(ctx)
	if err != nil {
		serverError(w, r, "query latest epoch", err)
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
			COALESCE(d.last_activity_epoch, 0),
			COALESCE(d.expiry_epoch, 0),
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
		item.ExpiryStatus, item.EpochsUntilExpiry = drepExpiryState(
			item.ExpiryEpoch,
			latestEpoch,
		)
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
	if !isHex(credential, 56) && !isHex(credential, 58) &&
		!isHex(credential, 64) {
		http.Error(w, "invalid drep credential", http.StatusBadRequest)
		return
	}
	credentialTag, ok := parseCredentialTagParam(r)
	if !ok {
		http.Error(
			w,
			"invalid or missing credential_tag",
			http.StatusBadRequest,
		)
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
			COALESCE(d.last_activity_epoch, 0),
			COALESCE(d.expiry_epoch, 0),
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
	firstSeen, lastRegistration, err := a.drepCertSlots(
		ctx,
		credential,
		credentialTag,
	)
	if err != nil {
		serverError(w, r, "query drep certificate slots", err)
		return
	}
	ret.DRep.FirstSeenSlot = firstSeenSlot(firstSeen, ret.DRep.AddedSlot)
	ret.DRep.LastRegistrationSlot = lastRegistration
	latestEpoch, err := a.latestEpochID(ctx)
	if err != nil {
		serverError(w, r, "query latest epoch", err)
		return
	}
	ret.DRep.ExpiryStatus, ret.DRep.EpochsUntilExpiry = drepExpiryState(
		ret.DRep.ExpiryEpoch,
		latestEpoch,
	)
	writeJSON(w, http.StatusOK, ret)
}

func (a *app) handleStakeLookup(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	credential := strings.ToLower(r.PathValue("credential"))
	if !isHex(credential, 56) && !isHex(credential, 58) &&
		!isHex(credential, 64) {
		http.Error(w, "invalid stake credential", http.StatusBadRequest)
		return
	}
	credentialTag, ok := parseCredentialTagParam(r)
	if !ok {
		http.Error(
			w,
			"invalid or missing credential_tag",
			http.StatusBadRequest,
		)
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
			active,
			COALESCE(expiration_epoch, 0)
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
		&ret.ExpirationEpoch,
	)
	if errors.Is(err, sql.ErrNoRows) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		serverError(w, r, "query account", err)
		return
	}
	if err := a.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM account_inactivity_activation
			WHERE staking_key = decode($1, 'hex')
				AND credential_tag = $2
		)
	`, credential, credentialTag).Scan(&ret.InactivityActivated); err != nil {
		serverError(w, r, "query account activation", err)
		return
	}
	ret.Rewards, err = a.accountRewards(ctx, credential, credentialTag)
	if err != nil {
		serverError(w, r, "query account rewards", err)
		return
	}
	ret.Withdrawals, err = a.accountWithdrawals(ctx, credential, credentialTag)
	if err != nil {
		serverError(w, r, "query account withdrawals", err)
		return
	}
	writeJSON(w, http.StatusOK, ret)
}

func (a *app) handleEpochs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit := boundedLimit(r.URL.Query().Get("limit"), 50, 250)
	// Every joined table is retained for the life of the database, so this
	// reaches every boundary the node captured. The per-credential reward
	// tables are the only ones pruned to the four-epoch snapshot window.
	rows, err := a.db.QueryContext(ctx, `
		SELECT
			COALESCE(e.epoch_id, 0),
			COALESCE(e.start_slot, 0),
			COALESCE(e.era_id, 0),
			COALESCE(p.treasury::text, ''),
			COALESCE(p.reserves::text, ''),
			COALESCE(p.fees::text, ''),
			COALESCE(p.rewards::text, ''),
			COALESCE(p.captured_slot, 0),
			COALESCE(es.total_active_stake::text, ''),
			COALESCE(es.total_pool_count, 0),
			COALESCE(es.total_delegators, 0),
			COALESCE(es.snapshot_ready, false),
			COALESCE(rs.total_active_stake::text, ''),
			COALESCE(rs.authoritative, false),
			(
				SELECT COUNT(*)
				FROM reward_pool_output rpo
				WHERE rpo.epoch = e.epoch_id
			) AS pool_output_count
		FROM epoch e
		LEFT JOIN reward_ada_pots p ON p.epoch = e.epoch_id
		LEFT JOIN epoch_summary es ON es.epoch = e.epoch_id
		LEFT JOIN reward_snapshot rs ON rs.epoch = e.epoch_id
			AND rs.snapshot_type = 'mark'
		ORDER BY e.epoch_id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		serverError(w, r, "query epochs", err)
		return
	}
	defer rows.Close()
	items := []epochRow{}
	for rows.Next() {
		var item epochRow
		if err := rows.Scan(
			&item.EpochID,
			&item.StartSlot,
			&item.EraID,
			&item.Treasury,
			&item.Reserves,
			&item.Fees,
			&item.Rewards,
			&item.PotsCapturedSlot,
			&item.ActiveStake,
			&item.PoolCount,
			&item.DelegatorCount,
			&item.SnapshotReady,
			&item.RewardBasisStake,
			&item.Authoritative,
			&item.PoolOutputCount,
		); err != nil {
			serverError(w, r, "scan epoch", err)
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		serverError(w, r, "read epochs", err)
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (a *app) proposalVotes(
	ctx context.Context,
	proposalID int64,
) ([]voteRow, voteStats, error) {
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
		item.ProposalGovTool = govtoolActionURL(
			a.govtoolURL,
			item.ProposalTxHash,
			item.ActionIndex,
		)
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

// drepCertSlots returns the credential's first on-chain appearance slot and the
// added_slot of its most recent real registration certificate. The first
// appearance survives deregister/re-register cycles because it also considers
// update and vote-delegation references. Both are 0 when no certificate history
// exists, which is the normal state for a core-mode or pre-backfill database.
func (a *app) drepCertSlots(
	ctx context.Context,
	credential string,
	credentialTag uint8,
) (uint64, uint64, error) {
	var firstSeen, lastRegistration sql.NullInt64
	// certificate_id filters out the synthetic registration rows a Mithril
	// ledger-state import writes at the bootstrap slot; only real on-chain
	// certificates count as a registration.
	err := a.db.QueryRowContext(ctx, `
		SELECT MIN(first_slot), MAX(registration_slot)
		FROM (
			SELECT
				MIN(added_slot) AS first_slot,
				MAX(
					CASE
						WHEN certificate_id IS NOT NULL AND certificate_id != 0
						THEN added_slot
					END
				) AS registration_slot
			FROM registration_drep
			WHERE drep_credential = decode($1, 'hex')
				AND credential_tag = $2::bigint

			UNION ALL
			SELECT MIN(added_slot), NULL
			FROM update_drep
			WHERE credential = decode($1, 'hex')
				AND credential_tag = $2::bigint

			UNION ALL
			SELECT MIN(added_slot), NULL
			FROM vote_delegation
			WHERE drep = decode($1, 'hex')
				AND drep_type = $2::bigint

			UNION ALL
			SELECT MIN(added_slot), NULL
			FROM stake_vote_delegation
			WHERE drep = decode($1, 'hex')
				AND drep_type = $2::bigint

			UNION ALL
			SELECT MIN(added_slot), NULL
			FROM vote_registration_delegation
			WHERE drep = decode($1, 'hex')
				AND drep_type = $2::bigint

			UNION ALL
			SELECT MIN(added_slot), NULL
			FROM stake_vote_registration_delegation
			WHERE drep = decode($1, 'hex')
				AND drep_type = $2::bigint
		) slots
	`, credential, credentialTag).Scan(&firstSeen, &lastRegistration)
	if err != nil {
		return 0, 0, err
	}
	return nullSlot(firstSeen), nullSlot(lastRegistration), nil
}

// latestEpochID returns the highest known epoch, invalid when the node has not
// recorded an epoch yet.
func (a *app) latestEpochID(ctx context.Context) (sql.NullInt64, error) {
	var ret sql.NullInt64
	if err := a.db.QueryRowContext(ctx, `
		SELECT MAX(epoch_id)
		FROM epoch
	`).Scan(&ret); err != nil {
		return sql.NullInt64{}, err
	}
	return ret, nil
}

func (a *app) accountRewards(
	ctx context.Context,
	credential string,
	credentialTag uint8,
) ([]accountReward, error) {
	rows, err := a.db.QueryContext(ctx, `
		SELECT
			epoch,
			COALESCE(encode(pool_key_hash, 'hex'), ''),
			reward_type,
			amount::text,
			spendable,
			boundary_slot
		FROM reward_account_output
		WHERE staking_key = decode($1, 'hex')
			AND credential_tag = $2
		ORDER BY epoch DESC, reward_type ASC, pool_key_hash ASC
		LIMIT 25
	`, credential, credentialTag)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := []accountReward{}
	for rows.Next() {
		var item accountReward
		if err := rows.Scan(
			&item.Epoch,
			&item.PoolKeyHash,
			&item.RewardType,
			&item.Amount,
			&item.Spendable,
			&item.BoundarySlot,
		); err != nil {
			return nil, err
		}
		ret = append(ret, item)
	}
	return ret, rows.Err()
}

// accountWithdrawals returns the account's reward withdrawals, preferring the
// CIP-0163 witness history and falling back to the reward journal for
// withdrawals the witness table never recorded.
//
// account_withdrawal_witness only gets a row when the node has the
// delegator-inactivity gate enabled (see BatchedTxIngestOpts.
// SkipWithdrawalWitnessWrite in database/batch.go): with the gate off -- the
// default, and every node not running CIP-0163 -- that insert is elided
// entirely as write amplification on a table nothing else reads. So on a
// gate-off node the first branch below returns nothing, and every withdrawal
// this endpoint shows comes from the second branch instead: non-zero
// withdrawals reconstructed from account_reward_delta, which is written
// unconditionally regardless of the gate. Zero-amount withdrawals leave no
// trace in account_reward_delta (only a witness row would have recorded
// them), so they are only visible here when the gate is on.
func (a *app) accountWithdrawals(
	ctx context.Context,
	credential string,
	credentialTag uint8,
) ([]withdrawalWitness, error) {
	rows, err := a.db.QueryContext(ctx, `
		SELECT tx_hash, added_slot, amount FROM (
			SELECT
				encode(w.tx_hash, 'hex') AS tx_hash,
				w.added_slot,
				COALESCE(d.amount::text, '') AS amount
			FROM account_withdrawal_witness w
			LEFT JOIN account_reward_delta d ON d.withdrawal = true
				AND d.tx_hash = w.tx_hash
				AND d.credential_tag = w.credential_tag
				AND d.staking_key = w.staking_key
				AND d.added_slot = w.added_slot
			WHERE w.staking_key = decode($1, 'hex')
				AND w.credential_tag = $2

			UNION ALL

			-- Gate-off fallback: every non-zero withdrawal the witness table
			-- above missed, reconstructed from the reward journal. Excludes
			-- rows already covered by a witness so a gate-on database (which
			-- has both) does not double-count a withdrawal.
			SELECT
				encode(d.tx_hash, 'hex') AS tx_hash,
				d.added_slot,
				d.amount::text AS amount
			FROM account_reward_delta d
			WHERE d.withdrawal = true
				AND d.staking_key = decode($1, 'hex')
				AND d.credential_tag = $2
				AND NOT EXISTS (
					SELECT 1 FROM account_withdrawal_witness w
					WHERE w.tx_hash = d.tx_hash
						AND w.credential_tag = d.credential_tag
						AND w.staking_key = d.staking_key
						AND w.added_slot = d.added_slot
				)
		) combined
		ORDER BY added_slot DESC, tx_hash ASC
		LIMIT 25
	`, credential, credentialTag)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := []withdrawalWitness{}
	for rows.Next() {
		var item withdrawalWitness
		if err := rows.Scan(
			&item.TxHash,
			&item.AddedSlot,
			&item.Amount,
		); err != nil {
			return nil, err
		}
		item.ZeroAmount = withdrawalZeroAmount(item.Amount)
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

// drepExpiredPredicate and drepUnexpiredPredicate mirror the Conway tally's
// inactivity test: a DRep is expired once expiry_epoch is set and has been
// reached, and a zero expiry_epoch means no activity has been recorded yet and
// is treated as unexpired. Both are independent of drep.active, which only
// clears on deregistration.
// expiry_epoch is a nullable column, so every comparison folds NULL into the
// unset value first.
const (
	drepExpiredPredicate = `COALESCE(d.expiry_epoch, 0) > 0
			AND COALESCE(d.expiry_epoch, 0) <=
				COALESCE((SELECT MAX(epoch_id) FROM epoch), 0)`
	drepUnexpiredPredicate = `(COALESCE(d.expiry_epoch, 0) = 0
			OR COALESCE(d.expiry_epoch, 0) >
				COALESCE((SELECT MAX(epoch_id) FROM epoch), 0))`
)

// drepExpiryPredicate maps the expiry filter to its SQL predicate. An empty
// filter matches every DRep and returns an empty predicate.
func drepExpiryPredicate(filter string) (string, bool) {
	switch filter {
	case "":
		return "", true
	case "expired":
		return drepExpiredPredicate, true
	case "active":
		return drepUnexpiredPredicate, true
	default:
		return "", false
	}
}

// drepExpiryState reports the inactivity state for a DRep's expiry epoch and
// how many epochs remain before it expires. A non-positive remainder means the
// DRep is already expired and its voting power is excluded from the tally.
func drepExpiryState(
	expiryEpoch uint64,
	latestEpoch sql.NullInt64,
) (string, *int64) {
	if expiryEpoch == 0 || expiryEpoch > math.MaxInt64 ||
		!latestEpoch.Valid || latestEpoch.Int64 < 0 {
		return "unknown", nil
	}
	remaining := int64(expiryEpoch) - latestEpoch.Int64
	if remaining <= 0 {
		return "expired", &remaining
	}
	return "active", &remaining
}

// firstSeenSlot falls back to the DRep's current registration slot when no
// certificate history is present to derive a first appearance from.
func firstSeenSlot(certSlot, addedSlot uint64) uint64 {
	if certSlot == 0 {
		return addedSlot
	}
	return certSlot
}

// withdrawalZeroAmount reports whether a withdrawal witness moved no reward.
func withdrawalZeroAmount(amount string) bool {
	trimmed := strings.TrimSpace(amount)
	return trimmed == "" || strings.Trim(trimmed, "0") == ""
}

func nullSlot(value sql.NullInt64) uint64 {
	if !value.Valid || value.Int64 <= 0 {
		return 0
	}
	return uint64(value.Int64)
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

func serverError(
	w http.ResponseWriter,
	r *http.Request,
	operation string,
	err error,
) {
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
