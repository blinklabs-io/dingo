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
	"maps"
	"sort"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gconway "github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// boundarySnapshotAfterEnactment follows Dijkstra EPOCH, whose SNAP runs after
// POOLREAP, governance credits and HARDFORK. Earlier eras run SNAP first.
func boundarySnapshotAfterEnactment(
	ctx context.Context,
	db queryer,
	boundarySlot uint64,
) (bool, error) {
	if boundarySlot == 0 {
		return false, nil
	}
	var eraID uint
	err := db.QueryRowContext(ctx, `SELECT era_id FROM epoch WHERE start_slot <= ? ORDER BY start_slot DESC LIMIT 1`, boundarySlot).
		Scan(&eraID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("resolve snapshot boundary era: %w", err)
	}
	if eraID >= gdijkstra.EraIdDijkstra {
		return true, nil
	}
	// At the transition SNAP precedes era translation, so epoch still names
	// Conway. Enacted parameters already carry the incoming major version.
	var raw []byte
	var parameterEra uint
	err = db.QueryRowContext(ctx, `SELECT era_id, cbor FROM pparams WHERE added_slot <= ? ORDER BY added_slot DESC, id DESC LIMIT 1`, boundarySlot).Scan(&parameterEra, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("resolve snapshot boundary parameters: %w", err)
	}
	if parameterEra >= gdijkstra.EraIdDijkstra {
		return true, nil
	}
	if parameterEra != gconway.EraIdConway || len(raw) == 0 {
		return false, nil
	}
	var params gconway.ConwayProtocolParameters
	if _, err := gcbor.Decode(raw, &params); err != nil {
		return false, fmt.Errorf("decode snapshot boundary parameters: %w", err)
	}
	return params.ProtocolVersion.Major >= lcommon.ProtocolVersionDijkstra, nil
}

type historicalStakeSource struct {
	table      string
	certType   uint
	registered int
}

type historicalRewardKey struct {
	tag uint8
	key string
}

type historicalWithdrawal struct {
	slot     int64
	id       int64
	previous uint64
}

// historicalRewards evaluates future reward credits only for the selected
// credentials.  Filters are split into bounded batches so the generated
// predicate stays below SQLite/PostgreSQL/MySQL parameter limits.
func (s *Store) historicalRewards(
	ctx context.Context,
	db queryer,
	slot uint64,
	selected map[historicalRewardKey]struct{},
) (map[historicalRewardKey]uint64, error) {
	return s.historicalRewardsAtBoundary(ctx, db, slot, 0, selected)
}

// historicalRewardsAtBoundary reconstructs the reward balance observed at an
// epoch SNAP boundary. Boundary credits marked PostSnapshot are still future
// credits relative to SNAP and must be removed, while unmarked credits at the
// boundary are already visible to the snapshot. A pending round applied at or
// before that point contributes its credits that are not written yet.
func (s *Store) historicalRewardsAtBoundary(
	ctx context.Context,
	db queryer,
	slot uint64,
	boundarySlot uint64,
	selected map[historicalRewardKey]struct{},
) (map[historicalRewardKey]uint64, error) {
	hasPending, err := pendingRewardCreditOutputsExist(ctx, db)
	if err != nil {
		return nil, err
	}
	visibleAt := slot
	if boundarySlot > 0 {
		visibleAt = boundarySlot
	}
	keys := make([]historicalRewardKey, 0, len(selected))
	for key := range selected {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].tag != keys[j].tag {
			return keys[i].tag < keys[j].tag
		}
		return keys[i].key < keys[j].key
	})
	ret := make(map[historicalRewardKey]uint64, len(selected))
	for start := 0; start < len(keys); start += 400 {
		end := min(start+400, len(keys))
		batchSelected := make(map[historicalRewardKey]struct{}, end-start)
		for _, key := range keys[start:end] {
			batchSelected[key] = struct{}{}
		}
		batch, err := historicalRewardsBatch(
			ctx,
			db,
			slot,
			boundarySlot,
			batchSelected,
		)
		if err != nil {
			return nil, err
		}
		if hasPending {
			pending, err := s.pendingCreditsForCredentials(
				ctx, db, visibleAt, batchSelected,
			)
			if err != nil {
				return nil, err
			}
			for ref, amount := range pending {
				if ^uint64(0)-batch[ref] < amount {
					return nil, errors.New("historical reward credit overflow")
				}
				batch[ref] += amount
			}
		}
		maps.Copy(ret, batch)
	}
	return ret, nil
}

// historicalRewardsBatch evaluates future reward credits in Go. Amounts are
// persisted as decimal text and can exceed a signed SQL integer; keeping the
// ordering logic here avoids lossy CAST/SUM arithmetic in SQLite.
func historicalRewardsBatch(
	ctx context.Context,
	db queryer,
	slot uint64,
	boundarySlot uint64,
	selected map[historicalRewardKey]struct{},
) (map[historicalRewardKey]uint64, error) {
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return nil, err
	}
	// A credit counts after the snapshot when its effective slot is above
	// threshold; at a boundary snapshot taken before enactment, a post-snapshot
	// credit at threshold counts too. A withdrawal anchors the reconstruction
	// when its effective slot is above the snapshot slot, or at or above the
	// boundary.
	threshold := slotValue
	withdrawalThreshold := slotValue
	withdrawalQuery := firstRewardWithdrawalAfterSQL
	creditQuery := rewardEventCreditsAfterSQL
	if boundarySlot > 0 {
		boundaryValue, boundaryErr := checkedInt64(boundarySlot)
		if boundaryErr != nil {
			return nil, boundaryErr
		}
		afterEnactment, err := boundarySnapshotAfterEnactment(
			ctx,
			db,
			boundarySlot,
		)
		if err != nil {
			return nil, err
		}
		threshold = boundaryValue
		withdrawalThreshold = boundaryValue
		withdrawalQuery = firstRewardWithdrawalFromSQL
		if !afterEnactment {
			creditQuery = rewardEventCreditsAfterOrPostSnapshotSQL
		}
	}
	keys := make([]historicalRewardKey, 0, len(selected))
	for key := range selected {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].tag != keys[j].tag {
			return keys[i].tag < keys[j].tag
		}
		return keys[i].key < keys[j].key
	})
	creditSlots, err := rewardCreditSlotsAfter(ctx, db, threshold)
	if err != nil {
		return nil, err
	}
	base := make(map[historicalRewardKey]uint64)
	withdrawals := make(map[historicalRewardKey]historicalWithdrawal)
	total := make(map[historicalRewardKey]uint64)
	beforeWithdrawal := make(map[historicalRewardKey]uint64)
	for _, ref := range keys {
		var accountID int64
		var raw sql.NullString
		var active sql.NullBool
		err := db.QueryRowContext(
			ctx, getAccountRewardStateSQL, ref.tag, []byte(ref.key),
		).Scan(&accountID, &raw, &active)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if raw.Valid && raw.String != "" {
			value, err := parseUint64("historical account reward", raw.String)
			if err != nil {
				return nil, err
			}
			base[ref] = value
		}
		var (
			withdrawalSlot int64
			txID           int64
			previousRaw    sql.NullString
		)
		err = db.QueryRowContext(
			ctx, withdrawalQuery, accountID, withdrawalThreshold,
		).Scan(&withdrawalSlot, &txID, &previousRaw)
		hasWithdrawal := false
		switch {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return nil, err
		default:
			hasWithdrawal = true
			previous := uint64(0)
			if previousRaw.Valid && previousRaw.String != "" {
				previous, err = parseUint64(
					"historical previous reward", previousRaw.String,
				)
				if err != nil {
					return nil, err
				}
			}
			withdrawals[ref] = historicalWithdrawal{
				slot:     withdrawalSlot,
				id:       txID,
				previous: previous,
			}
		}
		credit := func(effectiveSlot int64, raw sql.NullString) error {
			if !raw.Valid || raw.String == "" {
				return nil
			}
			value, err := parseUint64("historical future reward credit", raw.String)
			if err != nil {
				return err
			}
			if ^uint64(0)-total[ref] < value {
				return errors.New("historical reward credit overflow")
			}
			total[ref] += value
			// Credits of a slot precede its withdrawals.
			if hasWithdrawal && effectiveSlot <= withdrawalSlot {
				if ^uint64(0)-beforeWithdrawal[ref] < value {
					return errors.New("historical reward credit overflow")
				}
				beforeWithdrawal[ref] += value
			}
			return nil
		}
		for _, creditSlot := range creditSlots {
			if err := scanHistoricalCredits(
				ctx, db, roundRewardCreditsAtSlotSQL, credit, creditSlot, accountID,
			); err != nil {
				return nil, err
			}
		}
		creditArgs := []any{accountID, threshold}
		if creditQuery == rewardEventCreditsAfterOrPostSnapshotSQL {
			creditArgs = append(creditArgs, threshold)
		}
		if err := scanHistoricalCredits(
			ctx, db, creditQuery, credit, creditArgs...,
		); err != nil {
			return nil, err
		}
	}

	ret := make(map[historicalRewardKey]uint64, len(base)+len(total))
	maps.Copy(ret, base)
	for ref, withdrawal := range withdrawals {
		if beforeWithdrawal[ref] > withdrawal.previous {
			return nil, errors.New("historical reward underflow")
		}
		ret[ref] = withdrawal.previous - beforeWithdrawal[ref]
	}
	for ref, credits := range total {
		if _, hasWithdrawal := withdrawals[ref]; hasWithdrawal {
			continue
		}
		reward := ret[ref]
		if credits > reward {
			// Imported/pruned journals can retain more credit than the live
			// balance. Historical stake is unsigned; floor the reconstructed
			// reward instead of wrapping or rejecting the whole pool query.
			ret[ref] = 0
			continue
		}
		ret[ref] = reward - credits
	}
	return ret, nil
}

// The reconstruction reads each credential's journal through its account id.
// The Leios transaction context slot, when a credit's source is a transaction
// or a withdrawal's own transaction has one, replaces the journal slot.
const (
	// rewardCreditSlotsAfterSQL lists the credited boundaries once per read;
	// each credential then probes reward_credit by its primary key per slot. A
	// join between the two lets the planner scan every credit of the later
	// rounds and filter by account afterwards.
	rewardCreditSlotsAfterSQL = `-- name: GetRewardCreditSlotsAfter :many
SELECT boundary_slot FROM reward_credit_slot WHERE boundary_slot > ?`

	roundRewardCreditsAtSlotSQL = `-- name: GetRoundRewardCreditsAtSlot :many
SELECT boundary_slot, amount FROM reward_credit
WHERE boundary_slot = ? AND account_id = ?`

	rewardEventCreditsAfterSQL = `-- name: GetRewardEventCreditsAfter :many
SELECT COALESCE(lc.slot, e.slot), e.amount
FROM reward_event_credit e
LEFT JOIN reward_source s ON s.id = e.source_id
LEFT JOIN "transaction" tx ON tx.hash = s.hash
LEFT JOIN leios_transaction_context lc ON lc.transaction_id = tx.id
WHERE e.account_id = ? AND COALESCE(lc.slot, e.slot) > ?`

	rewardEventCreditsAfterOrPostSnapshotSQL = `-- name: GetRewardEventCreditsAfterOrPostSnapshot :many
SELECT COALESCE(lc.slot, e.slot), e.amount
FROM reward_event_credit e
LEFT JOIN reward_source s ON s.id = e.source_id
LEFT JOIN "transaction" tx ON tx.hash = s.hash
LEFT JOIN leios_transaction_context lc ON lc.transaction_id = tx.id
WHERE e.account_id = ? AND (
    COALESCE(lc.slot, e.slot) > ?
    OR (COALESCE(lc.slot, e.slot) = ? AND e.post_snapshot = TRUE)
)`

	firstRewardWithdrawalAfterSQL = `-- name: GetFirstRewardWithdrawalAfter :one
SELECT COALESCE(lc.slot, w.slot), w.tx_id, w.previous_reward
FROM reward_withdrawal w
LEFT JOIN leios_transaction_context lc ON lc.transaction_id = w.tx_id
WHERE w.account_id = ? AND COALESCE(lc.slot, w.slot) > ?
ORDER BY w.slot, w.tx_id
LIMIT 1`

	firstRewardWithdrawalFromSQL = `-- name: GetFirstRewardWithdrawalFrom :one
SELECT COALESCE(lc.slot, w.slot), w.tx_id, w.previous_reward
FROM reward_withdrawal w
LEFT JOIN leios_transaction_context lc ON lc.transaction_id = w.tx_id
WHERE w.account_id = ? AND COALESCE(lc.slot, w.slot) >= ?
ORDER BY w.slot, w.tx_id
LIMIT 1`
)

func rewardCreditSlotsAfter(
	ctx context.Context,
	db queryer,
	threshold int64,
) ([]int64, error) {
	rows, err := db.QueryContext(ctx, rewardCreditSlotsAfterSQL, threshold)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var slots []int64
	for rows.Next() {
		var slot int64
		if err := rows.Scan(&slot); err != nil {
			return nil, err
		}
		slots = append(slots, slot)
	}
	return slots, rows.Err()
}

func scanHistoricalCredits(
	ctx context.Context,
	db queryer,
	query string,
	credit func(int64, sql.NullString) error,
	args ...any,
) error {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var effectiveSlot int64
		var raw sql.NullString
		if err := rows.Scan(&effectiveSlot, &raw); err != nil {
			return err
		}
		if err := credit(effectiveSlot, raw); err != nil {
			return err
		}
	}
	return rows.Err()
}

// historicalRewardCredentialPredicate builds a bounded, deterministic filter
// for the credentials participating in one historical stake request.  Reward
// reconstruction used to scan every account and reward delta in the database,
// even when a caller requested a single pool; keeping the selected set in the
// SQL predicates makes the work proportional to the request.
func historicalRewardCredentialPredicate(
	selected map[historicalRewardKey]struct{},
) (string, []any) {
	return historicalRewardCredentialPredicateFor(selected, "")
}

// historicalRewardCredentialPredicateFor is
// historicalRewardCredentialPredicate with the columns qualified by alias.
// Keys are grouped per credential tag into one IN list, so SQLite can search
// the (credential_tag, staking_key) index per group instead of evaluating a
// long OR of conjunctions against every row of a different index.
func historicalRewardCredentialPredicateFor(
	selected map[historicalRewardKey]struct{},
	alias string,
) (string, []any) {
	if len(selected) == 0 {
		return "1 = 0", nil
	}
	prefix := ""
	if alias != "" {
		prefix = alias + "."
	}
	keys := make([]historicalRewardKey, 0, len(selected))
	for key := range selected {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].tag != keys[j].tag {
			return keys[i].tag < keys[j].tag
		}
		return keys[i].key < keys[j].key
	})
	var parts []string
	args := make([]any, 0, len(keys)+2)
	for start := 0; start < len(keys); {
		end := start
		for end < len(keys) && keys[end].tag == keys[start].tag {
			end++
		}
		parts = append(parts, fmt.Sprintf(
			"(%scredential_tag = ? AND %sstaking_key IN (%s))",
			prefix,
			prefix,
			strings.TrimSuffix(strings.Repeat("?,", end-start), ","),
		))
		args = append(args, keys[start].tag)
		for _, key := range keys[start:end] {
			args = append(args, []byte(key.key))
		}
		start = end
	}
	return strings.Join(parts, " OR "), args
}

var historicalDelegationSources = []historicalStakeSource{
	{"stake_delegation", uint(lcommon.CertificateTypeStakeDelegation), 0},
	{
		"stake_registration_delegation",
		uint(lcommon.CertificateTypeStakeRegistrationDelegation),
		0,
	},
	{
		"stake_vote_delegation",
		uint(lcommon.CertificateTypeStakeVoteDelegation),
		0,
	},
	{
		"stake_vote_registration_delegation",
		uint(lcommon.CertificateTypeStakeVoteRegistrationDelegation),
		0,
	},
}

var historicalRegistrationSources = []historicalStakeSource{
	{"registration", uint(lcommon.CertificateTypeRegistration), 1},
	{"stake_registration", uint(lcommon.CertificateTypeStakeRegistration), 1},
	{
		"stake_registration_delegation",
		uint(lcommon.CertificateTypeStakeRegistrationDelegation),
		1,
	},
	{
		"stake_vote_registration_delegation",
		uint(lcommon.CertificateTypeStakeVoteRegistrationDelegation),
		1,
	},
	{
		"vote_registration_delegation",
		uint(lcommon.CertificateTypeVoteRegistrationDelegation),
		1,
	},
	{"deregistration", uint(lcommon.CertificateTypeDeregistration), 0},
	{
		"stake_deregistration",
		uint(lcommon.CertificateTypeStakeDeregistration),
		0,
	},
}

func (s *Store) GetStakeByPoolsAtSlot(
	poolKeyHashes [][]byte,
	slot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	txn types.Txn,
) (map[string]uint64, map[string]uint64, error) {
	return s.getStakeByPoolsAtSlot(
		poolKeyHashes, slot, 0, nil, expiryEpoch, inactivityPeriod, txn,
	)
}

func (s *Store) GetEpochBoundaryStakeByPools(
	poolKeyHashes [][]byte,
	snapshotSlot uint64,
	boundarySlot uint64,
	incomingEraID *uint,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	txn types.Txn,
) (map[string]uint64, map[string]uint64, error) {
	if boundarySlot <= snapshotSlot {
		boundarySlot = 0
		incomingEraID = nil
	}
	return s.getStakeByPoolsAtSlot(
		poolKeyHashes, snapshotSlot, boundarySlot, incomingEraID,
		expiryEpoch, inactivityPeriod, txn,
	)
}

func (s *Store) getStakeByPoolsAtSlot(
	poolKeyHashes [][]byte,
	slot uint64,
	boundarySlot uint64,
	incomingEraID *uint,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	txn types.Txn,
) (map[string]uint64, map[string]uint64, error) {
	stakes := make(map[string]uint64, len(poolKeyHashes))
	delegators := make(map[string]uint64, len(poolKeyHashes))
	for _, hash := range poolKeyHashes {
		stakes[string(hash)] = 0
		delegators[string(hash)] = 0
	}
	if len(poolKeyHashes) == 0 {
		return stakes, delegators, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, nil, err
	}
	poolKeyHashes = dedupeByteSlices(poolKeyHashes)
	for start := 0; start < len(poolKeyHashes); start += 400 {
		end := min(start+400, len(poolKeyHashes))
		query, args, err := s.historicalStakeCTE(
			ctx,
			db,
			slot,
			boundarySlot,
			incomingEraID,
			expiryEpoch,
			inactivityPeriod,
			"active_delegation.pool_key_hash IN ("+
				bindPlaceholders(end-start)+")",
			byteSliceArgs(poolKeyHashes[start:end]),
		)
		if err != nil {
			return nil, nil, err
		}
		rows, err := db.QueryContext(ctx, query+`
SELECT pool_key_hash, credential_tag, staking_key, utxo_amount
FROM active_delegator_stake`,
			args...,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("query historical stake: %w", err)
		}
		type stakeKey struct {
			pool string
			tag  uint8
			key  string
		}
		amounts := make(map[stakeKey]uint64)
		selected := make(map[historicalRewardKey]struct{})
		for rows.Next() {
			var hash, key []byte
			var tag uint8
			var rawAmount sql.NullString
			if err := rows.Scan(&hash, &tag, &key, &rawAmount); err != nil {
				rows.Close()
				return nil, nil, err
			}
			ref := stakeKey{pool: string(hash), tag: tag, key: string(key)}
			if _, ok := amounts[ref]; !ok {
				amounts[ref] = 0
			}
			if rawAmount.Valid && rawAmount.String != "" {
				value, err := parseUint64(
					"historical UTxO amount",
					rawAmount.String,
				)
				if err != nil {
					rows.Close()
					return nil, nil, err
				}
				if ^uint64(0)-amounts[ref] < value {
					rows.Close()
					return nil, nil, errors.New("historical stake overflow")
				}
				amounts[ref] += value
			}
			selected[historicalRewardKey{tag: tag, key: string(key)}] = struct{}{}
		}
		if err := rows.Close(); err != nil {
			return nil, nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, nil, err
		}
		rewardsByCredential, err := s.historicalRewardsAtBoundary(
			ctx, db, slot, boundarySlot, selected,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("calculate historical rewards: %w", err)
		}
		for ref, amount := range amounts {
			stake := amount
			reward := rewardsByCredential[historicalRewardKey{tag: ref.tag, key: ref.key}]
			if ^uint64(0)-stake < reward {
				return nil, nil, errors.New("historical stake overflow")
			} else {
				stake += reward
			}
			delegators[ref.pool]++
			if ^uint64(0)-stakes[ref.pool] < stake {
				return nil, nil, errors.New("historical stake overflow")
			}
			stakes[ref.pool] += stake
		}
	}
	return stakes, delegators, nil
}

func (s *Store) GetPoolOwnerStakeAtSlot(
	ownerKeys [][]byte,
	slot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	txn types.Txn,
) (map[string]uint64, error) {
	ret := make(map[string]uint64)
	if len(ownerKeys) == 0 {
		return ret, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	for start := 0; start < len(ownerKeys); start += 400 {
		end := min(start+400, len(ownerKeys))
		query, args, err := s.historicalStakeCTE(
			ctx,
			db,
			slot,
			0,
			nil,
			expiryEpoch,
			inactivityPeriod,
			"active_delegation.credential_tag = 0 AND "+
				"active_delegation.staking_key IN ("+
				bindPlaceholders(end-start)+")",
			byteSliceArgs(ownerKeys[start:end]),
		)
		if err != nil {
			return nil, err
		}
		rows, err := db.QueryContext(ctx, query+`
SELECT pool_key_hash, staking_key, credential_tag, utxo_amount
FROM active_delegator_stake`,
			args...,
		)
		if err != nil {
			return nil, err
		}
		type ownerKey struct{ pool, key string }
		amounts := make(map[ownerKey]uint64)
		selected := make(map[historicalRewardKey]struct{})
		for rows.Next() {
			var pool, key []byte
			var tag uint8
			var rawAmount sql.NullString
			if err := rows.Scan(&pool, &key, &tag, &rawAmount); err != nil {
				rows.Close()
				return nil, err
			}
			ref := ownerKey{pool: string(pool), key: string(key)}
			if _, ok := amounts[ref]; !ok {
				amounts[ref] = 0
			}
			if rawAmount.Valid && rawAmount.String != "" {
				value, err := parseUint64(
					"historical UTxO amount",
					rawAmount.String,
				)
				if err != nil {
					rows.Close()
					return nil, err
				}
				if ^uint64(0)-amounts[ref] < value {
					rows.Close()
					return nil, errors.New("historical stake overflow")
				}
				amounts[ref] += value
			}
			selected[historicalRewardKey{tag: tag, key: string(key)}] = struct{}{}
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		rewardsByCredential, err := s.historicalRewards(ctx, db, slot, selected)
		if err != nil {
			return nil, fmt.Errorf("calculate historical rewards: %w", err)
		}
		for ref, amount := range amounts {
			reward := rewardsByCredential[historicalRewardKey{tag: 0, key: ref.key}]
			if ^uint64(0)-amount < reward {
				return nil, errors.New("historical stake overflow")
			}
			stake := amount + reward
			ret[types.PoolCredentialStakeKey([]byte(ref.pool), 0, []byte(ref.key))] = stake
		}
	}
	return ret, nil
}

func (s *Store) GetRewardStakeInputsForPools(
	poolKeyHashes [][]byte,
	slot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	txn types.Txn,
) ([]*models.RewardStakeInput, error) {
	return s.getRewardStakeInputsForPools(
		poolKeyHashes, slot, 0, false, nil,
		expiryEpoch, inactivityPeriod, txn,
	)
}

func (s *Store) GetEpochBoundaryRewardStakeInputsForPools(
	poolKeyHashes [][]byte,
	snapshotSlot uint64,
	boundarySlot uint64,
	incomingEraID *uint,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	txn types.Txn,
) ([]*models.RewardStakeInput, error) {
	if boundarySlot <= snapshotSlot {
		boundarySlot = 0
		incomingEraID = nil
	}
	return s.getRewardStakeInputsForPools(
		poolKeyHashes, snapshotSlot, boundarySlot, true, incomingEraID,
		expiryEpoch, inactivityPeriod, txn,
	)
}

func (s *Store) getRewardStakeInputsForPools(
	poolKeyHashes [][]byte,
	slot uint64,
	boundarySlot uint64,
	boundaryAware bool,
	incomingEraID *uint,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	txn types.Txn,
) ([]*models.RewardStakeInput, error) {
	if expiryEpoch == 0 && boundarySlot == 0 && !boundaryAware {
		inputs, err := s.GetLiveStakeInputsForPools(poolKeyHashes, 0, txn)
		if err != nil {
			return nil, err
		}
		ret := inputs[:0]
		for _, input := range inputs {
			if input.Stake > 0 {
				ret = append(ret, input)
			}
		}
		return ret, nil
	}
	if len(poolKeyHashes) == 0 {
		return nil, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	poolKeyHashes = dedupeByteSlices(poolKeyHashes)
	ret := []*models.RewardStakeInput{}
	for start := 0; start < len(poolKeyHashes); start += 400 {
		end := min(start+400, len(poolKeyHashes))
		query, args, err := s.historicalStakeCTE(
			ctx,
			db,
			slot,
			boundarySlot,
			incomingEraID,
			expiryEpoch,
			inactivityPeriod,
			"active_delegation.pool_key_hash IN ("+
				bindPlaceholders(end-start)+")",
			byteSliceArgs(poolKeyHashes[start:end]),
		)
		if err != nil {
			return nil, err
		}
		rows, err := db.QueryContext(ctx, query+`
SELECT pool_key_hash, credential_tag, staking_key, utxo_amount
FROM active_delegator_stake
ORDER BY pool_key_hash, credential_tag, staking_key`,
			args...,
		)
		if err != nil {
			return nil, err
		}
		type inputKey struct {
			pool string
			tag  uint8
			key  string
		}
		amountByInput := make(map[inputKey]uint64)
		selected := make(map[historicalRewardKey]struct{})
		for rows.Next() {
			var pool, key []byte
			var tag uint8
			var rawAmount sql.NullString
			if err := rows.Scan(
				&pool, &tag, &key, &rawAmount,
			); err != nil {
				rows.Close()
				return nil, err
			}
			ref := inputKey{pool: string(pool), tag: tag, key: string(key)}
			if _, ok := amountByInput[ref]; !ok {
				amountByInput[ref] = 0
			}
			if rawAmount.Valid && rawAmount.String != "" {
				value, err := parseUint64(
					"historical UTxO amount",
					rawAmount.String,
				)
				if err != nil {
					rows.Close()
					return nil, err
				}
				if ^uint64(0)-amountByInput[ref] < value {
					rows.Close()
					return nil, errors.New("historical stake overflow")
				}
				amountByInput[ref] += value
			}
			selected[historicalRewardKey{tag: tag, key: string(key)}] = struct{}{}
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		rewardsByCredential, err := s.historicalRewardsAtBoundary(
			ctx, db, slot, boundarySlot, selected,
		)
		if err != nil {
			return nil, fmt.Errorf("calculate historical rewards: %w", err)
		}
		for ref, amount := range amountByInput {
			stake := amount
			reward := rewardsByCredential[historicalRewardKey{tag: ref.tag, key: ref.key}]
			if ^uint64(0)-stake < reward {
				return nil, errors.New("historical stake overflow")
			}
			stake += reward
			if stake == 0 {
				continue
			}
			ret = append(
				ret,
				&models.RewardStakeInput{
					PoolKeyHash:   []byte(ref.pool),
					CredentialTag: ref.tag,
					StakingKey:    []byte(ref.key),
					Stake:         types.Uint64(stake),
					Registered:    true,
				},
			)
		}
	}
	return ret, nil
}

// historicalStakeCTE builds the delegated-stake CTE evaluated at slot.
// predicateArgs supplies the bind values for predicate; they are bound once per
// occurrence, and predicate appears twice when pointer stake is counted.
// boundarySlot is passed through to the pointer era gate; see
// pointerStakeCounted. Pass 0 for a plain "stake at slot" query with no
// epoch-boundary semantics.
func (s *Store) historicalStakeCTE(
	ctx context.Context,
	db queryer,
	slot uint64,
	boundarySlot uint64,
	incomingEraID *uint,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	predicate string,
	predicateArgs []any,
) (string, []any, error) {
	reapSlot := slot
	afterEnactment, err := boundarySnapshotAfterEnactment(ctx, db, boundarySlot)
	if err != nil {
		return "", nil, err
	}
	if afterEnactment {
		reapSlot = boundarySlot
	}
	query, args := activeDelegationSQL(slot, reapSlot)
	// Stake held at a pointer address reaches its credential only in the eras
	// that count it, and only through the position recorded in utxo_pointer.
	// When it is not counted the query is exactly what it was before pointer
	// resolution existed, and when no output uses a pointer address the extra
	// branch produces no rows.
	countPointerStake, err := pointerStakeCounted(
		ctx, db, slot, boundarySlot, incomingEraID,
	)
	if err != nil {
		return "", nil, err
	}
	if countPointerStake {
		resolution, resolutionArgs, err := pointerResolutionSQL(slot)
		if err != nil {
			return "", nil, err
		}
		query += resolution
		args = append(args, resolutionArgs...)
	}
	delegationSource := "active_delegation"
	expiryPredicate := ""
	if expiryEpoch > 0 {
		if inactivityPeriod == 0 {
			return "", nil, errors.New(
				"historical stake expiry enabled with zero inactivity period",
			)
		}
		expiration, expirationArgs, err := historicalExpirationSQL(
			ctx,
			db,
			slot,
			expiryEpoch,
			inactivityPeriod,
		)
		if err != nil {
			return "", nil, err
		}
		query += expiration
		args = append(args, expirationArgs...)
		// historical_expiration carries every active_delegation row with its
		// expiration, so it stands in for active_delegation rather than being
		// joined back to it: SQLite expands each reference to a CTE into its
		// own copy of the CTE chain beneath it, and each copy of
		// active_delegation costs over a hundred cursors.
		delegationSource = "historical_expiration active_delegation"
		expiryPredicate = `(active_delegation.expiration_epoch = 0
 OR active_delegation.expiration_epoch >= ?
 OR active_delegation.expiration_epoch IS NULL) AND `
	}
	query += `,
active_delegator_stake AS (
 SELECT active_delegation.pool_key_hash,
        active_delegation.credential_tag,
        active_delegation.staking_key,
        utxo.amount AS utxo_amount
 FROM ` + delegationSource + `
 LEFT JOIN utxo
   ON utxo.credential_tag = active_delegation.credential_tag
  AND utxo.staking_key = active_delegation.staking_key
  AND COALESCE((SELECT lc.slot FROM leios_transaction_context lc WHERE lc.transaction_id = COALESCE(utxo.transaction_id, utxo.collateral_return_for_tx_id)), utxo.added_slot) <= ?
  AND (utxo.deleted_slot = 0 OR COALESCE((SELECT lc.slot FROM leios_transaction_context lc JOIN "transaction" spent_tx ON spent_tx.id = lc.transaction_id WHERE spent_tx.hash = utxo.spent_at_tx_id), utxo.deleted_slot) > ?)
 WHERE ` + expiryPredicate + predicate
	args = append(args, slot, slot)
	if expiryEpoch > 0 {
		args = append(args, expiryEpoch)
	}
	args = append(args, predicateArgs...)
	if countPointerStake {
		// A pointer-held UTxO carries no staking_key, so it never joins the
		// branch above and cannot be counted twice. Rows are summed per
		// (pool, credential) by the callers, so UNION ALL adds stake without
		// disturbing the delegator count.
		query += `
 UNION ALL
 SELECT active_delegation.pool_key_hash,
        active_delegation.credential_tag,
        active_delegation.staking_key,
        utxo.amount AS utxo_amount
 FROM ` + delegationSource + `
 JOIN pointer_resolution
   ON pointer_resolution.credential_tag = active_delegation.credential_tag
  AND pointer_resolution.staking_key = active_delegation.staking_key
 JOIN utxo
   ON utxo.id = pointer_resolution.utxo_id
  AND COALESCE((SELECT lc.slot FROM leios_transaction_context lc WHERE lc.transaction_id = COALESCE(utxo.transaction_id, utxo.collateral_return_for_tx_id)), utxo.added_slot) <= ?
  AND (utxo.deleted_slot = 0 OR COALESCE((SELECT lc.slot FROM leios_transaction_context lc JOIN "transaction" spent_tx ON spent_tx.id = lc.transaction_id WHERE spent_tx.hash = utxo.spent_at_tx_id), utxo.deleted_slot) > ?)
 WHERE ` + expiryPredicate + predicate
		args = append(args, slot, slot)
		if expiryEpoch > 0 {
			args = append(args, expiryEpoch)
		}
		args = append(args, predicateArgs...)
	}
	query += `
)`
	return query, args, nil
}

// byteSliceArgs binds a chunk of credential or pool hashes as query arguments.
// historicalStakeCTE may emit its predicate more than once, so the values are
// handed to it rather than appended to the returned args by the caller.
func byteSliceArgs(values [][]byte) []any {
	ret := make([]any, 0, len(values))
	for _, value := range values {
		ret = append(ret, value)
	}
	return ret
}

// activeDelegationSQL resolves, for each credential, which pool its most
// recent surviving delegation certificate names as of slot.
//
// A delegation certificate is not the last word on the delegation: POOLREAP
// removes the delegations pointing at a pool reaped at an epoch boundary
// (ClearDelegationsToRetiredPool) and writes no certificate of its own, so a
// certificate predating a reap that still stands at slot must not put the
// credential back on that pool if the pool later re-registered and the
// credential never re-delegated. The active_delegation CTE's NOT EXISTS
// clause below ports poolReapedAfterDelegation's rollback-path guard
// (account.go) into this historical-reconstruction path, which had no
// equivalent guard: live incident, Preview epoch 646/647, pools
// 2bf19282e11384ccf60c9a3b0f5b6e00e74aed2cbd8bc9837ccc0966 and
// 881f9bc5415bb7381dc4b6571ab662eff5277d78d778d05614cdf0d4 each retired and
// later re-registered, and one-time delegators from before the reap who never
// re-delegated had their stake wrongly resurrected onto the pool by this
// query, producing a stake-distribution mismatch against Koios of exactly
// the reaped credentials' stake (dingo node-parity issue, epoch 647).
func activeDelegationSQL(slot uint64, reapSlot uint64) (string, []any) {
	return activeDelegationScopedSQL(slot, reapSlot, "", nil)
}

// activeDelegationScopedSQL is activeDelegationSQL restricted to the
// credentials that scopeSQL, a query yielding (credential_tag, staking_key)
// pairs, selects. An empty scopeSQL yields exactly activeDelegationSQL.
//
// Every CTE activeDelegationSQL builds is a per-credential function: the
// window ranks partition by credential, the account fallbacks test only their
// own credential's history, and the pool-reap guard reads only the
// delegation's pool and slot. Joining each event source to the scope therefore
// drops rows for other credentials without changing any row inside the scope,
// so a caller that only ever joins the result back to scoped credentials gets
// the same answer. What it buys is the join order: the ranking otherwise
// materialises every credential's history before the first outer row is
// read, which on mainnet is minutes of work per query when a handful of
// credentials are wanted.
func activeDelegationScopedSQL(
	slot uint64,
	reapSlot uint64,
	scopeSQL string,
	scopeArgs []any,
) (string, []any) {
	args := make(
		[]any,
		0,
		len(scopeArgs)+
			3*len(historicalDelegationSources)+
			3*len(historicalRegistrationSources)+2,
	)
	// CROSS JOIN is an ordering constraint in SQLite: the scope is read
	// first and each source is probed by its (credential_tag, staking_key)
	// index. A plain JOIN leaves the order to the planner's row estimate for
	// the CTE, which on a large source can still choose the full scan. The
	// join condition goes in WHERE (scopedWhere) because PostgreSQL rejects
	// ON after CROSS JOIN.
	scopedFrom := func(table string, alias string) string {
		if scopeSQL == "" {
			if alias == table {
				return "FROM " + table
			}
			return "FROM " + table + " " + alias
		}
		return "FROM credential_scope scope\nCROSS JOIN " + table + " " + alias
	}
	scopedWhere := func(alias string) string {
		if scopeSQL == "" {
			return ""
		}
		return alias + ".credential_tag = scope.credential_tag\n AND " +
			alias + ".staking_key = scope.staking_key\n AND "
	}
	prefix := `
WITH `
	if scopeSQL != "" {
		prefix += `credential_scope AS (` + scopeSQL + `
), `
		args = append(args, scopeArgs...)
	}
	delegationParts := make(
		[]string,
		0,
		len(historicalDelegationSources)+1,
	)
	for _, source := range historicalDelegationSources {
		delegationParts = append(delegationParts, fmt.Sprintf(`
SELECT event.credential_tag, event.staking_key, event.pool_key_hash,
       COALESCE((SELECT lc.slot FROM leios_transaction_context lc WHERE lc.transaction_id = tx.id), event.added_slot) AS added_slot, event.added_slot AS ownership_slot, tx.block_index, certs.cert_index
%[2]s
JOIN certs ON certs.id = event.certificate_id AND certs.cert_type = ?
JOIN "transaction" tx ON tx.id = certs.transaction_id
WHERE %[3]sCOALESCE((SELECT lc.slot FROM leios_transaction_context lc JOIN certs lc_cert ON lc_cert.transaction_id = lc.transaction_id WHERE lc_cert.id = event.certificate_id), event.added_slot) <= ?`, source.table, scopedFrom(source.table, "event"), scopedWhere("event")))
		args = append(args, source.certType, slot)
	}
	allHistoryTables := make(
		[]string,
		0,
		len(historicalDelegationSources)+
			len(historicalRegistrationSources),
	)
	for _, source := range historicalDelegationSources {
		allHistoryTables = append(allHistoryTables, source.table)
	}
	for _, source := range historicalRegistrationSources {
		allHistoryTables = append(allHistoryTables, source.table)
	}
	delegationParts = append(delegationParts, `
SELECT account.credential_tag, account.staking_key, account.pool,
       account.added_slot, account.added_slot, 0, 0
`+scopedFrom("account", "account")+`
WHERE `+scopedWhere("account")+`account.added_slot <= ? AND account.active = TRUE
  AND account.pool IS NOT NULL AND length(account.pool) > 0`+
		noHistorySQL("account", allHistoryTables))
	args = append(args, slot)
	for range allHistoryTables {
		args = append(args, slot)
	}
	registrationParts := make(
		[]string,
		0,
		len(historicalRegistrationSources)+1,
	)
	for _, source := range historicalRegistrationSources {
		registrationParts = append(registrationParts, fmt.Sprintf(`
SELECT event.credential_tag, event.staking_key, %d AS registered, COALESCE((SELECT lc.slot FROM leios_transaction_context lc WHERE lc.transaction_id = tx.id), event.added_slot) AS added_slot,
       event.added_slot AS ownership_slot, tx.block_index, certs.cert_index
%s
JOIN certs ON certs.id = event.certificate_id AND certs.cert_type = ?
JOIN "transaction" tx ON tx.id = certs.transaction_id
WHERE %sCOALESCE((SELECT lc.slot FROM leios_transaction_context lc JOIN certs lc_cert ON lc_cert.transaction_id = lc.transaction_id WHERE lc_cert.id = event.certificate_id), event.added_slot) <= ?`,
			source.registered,
			scopedFrom(source.table, "event"),
			scopedWhere("event"),
		))
		args = append(args, source.certType, slot)
	}
	registrationTables := make(
		[]string,
		0,
		len(historicalRegistrationSources),
	)
	for _, source := range historicalRegistrationSources {
		registrationTables = append(registrationTables, source.table)
	}
	registrationParts = append(registrationParts, `
SELECT account.credential_tag, account.staking_key,
       CASE WHEN account.active THEN 1 ELSE 0 END,
       account.created_slot, account.created_slot, 0, 0
`+scopedFrom("account", "account")+`
WHERE `+scopedWhere("account")+`account.created_slot <= ?`+
		noHistorySQL("account", registrationTables))
	args = append(args, slot)
	for range registrationTables {
		args = append(args, slot)
	}
	return prefix + `delegation_events AS (` + strings.Join(delegationParts, " UNION ALL ") + `
), ranked_delegation AS (
 SELECT *, ROW_NUMBER() OVER (
   PARTITION BY credential_tag, staking_key
   ORDER BY added_slot DESC, ownership_slot DESC, block_index DESC, cert_index DESC
 ) rn FROM delegation_events
), latest_delegation AS (
 SELECT * FROM ranked_delegation WHERE rn = 1
), registration_events AS (` + strings.Join(registrationParts, " UNION ALL ") + `
), ranked_registration AS (
 SELECT *, ROW_NUMBER() OVER (
   PARTITION BY credential_tag, staking_key
   ORDER BY added_slot DESC, ownership_slot DESC, block_index DESC, cert_index DESC
 ) rn FROM registration_events
), latest_registration AS (
 SELECT * FROM ranked_registration WHERE rn = 1
), active_delegation AS (
 SELECT delegation.pool_key_hash, delegation.credential_tag,
        delegation.staking_key
 FROM latest_delegation delegation
 JOIN latest_registration registration
   ON registration.credential_tag = delegation.credential_tag
  AND registration.staking_key = delegation.staking_key
 WHERE registration.registered = 1
   AND (delegation.added_slot > registration.added_slot
     OR (delegation.added_slot = registration.added_slot
       AND delegation.ownership_slot > registration.ownership_slot)
     OR (delegation.added_slot = registration.added_slot
       AND delegation.ownership_slot = registration.ownership_slot
       AND delegation.block_index > registration.block_index)
     OR (delegation.added_slot = registration.added_slot
       AND delegation.ownership_slot = registration.ownership_slot
       AND delegation.block_index = registration.block_index
       AND delegation.cert_index >= registration.cert_index))
   AND NOT EXISTS (
       SELECT 1
       FROM pool_retirement rt
       JOIN pool p ON p.id = rt.pool_id
       JOIN epoch e ON e.epoch_id = rt.epoch
       LEFT JOIN certs c ON c.id = rt.certificate_id
       LEFT JOIN "transaction" t ON t.id = c.transaction_id
       WHERE p.pool_key_hash = delegation.pool_key_hash
         AND COALESCE((SELECT lc.slot FROM leios_transaction_context lc JOIN certs lc_cert ON lc_cert.transaction_id = lc.transaction_id WHERE lc_cert.id = rt.certificate_id), rt.added_slot) <= ?
         AND e.start_slot > delegation.added_slot
         AND e.start_slot <= ?
         AND NOT EXISTS (
             SELECT 1
             FROM pool_registration pr
             LEFT JOIN certs c2 ON c2.id = pr.certificate_id
             LEFT JOIN "transaction" t2 ON t2.id = c2.transaction_id
             WHERE pr.pool_id = rt.pool_id
               AND COALESCE((SELECT lc.slot FROM leios_transaction_context lc JOIN certs lc_cert ON lc_cert.transaction_id = lc.transaction_id WHERE lc_cert.id = pr.certificate_id), pr.added_slot) < e.start_slot
               AND (
                   pr.added_slot > rt.added_slot
                   OR (pr.added_slot = rt.added_slot
                       AND COALESCE(t2.block_index, 0) > COALESCE(t.block_index, 0))
                   OR (pr.added_slot = rt.added_slot
                       AND COALESCE(t2.block_index, 0) = COALESCE(t.block_index, 0)
                       AND COALESCE(c2.cert_index, 0) > COALESCE(c.cert_index, 0))
               )
         )
         AND NOT EXISTS (
             SELECT 1
             FROM pool_retirement rt2
             LEFT JOIN certs c3 ON c3.id = rt2.certificate_id
             LEFT JOIN "transaction" t3 ON t3.id = c3.transaction_id
             WHERE rt2.pool_id = rt.pool_id
               AND rt2.id <> rt.id
               AND COALESCE((SELECT lc.slot FROM leios_transaction_context lc JOIN certs lc_cert ON lc_cert.transaction_id = lc.transaction_id WHERE lc_cert.id = rt2.certificate_id), rt2.added_slot) < e.start_slot
               AND (
                   rt2.added_slot > rt.added_slot
                   OR (rt2.added_slot = rt.added_slot
                       AND COALESCE(t3.block_index, 0) > COALESCE(t.block_index, 0))
                   OR (rt2.added_slot = rt.added_slot
                       AND COALESCE(t3.block_index, 0) = COALESCE(t.block_index, 0)
                       AND COALESCE(c3.cert_index, 0) > COALESCE(c.cert_index, 0))
               )
         )
   )
)`, append(args, slot, reapSlot)
}

// historicalExpirationSQL reconstructs each active-delegation credential's
// CIP-0163 expiration_epoch as of slot from witness history
// (accountWitnessTables, reward_withdrawal, account_withdrawal_witness).
// Absent the one-time activation floor described below: a witness at or
// before slot wins outright; failing that, a witness only after slot falls
// back to a creation-epoch-derived expiration instead (the account existed
// but nothing proves it was witnessed by slot); failing that -- no witness at
// all, before or after slot -- it falls back to the mutable
// account.expiration_epoch; a missing account row produces 0.
//
// The historical_expiration CTE it emits holds one row per active_delegation
// row, with that row's pool_key_hash, so a caller selects from it in place of
// active_delegation. It references active_delegation exactly once: SQLite
// expands every reference to a CTE into its own copy of the CTEs beneath it,
// and a statement whose copies open more cursors than SQLITE_LIMIT_EXPR_DEPTH
// fails to prepare.
//
// When the query's expiryEpoch is at or after the recorded CIP-0163
// activation epoch (activation != nil && *activation <= expiryEpoch), a
// credential recorded in the one-time activation membership
// (account_inactivity_activation) instead floors at
// activation_epoch + inactivityPeriod whenever its witness at or before slot
// is older than the activation epoch or altogether absent -- overriding
// every other branch above (the witness-at-or-before-slot match, the
// creation-epoch fallback, and the mutable-column fallback) for that
// credential. Only a witness at or before slot that is itself at or after
// the activation epoch, or a credential outside the activation membership,
// falls through to the ordinary fallback chain described above.
//
// This is only exact if two from-genesis nodes retain the same witness
// history for the same slot -- none of those tables is ever pruned by age,
// storage mode, or configurable retention; the only deletes are the
// rollback/lifecycle-truncate added_slot > slot statements
// (DeleteCertificatesAfterSlot and the account.go equivalents), which are
// keyed on consensus chain state, not per-node config. That is a guarantee
// about rows already written, not about whether a row is written at all:
// account_withdrawal_witness inserts are separately elided when
// DelegatorInactivityEnabled is off, which is the same
// network-wide setting this function's caller already requires to match, so
// it introduces no new divergence. See ARCHITECTURE.md's CIP-0163 section
// before adding any other deletion path for these tables.
func historicalExpirationSQL(
	ctx context.Context,
	db queryer,
	slot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
) (string, []any, error) {
	var value string
	err := db.QueryRowContext(ctx, `
SELECT value FROM sync_state
WHERE sync_key = 'delegator_inactivity_activated'
LIMIT 1`).Scan(&value)
	var activation *uint64
	if err == nil {
		parsed, parseErr := strconv.ParseUint(value, 10, 64)
		if parseErr != nil {
			return "", nil, parseErr
		}
		activation = &parsed
	} else if !errors.Is(err, sql.ErrNoRows) {
		return "", nil, err
	}
	// Each witness source is read through its (credential_tag, staking_key)
	// index, correlated on the outer active_delegation row, rather than
	// joined to active_delegation, which would copy that CTE once per source.
	parts := make([]string, 0, len(accountWitnessTables)+2)
	for _, table := range accountWitnessTables {
		parts = append(parts, `
  SELECT COALESCE((SELECT lc.slot FROM leios_transaction_context lc JOIN certs c ON c.transaction_id = lc.transaction_id WHERE c.id = witness.certificate_id), witness.added_slot) AS added_slot
  FROM `+table+` witness
  WHERE witness.credential_tag = active.credential_tag
    AND witness.staking_key = active.staking_key`)
	}
	parts = append(parts, `
  SELECT COALESCE((SELECT lc.slot FROM leios_transaction_context lc JOIN "transaction" tx ON tx.id = lc.transaction_id WHERE tx.hash = witness.tx_hash), witness.added_slot) AS added_slot
  FROM account_withdrawal_witness witness
  WHERE witness.credential_tag = active.credential_tag
    AND witness.staking_key = active.staking_key`, `
  SELECT COALESCE((SELECT lc.slot FROM leios_transaction_context lc WHERE lc.transaction_id = witness.tx_id), witness.slot) AS added_slot
  FROM reward_withdrawal witness
  WHERE witness.account_id = (
    SELECT wa.id FROM account wa
    WHERE wa.credential_tag = active.credential_tag
      AND wa.staking_key = active.staking_key)`)
	witnessEvents := strings.Join(parts, " UNION ALL ")
	args := make([]any, 1, 5)
	args[0] = slot
	activationCTE := ""
	activationJoin := ""
	expiration := `CASE
 WHEN account.id IS NULL THEN 0
 WHEN summary.witness_epoch IS NOT NULL
   THEN summary.witness_epoch + ?
 WHEN summary.latest_added_slot IS NOT NULL
   THEN COALESCE(created_epoch.epoch_id, 0) + ?
 ELSE COALESCE(account.expiration_epoch, 0) END`
	args = append(args, inactivityPeriod, inactivityPeriod)
	if activation != nil && *activation <= expiryEpoch {
		activationCTE = `, inactivity_activation AS (SELECT ? epoch_id)`
		args = []any{
			slot,
			*activation,
			inactivityPeriod,
			inactivityPeriod,
			inactivityPeriod,
		}
		activationJoin = `
CROSS JOIN inactivity_activation
LEFT JOIN account_inactivity_activation activation_account
  ON activation_account.credential_tag = summary.credential_tag
 AND activation_account.staking_key = summary.staking_key`
		expiration = `CASE
 WHEN account.id IS NULL THEN 0
 WHEN activation_account.staking_key IS NOT NULL
  AND (summary.witness_epoch IS NULL
    OR summary.witness_epoch < inactivity_activation.epoch_id)
   THEN inactivity_activation.epoch_id + ?
 WHEN summary.witness_epoch IS NOT NULL
   THEN summary.witness_epoch + ?
 WHEN summary.latest_added_slot IS NOT NULL
   THEN COALESCE(created_epoch.epoch_id, 0) + ?
 ELSE COALESCE(account.expiration_epoch, 0) END`
	}
	// DISTINCT changes no rows, because active_delegation holds one row per
	// credential, but it stops the planner from flattening historical_witness
	// into historical_expiration. Flattened, each reference to witness_epoch
	// in the CASE above would carry, and evaluate, its own copy of the
	// witness lookups.
	return `,
historical_witness AS (
 SELECT DISTINCT active.pool_key_hash, active.credential_tag, active.staking_key,
        (SELECT MAX(epoch.epoch_id)
         FROM (SELECT MAX(witness.added_slot) added_slot FROM (` + witnessEvents + `
              ) witness WHERE witness.added_slot <= ?) historical
         JOIN epoch
           ON historical.added_slot >= epoch.start_slot
          AND historical.added_slot < epoch.start_slot + epoch.length_in_slots
        ) witness_epoch,
        (SELECT MAX(witness.added_slot) FROM (` + witnessEvents + `
        ) witness) latest_added_slot
 FROM active_delegation active
)` + activationCTE + `,
historical_expiration AS (
 SELECT summary.pool_key_hash, summary.credential_tag, summary.staking_key,
        ` + expiration + ` expiration_epoch
 FROM historical_witness summary
 LEFT JOIN account
  ON account.credential_tag = summary.credential_tag
 AND account.staking_key = summary.staking_key
 LEFT JOIN epoch created_epoch
  ON account.created_slot > 0 AND account.created_slot >= created_epoch.start_slot
 AND account.created_slot < created_epoch.start_slot + created_epoch.length_in_slots
 ` + activationJoin + `
)`, args, nil
}

func noHistorySQL(alias string, tables []string) string {
	var ret strings.Builder
	for _, table := range tables {
		fmt.Fprintf(&ret, `
 AND NOT EXISTS (
   SELECT 1 FROM %s history
   WHERE history.credential_tag = %s.credential_tag
     AND history.staking_key = %s.staking_key
     AND history.added_slot <= ?
 )`, table, alias, alias)
	}
	return ret.String()
}

// GetEpochBoundaryDelegatedPoolKeyHashes returns every pool key hash the
// boundary reconstruction attributes stake to at snapshotSlot, whether or not
// that pool is still registered. It is the historical-path counterpart of
// GetDelegatedPoolKeyHashes and serves the same sigma_a denominator; see that
// function for why the denominator must not be enumerated from the active pool
// set.
//
// It reconstructs from the same CTE the stake fetch uses, with the pool
// predicate relaxed to "has a delegation at all", and applies neither the
// expiry nor the inactivity gate: the result only widens the set of pools the
// stake fetch is asked about, and that fetch applies both.
func (s *Store) GetEpochBoundaryDelegatedPoolKeyHashes(
	snapshotSlot uint64,
	boundarySlot uint64,
	txn types.Txn,
) ([][]byte, error) {
	if boundarySlot <= snapshotSlot {
		boundarySlot = 0
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetEpochBoundaryDelegatedPoolKeyHashes: resolve db: %w",
			err,
		)
	}
	query, args, err := s.historicalStakeCTE(
		ctx,
		db,
		snapshotSlot,
		boundarySlot,
		nil,
		0,
		0,
		"active_delegation.pool_key_hash IS NOT NULL",
		nil,
	)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, query+`
SELECT DISTINCT pool_key_hash FROM active_delegator_stake`, args...)
	if err != nil {
		return nil, fmt.Errorf(
			"GetEpochBoundaryDelegatedPoolKeyHashes: %w",
			err,
		)
	}
	defer rows.Close()
	ret := [][]byte{}
	for rows.Next() {
		var hash []byte
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		if len(hash) == 0 {
			continue
		}
		ret = append(ret, hash)
	}
	return ret, rows.Err()
}
