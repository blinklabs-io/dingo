// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

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
func historicalRewards(
	db queryer,
	slot uint64,
	selected map[historicalRewardKey]struct{},
) (map[historicalRewardKey]uint64, error) {
	return historicalRewardsAtBoundary(db, slot, 0, selected)
}

// historicalRewardsAtBoundary reconstructs the reward balance observed at an
// epoch SNAP boundary. Boundary credits marked PostSnapshot are still future
// credits relative to SNAP and must be removed, while unmarked credits at the
// boundary are already visible to the snapshot.
func historicalRewardsAtBoundary(
	db queryer,
	slot uint64,
	boundarySlot uint64,
	selected map[historicalRewardKey]struct{},
) (map[historicalRewardKey]uint64, error) {
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
			db,
			slot,
			boundarySlot,
			batchSelected,
		)
		if err != nil {
			return nil, err
		}
		maps.Copy(ret, batch)
	}
	return ret, nil
}

// historicalRewardsBatch evaluates future reward credits in Go. Amounts are
// persisted as decimal text and can exceed a signed SQL integer; keeping the
// ordering logic here avoids lossy CAST/SUM arithmetic in SQLite.
func historicalRewardsBatch(
	db queryer,
	slot uint64,
	boundarySlot uint64,
	selected map[historicalRewardKey]struct{},
) (map[historicalRewardKey]uint64, error) {
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return nil, err
	}
	base := make(map[historicalRewardKey]uint64)
	predicate, predicateArgs := historicalRewardCredentialPredicate(selected)
	rows, err := db.QueryContext(
		context.Background(),
		"SELECT credential_tag, staking_key, reward FROM account WHERE "+predicate,
		predicateArgs...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var tag uint8
		var key []byte
		var raw sql.NullString
		if err := rows.Scan(&tag, &key, &raw); err != nil {
			rows.Close()
			return nil, err
		}
		if !raw.Valid || raw.String == "" {
			continue
		}
		value, err := parseUint64("historical account reward", raw.String)
		if err != nil {
			rows.Close()
			return nil, err
		}
		base[historicalRewardKey{tag: tag, key: string(key)}] = value
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	withdrawals := make(map[historicalRewardKey]historicalWithdrawal)
	withdrawalValue := slotValue
	if boundarySlot > 0 {
		withdrawalValue, err = checkedInt64(boundarySlot)
		if err != nil {
			return nil, err
		}
	}
	withdrawalOp := ">"
	if boundarySlot > 0 {
		withdrawalOp = ">="
	}
	withdrawalArgs := append([]any{withdrawalValue}, predicateArgs...)
	rows, err = db.QueryContext(context.Background(), `
SELECT credential_tag, staking_key, id, added_slot, previous_reward
FROM account_reward_delta
WHERE withdrawal = TRUE AND added_slot `+withdrawalOp+` ? AND (`+predicate+`)
	ORDER BY credential_tag, staking_key, added_slot, id`, withdrawalArgs...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var tag uint8
		var key []byte
		var id, addedSlot int64
		var raw sql.NullString
		if err := rows.Scan(&tag, &key, &id, &addedSlot, &raw); err != nil {
			rows.Close()
			return nil, err
		}
		ref := historicalRewardKey{tag: tag, key: string(key)}
		if _, exists := withdrawals[ref]; exists {
			continue
		}
		previous := uint64(0)
		if raw.Valid && raw.String != "" {
			previous, err = parseUint64(
				"historical previous reward",
				raw.String,
			)
			if err != nil {
				rows.Close()
				return nil, err
			}
		}
		withdrawals[ref] = historicalWithdrawal{
			slot:     addedSlot,
			id:       id,
			previous: previous,
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	total := make(map[historicalRewardKey]uint64)
	beforeWithdrawal := make(map[historicalRewardKey]uint64)
	futureRewardPredicate := "added_slot > ?"
	creditArgs := make([]any, 0, 1+len(predicateArgs))
	creditArgs = append(creditArgs, slotValue)
	if boundarySlot > 0 {
		boundaryValue, boundaryErr := checkedInt64(boundarySlot)
		if boundaryErr != nil {
			return nil, boundaryErr
		}
		futureRewardPredicate = "(added_slot > ? OR (added_slot = ? AND post_snapshot = TRUE))"
		creditArgs = []any{boundaryValue, boundaryValue}
	}
	creditArgs = append(creditArgs, predicateArgs...)
	rows, err = db.QueryContext(context.Background(), `
SELECT credential_tag, staking_key, id, added_slot, amount
FROM account_reward_delta
WHERE withdrawal = FALSE AND `+futureRewardPredicate+` AND (`+predicate+`)
	ORDER BY credential_tag, staking_key, added_slot, id`, creditArgs...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var tag uint8
		var key []byte
		var id, addedSlot int64
		var raw sql.NullString
		if err := rows.Scan(&tag, &key, &id, &addedSlot, &raw); err != nil {
			rows.Close()
			return nil, err
		}
		if !raw.Valid || raw.String == "" {
			continue
		}
		value, err := parseUint64("historical future reward credit", raw.String)
		if err != nil {
			rows.Close()
			return nil, err
		}
		ref := historicalRewardKey{tag: tag, key: string(key)}
		if ^uint64(0)-total[ref] < value {
			rows.Close()
			return nil, errors.New("historical reward credit overflow")
		}
		total[ref] += value
		withdrawal, hasWithdrawal := withdrawals[ref]
		if hasWithdrawal && (addedSlot < withdrawal.slot ||
			(addedSlot == withdrawal.slot && id < withdrawal.id)) {
			if ^uint64(0)-beforeWithdrawal[ref] < value {
				rows.Close()
				return nil, errors.New("historical reward credit overflow")
			}
			beforeWithdrawal[ref] += value
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
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

// historicalRewardCredentialPredicate builds a bounded, deterministic filter
// for the credentials participating in one historical stake request.  Reward
// reconstruction used to scan every account and reward delta in the database,
// even when a caller requested a single pool; keeping the selected set in the
// SQL predicates makes the work proportional to the request.
func historicalRewardCredentialPredicate(
	selected map[historicalRewardKey]struct{},
) (string, []any) {
	if len(selected) == 0 {
		return "1 = 0", nil
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
	parts := make([]string, 0, len(keys))
	args := make([]any, 0, len(keys)*2)
	for _, key := range keys {
		parts = append(parts, "(credential_tag = ? AND staking_key = ?)")
		args = append(args, key.tag, []byte(key.key))
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
		poolKeyHashes, slot, 0, expiryEpoch, inactivityPeriod, txn,
	)
}

func (s *Store) GetEpochBoundaryStakeByPools(
	poolKeyHashes [][]byte,
	snapshotSlot uint64,
	boundarySlot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	txn types.Txn,
) (map[string]uint64, map[string]uint64, error) {
	if boundarySlot <= snapshotSlot {
		boundarySlot = 0
	}
	return s.getStakeByPoolsAtSlot(
		poolKeyHashes, snapshotSlot, boundarySlot,
		expiryEpoch, inactivityPeriod, txn,
	)
}

func (s *Store) getStakeByPoolsAtSlot(
	poolKeyHashes [][]byte,
	slot uint64,
	boundarySlot uint64,
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
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, nil, err
	}
	poolKeyHashes = dedupeByteSlices(poolKeyHashes)
	for start := 0; start < len(poolKeyHashes); start += 400 {
		end := min(start+400, len(poolKeyHashes))
		query, args, err := s.historicalStakeCTE(
			db,
			slot,
			expiryEpoch,
			inactivityPeriod,
			"active_delegation.pool_key_hash IN ("+
				bindPlaceholders(end-start)+")",
		)
		if err != nil {
			return nil, nil, err
		}
		for _, hash := range poolKeyHashes[start:end] {
			args = append(args, hash)
		}
		rows, err := db.QueryContext(context.Background(), query+`
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
		rewardsByCredential, err := historicalRewardsAtBoundary(
			db, slot, boundarySlot, selected,
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
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	for start := 0; start < len(ownerKeys); start += 400 {
		end := min(start+400, len(ownerKeys))
		query, args, err := s.historicalStakeCTE(
			db,
			slot,
			expiryEpoch,
			inactivityPeriod,
			"active_delegation.credential_tag = 0 AND "+
				"active_delegation.staking_key IN ("+
				bindPlaceholders(end-start)+")",
		)
		if err != nil {
			return nil, err
		}
		for _, key := range ownerKeys[start:end] {
			args = append(args, key)
		}
		rows, err := db.QueryContext(context.Background(), query+`
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
		rewardsByCredential, err := historicalRewards(db, slot, selected)
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
		poolKeyHashes, slot, 0, false, expiryEpoch, inactivityPeriod, txn,
	)
}

func (s *Store) GetEpochBoundaryRewardStakeInputsForPools(
	poolKeyHashes [][]byte,
	snapshotSlot uint64,
	boundarySlot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	txn types.Txn,
) ([]*models.RewardStakeInput, error) {
	if boundarySlot <= snapshotSlot {
		boundarySlot = 0
	}
	return s.getRewardStakeInputsForPools(
		poolKeyHashes, snapshotSlot, boundarySlot, true,
		expiryEpoch, inactivityPeriod, txn,
	)
}

func (s *Store) getRewardStakeInputsForPools(
	poolKeyHashes [][]byte,
	slot uint64,
	boundarySlot uint64,
	boundaryAware bool,
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
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	poolKeyHashes = dedupeByteSlices(poolKeyHashes)
	ret := []*models.RewardStakeInput{}
	for start := 0; start < len(poolKeyHashes); start += 400 {
		end := min(start+400, len(poolKeyHashes))
		query, args, err := s.historicalStakeCTE(
			db,
			slot,
			expiryEpoch,
			inactivityPeriod,
			"active_delegation.pool_key_hash IN ("+
				bindPlaceholders(end-start)+")",
		)
		if err != nil {
			return nil, err
		}
		for _, hash := range poolKeyHashes[start:end] {
			args = append(args, hash)
		}
		rows, err := db.QueryContext(context.Background(), query+`
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
		rewardsByCredential, err := historicalRewardsAtBoundary(
			db, slot, boundarySlot, selected,
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

func (s *Store) historicalStakeCTE(
	db queryer,
	slot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	predicate string,
) (string, []any, error) {
	query, args := activeDelegationSQL(slot)
	expiryJoin := ""
	expiryPredicate := ""
	if expiryEpoch > 0 {
		if inactivityPeriod == 0 {
			return "", nil, errors.New(
				"historical stake expiry enabled with zero inactivity period",
			)
		}
		expiration, expirationArgs, err := historicalExpirationSQL(
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
		expiryJoin = `
LEFT JOIN historical_expiration expiry
  ON expiry.credential_tag = active_delegation.credential_tag
 AND expiry.staking_key = active_delegation.staking_key`
		expiryPredicate = `(expiry.expiration_epoch = 0
 OR expiry.expiration_epoch >= ? OR expiry.expiration_epoch IS NULL) AND `
	}
	query += `,
active_delegator_stake AS (
 SELECT active_delegation.pool_key_hash,
        active_delegation.credential_tag,
        active_delegation.staking_key,
        utxo.amount AS utxo_amount
 FROM active_delegation
 LEFT JOIN utxo
   ON utxo.credential_tag = active_delegation.credential_tag
  AND utxo.staking_key = active_delegation.staking_key
  AND utxo.added_slot <= ?
  AND (utxo.deleted_slot = 0 OR utxo.deleted_slot > ?)
` + expiryJoin + `
 WHERE ` + expiryPredicate + predicate + `
)`
	args = append(args, slot, slot)
	if expiryEpoch > 0 {
		args = append(args, expiryEpoch)
	}
	return query, args, nil
}

func activeDelegationSQL(slot uint64) (string, []any) {
	args := make(
		[]any,
		0,
		3*len(historicalDelegationSources)+
			3*len(historicalRegistrationSources)+2,
	)
	delegationParts := make(
		[]string,
		0,
		len(historicalDelegationSources)+1,
	)
	for _, source := range historicalDelegationSources {
		delegationParts = append(delegationParts, fmt.Sprintf(`
SELECT event.credential_tag, event.staking_key, event.pool_key_hash,
       event.added_slot, tx.block_index, certs.cert_index
FROM %[1]s event
JOIN certs ON certs.id = event.certificate_id AND certs.cert_type = ?
JOIN "transaction" tx ON tx.id = certs.transaction_id
WHERE event.added_slot <= ?`, source.table))
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
       account.added_slot, 0, 0
FROM account
WHERE account.added_slot <= ? AND account.active = TRUE
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
SELECT event.credential_tag, event.staking_key, %d AS registered, event.added_slot,
       tx.block_index, certs.cert_index
FROM %s event
JOIN certs ON certs.id = event.certificate_id AND certs.cert_type = ?
JOIN "transaction" tx ON tx.id = certs.transaction_id
WHERE event.added_slot <= ?`,
			source.registered,
			source.table,
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
       account.created_slot, 0, 0
FROM account
WHERE account.created_slot <= ?`+
		noHistorySQL("account", registrationTables))
	args = append(args, slot)
	for range registrationTables {
		args = append(args, slot)
	}
	return `
WITH delegation_events AS (` + strings.Join(delegationParts, " UNION ALL ") + `
), ranked_delegation AS (
 SELECT *, ROW_NUMBER() OVER (
   PARTITION BY credential_tag, staking_key
   ORDER BY added_slot DESC, block_index DESC, cert_index DESC
 ) rn FROM delegation_events
), latest_delegation AS (
 SELECT * FROM ranked_delegation WHERE rn = 1
), registration_events AS (` + strings.Join(registrationParts, " UNION ALL ") + `
), ranked_registration AS (
 SELECT *, ROW_NUMBER() OVER (
   PARTITION BY credential_tag, staking_key
   ORDER BY added_slot DESC, block_index DESC, cert_index DESC
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
       AND delegation.block_index > registration.block_index)
     OR (delegation.added_slot = registration.added_slot
       AND delegation.block_index = registration.block_index
       AND delegation.cert_index >= registration.cert_index))
)`, args
}

// historicalExpirationSQL reconstructs each active-delegation credential's
// CIP-0163 expiration_epoch as of slot from witness history (accountWitnessTables,
// account_reward_delta, account_withdrawal_witness), falling back to the
// mutable account.expiration_epoch only when no witness is retained at or
// before slot. This is only exact if two from-genesis nodes retain the same
// witness history for the same slot -- none of those tables is ever pruned by
// age, storage mode, or configurable retention; the only deletes are the
// rollback/lifecycle-truncate added_slot > slot statements
// (DeleteCertificatesAfterSlot and the account.go equivalents), which are
// keyed on consensus chain state, not per-node config. See ARCHITECTURE.md's
// CIP-0163 section (issue #2920) before adding any other deletion path for
// these tables.
func historicalExpirationSQL(
	db queryer,
	slot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
) (string, []any, error) {
	var value string
	err := db.QueryRowContext(context.Background(), `
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
	parts := []string{}
	for _, table := range accountWitnessTables {
		parts = append(parts, `
SELECT witness.credential_tag, witness.staking_key, witness.added_slot
FROM `+table+` witness
JOIN active_delegation active
  ON active.credential_tag = witness.credential_tag
 AND active.staking_key = witness.staking_key`)
	}
	parts = append(parts, `
SELECT witness.credential_tag, witness.staking_key, witness.added_slot
FROM account_withdrawal_witness witness
JOIN active_delegation active
  ON active.credential_tag = witness.credential_tag
 AND active.staking_key = witness.staking_key`, `
SELECT witness.credential_tag, witness.staking_key, witness.added_slot
FROM account_reward_delta witness
JOIN active_delegation active
  ON active.credential_tag = witness.credential_tag
 AND active.staking_key = witness.staking_key
WHERE witness.withdrawal = TRUE`)
	args := make([]any, 1, 5)
	args[0] = slot
	activationCTE := ""
	activationJoin := ""
	expiration := `CASE
 WHEN account.id IS NULL THEN 0
 WHEN witness_epoch.epoch_id IS NOT NULL
   THEN witness_epoch.epoch_id + ?
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
  ON activation_account.credential_tag = active.credential_tag
 AND activation_account.staking_key = active.staking_key`
		expiration = `CASE
 WHEN account.id IS NULL THEN 0
 WHEN activation_account.staking_key IS NOT NULL
  AND (witness_epoch.epoch_id IS NULL
    OR witness_epoch.epoch_id < inactivity_activation.epoch_id)
   THEN inactivity_activation.epoch_id + ?
 WHEN witness_epoch.epoch_id IS NOT NULL
   THEN witness_epoch.epoch_id + ?
 WHEN summary.latest_added_slot IS NOT NULL
   THEN COALESCE(created_epoch.epoch_id, 0) + ?
 ELSE COALESCE(account.expiration_epoch, 0) END`
	}
	return `,
account_witness_events AS (` + strings.Join(parts, " UNION ALL ") + `
), witness_summary AS (
 SELECT credential_tag, staking_key,
        MAX(CASE WHEN added_slot <= ? THEN added_slot END) historical_added_slot,
        MAX(added_slot) latest_added_slot
 FROM account_witness_events GROUP BY credential_tag, staking_key
), historical_witness_epoch AS (
 SELECT summary.credential_tag, summary.staking_key,
        MAX(epoch.epoch_id) epoch_id
 FROM witness_summary summary
 JOIN epoch ON summary.historical_added_slot >= epoch.start_slot
  AND summary.historical_added_slot < epoch.start_slot + epoch.length_in_slots
 GROUP BY summary.credential_tag, summary.staking_key
)` + activationCTE + `,
historical_expiration AS (
 SELECT active.credential_tag, active.staking_key, ` + expiration + ` expiration_epoch
 FROM active_delegation active
 LEFT JOIN account
  ON account.credential_tag = active.credential_tag
 AND account.staking_key = active.staking_key
 LEFT JOIN historical_witness_epoch witness_epoch
  ON witness_epoch.credential_tag = active.credential_tag
 AND witness_epoch.staking_key = active.staking_key
 LEFT JOIN witness_summary summary
  ON summary.credential_tag = active.credential_tag
 AND summary.staking_key = active.staking_key
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
