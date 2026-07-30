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
SELECT pool_key_hash, COUNT(*), COALESCE(SUM(total_stake), 0)
FROM active_delegator_stake GROUP BY pool_key_hash`,
			args...,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("query historical stake: %w", err)
		}
		for rows.Next() {
			var hash []byte
			var count uint64
			var stake uint64
			if err := rows.Scan(&hash, &count, &stake); err != nil {
				rows.Close()
				return nil, nil, err
			}
			delegators[string(hash)] = count
			stakes[string(hash)] = stake
		}
		if err := rows.Close(); err != nil {
			return nil, nil, err
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
SELECT pool_key_hash, staking_key, total_stake
FROM active_delegator_stake`,
			args...,
		)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var pool []byte
			var key []byte
			var stake uint64
			if err := rows.Scan(&pool, &key, &stake); err != nil {
				rows.Close()
				return nil, err
			}
			ret[types.PoolCredentialStakeKey(pool, 0, key)] = stake
		}
		if err := rows.Close(); err != nil {
			return nil, err
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
	if expiryEpoch == 0 {
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
SELECT pool_key_hash, credential_tag, staking_key, total_stake
FROM active_delegator_stake
WHERE total_stake > 0
ORDER BY pool_key_hash, credential_tag, staking_key`,
			args...,
		)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var input models.RewardStakeInput
			var stake uint64
			if err := rows.Scan(
				&input.PoolKeyHash,
				&input.CredentialTag,
				&input.StakingKey,
				&stake,
			); err != nil {
				rows.Close()
				return nil, err
			}
			input.Stake = types.Uint64(stake)
			input.Registered = true
			ret = append(ret, &input)
		}
		if err := rows.Close(); err != nil {
			return nil, err
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
ranked_future_withdrawal AS (
 SELECT credential_tag, staking_key, id, added_slot,
        CAST(previous_reward AS INTEGER) previous_reward,
        ROW_NUMBER() OVER (
          PARTITION BY credential_tag, staking_key
          ORDER BY added_slot, id
        ) event_order
 FROM account_reward_delta
 WHERE withdrawal = TRUE AND added_slot > ?
),
first_future_withdrawal AS (
 SELECT credential_tag, staking_key, id, added_slot, previous_reward
 FROM ranked_future_withdrawal WHERE event_order = 1
),
future_credit AS (
 SELECT credit.credential_tag, credit.staking_key,
        COALESCE(SUM(CAST(credit.amount AS INTEGER)), 0) total,
        COALESCE(SUM(CASE
          WHEN withdrawal.id IS NOT NULL
           AND (credit.added_slot < withdrawal.added_slot
             OR (credit.added_slot = withdrawal.added_slot
               AND credit.id < withdrawal.id))
          THEN CAST(credit.amount AS INTEGER) ELSE 0 END), 0)
          before_first_withdrawal
 FROM account_reward_delta credit
 LEFT JOIN first_future_withdrawal withdrawal
   ON withdrawal.credential_tag = credit.credential_tag
  AND withdrawal.staking_key = credit.staking_key
 WHERE credit.withdrawal = FALSE AND credit.added_slot > ?
 GROUP BY credit.credential_tag, credit.staking_key
),
historical_reward AS (
 SELECT active_delegation.credential_tag, active_delegation.staking_key,
        CASE WHEN withdrawal.id IS NOT NULL
          THEN withdrawal.previous_reward
             - COALESCE(credit.before_first_withdrawal, 0)
          ELSE COALESCE(CAST(account.reward AS INTEGER), 0)
             - COALESCE(credit.total, 0) END reward
 FROM active_delegation
 LEFT JOIN account
   ON account.credential_tag = active_delegation.credential_tag
  AND account.staking_key = active_delegation.staking_key
 LEFT JOIN first_future_withdrawal withdrawal
   ON withdrawal.credential_tag = active_delegation.credential_tag
  AND withdrawal.staking_key = active_delegation.staking_key
 LEFT JOIN future_credit credit
   ON credit.credential_tag = active_delegation.credential_tag
  AND credit.staking_key = active_delegation.staking_key
),
active_delegator_stake AS (
 SELECT active_delegation.pool_key_hash,
        active_delegation.credential_tag,
        active_delegation.staking_key,
        COALESCE(SUM(CAST(utxo.amount AS INTEGER)), 0)
          + COALESCE(MAX(historical_reward.reward), 0) total_stake
 FROM active_delegation
 LEFT JOIN utxo
   ON utxo.credential_tag = active_delegation.credential_tag
  AND utxo.staking_key = active_delegation.staking_key
  AND utxo.added_slot <= ?
  AND (utxo.deleted_slot = 0 OR utxo.deleted_slot > ?)
 LEFT JOIN historical_reward
   ON historical_reward.credential_tag = active_delegation.credential_tag
  AND historical_reward.staking_key = active_delegation.staking_key
` + expiryJoin + `
 WHERE ` + expiryPredicate + predicate + `
 GROUP BY active_delegation.pool_key_hash,
          active_delegation.credential_tag,
          active_delegation.staking_key
)`
	args = append(args, slot, slot, slot, slot)
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
       account.added_slot, 0, 0
FROM account
WHERE account.added_slot <= ?`+
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
