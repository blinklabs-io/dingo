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

//nolint:gosec,sqlclosecheck // SQL INTEGER mappings preserve the unsigned domain API; cursors are explicitly closed before dependent queries.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

func (s *Store) refreshRewardLiveStakeAggregate(
	db queryer,
	ref models.StakeCredentialRef,
	slot uint64,
) error {
	if len(ref.Key) == 0 {
		return nil
	}
	var reward sql.NullString
	var pool []byte
	var active sql.NullBool
	var addedSlot sql.NullInt64
	accountErr := db.QueryRowContext(context.Background(), `
SELECT reward, pool, active, added_slot
FROM account
WHERE credential_tag = ? AND staking_key = ?`,
		ref.Tag,
		ref.Key,
	).Scan(&reward, &pool, &active, &addedSlot)
	if accountErr != nil && !errors.Is(accountErr, sql.ErrNoRows) {
		return fmt.Errorf("query reward live stake account: %w", accountErr)
	}
	var utxoStake int64
	if err := db.QueryRowContext(context.Background(), `
SELECT CAST(COALESCE(SUM(CAST(amount AS INTEGER)), 0) AS INTEGER)
FROM utxo
WHERE credential_tag = ? AND staking_key = ? AND deleted_slot = 0`,
		ref.Tag,
		ref.Key,
	).Scan(&utxoStake); err != nil {
		return fmt.Errorf("sum reward live stake UTxOs: %w", err)
	}
	rewardStake := uint64(0)
	registered := false
	if accountErr == nil {
		registered = active.Bool
		if reward.Valid {
			value, err := parseUint64("reward live stake reward", reward.String)
			if err != nil {
				return err
			}
			rewardStake = value
		}
	}
	utxoValue := uint64(utxoStake)
	if utxoValue > ^uint64(0)-rewardStake {
		return fmt.Errorf(
			"reward live stake overflow for credential %d:%x",
			ref.Tag,
			ref.Key,
		)
	}
	total := utxoValue + rewardStake
	if errors.Is(accountErr, sql.ErrNoRows) && total == 0 {
		_, err := db.ExecContext(context.Background(), `
DELETE FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`,
			ref.Tag,
			ref.Key,
		)
		return err
	}
	if !registered {
		pool = nil
	}
	delegationSlot := int64(0)
	if registered && len(pool) > 0 {
		delegationSlot = addedSlot.Int64
	}
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(context.Background(), `
INSERT INTO reward_live_stake (
    credential_tag, staking_key, pool_key_hash, utxo_stake, reward_stake,
    total_stake, registered, pool_delegation_slot,
    pool_delegation_block_index, pool_delegation_cert_index, updated_slot,
    calculation_version
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
ON CONFLICT (credential_tag, staking_key) DO UPDATE SET
    pool_key_hash = excluded.pool_key_hash,
    utxo_stake = excluded.utxo_stake,
    reward_stake = excluded.reward_stake,
    total_stake = excluded.total_stake,
    registered = excluded.registered,
    pool_delegation_slot = excluded.pool_delegation_slot,
    pool_delegation_block_index = excluded.pool_delegation_block_index,
    pool_delegation_cert_index = excluded.pool_delegation_cert_index,
    updated_slot = excluded.updated_slot,
    calculation_version = excluded.calculation_version`,
		ref.Tag,
		ref.Key,
		pool,
		decimalUint64(types.Uint64(utxoValue)),
		decimalUint64(types.Uint64(rewardStake)),
		decimalUint64(types.Uint64(total)),
		registered,
		delegationSlot,
		slotValue,
		models.RewardStakeCalculationVersion,
	)
	if err != nil {
		return fmt.Errorf("upsert reward live stake: %w", err)
	}
	return nil
}

func (s *Store) RebuildRewardLiveStake(
	slot uint64,
	txn types.Txn,
) error {
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	return s.withWriteTransaction(
		context.Background(),
		txn,
		func(db queryer) error {
			var invalid bool
			if err := db.QueryRowContext(context.Background(), `
SELECT EXISTS (
    SELECT 1 FROM account
    WHERE staking_key IS NULL OR LENGTH(staking_key) = 0
)`).Scan(&invalid); err != nil {
				return err
			}
			if invalid {
				return errors.New(
					"reward live stake source contains a null or empty account credential",
				)
			}
			if _, err := db.ExecContext(
				context.Background(),
				"DELETE FROM reward_live_stake",
			); err != nil {
				return fmt.Errorf("clear reward live stake: %w", err)
			}
			_, err := db.ExecContext(context.Background(), `
INSERT INTO reward_live_stake (
    credential_tag, staking_key, pool_key_hash, utxo_stake, reward_stake,
    total_stake, registered, pool_delegation_slot,
    pool_delegation_block_index, pool_delegation_cert_index, updated_slot,
    calculation_version
)
WITH latest_delegation AS (
    SELECT credential_tag, staking_key, pool_key_hash, added_slot,
           block_index, cert_index
    FROM (
        SELECT delegation.*,
               ROW_NUMBER() OVER (
                   PARTITION BY credential_tag, staking_key
                   ORDER BY added_slot DESC, block_index DESC, cert_index DESC
               ) AS rn
        FROM (
            SELECT sd.credential_tag, sd.staking_key, sd.pool_key_hash,
                   sd.added_slot, COALESCE(tx.block_index, 0) AS block_index,
                   COALESCE(c.cert_index, 0) AS cert_index
            FROM stake_delegation sd
            LEFT JOIN certs c ON c.id = sd.certificate_id
            LEFT JOIN "transaction" tx ON tx.id = c.transaction_id
            UNION ALL
            SELECT srd.credential_tag, srd.staking_key, srd.pool_key_hash,
                   srd.added_slot, COALESCE(tx.block_index, 0),
                   COALESCE(c.cert_index, 0)
            FROM stake_registration_delegation srd
            LEFT JOIN certs c ON c.id = srd.certificate_id
            LEFT JOIN "transaction" tx ON tx.id = c.transaction_id
            UNION ALL
            SELECT svd.credential_tag, svd.staking_key, svd.pool_key_hash,
                   svd.added_slot, COALESCE(tx.block_index, 0),
                   COALESCE(c.cert_index, 0)
            FROM stake_vote_delegation svd
            LEFT JOIN certs c ON c.id = svd.certificate_id
            LEFT JOIN "transaction" tx ON tx.id = c.transaction_id
            UNION ALL
            SELECT svrd.credential_tag, svrd.staking_key, svrd.pool_key_hash,
                   svrd.added_slot, COALESCE(tx.block_index, 0),
                   COALESCE(c.cert_index, 0)
            FROM stake_vote_registration_delegation svrd
            LEFT JOIN certs c ON c.id = svrd.certificate_id
            LEFT JOIN "transaction" tx ON tx.id = c.transaction_id
        ) delegation
    ) ranked_delegation
    WHERE rn = 1
)
SELECT creds.credential_tag, creds.staking_key,
       CASE WHEN account.active = TRUE THEN account.pool ELSE NULL END,
       COALESCE(SUM(CAST(utxo.amount AS INTEGER)), 0),
       COALESCE(CAST(account.reward AS INTEGER), 0),
       COALESCE(SUM(CAST(utxo.amount AS INTEGER)), 0)
           + COALESCE(CAST(account.reward AS INTEGER), 0),
       COALESCE(account.active, FALSE),
       CASE WHEN account.active = TRUE AND LENGTH(account.pool) > 0
            THEN COALESCE(latest_delegation.added_slot, account.added_slot, 0)
            ELSE 0 END,
       CASE WHEN account.active = TRUE AND LENGTH(account.pool) > 0
            THEN COALESCE(latest_delegation.block_index, 0) ELSE 0 END,
       CASE WHEN account.active = TRUE AND LENGTH(account.pool) > 0
            THEN COALESCE(latest_delegation.cert_index, 0) ELSE 0 END,
       ?,
       ?
FROM (
    SELECT credential_tag, staking_key FROM account
    UNION
    SELECT credential_tag, staking_key FROM utxo
    WHERE deleted_slot = 0
      AND staking_key IS NOT NULL
      AND LENGTH(staking_key) > 0
) creds
LEFT JOIN account
  ON account.credential_tag = creds.credential_tag
 AND account.staking_key = creds.staking_key
LEFT JOIN latest_delegation
  ON latest_delegation.credential_tag = account.credential_tag
 AND latest_delegation.staking_key = account.staking_key
 AND latest_delegation.pool_key_hash = account.pool
LEFT JOIN utxo
  ON utxo.credential_tag = creds.credential_tag
 AND utxo.staking_key = creds.staking_key
 AND utxo.deleted_slot = 0
GROUP BY creds.credential_tag, creds.staking_key, account.credential_tag,
         account.staking_key, account.pool, account.reward, account.active,
         account.added_slot, latest_delegation.added_slot,
         latest_delegation.block_index, latest_delegation.cert_index`,
				slotValue,
				models.RewardStakeCalculationVersion,
			)
			if err != nil {
				return fmt.Errorf("populate reward live stake: %w", err)
			}
			return nil
		},
	)
}

func (s *Store) RewardLiveStakeNeedsBackfill(
	txn types.Txn,
) (bool, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return false, fmt.Errorf(
			"reward live stake needs backfill: resolve db: %w",
			err,
		)
	}
	var stale bool
	err = db.QueryRowContext(context.Background(), `
SELECT EXISTS (
    SELECT 1
    FROM (
        SELECT credential_tag, staking_key FROM account
        WHERE LENGTH(staking_key) > 0
        UNION
        SELECT credential_tag, staking_key FROM utxo
        WHERE deleted_slot = 0 AND LENGTH(staking_key) > 0
    ) canonical_credentials
    LEFT JOIN account
      ON account.credential_tag = canonical_credentials.credential_tag
     AND account.staking_key = canonical_credentials.staking_key
    LEFT JOIN utxo
      ON utxo.credential_tag = canonical_credentials.credential_tag
     AND utxo.staking_key = canonical_credentials.staking_key
     AND utxo.deleted_slot = 0
    LEFT JOIN reward_live_stake
      ON reward_live_stake.credential_tag =
             canonical_credentials.credential_tag
     AND reward_live_stake.staking_key =
             canonical_credentials.staking_key
    GROUP BY canonical_credentials.credential_tag,
             canonical_credentials.staking_key,
             account.pool, account.reward, account.active,
             reward_live_stake.id,
             reward_live_stake.calculation_version,
             reward_live_stake.utxo_stake,
             reward_live_stake.reward_stake,
             reward_live_stake.total_stake,
             reward_live_stake.registered,
             reward_live_stake.pool_key_hash
    HAVING reward_live_stake.id IS NULL
        OR reward_live_stake.calculation_version <> ?
        OR CAST(reward_live_stake.utxo_stake AS INTEGER) <>
           COALESCE(SUM(CAST(utxo.amount AS INTEGER)), 0)
        OR CAST(reward_live_stake.reward_stake AS INTEGER) <>
           COALESCE(CAST(account.reward AS INTEGER), 0)
        OR CAST(reward_live_stake.total_stake AS INTEGER) <>
           COALESCE(SUM(CAST(utxo.amount AS INTEGER)), 0)
             + COALESCE(CAST(account.reward AS INTEGER), 0)
        OR reward_live_stake.registered <> COALESCE(account.active, FALSE)
        OR (
            COALESCE(account.active, FALSE) = TRUE
            AND LENGTH(COALESCE(account.pool, '')) > 0
            AND account.pool <> reward_live_stake.pool_key_hash
        )
        OR (
            (COALESCE(account.active, FALSE) = FALSE
             OR LENGTH(COALESCE(account.pool, '')) = 0)
            AND reward_live_stake.pool_key_hash IS NOT NULL
        )
    LIMIT 1
)
OR EXISTS (
    SELECT 1
    FROM reward_live_stake
    WHERE NOT EXISTS (
        SELECT 1 FROM account
        WHERE account.credential_tag = reward_live_stake.credential_tag
          AND account.staking_key = reward_live_stake.staking_key
    )
      AND NOT EXISTS (
        SELECT 1 FROM utxo
        WHERE utxo.credential_tag = reward_live_stake.credential_tag
          AND utxo.staking_key = reward_live_stake.staking_key
          AND utxo.deleted_slot = 0
    )
)`,
		models.RewardStakeCalculationVersion,
	).Scan(&stale)
	if err != nil {
		return false, fmt.Errorf(
			"checking reward live stake consistency: %w",
			err,
		)
	}
	return stale, nil
}

func (s *Store) StaleConsensusStakeSnapshotsExist(
	txn types.Txn,
) (bool, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return false, fmt.Errorf(
			"stale consensus stake snapshots: resolve db: %w",
			err,
		)
	}
	var stale bool
	err = db.QueryRowContext(context.Background(), `
SELECT EXISTS (
    SELECT 1 FROM pool_stake_snapshot
    WHERE snapshot_type IN ('mark', 'set', 'go')
      AND calculation_version <> ?
) OR EXISTS (
    SELECT 1 FROM reward_snapshot
    WHERE snapshot_type = 'mark'
      AND authoritative = TRUE
      AND calculation_version <> ?
)`,
		models.RewardStakeCalculationVersion,
		models.RewardStakeCalculationVersion,
	).Scan(&stale)
	if err != nil {
		return false, fmt.Errorf(
			"checking stake snapshot provenance: %w",
			err,
		)
	}
	return stale, nil
}

func (s *Store) GetLiveStakeInputsForPools(
	poolKeyHashes [][]byte,
	expiryEpoch uint64,
	txn types.Txn,
) ([]*models.RewardStakeInput, error) {
	if len(poolKeyHashes) == 0 {
		return nil, nil
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetLiveStakeInputsForPools: resolve db: %w",
			err,
		)
	}
	poolKeyHashes = dedupeByteSlices(poolKeyHashes)
	chunkSize := s.dialect.ParameterLimit()
	if expiryEpoch > 0 {
		chunkSize--
	}
	ret := make([]*models.RewardStakeInput, 0)
	for start := 0; start < len(poolKeyHashes); start += chunkSize {
		end := min(start+chunkSize, len(poolKeyHashes))
		chunk := poolKeyHashes[start:end]
		args := make([]any, 0, len(chunk)+1)
		for i := range chunk {
			args = append(args, chunk[i])
		}
		join := ""
		expiry := ""
		if expiryEpoch > 0 {
			join = `
LEFT JOIN account acct
  ON acct.credential_tag = rls.credential_tag
 AND acct.staking_key = rls.staking_key`
			expiry = `
  AND (acct.expiration_epoch = 0
       OR acct.expiration_epoch >= ?
       OR acct.expiration_epoch IS NULL)`
			args = append(args, expiryEpoch)
		}
		query := `
SELECT rls.pool_key_hash, rls.staking_key, rls.credential_tag,
       rls.total_stake
FROM reward_live_stake rls` + join + `
WHERE rls.pool_key_hash IN (` + bindPlaceholders(len(chunk)) + `)
  AND rls.registered = TRUE` + expiry + `
ORDER BY rls.pool_key_hash ASC, rls.credential_tag ASC,
         rls.staking_key ASC`
		rows, err := db.QueryContext(
			context.Background(),
			s.dialect.Rebind(query),
			args...,
		)
		if err != nil {
			return nil, fmt.Errorf("GetLiveStakeInputsForPools: %w", err)
		}
		for rows.Next() {
			var item models.RewardStakeInput
			var credentialTag int64
			var stake string
			if err := rows.Scan(
				&item.PoolKeyHash,
				&item.StakingKey,
				&credentialTag,
				&stake,
			); err != nil {
				rows.Close()
				return nil, err
			}
			value, err := parseUint64("live stake", stake)
			if err != nil {
				rows.Close()
				return nil, err
			}
			item.CredentialTag = uint8(credentialTag)
			item.Stake = types.Uint64(value)
			item.Registered = true
			ret = append(ret, &item)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func dedupeByteSlices(values [][]byte) [][]byte {
	if len(values) <= 1 {
		return values
	}
	ret := make([][]byte, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for i := range values {
		key := string(values[i])
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ret = append(ret, values[i])
	}
	return ret
}
