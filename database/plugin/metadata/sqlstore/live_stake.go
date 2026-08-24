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
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

func (s *Store) refreshRewardLiveStakeAggregate(
	ctx context.Context,
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
	accountErr := db.QueryRowContext(ctx, `
SELECT reward, pool, active, added_slot
FROM account
WHERE credential_tag = ? AND staking_key = ?`,
		ref.Tag,
		ref.Key,
	).Scan(&reward, &pool, &active, &addedSlot)
	if accountErr != nil && !errors.Is(accountErr, sql.ErrNoRows) {
		return fmt.Errorf("query reward live stake account: %w", accountErr)
	}
	utxoStake, err := sumUint64Rows(ctx, db, `
SELECT amount
FROM utxo
WHERE credential_tag = ? AND staking_key = ? AND deleted_slot = 0`,
		ref.Tag, ref.Key)
	if err != nil {
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
	utxoValue := utxoStake
	if utxoValue > ^uint64(0)-rewardStake {
		return fmt.Errorf(
			"reward live stake overflow for credential %d:%x",
			ref.Tag,
			ref.Key,
		)
	}
	total := utxoValue + rewardStake
	if errors.Is(accountErr, sql.ErrNoRows) && total == 0 {
		_, err := db.ExecContext(ctx, `
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
	_, err = db.ExecContext(ctx, `
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
		txn,
		func(db queryer, ctx context.Context) error {
			if _, err := db.ExecContext(
				ctx,
				"DELETE FROM reward_live_stake",
			); err != nil {
				return fmt.Errorf("clear reward live stake: %w", err)
			}
			// Keep amount arithmetic in Go. SQL INTEGER is signed and cannot
			// represent every valid lovelace value.
			utxoRows, err := db.QueryContext(ctx,
				`SELECT credential_tag, staking_key, amount FROM utxo
WHERE deleted_slot = 0 AND staking_key IS NOT NULL AND LENGTH(staking_key) > 0`)
			if err != nil {
				return fmt.Errorf("load reward live stake UTxOs: %w", err)
			}
			type credentialKey struct {
				tag uint8
				key string
			}
			utxoStakes := make(map[credentialKey]uint64)
			for utxoRows.Next() {
				var tag uint8
				var key []byte
				var raw sql.NullString
				if err := utxoRows.Scan(&tag, &key, &raw); err != nil {
					utxoRows.Close()
					return fmt.Errorf("scan reward live stake UTxO: %w", err)
				}
				if !raw.Valid || raw.String == "" {
					continue
				}
				amount, err := parseUint64(
					"reward live stake UTxO amount",
					raw.String,
				)
				if err != nil {
					utxoRows.Close()
					return err
				}
				ref := credentialKey{tag: tag, key: string(key)}
				if ^uint64(0)-utxoStakes[ref] < amount {
					utxoRows.Close()
					return fmt.Errorf(
						"reward live stake UTxO overflow for credential %d:%x",
						tag,
						key,
					)
				}
				utxoStakes[ref] += amount
			}
			if err := utxoRows.Err(); err != nil {
				utxoRows.Close()
				return fmt.Errorf("iterate reward live stake UTxOs: %w", err)
			}
			if err := utxoRows.Close(); err != nil {
				return fmt.Errorf("close reward live stake UTxOs: %w", err)
			}

			rows, err := db.QueryContext(ctx, `
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
       account.reward, account.active, account.added_slot,
       latest_delegation.added_slot,
       latest_delegation.block_index,
       latest_delegation.cert_index
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
			 AND latest_delegation.pool_key_hash = account.pool`,
			)
			if err != nil {
				return fmt.Errorf("load reward live stake credentials: %w", err)
			}
			// Materialize the credential cursor before issuing any upserts.  On
			// PostgreSQL a query keeps the transaction's sole connection busy
			// until its rows are closed; attempting the first INSERT while this
			// cursor is open therefore blocks/fails with a connection error.
			type rewardLiveStakeCredential struct {
				tag             uint8
				key, pool       []byte
				reward          sql.NullString
				active          sql.NullBool
				addedSlot       sql.NullInt64
				delegationSlot  sql.NullInt64
				delegationBlock sql.NullInt64
				delegationCert  sql.NullInt64
			}
			credentials := make([]rewardLiveStakeCredential, 0)
			for rows.Next() {
				var credential rewardLiveStakeCredential
				if err := rows.Scan(&credential.tag, &credential.key, &credential.pool,
					&credential.reward, &credential.active, &credential.addedSlot,
					&credential.delegationSlot, &credential.delegationBlock, &credential.delegationCert,
				); err != nil {
					_ = rows.Close()
					return fmt.Errorf(
						"scan reward live stake credential: %w",
						err,
					)
				}
				credentials = append(credentials, credential)
			}
			if err := rows.Err(); err != nil {
				_ = rows.Close()
				return fmt.Errorf(
					"iterate reward live stake credentials: %w",
					err,
				)
			}
			if err := rows.Close(); err != nil {
				return fmt.Errorf(
					"close reward live stake credentials: %w",
					err,
				)
			}
			values := make([]rewardLiveStakeRow, 0, len(credentials))
			for _, credential := range credentials {
				tag := credential.tag
				key := credential.key
				pool := credential.pool
				ref := credentialKey{tag: tag, key: string(key)}
				utxoStake := utxoStakes[ref]
				rewardStake := uint64(0)
				if credential.reward.Valid && credential.reward.String != "" {
					rewardStake, err = parseUint64(
						"reward live stake reward",
						credential.reward.String,
					)
					if err != nil {
						return err
					}
				}
				if ^uint64(0)-utxoStake < rewardStake {
					return fmt.Errorf(
						"reward live stake overflow for credential %d:%x",
						tag,
						key,
					)
				}
				total := utxoStake + rewardStake
				registered := credential.active.Valid && credential.active.Bool
				if !registered {
					pool = nil
				}
				delegSlot := int64(0)
				blockIndex := int64(0)
				certIndex := int64(0)
				if registered && len(pool) > 0 {
					if credential.delegationSlot.Valid {
						delegSlot = credential.delegationSlot.Int64
					} else if credential.addedSlot.Valid {
						delegSlot = credential.addedSlot.Int64
					}
					if credential.delegationBlock.Valid {
						blockIndex = credential.delegationBlock.Int64
					}
					if credential.delegationCert.Valid {
						certIndex = credential.delegationCert.Int64
					}
				}
				values = append(values, rewardLiveStakeRow{
					tag:             tag,
					key:             key,
					pool:            pool,
					utxoStake:       utxoStake,
					rewardStake:     rewardStake,
					totalStake:      total,
					registered:      registered,
					delegationSlot:  delegSlot,
					delegationBlock: blockIndex,
					delegationCert:  certIndex,
				})
			}
			if err := s.insertRewardLiveStakeRows(ctx, db, values, slotValue); err != nil {
				return err
			}
			return nil
		},
	)
}

// rewardLiveStakeRow is the materialized form of one canonical credential
// used by RebuildRewardLiveStake. Keeping this separate from the scan row
// allows the rebuild to issue bounded multi-row upserts instead of one round
// trip per credential.
type rewardLiveStakeRow struct {
	tag                             uint8
	key, pool                       []byte
	utxoStake, rewardStake          uint64
	totalStake                      uint64
	registered                      bool
	delegationSlot, delegationBlock int64
	delegationCert                  int64
}

func (s *Store) insertRewardLiveStakeRows(
	ctx context.Context,
	db queryer,
	rows []rewardLiveStakeRow,
	updatedSlot int64,
) error {
	if len(rows) == 0 {
		return nil
	}
	const columns = `credential_tag, staking_key, pool_key_hash,
 utxo_stake, reward_stake, total_stake, registered, pool_delegation_slot,
 pool_delegation_block_index, pool_delegation_cert_index, updated_slot,
 calculation_version`
	const valuesPerRow = 12
	chunkSize := max(1, s.dialect.ParameterLimit()/valuesPerRow)
	for start := 0; start < len(rows); start += chunkSize {
		end := min(start+chunkSize, len(rows))
		placeholders := make([]string, end-start)
		args := make([]any, 0, (end-start)*valuesPerRow)
		for index, row := range rows[start:end] {
			placeholders[index] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			args = append(args,
				row.tag,
				row.key,
				row.pool,
				decimalUint64(types.Uint64(row.utxoStake)),
				decimalUint64(types.Uint64(row.rewardStake)),
				decimalUint64(types.Uint64(row.totalStake)),
				row.registered,
				row.delegationSlot,
				row.delegationBlock,
				row.delegationCert,
				updatedSlot,
				models.RewardStakeCalculationVersion,
			)
		}
		query := `INSERT INTO reward_live_stake (` + columns + `)
VALUES ` + strings.Join(placeholders, ", ") + `
ON CONFLICT (credential_tag, staking_key) DO UPDATE SET
 pool_key_hash = excluded.pool_key_hash, utxo_stake = excluded.utxo_stake,
 reward_stake = excluded.reward_stake, total_stake = excluded.total_stake,
 registered = excluded.registered, pool_delegation_slot = excluded.pool_delegation_slot,
 pool_delegation_block_index = excluded.pool_delegation_block_index,
 pool_delegation_cert_index = excluded.pool_delegation_cert_index,
 updated_slot = excluded.updated_slot, calculation_version = excluded.calculation_version`
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("populate reward live stake: %w", err)
		}
	}
	return nil
}

func (s *Store) RewardLiveStakeNeedsBackfill(
	txn types.Txn,
) (bool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return false, fmt.Errorf(
			"reward live stake needs backfill: resolve db: %w",
			err,
		)
	}
	type credentialKey struct {
		tag uint8
		key string
	}
	utxoStakes := make(map[credentialKey]uint64)
	utxoRows, err := db.QueryContext(
		ctx,
		`SELECT credential_tag, staking_key, amount FROM utxo WHERE deleted_slot = 0 AND staking_key IS NOT NULL AND LENGTH(staking_key) > 0`,
	)
	if err != nil {
		return false, fmt.Errorf("load reward live stake UTxOs: %w", err)
	}
	for utxoRows.Next() {
		var tag uint8
		var key []byte
		var raw sql.NullString
		if err := utxoRows.Scan(&tag, &key, &raw); err != nil {
			utxoRows.Close()
			return false, err
		}
		if !raw.Valid || raw.String == "" {
			continue
		}
		value, err := parseUint64("reward live stake UTxO amount", raw.String)
		if err != nil {
			utxoRows.Close()
			return false, err
		}
		ref := credentialKey{tag: tag, key: string(key)}
		if ^uint64(0)-utxoStakes[ref] < value {
			utxoRows.Close()
			return false, errors.New("reward live stake UTxO overflow")
		}
		utxoStakes[ref] += value
	}
	if err := utxoRows.Err(); err != nil {
		utxoRows.Close()
		return false, err
	}
	if err := utxoRows.Close(); err != nil {
		return false, err
	}
	rows, err := db.QueryContext(ctx, `
SELECT canonical.credential_tag, canonical.staking_key,
       account.reward, account.active, account.pool,
       reward_live_stake.id, reward_live_stake.calculation_version,
       reward_live_stake.utxo_stake, reward_live_stake.reward_stake,
       reward_live_stake.total_stake, reward_live_stake.registered,
       reward_live_stake.pool_key_hash
FROM (
  SELECT credential_tag, staking_key FROM account WHERE LENGTH(staking_key) > 0
  UNION SELECT credential_tag, staking_key FROM utxo
    WHERE deleted_slot = 0 AND staking_key IS NOT NULL AND LENGTH(staking_key) > 0
) canonical
LEFT JOIN account ON account.credential_tag = canonical.credential_tag
 AND account.staking_key = canonical.staking_key
LEFT JOIN reward_live_stake ON reward_live_stake.credential_tag = canonical.credential_tag
 AND reward_live_stake.staking_key = canonical.staking_key`)
	if err != nil {
		return false, fmt.Errorf("load reward live stake consistency: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tag uint8
		var key, pool, storedPool []byte
		var reward, storedUtxo, storedReward, storedTotal sql.NullString
		var active, registered sql.NullBool
		var id, version sql.NullInt64
		if err := rows.Scan(&tag, &key, &reward, &active, &pool, &id, &version, &storedUtxo, &storedReward, &storedTotal, &registered, &storedPool); err != nil {
			return false, err
		}
		ref := credentialKey{tag: tag, key: string(key)}
		utxoStake := utxoStakes[ref]
		rewardStake := uint64(0)
		if reward.Valid && reward.String != "" {
			rewardStake, err = parseUint64(
				"reward live stake reward",
				reward.String,
			)
			if err != nil {
				return false, err
			}
		}
		if ^uint64(0)-utxoStake < rewardStake {
			return false, errors.New("reward live stake overflow")
		}
		total := utxoStake + rewardStake
		if !id.Valid || !version.Valid ||
			uint64(
				version.Int64,
			) != uint64(
				models.RewardStakeCalculationVersion,
			) ||
			!storedUtxo.Valid ||
			!storedReward.Valid ||
			!storedTotal.Valid {
			return true, nil
		}
		storedUtxoValue, err := parseUint64(
			"stored UTxO stake",
			storedUtxo.String,
		)
		if err != nil {
			return false, err
		}
		storedRewardValue, err := parseUint64(
			"stored reward stake",
			storedReward.String,
		)
		if err != nil {
			return false, err
		}
		storedTotalValue, err := parseUint64(
			"stored total stake",
			storedTotal.String,
		)
		if err != nil {
			return false, err
		}
		if storedUtxoValue != utxoStake || storedRewardValue != rewardStake ||
			storedTotalValue != total ||
			registered.Bool != active.Bool {
			return true, nil
		}
		if active.Bool && len(pool) > 0 && string(pool) != string(storedPool) {
			return true, nil
		}
		if (!active.Bool || len(pool) == 0) && len(storedPool) > 0 {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	var orphan bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM reward_live_stake r WHERE NOT EXISTS (SELECT 1 FROM account a WHERE a.credential_tag = r.credential_tag AND a.staking_key = r.staking_key) AND NOT EXISTS (SELECT 1 FROM utxo u WHERE u.credential_tag = r.credential_tag AND u.staking_key = r.staking_key AND u.deleted_slot = 0))`).Scan(&orphan); err != nil {
		return false, err
	}
	return orphan, nil
}

func (s *Store) StaleConsensusStakeSnapshotsExist(
	txn types.Txn,
) (bool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return false, fmt.Errorf(
			"stale consensus stake snapshots: resolve db: %w",
			err,
		)
	}
	var stale bool
	err = db.QueryRowContext(ctx, `
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
	db, ctx, err := s.readDBFromTxn(txn)
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
			ctx,
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
