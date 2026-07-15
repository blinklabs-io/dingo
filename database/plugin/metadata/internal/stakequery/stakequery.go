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

package stakequery

import (
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/sqldialect"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

type delegationSource struct {
	table    string
	certType uint
}

type registrationSource struct {
	table      string
	certType   uint
	registered int
}

const (
	defaultPoolQueryChunkSize = 800
	largePoolQueryChunkSize   = 5000
)

// GetStakeByPoolsAtSlot returns delegated stake and delegator counts at a
// historical slot. It uses certificate history to find each credential's latest
// stake delegation and registration state, and account rows as synthetic state
// for imported/bootstrap data where the certificate history is unavailable.
func GetStakeByPoolsAtSlot(
	db *gorm.DB,
	poolKeyHashes [][]byte,
	slot uint64,
) (map[string]uint64, map[string]uint64, error) {
	stakeMap := make(map[string]uint64, len(poolKeyHashes))
	delegatorMap := make(map[string]uint64, len(poolKeyHashes))
	for _, hash := range poolKeyHashes {
		stakeMap[string(hash)] = 0
		delegatorMap[string(hash)] = 0
	}
	if len(poolKeyHashes) == 0 {
		return stakeMap, delegatorMap, nil
	}

	query, args := historicalDelegatorStakeCTE(
		db, slot, "active_delegation.pool_key_hash IN ?",
	)
	query += `
SELECT pool_key_hash,
	COUNT(*) AS delegator_count,
	COALESCE(SUM(total_stake), 0) AS total_stake
FROM active_delegator_stake
GROUP BY pool_key_hash`

	type stakeResult struct {
		PoolKeyHash    []byte `gorm:"column:pool_key_hash"`
		DelegatorCount uint64 `gorm:"column:delegator_count"`
		TotalStake     uint64 `gorm:"column:total_stake"`
	}
	var results []stakeResult
	chunkSize := poolQueryChunkSize(db)
	for start := 0; start < len(poolKeyHashes); start += chunkSize {
		end := min(start+chunkSize, len(poolKeyHashes))
		chunk := poolKeyHashes[start:end]
		queryArgs := append(append([]any(nil), args...), chunk)
		var chunkResults []stakeResult
		if err := db.Raw(query, queryArgs...).Scan(&chunkResults).Error; err != nil {
			return nil, nil, fmt.Errorf(
				"query historical stake: %w",
				err,
			)
		}
		results = append(results, chunkResults...)
	}
	for _, row := range results {
		delegatorMap[string(row.PoolKeyHash)] = row.DelegatorCount
		stakeMap[string(row.PoolKeyHash)] = row.TotalStake
	}

	return stakeMap, delegatorMap, nil
}

func historicalDelegatorStakeCTE(
	db *gorm.DB,
	slot uint64,
	predicate string,
) (string, []any) {
	cte, args := activeDelegationCTE(db, slot)
	query := cte + fmt.Sprintf(`,
ranked_future_withdrawal AS (
	SELECT withdrawal.credential_tag,
		withdrawal.staking_key,
		withdrawal.id,
		withdrawal.added_slot,
		CAST(withdrawal.previous_reward AS %[1]s) AS previous_reward,
		ROW_NUMBER() OVER (
			PARTITION BY withdrawal.credential_tag, withdrawal.staking_key
			ORDER BY withdrawal.added_slot, withdrawal.id
		) AS event_order
	FROM account_reward_delta withdrawal
	WHERE withdrawal.withdrawal = TRUE
		AND withdrawal.added_slot > ?
),
first_future_withdrawal AS (
	SELECT credential_tag,
		staking_key,
		id,
		added_slot,
		previous_reward
	FROM ranked_future_withdrawal
	WHERE event_order = 1
),
future_credit AS (
	SELECT credit.credential_tag,
		credit.staking_key,
		COALESCE(SUM(CAST(credit.amount AS %[1]s)), 0) AS total,
		COALESCE(SUM(CASE
			WHEN first_future_withdrawal.id IS NOT NULL
				AND (credit.added_slot < first_future_withdrawal.added_slot
					OR (credit.added_slot = first_future_withdrawal.added_slot
						AND credit.id < first_future_withdrawal.id))
			THEN CAST(credit.amount AS %[1]s)
			ELSE 0
		END), 0) AS before_first_withdrawal
	FROM account_reward_delta credit
	LEFT JOIN first_future_withdrawal
		ON first_future_withdrawal.credential_tag = credit.credential_tag
		AND first_future_withdrawal.staking_key = credit.staking_key
	WHERE credit.withdrawal = FALSE
		AND credit.added_slot > ?
	GROUP BY credit.credential_tag,
		credit.staking_key
),
historical_reward AS (
	SELECT active_delegation.credential_tag,
		active_delegation.staking_key,
		CASE
			WHEN first_future_withdrawal.id IS NOT NULL THEN
				first_future_withdrawal.previous_reward
					- COALESCE(future_credit.before_first_withdrawal, 0)
			ELSE COALESCE(CAST(account.reward AS %[1]s), 0)
				- COALESCE(future_credit.total, 0)
		END AS reward
	FROM active_delegation
	LEFT JOIN account
		ON account.credential_tag = active_delegation.credential_tag
		AND account.staking_key = active_delegation.staking_key
	LEFT JOIN first_future_withdrawal
		ON first_future_withdrawal.credential_tag = active_delegation.credential_tag
		AND first_future_withdrawal.staking_key = active_delegation.staking_key
	LEFT JOIN future_credit
		ON future_credit.credential_tag = active_delegation.credential_tag
		AND future_credit.staking_key = active_delegation.staking_key
),
active_delegator_stake AS (
	SELECT active_delegation.pool_key_hash,
		active_delegation.credential_tag,
		active_delegation.staking_key,
		COALESCE(SUM(CAST(utxo.amount AS %[1]s)), 0)
			+ COALESCE(MAX(historical_reward.reward), 0) AS total_stake
	FROM active_delegation
	LEFT JOIN utxo
		ON utxo.credential_tag = active_delegation.credential_tag
		AND utxo.staking_key = active_delegation.staking_key
		AND utxo.added_slot <= ?
		AND (utxo.deleted_slot = 0 OR utxo.deleted_slot > ?)
	LEFT JOIN historical_reward
		ON historical_reward.credential_tag = active_delegation.credential_tag
		AND historical_reward.staking_key = active_delegation.staking_key
	WHERE %[2]s
	GROUP BY active_delegation.pool_key_hash,
		active_delegation.credential_tag,
		active_delegation.staking_key
)`, utxoAmountCastType(db), predicate)
	args = append(args, slot, slot, slot, slot)
	return query, args
}

// GetPoolOwnerStakeAtSlot returns stake only for the requested key-hash owner
// credentials, and only under the pool to which each credential was delegated
// at slot. The result cardinality is bounded by the owner set, not by all pool
// delegators.
func GetPoolOwnerStakeAtSlot(
	db *gorm.DB,
	ownerKeys [][]byte,
	slot uint64,
) (map[string]uint64, error) {
	out := make(map[string]uint64)
	if len(ownerKeys) == 0 {
		return out, nil
	}
	query, args := historicalDelegatorStakeCTE(
		db, slot,
		"active_delegation.credential_tag = 0 "+
			"AND active_delegation.staking_key IN ?",
	)
	query += `
SELECT pool_key_hash, staking_key, total_stake
FROM active_delegator_stake`

	type ownerStakeRow struct {
		PoolKeyHash []byte `gorm:"column:pool_key_hash"`
		StakingKey  []byte `gorm:"column:staking_key"`
		TotalStake  uint64 `gorm:"column:total_stake"`
	}
	chunkSize := poolQueryChunkSize(db)
	for start := 0; start < len(ownerKeys); start += chunkSize {
		end := min(start+chunkSize, len(ownerKeys))
		queryArgs := append(append([]any(nil), args...), ownerKeys[start:end])
		var rows []ownerStakeRow
		if err := db.Raw(query, queryArgs...).Scan(&rows).Error; err != nil {
			return nil, fmt.Errorf("query historical pool-owner stake: %w", err)
		}
		for _, row := range rows {
			out[types.PoolCredentialStakeKey(
				row.PoolKeyHash, 0, row.StakingKey,
			)] = row.TotalStake
		}
	}
	return out, nil
}

func poolQueryChunkSize(db *gorm.DB) int {
	if db != nil {
		name := db.Name()
		if strings.EqualFold(name, "postgres") ||
			strings.EqualFold(name, "mysql") {
			return largePoolQueryChunkSize
		}
	}
	return defaultPoolQueryChunkSize
}

func activeDelegationCTE(db *gorm.DB, slot uint64) (string, []any) {
	txTable := transactionTableName(db)
	args := make([]any, 0, 32)
	delegationSrcs := delegationSources()
	registrationSrcs := registrationSources()

	delegationParts := make([]string, 0, len(delegationSrcs)+1)
	for _, source := range delegationSrcs {
		delegationParts = append(delegationParts, fmt.Sprintf(`
SELECT %[1]s.credential_tag,
	%[1]s.staking_key,
	%[1]s.pool_key_hash,
	%[1]s.added_slot,
	tx.block_index,
	certs.cert_index
FROM %[1]s
INNER JOIN certs
	ON certs.id = %[1]s.certificate_id
	AND certs.cert_type = ?
INNER JOIN %[2]s tx
	ON tx.id = certs.transaction_id
WHERE %[1]s.added_slot <= ?`, source.table, txTable))
		args = append(args, source.certType, slot)
	}
	delegationFallbackTables := delegationFallbackBlockTables(
		delegationSrcs,
		registrationSrcs,
	)
	delegationParts = append(delegationParts, `
SELECT account.credential_tag,
	account.staking_key,
	account.pool AS pool_key_hash,
	account.added_slot,
	0 AS block_index,
	0 AS cert_index
FROM account
WHERE account.added_slot <= ?
	AND account.active = TRUE
	AND account.pool IS NOT NULL
	AND length(account.pool) > 0
`+noCredentialHistoryPredicate("account", delegationFallbackTables))
	args = append(args, slot)
	for range delegationFallbackTables {
		args = append(args, slot)
	}

	registrationParts := make([]string, 0, len(registrationSrcs)+1)
	for _, source := range registrationSrcs {
		// registered is a trusted compile-time constant (0/1). Inline it as a
		// literal rather than a bound parameter: a parameter placed directly in
		// a UNION output column is inferred as text by postgres at parse time,
		// which then fails to unify with the integer account-fallback branch
		// ("UNION types text and integer cannot be matched").
		registrationParts = append(registrationParts, fmt.Sprintf(`
SELECT %[1]s.credential_tag,
	%[1]s.staking_key,
	%[3]d AS registered,
	%[1]s.added_slot,
	tx.block_index,
	certs.cert_index
FROM %[1]s
INNER JOIN certs
	ON certs.id = %[1]s.certificate_id
	AND certs.cert_type = ?
INNER JOIN %[2]s tx
	ON tx.id = certs.transaction_id
WHERE %[1]s.added_slot <= ?`, source.table, txTable, source.registered))
		args = append(args, source.certType, slot)
	}
	registrationFallbackTables := registrationFallbackBlockTables(
		registrationSrcs,
	)
	registrationParts = append(registrationParts, `
SELECT account.credential_tag,
	account.staking_key,
	CASE WHEN account.active THEN 1 ELSE 0 END AS registered,
	account.added_slot,
	0 AS block_index,
0 AS cert_index
FROM account
WHERE account.added_slot <= ?
`+noCredentialHistoryPredicate("account", registrationFallbackTables))
	args = append(args, slot)
	for range registrationFallbackTables {
		args = append(args, slot)
	}

	return fmt.Sprintf(`
WITH delegation_events AS (
%s
),
ranked_delegation AS (
	SELECT credential_tag,
		staking_key,
		pool_key_hash,
		added_slot,
		block_index,
		cert_index,
		ROW_NUMBER() OVER (
			PARTITION BY credential_tag, staking_key
			ORDER BY added_slot DESC, block_index DESC, cert_index DESC
		) AS rn
	FROM delegation_events
),
latest_delegation AS (
	SELECT credential_tag,
		staking_key,
		pool_key_hash,
		added_slot,
		block_index,
		cert_index
	FROM ranked_delegation
	WHERE rn = 1
),
registration_events AS (
%s
),
ranked_registration AS (
	SELECT credential_tag,
		staking_key,
		registered,
		added_slot,
		block_index,
		cert_index,
		ROW_NUMBER() OVER (
			PARTITION BY credential_tag, staking_key
			ORDER BY added_slot DESC, block_index DESC, cert_index DESC
		) AS rn
	FROM registration_events
),
latest_registration AS (
	SELECT credential_tag,
		staking_key,
		registered,
		added_slot,
		block_index,
		cert_index
	FROM ranked_registration
	WHERE rn = 1
),
active_delegation AS (
	SELECT latest_delegation.pool_key_hash,
		latest_delegation.credential_tag,
		latest_delegation.staking_key
	FROM latest_delegation
	INNER JOIN latest_registration
		ON latest_registration.credential_tag = latest_delegation.credential_tag
		AND latest_registration.staking_key = latest_delegation.staking_key
	WHERE latest_registration.registered = 1
		AND (
			latest_delegation.added_slot > latest_registration.added_slot
			OR (
				latest_delegation.added_slot = latest_registration.added_slot
				AND latest_delegation.block_index > latest_registration.block_index
			)
			OR (
				latest_delegation.added_slot = latest_registration.added_slot
				AND latest_delegation.block_index = latest_registration.block_index
				AND latest_delegation.cert_index >= latest_registration.cert_index
			)
		)
)`, strings.Join(delegationParts, "\nUNION ALL\n"), strings.Join(registrationParts, "\nUNION ALL\n")), args
}

func transactionTableName(db *gorm.DB) string {
	return sqldialect.TransactionTableName(db)
}

// utxoAmountCastType returns the backend-native integer type used to cast the
// text-encoded utxo.amount column before summation. Matches the casts used by
// the DRep voting-power queries in each plugin. See sqldialect.IntegerCastType.
func utxoAmountCastType(db *gorm.DB) string {
	return sqldialect.IntegerCastType(db)
}

func delegationFallbackBlockTables(
	delegationSrcs []delegationSource,
	registrationSrcs []registrationSource,
) []string {
	tables := make(
		[]string,
		0,
		len(delegationSrcs)+len(registrationSrcs),
	)
	for _, source := range delegationSrcs {
		tables = append(tables, source.table)
	}
	for _, source := range registrationSrcs {
		tables = append(tables, source.table)
	}
	return tables
}

func registrationFallbackBlockTables(
	registrationSrcs []registrationSource,
) []string {
	tables := make([]string, 0, len(registrationSrcs))
	for _, source := range registrationSrcs {
		tables = append(tables, source.table)
	}
	return tables
}

func noCredentialHistoryPredicate(
	accountAlias string,
	tableNames []string,
) string {
	var out strings.Builder
	for _, tableName := range tableNames {
		fmt.Fprintf(&out, `
	AND NOT EXISTS (
		SELECT 1
		FROM %s history
		WHERE history.credential_tag = %s.credential_tag
			AND history.staking_key = %s.staking_key
			AND history.added_slot <= ?
	)`, tableName, accountAlias, accountAlias)
	}
	return out.String()
}

func delegationSources() []delegationSource {
	return []delegationSource{
		{
			table:    "stake_delegation",
			certType: uint(lcommon.CertificateTypeStakeDelegation),
		},
		{
			table:    "stake_registration_delegation",
			certType: uint(lcommon.CertificateTypeStakeRegistrationDelegation),
		},
		{
			table:    "stake_vote_delegation",
			certType: uint(lcommon.CertificateTypeStakeVoteDelegation),
		},
		{
			table: "stake_vote_registration_delegation",
			certType: uint(
				lcommon.CertificateTypeStakeVoteRegistrationDelegation,
			),
		},
	}
}

func registrationSources() []registrationSource {
	return []registrationSource{
		{
			table:      "registration",
			certType:   uint(lcommon.CertificateTypeRegistration),
			registered: 1,
		},
		{
			table:      "stake_registration",
			certType:   uint(lcommon.CertificateTypeStakeRegistration),
			registered: 1,
		},
		{
			table: "stake_registration_delegation",
			certType: uint(
				lcommon.CertificateTypeStakeRegistrationDelegation,
			),
			registered: 1,
		},
		{
			table: "stake_vote_registration_delegation",
			certType: uint(
				lcommon.CertificateTypeStakeVoteRegistrationDelegation,
			),
			registered: 1,
		},
		{
			table:      "vote_registration_delegation",
			certType:   uint(lcommon.CertificateTypeVoteRegistrationDelegation),
			registered: 1,
		},
		{
			table:      "deregistration",
			certType:   uint(lcommon.CertificateTypeDeregistration),
			registered: 0,
		},
		{
			table:      "stake_deregistration",
			certType:   uint(lcommon.CertificateTypeStakeDeregistration),
			registered: 0,
		},
	}
}
