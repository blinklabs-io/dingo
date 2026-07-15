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

	cte, args := activeDelegationCTE(db, slot)

	// utxo.amount and account.reward are stored as text by types.Uint64 on
	// postgres and mysql, so SUM()/arithmetic over the raw columns fails there
	// ("function sum(text) does not exist" / "UNION types text and integer
	// cannot be matched"). Cast to the backend's native integer type first,
	// mirroring the DRep voting-power queries. sqlite is loosely typed but the
	// cast keeps the backends consistent.
	//
	// Per-credential stake is live UTxO lovelace PLUS the delegator's
	// reward-account balance (issue #2813): summing UTxOs alone undercounts
	// each pool's mark stake by ~10% (worse for pools whose delegators hold
	// most of their stake as rewards), understating sigma so that canonical
	// blocks fail the "VRF leader value exceeds stake-derived threshold"
	// check and wedge networks that enforce it (preview/preprod/mainnet).
	// The reward term is added at the credential level exactly once via
	// MAX(account.reward): idx_account_credential is unique per
	// (credential_tag, staking_key), so MAX picks the single reward value
	// without multiplying it across the UTxO join fan-out (SUM would).
	//
	// CAVEAT (interim fix): account.reward is the LIVE balance, which drifts
	// from the reward balance at the boundary slot. This greatly reduces the
	// gross undercount and unblocks pools whose delegators' entire stake sits
	// in rewards, but it is NOT consensus-exact. The boundary-accurate source
	// is the #1959 reward engine sampling the reward at the snapshot slot,
	// which is not wired into snapshots yet. Do NOT enable
	// SkipLeaderStakeThresholdCheck on non-musashi networks as a workaround.
	query := cte + fmt.Sprintf(`,
active_delegator_stake AS (
	SELECT active_delegation.pool_key_hash,
		active_delegation.credential_tag,
		active_delegation.staking_key,
		COALESCE(SUM(CAST(utxo.amount AS %[1]s)), 0)
			+ COALESCE(MAX(CAST(account.reward AS %[1]s)), 0) AS total_stake
	FROM active_delegation
	LEFT JOIN utxo
		ON utxo.credential_tag = active_delegation.credential_tag
		AND utxo.staking_key = active_delegation.staking_key
		AND utxo.added_slot <= ?
		AND (utxo.deleted_slot = 0 OR utxo.deleted_slot > ?)
	LEFT JOIN account
		ON account.credential_tag = active_delegation.credential_tag
		AND account.staking_key = active_delegation.staking_key
	WHERE active_delegation.pool_key_hash IN ?
	GROUP BY active_delegation.pool_key_hash,
		active_delegation.credential_tag,
		active_delegation.staking_key
)
SELECT pool_key_hash,
	COUNT(*) AS delegator_count,
	COALESCE(SUM(total_stake), 0) AS total_stake
FROM active_delegator_stake
GROUP BY pool_key_hash`, utxoAmountCastType(db))

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
		queryArgs := append(append([]any(nil), args...), slot, slot, chunk)
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
	registrationFallbackTables := registrationFallbackBlockTables(registrationSrcs)
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

func registrationFallbackBlockTables(registrationSrcs []registrationSource) []string {
	tables := make([]string, 0, len(registrationSrcs))
	for _, source := range registrationSrcs {
		tables = append(tables, source.table)
	}
	return tables
}

func noCredentialHistoryPredicate(accountAlias string, tableNames []string) string {
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
			table:    "stake_vote_registration_delegation",
			certType: uint(lcommon.CertificateTypeStakeVoteRegistrationDelegation),
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
			table:      "stake_registration_delegation",
			certType:   uint(lcommon.CertificateTypeStakeRegistrationDelegation),
			registered: 1,
		},
		{
			table:      "stake_vote_registration_delegation",
			certType:   uint(lcommon.CertificateTypeStakeVoteRegistrationDelegation),
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
