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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/accountwitness"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/rewardstate"
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
	defaultPoolQueryChunkSize           = 800
	largePoolQueryChunkSize             = 5000
	delegatorInactivityActivatedSyncKey = "delegator_inactivity_activated"
)

// GetStakeByPoolsAtSlot returns delegated stake and delegator counts at a
// historical slot. It uses certificate history to find each credential's latest
// stake delegation and registration state, and account rows as synthetic state
// for imported/bootstrap data where the certificate history is unavailable.
func GetStakeByPoolsAtSlot(
	db *gorm.DB,
	poolKeyHashes [][]byte,
	slot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
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

	query, args, err := historicalDelegatorStakeCTE(
		db, slot, expiryEpoch, inactivityPeriod,
		"active_delegation.pool_key_hash IN ?",
	)
	if err != nil {
		return nil, nil, err
	}
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

// GetRewardStakeInputsByPoolsAtSlot returns positive per-credential delegated
// stake for the requested pools reconstructed at slot, from the same
// active_delegator_stake CTE that GetStakeByPoolsAtSlot aggregates for
// leader-election pool totals. Sourcing both halves of the epoch-boundary
// snapshot from one CTE makes the reward-basis inputs agree with the
// leader-election stake by construction: identical credential membership,
// identical slot-accurate stake values, and the identical CIP-0163 historical
// expiry filter (expiration reconstructed at slot, not read from the mutable
// live account.expiration_epoch column). It is used only when the
// delegator-inactivity gate is active (expiryEpoch > 0); the gate-off reward
// path stays on the live aggregate (StakeInputsForPools) to remain
// byte-identical to the pre-CIP query.
func GetRewardStakeInputsByPoolsAtSlot(
	db *gorm.DB,
	poolKeyHashes [][]byte,
	slot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
) ([]*models.RewardStakeInput, error) {
	if len(poolKeyHashes) == 0 {
		return nil, nil
	}
	poolKeyHashes = rewardstate.DedupePoolKeyHashes(poolKeyHashes)

	query, args, err := historicalDelegatorStakeCTE(
		db, slot, expiryEpoch, inactivityPeriod,
		"active_delegation.pool_key_hash IN ?",
	)
	if err != nil {
		return nil, err
	}
	// Only positive stake contributes a reward input, matching the live-path
	// filter (StakeInputsForPools drops total_stake = 0). Ordering mirrors the
	// live path for stable output.
	query += `
SELECT pool_key_hash, credential_tag, staking_key, total_stake
FROM active_delegator_stake
WHERE total_stake > 0
ORDER BY pool_key_hash ASC, credential_tag ASC, staking_key ASC`

	type inputRow struct {
		PoolKeyHash   []byte `gorm:"column:pool_key_hash"`
		StakingKey    []byte `gorm:"column:staking_key"`
		CredentialTag uint8  `gorm:"column:credential_tag"`
		TotalStake    uint64 `gorm:"column:total_stake"`
	}
	var ret []*models.RewardStakeInput
	chunkSize := poolQueryChunkSize(db)
	for start := 0; start < len(poolKeyHashes); start += chunkSize {
		end := min(start+chunkSize, len(poolKeyHashes))
		queryArgs := append(append([]any(nil), args...), poolKeyHashes[start:end])
		var rows []inputRow
		if err := db.Raw(query, queryArgs...).Scan(&rows).Error; err != nil {
			return nil, fmt.Errorf(
				"query historical reward stake inputs: %w", err,
			)
		}
		for _, row := range rows {
			ret = append(ret, &models.RewardStakeInput{
				PoolKeyHash:   append([]byte(nil), row.PoolKeyHash...),
				StakingKey:    append([]byte(nil), row.StakingKey...),
				CredentialTag: row.CredentialTag,
				Stake:         types.Uint64(row.TotalStake),
				// active_delegator_stake contains only actively-delegated
				// credentials at slot, so every row is registered.
				Registered: true,
			})
		}
	}
	return ret, nil
}

// historicalDelegatorStakeCTE builds the shared per-credential stake
// aggregation. When expiryEpoch > 0 the CIP-0163 reward-account inactivity gate
// is active: a LEFT JOIN to account plus a WHERE clause drop credentials whose
// account expired before expiryEpoch (nonzero expiration_epoch < expiryEpoch),
// while credentials with no account row (LEFT JOIN NULL) or expiration_epoch 0
// stay active. When expiryEpoch == 0 the gate is off and the generated SQL and
// args are byte-identical to the pre-CIP query (no join, no clause, no arg).
func historicalDelegatorStakeCTE(
	db *gorm.DB,
	slot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	predicate string,
) (string, []any, error) {
	cte, args := activeDelegationCTE(db, slot)
	// The expiry gate is hand-written positional-arg SQL. Its "?" sits in the
	// WHERE clause ahead of the caller-supplied predicate's `IN ?`, so the
	// expiryEpoch bind arg is appended right after the four slot args below and
	// before the caller appends the pool chunk. Keeping the clause ahead of the
	// predicate preserves the "caller appends last" contract of both callers.
	var expiryCTE, expiryJoin, expiryPredicate string
	if expiryEpoch > 0 {
		if inactivityPeriod == 0 {
			return "", nil, errors.New(
				"historical stake expiry enabled with zero inactivity period",
			)
		}
		activationEpoch, err := loadActivationEpoch(db)
		if err != nil {
			return "", nil, err
		}
		var expiryArgs []any
		expiryCTE, expiryArgs = historicalExpirationCTE(
			slot, inactivityPeriod, activationEpoch, expiryEpoch,
		)
		args = append(args, expiryArgs...)
		expiryJoin = `
	LEFT JOIN historical_expiration expiry_acct
		ON expiry_acct.credential_tag = active_delegation.credential_tag
		AND expiry_acct.staking_key = active_delegation.staking_key`
		expiryPredicate = `(expiry_acct.expiration_epoch = 0
		OR expiry_acct.expiration_epoch >= ?
		OR expiry_acct.expiration_epoch IS NULL) AND `
	}
	query := cte + expiryCTE + fmt.Sprintf(`,
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
		AND historical_reward.staking_key = active_delegation.staking_key%[3]s
	WHERE %[4]s%[2]s
	GROUP BY active_delegation.pool_key_hash,
		active_delegation.credential_tag,
		active_delegation.staking_key
)`, utxoAmountCastType(db), predicate, expiryJoin, expiryPredicate)
	args = append(args, slot, slot, slot, slot)
	if expiryEpoch > 0 {
		args = append(args, expiryEpoch)
	}
	return query, args, nil
}

// GetPoolOwnerStakeAtSlot returns stake only for the requested key-hash owner
// credentials, and only under the pool to which each credential was delegated
// at slot. The result cardinality is bounded by the owner set, not by all pool
// delegators.
func GetPoolOwnerStakeAtSlot(
	db *gorm.DB,
	ownerKeys [][]byte,
	slot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
) (map[string]uint64, error) {
	out := make(map[string]uint64)
	if len(ownerKeys) == 0 {
		return out, nil
	}
	query, args, err := historicalDelegatorStakeCTE(
		db, slot, expiryEpoch, inactivityPeriod,
		"active_delegation.credential_tag = 0 "+
			"AND active_delegation.staking_key IN ?",
	)
	if err != nil {
		return nil, err
	}
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

// loadActivationEpoch returns the durable one-time CIP-0163 activation epoch.
// Reading and parsing it here keeps malformed consensus state from being
// silently coerced by backend-specific SQL casts.
func loadActivationEpoch(db *gorm.DB) (*uint64, error) {
	var state models.SyncState
	result := db.Where(
		"sync_key = ?", delegatorInactivityActivatedSyncKey,
	).Limit(1).Find(&state)
	if result.Error != nil {
		return nil, fmt.Errorf("load delegator inactivity activation: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return nil, nil
	}
	epoch, err := strconv.ParseUint(state.Value, 10, 64)
	if err != nil {
		return nil, fmt.Errorf(
			"parse delegator inactivity activation %q: %w", state.Value, err,
		)
	}
	return &epoch, nil
}

// historicalExpirationCTE reconstructs expiration at slot from the latest
// locally retained witness at or before that slot. The mutable account value is
// used only for imported/bootstrap credentials with no reconstructable witness
// or activation floor.
func historicalExpirationCTE(
	slot uint64,
	inactivityPeriod uint64,
	activationEpoch *uint64,
	expiryEpoch uint64,
) (string, []any) {
	witnessParts := make([]string, 0, len(accountwitness.CertTableNames())+2)
	args := make([]any, 0, len(accountwitness.CertTableNames())+4)
	for _, table := range accountwitness.CertTableNames() {
		witnessParts = append(witnessParts, fmt.Sprintf(`
SELECT witness.credential_tag, witness.staking_key, witness.added_slot
FROM %s witness
INNER JOIN active_delegation
	ON active_delegation.credential_tag = witness.credential_tag
	AND active_delegation.staking_key = witness.staking_key`, table))
	}
	witnessParts = append(witnessParts, `
SELECT witness.credential_tag, witness.staking_key, witness.added_slot
FROM account_withdrawal_witness witness
INNER JOIN active_delegation
	ON active_delegation.credential_tag = witness.credential_tag
	AND active_delegation.staking_key = witness.staking_key`)
	witnessParts = append(witnessParts, `
SELECT witness.credential_tag, witness.staking_key, witness.added_slot
FROM account_reward_delta witness
INNER JOIN active_delegation
	ON active_delegation.credential_tag = witness.credential_tag
	AND active_delegation.staking_key = witness.staking_key
WHERE witness.withdrawal = TRUE`)
	args = append(args, slot)

	activationCTE := ""
	createdEpochJoin := ""
	expirationExpr := `CASE
		WHEN account.id IS NULL THEN 0
		WHEN historical_witness_epoch.epoch_id IS NOT NULL
			THEN historical_witness_epoch.epoch_id + ?
		WHEN witness_summary.latest_added_slot IS NOT NULL
			THEN COALESCE(created_epoch.epoch_id, 0) + ?
		ELSE COALESCE(account.expiration_epoch, 0)
	END`
	createdEpochJoin = `
	LEFT JOIN epoch created_epoch
		ON account.created_slot > 0
		AND account.created_slot >= created_epoch.start_slot
		AND account.created_slot < created_epoch.start_slot + created_epoch.length_in_slots`
	if activationEpoch != nil && *activationEpoch <= expiryEpoch {
		activationCTE = `,
inactivity_activation AS (
	SELECT ? AS epoch_id
)`
		args = append(args, *activationEpoch)
		createdEpochJoin = `
	LEFT JOIN epoch created_epoch
		ON account.created_slot > 0
		AND account.created_slot >= created_epoch.start_slot
		AND account.created_slot < created_epoch.start_slot + created_epoch.length_in_slots
	CROSS JOIN inactivity_activation
	LEFT JOIN account_inactivity_activation activation_account
		ON activation_account.credential_tag = active_delegation.credential_tag
		AND activation_account.staking_key = active_delegation.staking_key`
		expirationExpr = `CASE
		WHEN account.id IS NULL THEN 0
		WHEN activation_account.staking_key IS NOT NULL
			AND (historical_witness_epoch.epoch_id IS NULL
				OR historical_witness_epoch.epoch_id < inactivity_activation.epoch_id)
			THEN inactivity_activation.epoch_id + ?
		WHEN historical_witness_epoch.epoch_id IS NOT NULL
			THEN historical_witness_epoch.epoch_id + ?
		WHEN witness_summary.latest_added_slot IS NOT NULL
			THEN COALESCE(created_epoch.epoch_id, 0) + ?
		ELSE COALESCE(account.expiration_epoch, 0)
	END`
		args = append(args, inactivityPeriod, inactivityPeriod, inactivityPeriod)
	} else {
		args = append(args, inactivityPeriod, inactivityPeriod)
	}

	return fmt.Sprintf(`,
account_witness_events AS (
%s
),
witness_summary AS (
	SELECT credential_tag,
		staking_key,
		MAX(CASE WHEN added_slot <= ? THEN added_slot END) AS historical_added_slot,
		MAX(added_slot) AS latest_added_slot
	FROM account_witness_events
	GROUP BY credential_tag, staking_key
),
historical_witness_epoch AS (
	SELECT witness_summary.credential_tag,
		witness_summary.staking_key,
		MAX(epoch.epoch_id) AS epoch_id
	FROM witness_summary
	INNER JOIN epoch
		ON witness_summary.historical_added_slot >= epoch.start_slot
		AND witness_summary.historical_added_slot < epoch.start_slot + epoch.length_in_slots
	GROUP BY witness_summary.credential_tag,
		witness_summary.staking_key
)%s,
historical_expiration AS (
	SELECT active_delegation.credential_tag,
		active_delegation.staking_key,
		%s AS expiration_epoch
	FROM active_delegation
	LEFT JOIN account
		ON account.credential_tag = active_delegation.credential_tag
		AND account.staking_key = active_delegation.staking_key
	LEFT JOIN historical_witness_epoch
		ON historical_witness_epoch.credential_tag = active_delegation.credential_tag
		AND historical_witness_epoch.staking_key = active_delegation.staking_key
	LEFT JOIN witness_summary
		ON witness_summary.credential_tag = active_delegation.credential_tag
		AND witness_summary.staking_key = active_delegation.staking_key%s
)`, strings.Join(witnessParts, "\nUNION ALL\n"), activationCTE, expirationExpr, createdEpochJoin), args
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
