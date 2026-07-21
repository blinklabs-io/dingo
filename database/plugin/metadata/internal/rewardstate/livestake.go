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

package rewardstate

import (
	"errors"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CredentialSlotRef pairs a stake credential with the slot at which its
// reward-live-stake aggregate should be recomputed. Each backend keeps its
// own local map type (keyed the same way, via
// models.StakeCredentialRef.MapKey) because callers outside the scope of
// this package (account.go, batch_accumulator.go, transaction.go, utxo.go)
// destructure the map value's fields directly; this type exists for the
// query helpers in this file that don't need the backend-local field names.
type CredentialSlotRef struct {
	Ref  models.StakeCredentialRef
	Slot uint64
}

// UtxoSpendRef mirrors the subset of each backend's local utxoSpend row
// (defined in batch_accumulator.go) that StakeRefsFromUtxoSpends needs: the
// spent UTxO's identity and the slot the spend was recorded at.
type UtxoSpendRef struct {
	TxId      []byte
	OutputIdx uint32
	Slot      uint64
}

// PoolDelegationRecord is the subset of a certificate record needed to order
// and apply a stake credential's most recent pool delegation. It mirrors
// each backend's local certRecord (defined in account.go) restricted to the
// fields RefreshLiveStakeAggregate needs.
type PoolDelegationRecord struct {
	Pool       []byte
	AddedSlot  uint64
	BlockIndex uint64
	CertIndex  uint32
}

// PoolDelegationCache maps a stake credential's MapKey (see
// models.StakeCredentialRef.MapKey) to its most recent pool delegation
// certificate, as populated by each backend's batchFetchCerts. A nil cache
// (or a missing entry) means "no certificate history available"; callers
// fall back to the account row's own AddedSlot.
type PoolDelegationCache map[string]PoolDelegationRecord

func utxoSpendRefKey(txId []byte, outputIdx uint32) string {
	return fmt.Sprintf("%x:%d", txId, outputIdx)
}

// CurrentTipSlot returns the metadata tip slot using the caller's transaction.
// A fresh database has no tip row and therefore returns slot zero.
func CurrentTipSlot(db *gorm.DB) (uint64, error) {
	var slot uint64
	if err := db.Model(&models.Tip{}).
		Select("COALESCE(MAX(slot), 0)").
		Scan(&slot).Error; err != nil {
		return 0, fmt.Errorf("query metadata tip slot: %w", err)
	}
	return slot, nil
}

// StakeRefsFromUtxoIDs resolves a batch of UTxO identifiers (tx hash +
// output index) to their stake credentials and merges them into a fresh
// credential/slot map. When slot is 0, the ref's slot is derived per-row
// from the UTxO's own AddedSlot/DeletedSlot (used by rollback-driven
// refreshes, where each UTxO may need a different recompute slot).
// chunkSize bounds how many UTxOs are placed in a single SQL statement.
func StakeRefsFromUtxoIDs(
	db *gorm.DB,
	utxos []models.UtxoId,
	slot uint64,
	chunkSize int,
) (map[string]CredentialSlotRef, error) {
	return stakeRefsFromUtxoIDs(db, utxos, slot, chunkSize, false)
}

// StakeRefsFromLiveUtxoIDs is equivalent to StakeRefsFromUtxoIDs but ignores
// already-spent rows. Physical cleanup of spent UTxOs must not recompute live
// stake: the spend path already removed their value, and recomputing at a
// historical DeletedSlot can replace a newer delegation aggregate.
func StakeRefsFromLiveUtxoIDs(
	db *gorm.DB,
	utxos []models.UtxoId,
	slot uint64,
	chunkSize int,
) (map[string]CredentialSlotRef, error) {
	return stakeRefsFromUtxoIDs(db, utxos, slot, chunkSize, true)
}

func stakeRefsFromUtxoIDs(
	db *gorm.DB,
	utxos []models.UtxoId,
	slot uint64,
	chunkSize int,
	liveOnly bool,
) (map[string]CredentialSlotRef, error) {
	refs := make(map[string]CredentialSlotRef)
	if len(utxos) == 0 {
		return refs, nil
	}
	for start := 0; start < len(utxos); start += chunkSize {
		end := min(start+chunkSize, len(utxos))
		chunk := utxos[start:end]
		conditions := make([]string, 0, len(chunk))
		args := make([]any, 0, len(chunk)*2)
		for _, u := range chunk {
			conditions = append(conditions, "(tx_id = ? AND output_idx = ?)")
			args = append(args, u.Hash, u.Idx)
		}
		var rows []struct {
			StakingKey    []byte
			CredentialTag uint8
			AddedSlot     uint64
			DeletedSlot   uint64
		}
		// Do NOT add a SQL `deleted_slot = 0` filter for liveOnly here.
		// The OR-chain over (tx_id, output_idx) resolves through the unique
		// index tx_id_output_idx (SQLite MULTI-INDEX OR: one index lookup per
		// pair). Adding `AND deleted_slot = 0` makes SQLite switch to the
		// deleted_slot index (idx_utxo_deleted_payment_script) and scan every
		// live UTxO evaluating the whole OR-chain per row (O(live_utxos x
		// terms)), which wedges the node at every epoch boundary during
		// from-genesis sync. Because (tx_id, output_idx) is UNIQUE the OR-chain
		// returns at most one row per pair, so filtering spent rows in Go below
		// is trivially cheap and yields an identical result set.
		query := db.Model(&models.Utxo{}).
			Select("credential_tag", "staking_key", "added_slot", "deleted_slot").
			Where(strings.Join(conditions, " OR "), args...)
		if err := query.Find(&rows).Error; err != nil {
			return nil, err
		}
		for _, row := range rows {
			if liveOnly && row.DeletedSlot != 0 {
				continue
			}
			refSlot := slot
			if refSlot == 0 {
				refSlot = row.AddedSlot
				if row.DeletedSlot > 0 {
					refSlot = row.DeletedSlot
				}
			}
			AddStakeRef(
				refs,
				models.NewStakeCredentialRef(
					row.CredentialTag,
					row.StakingKey,
				),
				refSlot,
			)
		}
	}
	return refs, nil
}

// StakeRefsFromUtxoSpends resolves a batch of just-spent UTxOs to their
// stake credentials, keyed to the slot each spend was recorded at, and
// merges them into a fresh credential/slot map. chunkSize bounds how many
// spends are placed in a single SQL statement.
func StakeRefsFromUtxoSpends(
	db *gorm.DB,
	spends []UtxoSpendRef,
	chunkSize int,
) (map[string]CredentialSlotRef, error) {
	refs := make(map[string]CredentialSlotRef)
	if len(spends) == 0 {
		return refs, nil
	}
	for start := 0; start < len(spends); start += chunkSize {
		end := min(start+chunkSize, len(spends))
		chunk := spends[start:end]
		conditions := make([]string, 0, len(chunk))
		args := make([]any, 0, len(chunk)*2)
		slots := make(map[string]uint64, len(chunk))
		for _, s := range chunk {
			conditions = append(conditions, "(tx_id = ? AND output_idx = ?)")
			args = append(args, s.TxId, s.OutputIdx)
			slots[utxoSpendRefKey(s.TxId, s.OutputIdx)] = s.Slot
		}
		var rows []struct {
			TxId          []byte `gorm:"column:tx_id"`
			StakingKey    []byte
			CredentialTag uint8
			OutputIdx     uint32
		}
		if err := db.Model(&models.Utxo{}).
			Select("tx_id", "output_idx", "credential_tag", "staking_key").
			Where(strings.Join(conditions, " OR "), args...).
			Find(&rows).Error; err != nil {
			return nil, err
		}
		for _, row := range rows {
			AddStakeRef(
				refs,
				models.NewStakeCredentialRef(
					row.CredentialTag,
					row.StakingKey,
				),
				slots[utxoSpendRefKey(row.TxId, row.OutputIdx)],
			)
		}
	}
	return refs, nil
}

// AddStakeRef merges one (credential, slot) pair into refs, keeping the
// highest slot recorded for that credential. A ref with an empty key (no
// staking credential on the UTxO/output) is ignored.
func AddStakeRef(
	refs map[string]CredentialSlotRef,
	ref models.StakeCredentialRef,
	slot uint64,
) {
	if len(ref.Key) == 0 {
		return
	}
	key := ref.MapKey()
	if existing, ok := refs[key]; ok {
		if slot > existing.Slot {
			existing.Slot = slot
			refs[key] = existing
		}
		return
	}
	refs[key] = CredentialSlotRef{Ref: ref, Slot: slot}
}

// ValidateLiveStakeRef reports whether ref is a well-formed stake credential
// for the reward_live_stake table: an empty key is allowed (nothing to
// touch), but a non-empty key must have a valid tag (0=key, 1=script) and be
// exactly 28 bytes.
func ValidateLiveStakeRef(ref models.StakeCredentialRef) error {
	if len(ref.Key) == 0 {
		return nil
	}
	if ref.Tag > 1 {
		return fmt.Errorf(
			"invalid reward live stake credential tag %d for %x",
			ref.Tag,
			ref.Key,
		)
	}
	if len(ref.Key) != 28 {
		return fmt.Errorf(
			"invalid reward live stake credential length %d for tag %d",
			len(ref.Key),
			ref.Tag,
		)
	}
	return nil
}

// GroupRefsBySlot validates every ref in refs and groups them by the slot at
// which their reward-live-stake aggregate should be recomputed, skipping
// refs with no stake credential key. Callers use the returned map to
// batch-fetch certificate state (batchFetchCerts) once per slot before
// calling RefreshLiveStakeAggregate for each ref.
func GroupRefsBySlot(
	refs map[string]CredentialSlotRef,
) (map[uint64][]models.StakeCredentialRef, error) {
	refsBySlot := make(map[uint64][]models.StakeCredentialRef)
	for _, item := range refs {
		if err := ValidateLiveStakeRef(item.Ref); err != nil {
			return nil, err
		}
		if len(item.Ref.Key) == 0 {
			continue
		}
		refsBySlot[item.Slot] = append(refsBySlot[item.Slot], item.Ref)
	}
	return refsBySlot, nil
}

// CreditLiveStakeDelta adds amount to an existing reward_live_stake row's
// reward_stake and total_stake in a single UPDATE, avoiding the full account
// SELECT + UTxO SUM + upsert that RefreshLiveStakeAggregate performs. This is
// safe only for a pure reward-stake credit: pool delegation, registered, and
// utxo_stake are left untouched, and the row's existing
// utxo_stake + (old reward_stake + amount) must still equal the new
// total_stake, which holds because total_stake is only ever set from a full
// refresh's utxoTotal+rewardStake sum.
//
// reward_stake/total_stake are types.Uint64 columns stored as decimal-string
// TEXT (see types.Uint64.Value) on every backend, so the update casts to the
// backend's native integer type, adds, and casts back to a string type
// rather than relying on column affinity or implicit numeric-to-string
// conversion.
//
// CreditLiveStakeDelta must run inside an active transaction. MySQL and
// Postgres take a SELECT ... FOR UPDATE row lock for the overflow check;
// SQLite drops that clause, but its single-writer transaction semantics provide
// the required serialization. All callers use a caller-supplied transaction or
// db.Transaction so the check and UPDATE remain one atomic operation.
//
// Returns true if a row was updated; false (with no error) when no
// reward_live_stake row exists yet for this credential, in which case the
// caller must fall back to RefreshLiveStakeAggregate.
func CreditLiveStakeDelta(
	db *gorm.DB,
	ref models.StakeCredentialRef,
	amount uint64,
	slot uint64,
) (bool, error) {
	if err := ValidateLiveStakeRef(ref); err != nil {
		return false, err
	}
	if len(ref.Key) == 0 {
		return false, nil
	}
	var existing models.RewardLiveStake
	result := db.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where(
			"credential_tag = ? AND staking_key = ?",
			ref.Tag,
			ref.Key,
		).First(&existing)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, fmt.Errorf(
			"query reward live stake credit delta: %w",
			result.Error,
		)
	}
	if uint64(existing.TotalStake) > ^uint64(0)-amount {
		return false, fmt.Errorf(
			"reward live stake overflow for credential %d:%x",
			ref.Tag,
			ref.Key,
		)
	}
	// amount is bound as types.Uint64 (decimal-string driver.Value), not a
	// raw Go uint64: database/sql cannot bind a uint64 whose high bit is set
	// (it doesn't fit driver.Value's int64), which a raw arg would hit for
	// values above math.MaxInt64.
	sql := fmt.Sprintf(
		`UPDATE reward_live_stake
		 SET reward_stake = CAST(CAST(reward_stake AS %[1]s) + %[3]s AS %[2]s),
		     total_stake = CAST(CAST(total_stake AS %[1]s) + %[3]s AS %[2]s),
		     updated_slot = ?
		 WHERE credential_tag = ? AND staking_key = ?`,
		integerCastType(db), textCastType(db), arithmeticParam(db),
	)
	result = db.Exec(
		sql,
		types.Uint64(amount), types.Uint64(amount), slot, ref.Tag, ref.Key,
	)
	if result.Error != nil {
		return false, fmt.Errorf(
			"apply reward live stake credit delta: %w",
			result.Error,
		)
	}
	return result.RowsAffected > 0, nil
}

// RefreshLiveStakeAggregate recomputes and upserts the reward_live_stake row
// for a single stake credential from current account/UTxO state. This is the
// per-credential engine shared by RebuildLiveStake's full rebuild and every
// incremental refresh path (account changes, UTxO create/spend, rollback).
//
// poolDelegationCache is the (optional) batch-fetched certificate state for
// this slot; nil (or a missing entry) falls back to the account row's own
// AddedSlot for the pool-delegation ordering, which is used for accounts
// imported without certificate history (e.g. genesis/Mithril bootstrap).
func RefreshLiveStakeAggregate(
	db *gorm.DB,
	ref models.StakeCredentialRef,
	slot uint64,
	poolDelegationCache PoolDelegationCache,
) error {
	if err := ValidateLiveStakeRef(ref); err != nil {
		return err
	}
	if len(ref.Key) == 0 {
		return nil
	}

	var account models.Account
	accountErr := db.Where(
		"credential_tag = ? AND staking_key = ?",
		ref.Tag,
		ref.Key,
	).First(&account).Error
	if accountErr != nil && !errors.Is(accountErr, gorm.ErrRecordNotFound) {
		return fmt.Errorf("query reward live stake account: %w", accountErr)
	}

	var utxoStake types.Uint64
	// utxo.amount is stored as decimal-string TEXT (types.Uint64), so SUM()
	// requires an explicit cast to the backend's native integer type first;
	// see integerCastType.
	//
	// Known limitation: only base-address stake is attributed — utxo rows
	// carry the staking key hash extracted at ingestion, and pointer
	// addresses (Shelley..Babbage) yield no staking key, so lovelace held at
	// a resolvable pointer address is not counted toward its credential the
	// way cardano-ledger's stakeDistr resolves ptrs. Mainnet pointer usage
	// is a handful of dust-holding addresses; exact from-genesis mainnet
	// replay of those eras would need ptr-triple storage on utxo rows plus
	// an incrementally maintained ptr->credential map.
	if err := db.Model(&models.Utxo{}).
		Where(
			"credential_tag = ? AND staking_key = ? AND deleted_slot = 0",
			ref.Tag,
			ref.Key,
		).
		Select(fmt.Sprintf(
			"COALESCE(SUM(CAST(amount AS %s)), 0)",
			integerCastType(db),
		)).
		Scan(&utxoStake).Error; err != nil {
		return fmt.Errorf("sum reward live stake UTxOs: %w", err)
	}

	rewardStake := uint64(0)
	registered := false
	var pool []byte
	poolDelegation := PoolDelegationRecord{}
	if accountErr == nil {
		rewardStake = uint64(account.Reward)
		registered = account.Active
		if account.Active && len(account.Pool) > 0 {
			pool = append([]byte(nil), account.Pool...)
			poolDelegation = poolDelegationForAccount(
				ref,
				account,
				poolDelegationCache,
			)
		}
	}
	utxoTotal := uint64(utxoStake)
	if utxoTotal > ^uint64(0)-rewardStake {
		return fmt.Errorf(
			"reward live stake overflow for credential %d:%x",
			ref.Tag,
			ref.Key,
		)
	}
	totalStake := utxoTotal + rewardStake

	if accountErr != nil && totalStake == 0 {
		return db.Where(
			"credential_tag = ? AND staking_key = ?",
			ref.Tag,
			ref.Key,
		).Delete(&models.RewardLiveStake{}).Error
	}

	var existing models.RewardLiveStake
	result := db.Where(
		"credential_tag = ? AND staking_key = ?",
		ref.Tag,
		ref.Key,
	).First(&existing)
	values := map[string]any{
		"pool_key_hash":               pool,
		"utxo_stake":                  types.Uint64(utxoTotal),
		"reward_stake":                types.Uint64(rewardStake),
		"total_stake":                 types.Uint64(totalStake),
		"registered":                  registered,
		"pool_delegation_slot":        poolDelegation.AddedSlot,
		"pool_delegation_block_index": poolDelegation.BlockIndex,
		"pool_delegation_cert_index":  poolDelegation.CertIndex,
		"updated_slot":                slot,
	}
	if result.Error == nil {
		if err := db.Model(&existing).Updates(values).Error; err != nil {
			return fmt.Errorf("update reward live stake: %w", err)
		}
		return nil
	}
	if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return fmt.Errorf("query reward live stake: %w", result.Error)
	}
	row := models.RewardLiveStake{
		StakingKey:               append([]byte(nil), ref.Key...),
		CredentialTag:            ref.Tag,
		PoolKeyHash:              pool,
		UtxoStake:                types.Uint64(utxoTotal),
		RewardStake:              types.Uint64(rewardStake),
		TotalStake:               types.Uint64(totalStake),
		Registered:               registered,
		PoolDelegationSlot:       poolDelegation.AddedSlot,
		PoolDelegationBlockIndex: poolDelegation.BlockIndex,
		PoolDelegationCertIndex:  poolDelegation.CertIndex,
		UpdatedSlot:              slot,
	}
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "credential_tag"},
			{Name: "staking_key"},
		},
		DoUpdates: clause.Assignments(values),
	}).Create(&row).Error; err != nil {
		return fmt.Errorf("create reward live stake: %w", err)
	}
	return nil
}

func poolDelegationForAccount(
	ref models.StakeCredentialRef,
	account models.Account,
	cache PoolDelegationCache,
) PoolDelegationRecord {
	fallback := PoolDelegationRecord{
		Pool:      account.Pool,
		AddedSlot: account.AddedSlot,
	}
	if cache == nil {
		return fallback
	}
	if rec, ok := cache[ref.MapKey()]; ok &&
		string(rec.Pool) == string(account.Pool) {
		return rec
	}
	return fallback
}

// ValidateLiveStakeSourceCredentials checks that every account and
// live-UTxO credential feeding the reward_live_stake rebuild is well-formed
// (tag 0 or 1, exactly 28 bytes), returning a descriptive error naming which
// source table and how many rows are invalid.
func ValidateLiveStakeSourceCredentials(db *gorm.DB) error {
	var accountCount int64
	if err := db.Model(&models.Account{}).
		Where(
			"staking_key IS NULL OR credential_tag > ? OR LENGTH(staking_key) != ?",
			1,
			28,
		).
		Count(&accountCount).Error; err != nil {
		return fmt.Errorf(
			"validate reward live stake account credentials: %w",
			err,
		)
	}
	if accountCount > 0 {
		return fmt.Errorf(
			"reward live stake source has %d invalid account credentials",
			accountCount,
		)
	}

	var utxoCount int64
	if err := db.Model(&models.Utxo{}).
		Where(
			"deleted_slot = ? AND LENGTH(staking_key) > ? AND (credential_tag > ? OR LENGTH(staking_key) != ?)",
			0,
			0,
			1,
			28,
		).
		Count(&utxoCount).Error; err != nil {
		return fmt.Errorf(
			"validate reward live stake UTxO credentials: %w",
			err,
		)
	}
	if utxoCount > 0 {
		return fmt.Errorf(
			"reward live stake source has %d invalid UTxO credentials",
			utxoCount,
		)
	}
	return nil
}

// RebuildLiveStake fully repopulates the reward_live_stake table from
// current account/UTxO/certificate state. Callers are responsible for
// transaction scoping (db is used as-is, with no internal
// transaction/savepoint).
//
// utxoJoinHint is an optional backend-specific fragment inserted immediately
// after `LEFT JOIN utxo` (e.g. sqlite's `INDEXED BY <index>` query-planner
// hint); pass "" when the backend has no equivalent syntax.
//
// The dialect casts below are dispatched once per call via integerCastType,
// transactionTableName, and boolLiteral: this is the single shared SQL that
// replaces three near-identical per-backend copies, one of which (mysql) had
// drifted to omit the CAST on account.reward for the reward_stake column
// that postgres already applied. Casting account.reward uniformly here
// removes that drift by construction.
func RebuildLiveStake(db *gorm.DB, slot uint64, utxoJoinHint string) error {
	if err := ValidateLiveStakeSourceCredentials(db); err != nil {
		return err
	}
	if err := db.Exec("DELETE FROM reward_live_stake").Error; err != nil {
		return fmt.Errorf("clear reward live stake: %w", err)
	}

	castType := integerCastType(db)
	txTable := transactionTableName(db)
	trueLit := boolLiteral(db, true)
	falseLit := boolLiteral(db, false)
	utxoJoin := "LEFT JOIN utxo"
	if utxoJoinHint != "" {
		utxoJoin = utxoJoin + " " + utxoJoinHint
	}

	sql := fmt.Sprintf(`
		INSERT INTO reward_live_stake (
			credential_tag,
			staking_key,
			pool_key_hash,
			utxo_stake,
			reward_stake,
			total_stake,
			registered,
			pool_delegation_slot,
			pool_delegation_block_index,
			pool_delegation_cert_index,
			updated_slot
		)
		WITH latest_delegation AS (
			SELECT credential_tag, staking_key, pool_key_hash, added_slot, block_index, cert_index
			FROM (
				SELECT delegation.*,
					ROW_NUMBER() OVER (
						PARTITION BY credential_tag, staking_key
						ORDER BY added_slot DESC, block_index DESC, cert_index DESC
					) AS rn
				FROM (
					SELECT sd.credential_tag, sd.staking_key, sd.pool_key_hash, sd.added_slot,
						COALESCE(tx.block_index, 0) AS block_index,
						COALESCE(c.cert_index, 0) AS cert_index
					FROM stake_delegation sd
					LEFT JOIN certs c ON c.id = sd.certificate_id
					LEFT JOIN %[2]s tx ON tx.id = c.transaction_id
					UNION ALL
					SELECT srd.credential_tag, srd.staking_key, srd.pool_key_hash, srd.added_slot,
						COALESCE(tx.block_index, 0) AS block_index,
						COALESCE(c.cert_index, 0) AS cert_index
					FROM stake_registration_delegation srd
					LEFT JOIN certs c ON c.id = srd.certificate_id
					LEFT JOIN %[2]s tx ON tx.id = c.transaction_id
					UNION ALL
					SELECT svd.credential_tag, svd.staking_key, svd.pool_key_hash, svd.added_slot,
						COALESCE(tx.block_index, 0) AS block_index,
						COALESCE(c.cert_index, 0) AS cert_index
					FROM stake_vote_delegation svd
					LEFT JOIN certs c ON c.id = svd.certificate_id
					LEFT JOIN %[2]s tx ON tx.id = c.transaction_id
					UNION ALL
					SELECT svrd.credential_tag, svrd.staking_key, svrd.pool_key_hash, svrd.added_slot,
						COALESCE(tx.block_index, 0) AS block_index,
						COALESCE(c.cert_index, 0) AS cert_index
					FROM stake_vote_registration_delegation svrd
					LEFT JOIN certs c ON c.id = svrd.certificate_id
					LEFT JOIN %[2]s tx ON tx.id = c.transaction_id
				) AS delegation
			) AS ranked_delegation
			WHERE rn = 1
		)
		SELECT
			creds.credential_tag,
			creds.staking_key,
			CASE
				WHEN account.active = %[3]s THEN account.pool
				ELSE NULL
			END AS pool_key_hash,
			COALESCE(SUM(CAST(utxo.amount AS %[1]s)), 0) AS utxo_stake,
			COALESCE(CAST(account.reward AS %[1]s), 0) AS reward_stake,
			COALESCE(SUM(CAST(utxo.amount AS %[1]s)), 0) + COALESCE(CAST(account.reward AS %[1]s), 0) AS total_stake,
			COALESCE(account.active, %[4]s) AS registered,
			CASE
				WHEN account.active = %[3]s AND LENGTH(account.pool) > 0
				THEN COALESCE(latest_delegation.added_slot, account.added_slot, 0)
				ELSE 0
			END AS pool_delegation_slot,
			CASE
				WHEN account.active = %[3]s AND LENGTH(account.pool) > 0
				THEN COALESCE(latest_delegation.block_index, 0)
				ELSE 0
			END AS pool_delegation_block_index,
			CASE
				WHEN account.active = %[3]s AND LENGTH(account.pool) > 0
				THEN COALESCE(latest_delegation.cert_index, 0)
				ELSE 0
			END AS pool_delegation_cert_index,
			? AS updated_slot
		FROM (
			SELECT credential_tag, staking_key FROM account
			UNION
			SELECT credential_tag, staking_key
			FROM utxo
			WHERE deleted_slot = 0 AND LENGTH(staking_key) > 0
		) AS creds
		LEFT JOIN account
			ON account.credential_tag = creds.credential_tag
			AND account.staking_key = creds.staking_key
		LEFT JOIN latest_delegation
			ON latest_delegation.credential_tag = account.credential_tag
			AND latest_delegation.staking_key = account.staking_key
			AND latest_delegation.pool_key_hash = account.pool
		%[5]s
			ON utxo.credential_tag = creds.credential_tag
			AND utxo.staking_key = creds.staking_key
			AND utxo.deleted_slot = 0
		GROUP BY
			creds.credential_tag,
			creds.staking_key,
			account.credential_tag,
			account.staking_key,
			account.pool,
			account.reward,
			account.active,
			account.added_slot,
			latest_delegation.added_slot,
			latest_delegation.block_index,
			latest_delegation.cert_index
	`, castType, txTable, trueLit, falseLit, utxoJoin)
	if err := db.Exec(sql, slot).Error; err != nil {
		return fmt.Errorf("populate reward live stake: %w", err)
	}
	return nil
}

// LiveStakeNeedsBackfill reports whether any canonical account or live-UTxO
// credential is missing from reward_live_stake. Testing for missing keys, not
// merely an empty aggregate, also repairs upgraded databases where post-upgrade
// writes populated only part of the table before the first restart.
func LiveStakeNeedsBackfill(db *gorm.DB) (bool, error) {
	var missing bool
	if err := db.Raw(`
		SELECT EXISTS (
			SELECT 1
		FROM (
			SELECT credential_tag, staking_key
			FROM account
			WHERE LENGTH(staking_key) > 0
			UNION ALL
			SELECT credential_tag, staking_key
			FROM utxo
			WHERE deleted_slot = 0 AND LENGTH(staking_key) > 0
		) AS canonical_credentials
		WHERE NOT EXISTS (
			SELECT 1
			FROM reward_live_stake
			WHERE reward_live_stake.credential_tag =
				canonical_credentials.credential_tag
			AND reward_live_stake.staking_key =
				canonical_credentials.staking_key
		)
			LIMIT 1
		)
	`).Scan(&missing).Error; err != nil {
		return false, fmt.Errorf(
			"checking missing reward live stake credentials: %w",
			err,
		)
	}
	return missing, nil
}
