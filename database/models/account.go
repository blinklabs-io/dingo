// Copyright 2025 Blink Labs Software
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

package models

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/btcsuite/btcd/btcutil/bech32"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrAccountNotFound = errors.New("account not found")

// AccountCreatedSlotUnset is the sentinel the account create helpers stamp on a
// freshly built (not-yet-persisted) account so the save helpers can resolve
// Account.CreatedSlot to the account's AddedSlot at insert time without
// overwriting the immutable CreatedSlot of an existing row (which is loaded from
// the database and is never equal to this sentinel). It is math.MaxInt64 rather
// than ^uint64(0) because database/sql cannot bind a uint64 with the high bit
// set, and no real slot ever reaches it.
const AccountCreatedSlotUnset = uint64(math.MaxInt64)

const (
	DrepTypeAddrKeyHash uint64 = iota
	DrepTypeScriptHash
	DrepTypeAlwaysAbstain
	DrepTypeAlwaysNoConfidence
)

func DrepTypeFromInt(drepType int) (uint64, error) {
	switch drepType {
	case 0:
		return DrepTypeAddrKeyHash, nil
	case 1:
		return DrepTypeScriptHash, nil
	case 2:
		return DrepTypeAlwaysAbstain, nil
	case 3:
		return DrepTypeAlwaysNoConfidence, nil
	default:
		return 0, fmt.Errorf("unknown drep type: %d", drepType)
	}
}

func CredentialTagFromUint(tag uint) (uint8, error) {
	return CredentialTagFromUint64(uint64(tag))
}

func CredentialTagFromUint64(tag uint64) (uint8, error) {
	switch tag {
	case 0:
		return 0, nil
	case 1:
		return 1, nil
	default:
		return 0, fmt.Errorf("unsupported stake credential tag: %d", tag)
	}
}

// ValidatePredefinedDrepTypes rejects credential-backed DRep delegation
// types. GetDRepVotingPowerByType is only for predefined, credentialless
// DRep options.
func ValidatePredefinedDrepTypes(drepTypes []uint64) error {
	for _, drepType := range drepTypes {
		switch drepType {
		case DrepTypeAddrKeyHash, DrepTypeScriptHash:
			return fmt.Errorf(
				"drep type %d is credential-backed; use credential voting power",
				drepType,
			)
		case DrepTypeAlwaysAbstain, DrepTypeAlwaysNoConfidence:
			continue
		default:
			return fmt.Errorf("unknown predefined drep type: %d", drepType)
		}
	}
	return nil
}

type Account struct {
	StakingKey    []byte `gorm:"uniqueIndex:idx_account_credential,priority:2;size:28;index:idx_account_drep_active_staking_key,priority:4;index:idx_account_drep_type_active_staking_key,priority:4;index:idx_account_active_pool_staking_key,priority:4"`
	CredentialTag uint8  `gorm:"uniqueIndex:idx_account_credential,priority:1;not null;default:0;index:idx_account_drep_active_staking_key,priority:3;index:idx_account_drep_type_active_staking_key,priority:3;index:idx_account_active_pool_staking_key,priority:3"`
	Pool          []byte `gorm:"index;size:28;index:idx_account_active_pool_staking_key,priority:2"`
	Drep          []byte `gorm:"index;size:28;index:idx_account_drep_active_staking_key,priority:1"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
	// CreatedSlot is the slot at which this account row was first created
	// (0 for Shelley-genesis delegated accounts). Unlike AddedSlot it is
	// immutable after creation — never bumped by later delegation/registration
	// changes. See AccountCreatedSlotUnset for the sentinel used by the
	// create/save helpers.
	CreatedSlot   uint64 `gorm:"not null;default:0"`
	CertificateID uint   `gorm:"index"`
	Reward        types.Uint64
	// DrepType is the DRep delegation type code, an internal enum
	// matching the Cardano ledger CBOR sum-type tag:
	//   0 = key credential, 1 = script credential,
	//   2 = AlwaysAbstain, 3 = AlwaysNoConfidence.
	// A zero value (0) means either "key credential" or "no delegation set",
	// disambiguated by whether Drep is nil.
	DrepType uint64 `gorm:"default:0;index:idx_account_drep_type_active_staking_key,priority:1"`
	Active   bool   `gorm:"default:true;index:idx_account_drep_active_staking_key,priority:2;index:idx_account_drep_type_active_staking_key,priority:2;index:idx_account_active_pool_staking_key,priority:1"`
}

func (a *Account) TableName() string {
	return "account"
}

// MigrateAccountCredentialTagIndex drops the legacy hash-only account
// uniqueness so AutoMigrate can create the tag-aware composite unique index.
func MigrateAccountCredentialTagIndex(db *gorm.DB, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if !db.Migrator().HasTable(&Account{}) {
		return nil
	}
	if !db.Migrator().HasIndex(&Account{}, "idx_account_staking_key") {
		return nil
	}
	logger.Info(
		"dropping legacy account staking_key unique index before tag-aware migration",
	)
	if err := db.Migrator().DropIndex(
		&Account{},
		"idx_account_staking_key",
	); err != nil {
		return fmt.Errorf("drop account staking_key index: %w", err)
	}
	return nil
}

// MigrateAccountRewardDeltaCredentialTagIndex drops the legacy unique index on
// account_reward_delta before AutoMigrate adds credential_tag to it. The old
// index covers (withdrawal, tx_hash, staking_key); the new one adds
// credential_tag so key and script reward accounts with the same hash are
// tracked independently.
func MigrateAccountRewardDeltaCredentialTagIndex(db *gorm.DB, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if !db.Migrator().HasTable(&AccountRewardDelta{}) {
		return nil
	}
	if db.Migrator().HasIndex(&AccountRewardDelta{}, "idx_account_reward_delta_w_tx_s") {
		logger.Info(
			"dropping legacy account_reward_delta unique index before tag-aware migration",
		)
		if err := db.Migrator().DropIndex(
			&AccountRewardDelta{},
			"idx_account_reward_delta_w_tx_s",
		); err != nil {
			return fmt.Errorf("drop account_reward_delta unique index: %w", err)
		}
	}
	// If credential_tag is already present the index migration has already run,
	// but keep the tx_hash normalization in this path so older databases that
	// only completed the tag-aware step are hardened before AutoMigrate creates
	// the current unique index.
	if err := normalizeAccountRewardDeltaTxHash(db, logger); err != nil {
		return err
	}
	return nil
}

// MigrateAccountRewardDeltaSlotIndex drops the slot-less unique index on
// account_reward_delta so AutoMigrate can recreate it with added_slot included.
//
// The old index (withdrawal, tx_hash, credential_tag, staking_key) keyed every
// credit delta without a per-event discriminator for an account onto a single
// row, so a second per-epoch credit — a MIR reward, a POOLREAP refund, or a
// governance deposit refund in a later epoch — collided on a clean first pass,
// and a crash-replayed epoch-rollover credit collided on restart. The new index
// idx_account_reward_delta_w_tx_s_slot adds added_slot so per-epoch credits are
// distinct rows while a replayed same-slot credit still maps to the same row
// (handled idempotently by the credit/withdrawal writers via OnConflict).
//
// AutoMigrate does not alter an existing index in place, so the old index is
// dropped here by name; AutoMigrate then creates the renamed slot-aware index.
func MigrateAccountRewardDeltaSlotIndex(db *gorm.DB, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if !db.Migrator().HasTable(&AccountRewardDelta{}) {
		return nil
	}
	if db.Migrator().HasIndex(
		&AccountRewardDelta{},
		"idx_account_reward_delta_w_tx_s",
	) {
		logger.Info(
			"dropping slot-less account_reward_delta unique index before slot-aware migration",
		)
		if err := db.Migrator().DropIndex(
			&AccountRewardDelta{},
			"idx_account_reward_delta_w_tx_s",
		); err != nil {
			return fmt.Errorf(
				"drop slot-less account_reward_delta unique index: %w",
				err,
			)
		}
	}
	if err := normalizeAccountRewardDeltaTxHash(db, logger); err != nil {
		return err
	}
	return nil
}

func normalizeAccountRewardDeltaTxHash(db *gorm.DB, logger *slog.Logger) error {
	if !db.Migrator().HasColumn(&AccountRewardDelta{}, "tx_hash") {
		return nil
	}
	var nullCount int64
	if err := db.Model(&AccountRewardDelta{}).
		Where("tx_hash IS NULL").
		Count(&nullCount).Error; err != nil {
		return fmt.Errorf("count account_reward_delta null tx_hash rows: %w", err)
	}
	if nullCount > 0 {
		logger.Info(
			"backfilling account_reward_delta null tx_hash values before not-null migration",
			"rows",
			nullCount,
		)
		if err := db.Model(&AccountRewardDelta{}).
			Where("tx_hash IS NULL").
			Update("tx_hash", []byte{}).Error; err != nil {
			return fmt.Errorf("backfill account_reward_delta tx_hash: %w", err)
		}
	}

	nullable, ok, err := accountRewardDeltaTxHashNullable(db)
	if err != nil {
		return err
	}
	if ok && !nullable {
		return nil
	}
	logger.Info(
		"making account_reward_delta tx_hash non-null",
	)
	if db.Name() == "sqlite" {
		return alterSQLiteAccountRewardDeltaTxHashNotNull(db)
	}
	if err := db.Migrator().AlterColumn(&AccountRewardDelta{}, "TxHash"); err != nil {
		return fmt.Errorf("alter account_reward_delta tx_hash not null: %w", err)
	}
	return nil
}

// accountCreatedSlotBackfillPhase is the backfill_checkpoint phase key that
// durably records one-time completion of the created_slot backfill.
const accountCreatedSlotBackfillPhase = "account_created_slot"

const createdSlotBackfillChunk = 400

// BackfillAccountCreatedSlot stamps Account.CreatedSlot for pre-existing,
// certificate-created rows from their earliest registration certificate.
// Genesis-created accounts keep the default 0 even if they were registered
// again later.
func BackfillAccountCreatedSlot(db *gorm.DB, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if !db.Migrator().HasTable(&Account{}) {
		return nil
	}
	done, err := accountCreatedSlotBackfillCompleted(db)
	if err != nil {
		return err
	}
	if done {
		return nil
	}
	regTables := []string{
		(&StakeRegistration{}).TableName(),
		(&StakeRegistrationDelegation{}).TableName(),
		(&StakeVoteRegistrationDelegation{}).TableName(),
		(&VoteRegistrationDelegation{}).TableName(),
		(&Registration{}).TableName(),
	}
	genesisEvidenceTables := []string{
		(&Registration{}).TableName(),
		(&StakeDelegation{}).TableName(),
		(&StakeVoteDelegation{}).TableName(),
		(&VoteDelegation{}).TableName(),
	}
	existingGenesisEvidenceTables := make([]string, 0, len(genesisEvidenceTables))
	for _, table := range genesisEvidenceTables {
		if db.Migrator().HasTable(table) {
			existingGenesisEvidenceTables = append(
				existingGenesisEvidenceTables,
				table,
			)
		}
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var updated int64
		for _, table := range regTables {
			if !tx.Migrator().HasTable(table) {
				continue
			}
			tableUpdated, err := backfillAccountCreatedSlotFromTable(
				tx,
				table,
				existingGenesisEvidenceTables,
			)
			if err != nil {
				return err
			}
			updated += tableUpdated
		}
		if updated > 0 {
			logger.Info(
				"backfilled account created_slot from registration history",
				"accounts", updated,
			)
		}
		return markAccountCreatedSlotBackfillComplete(tx)
	})
}

type accountCreatedSlotRegistration struct {
	CredentialTag uint8
	StakingKey    []byte
	MinSlot       uint64
}

func backfillAccountCreatedSlotFromTable(
	tx *gorm.DB,
	table string,
	genesisEvidenceTables []string,
) (int64, error) {
	var (
		lastTag uint8
		lastKey []byte
		updated int64
	)
	for {
		var rows []accountCreatedSlotRegistration
		query := tx.Table(table).
			Select("credential_tag, staking_key, MIN(added_slot) AS min_slot").
			Where("staking_key IS NOT NULL").
			Group("credential_tag, staking_key").
			Order("credential_tag, staking_key").
			Limit(createdSlotBackfillChunk)
		if lastKey != nil {
			query = query.Where(
				"credential_tag > ? OR (credential_tag = ? AND staking_key > ?)",
				lastTag,
				lastTag,
				lastKey,
			)
		}
		if err := query.Scan(&rows).Error; err != nil {
			return updated, fmt.Errorf(
				"created_slot backfill scan %s: %w",
				table,
				err,
			)
		}
		if len(rows) == 0 {
			return updated, nil
		}
		earliest := make(map[string]uint64, len(rows))
		for _, row := range rows {
			key := StakeCredentialRef{
				Tag: row.CredentialTag,
				Key: row.StakingKey,
			}.MapKey()
			earliest[key] = row.MinSlot
		}
		chunkUpdated, err := applyAccountCreatedSlotBackfill(
			tx,
			earliest,
			genesisEvidenceTables,
		)
		if err != nil {
			return updated, fmt.Errorf(
				"created_slot backfill update from %s: %w",
				table,
				err,
			)
		}
		updated += chunkUpdated
		last := rows[len(rows)-1]
		lastTag = last.CredentialTag
		lastKey = slices.Clone(last.StakingKey)
	}
}

func applyAccountCreatedSlotBackfill(
	tx *gorm.DB,
	earliest map[string]uint64,
	genesisEvidenceTables []string,
) (int64, error) {
	if tx.Name() == "sqlite" {
		return applyAccountCreatedSlotBackfillPerSlot(
			tx,
			earliest,
			genesisEvidenceTables,
		)
	}
	return applyAccountCreatedSlotBackfillCase(
		tx,
		earliest,
		genesisEvidenceTables,
	)
}

func withoutGenesisCreatedAccount(
	tx *gorm.DB,
	genesisEvidenceTables []string,
) *gorm.DB {
	tx = tx.Where("account.certificate_id <> 0")
	for _, table := range genesisEvidenceTables {
		evidence := tx.Session(&gorm.Session{NewDB: true}).
			Table(table + " AS genesis_evidence").
			Select("1").
			Where("genesis_evidence.credential_tag = account.credential_tag").
			Where("genesis_evidence.staking_key = account.staking_key").
			Where("genesis_evidence.added_slot = 0")
		tx = tx.Where("NOT EXISTS (?)", evidence)
	}
	return tx
}

func applyAccountCreatedSlotBackfillPerSlot(
	tx *gorm.DB,
	earliest map[string]uint64,
	genesisEvidenceTables []string,
) (int64, error) {
	bySlot := make(map[uint64][][]any)
	for key, slot := range earliest {
		bySlot[slot] = append(bySlot[slot], []any{key[0], []byte(key[1:])})
	}
	var updated int64
	for slot, pairs := range bySlot {
		result := withoutGenesisCreatedAccount(
			tx.Model(&Account{}),
			genesisEvidenceTables,
		).
			Where("(credential_tag, staking_key) IN ?", pairs).
			Where("created_slot = 0 OR created_slot > ?", slot).
			Update("created_slot", slot)
		if result.Error != nil {
			return updated, result.Error
		}
		updated += result.RowsAffected
	}
	return updated, nil
}

func applyAccountCreatedSlotBackfillCase(
	tx *gorm.DB,
	earliest map[string]uint64,
	genesisEvidenceTables []string,
) (int64, error) {
	var caseSQL strings.Builder
	caseSQL.WriteString("CASE")
	caseArgs := make([]any, 0, len(earliest)*2)
	inPairs := make([][]any, 0, len(earliest))
	for key, slot := range earliest {
		caseSQL.WriteString(
			" WHEN credential_tag = ? AND staking_key = ? THEN ",
		)
		caseSQL.WriteString(strconv.FormatUint(slot, 10))
		caseArgs = append(caseArgs, key[0], []byte(key[1:]))
		inPairs = append(inPairs, []any{key[0], []byte(key[1:])})
	}
	caseSQL.WriteString(" END")
	result := withoutGenesisCreatedAccount(
		tx.Model(&Account{}),
		genesisEvidenceTables,
	).
		Where("(credential_tag, staking_key) IN ?", inPairs).
		Where("created_slot = 0 OR created_slot > "+caseSQL.String(), caseArgs...).
		Update("created_slot", gorm.Expr(caseSQL.String(), caseArgs...))
	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}

func accountCreatedSlotBackfillCompleted(db *gorm.DB) (bool, error) {
	if !db.Migrator().HasTable(&BackfillCheckpoint{}) {
		return false, nil
	}
	var checkpoint BackfillCheckpoint
	err := db.Where("phase = ?", accountCreatedSlotBackfillPhase).
		First(&checkpoint).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, fmt.Errorf(
			"check account created_slot backfill checkpoint: %w", err,
		)
	}
	return checkpoint.Completed, nil
}

func markAccountCreatedSlotBackfillComplete(db *gorm.DB) error {
	now := time.Now()
	checkpoint := BackfillCheckpoint{
		Phase:     accountCreatedSlotBackfillPhase,
		StartedAt: now,
		UpdatedAt: now,
		Completed: true,
	}
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "phase"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"updated_at",
			"completed",
		}),
	}).Create(&checkpoint).Error; err != nil {
		return fmt.Errorf("mark account created_slot backfill complete: %w", err)
	}
	return nil
}

type sqliteColumnInfo struct {
	DfltValue sql.NullString `gorm:"column:dflt_value"`
	Name      string
	Type      string
	CID       int
	NotNull   int `gorm:"column:notnull"`
	PK        int
}

func alterSQLiteAccountRewardDeltaTxHashNotNull(db *gorm.DB) error {
	var columns []sqliteColumnInfo
	if err := db.Raw("PRAGMA table_info(account_reward_delta)").
		Scan(&columns).Error; err != nil {
		return fmt.Errorf("inspect account_reward_delta sqlite columns: %w", err)
	}
	if len(columns) == 0 {
		return nil
	}
	hasTxHash := false
	columnDefs := make([]string, 0, len(columns))
	columnNames := make([]string, 0, len(columns))
	for _, column := range columns {
		if column.Name == "tx_hash" {
			hasTxHash = true
		}
		name := quoteSQLiteIdentifier(column.Name)
		columnNames = append(columnNames, name)
		def := []string{name}
		if column.Type != "" {
			def = append(def, column.Type)
		}
		if column.PK > 0 {
			def = append(def, "PRIMARY KEY")
		}
		if column.NotNull != 0 || column.Name == "tx_hash" {
			def = append(def, "NOT NULL")
		}
		if column.DfltValue.Valid {
			def = append(def, "DEFAULT "+column.DfltValue.String)
		}
		columnDefs = append(columnDefs, strings.Join(def, " "))
	}
	if !hasTxHash {
		return nil
	}

	const tempTable = "account_reward_delta__tx_hash_not_null"
	tempIdent := quoteSQLiteIdentifier(tempTable)
	columnsSQL := strings.Join(columnNames, ", ")
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("DROP TABLE IF EXISTS " + tempIdent).Error; err != nil {
			return fmt.Errorf("drop account_reward_delta temp table: %w", err)
		}
		if err := tx.Exec(
			"CREATE TABLE " + tempIdent + " (" +
				strings.Join(columnDefs, ", ") +
				")",
		).Error; err != nil {
			return fmt.Errorf("create account_reward_delta temp table: %w", err)
		}
		if err := tx.Exec(
			"INSERT INTO " + tempIdent + " (" + columnsSQL + ") " +
				"SELECT " + columnsSQL + " FROM account_reward_delta",
		).Error; err != nil {
			return fmt.Errorf("copy account_reward_delta rows: %w", err)
		}
		if err := tx.Exec("DROP TABLE account_reward_delta").Error; err != nil {
			return fmt.Errorf("drop old account_reward_delta table: %w", err)
		}
		if err := tx.Exec(
			"ALTER TABLE " + tempIdent + " RENAME TO account_reward_delta",
		).Error; err != nil {
			return fmt.Errorf("rename account_reward_delta temp table: %w", err)
		}
		return nil
	})
}

func quoteSQLiteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func accountRewardDeltaTxHashNullable(db *gorm.DB) (bool, bool, error) {
	columnTypes, err := db.Migrator().ColumnTypes(&AccountRewardDelta{})
	if err != nil {
		return false, false, fmt.Errorf(
			"inspect account_reward_delta columns: %w",
			err,
		)
	}
	for _, columnType := range columnTypes {
		if columnType.Name() != "tx_hash" {
			continue
		}
		nullable, ok := columnType.Nullable()
		return nullable, ok, nil
	}
	return false, false, nil
}

type StakeCredentialRef struct {
	Tag uint8
	Key []byte
}

func NewStakeCredentialRef(tag uint8, key []byte) StakeCredentialRef {
	return StakeCredentialRef{
		Tag: tag,
		Key: key,
	}
}

func (r StakeCredentialRef) MapKey() string {
	return string([]byte{r.Tag}) + string(r.Key)
}

// AccountRewardDelta records reward-account balance changes that are not
// otherwise represented by a rollback-aware certificate row. Credits store the
// credited Amount. Withdrawals store the withdrawal Amount, PreviousReward, and
// TxHash so rollback can restore the cleared reward balance.
//
// The unique index idx_account_reward_delta_w_tx_s_slot includes AddedSlot.
// Withdrawal deltas are keyed by their (non-empty) TxHash, so each is unique
// regardless of slot. Credit deltas (deposit refunds, MIR, POOLREAP) use an
// event discriminator in TxHash when one is available and otherwise use the
// normalized empty value. Without AddedSlot, repeated per-epoch credits to a
// given account could still collapse onto a single row — colliding across
// epochs even on a clean first pass and breaking per-row rollback accounting in
// DeleteAccountRewardsAfterSlot. Including AddedSlot makes each per-epoch
// credit a distinct row while keeping a replayed epoch-rollover credit (same
// account, same event discriminator, same boundary slot) mapped to the same row
// so it can be skipped idempotently instead of erroring.
type AccountRewardDelta struct {
	StakingKey     []byte       `gorm:"index:idx_account_reward_delta_credential,priority:2;size:28;not null;uniqueIndex:idx_account_reward_delta_w_tx_s_slot,priority:4"`
	CredentialTag  uint8        `gorm:"index:idx_account_reward_delta_credential,priority:1;not null;default:0;uniqueIndex:idx_account_reward_delta_w_tx_s_slot,priority:3"`
	TxHash         []byte       `gorm:"index;size:32;not null;uniqueIndex:idx_account_reward_delta_w_tx_s_slot,priority:2"`
	Amount         types.Uint64 `gorm:"not null"`
	PreviousReward types.Uint64
	ID             uint   `gorm:"primarykey"`
	AddedSlot      uint64 `gorm:"index;not null;uniqueIndex:idx_account_reward_delta_w_tx_s_slot,priority:5"`
	Withdrawal     bool   `gorm:"index;not null;default:false;uniqueIndex:idx_account_reward_delta_w_tx_s_slot,priority:1"`
}

func (AccountRewardDelta) TableName() string {
	return "account_reward_delta"
}

// String returns the bech32-encoded representation of the Account's StakingKey
// with the "stake" human-readable part. Returns an error if the StakingKey is
// empty or if encoding fails.
func (a *Account) String() (string, error) {
	if len(a.StakingKey) == 0 {
		return "", errors.New("staking key is empty")
	}
	// Convert data to base32 and encode as bech32
	convData, err := bech32.ConvertBits(a.StakingKey, 8, 5, true)
	if err != nil {
		return "", fmt.Errorf("failed to convert bits: %w", err)
	}
	encoded, err := bech32.Encode("stake", convData)
	if err != nil {
		return "", fmt.Errorf("failed to encode bech32: %w", err)
	}
	return encoded, nil
}

type Deregistration struct {
	StakingKey    []byte `gorm:"index:idx_deregistration_credential,priority:2;size:28"`
	CredentialTag uint8  `gorm:"index:idx_deregistration_credential,priority:1;not null;default:0"`
	ID            uint   `gorm:"primarykey"`
	CertificateID uint   `gorm:"index"`
	AddedSlot     uint64 `gorm:"index"`
	Amount        types.Uint64
}

func (Deregistration) TableName() string {
	return "deregistration"
}

type Registration struct {
	StakingKey    []byte `gorm:"index:idx_registration_credential,priority:2;size:28"`
	CredentialTag uint8  `gorm:"index:idx_registration_credential,priority:1;not null;default:0"`
	ID            uint   `gorm:"primarykey"`
	CertificateID uint   `gorm:"index"`
	AddedSlot     uint64 `gorm:"index"`
	DepositAmount types.Uint64
}

func (Registration) TableName() string {
	return "registration"
}

type StakeDelegation struct {
	StakingKey    []byte `gorm:"index:idx_stake_delegation_credential,priority:2;size:28"`
	CredentialTag uint8  `gorm:"index:idx_stake_delegation_credential,priority:1;not null;default:0"`
	PoolKeyHash   []byte `gorm:"index;size:28"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
}

func (StakeDelegation) TableName() string {
	return "stake_delegation"
}

type StakeDeregistration struct {
	StakingKey    []byte `gorm:"index:idx_stake_deregistration_credential,priority:2;size:28"`
	CredentialTag uint8  `gorm:"index:idx_stake_deregistration_credential,priority:1;not null;default:0"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
}

func (StakeDeregistration) TableName() string {
	return "stake_deregistration"
}

type StakeRegistration struct {
	StakingKey    []byte `gorm:"index:idx_stake_registration_credential,priority:2;size:28"`
	CredentialTag uint8  `gorm:"index:idx_stake_registration_credential,priority:1;not null;default:0"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
	DepositAmount types.Uint64
}

func (StakeRegistration) TableName() string {
	return "stake_registration"
}

type StakeRegistrationDelegation struct {
	StakingKey    []byte `gorm:"index:idx_stake_registration_delegation_credential,priority:2;size:28"`
	CredentialTag uint8  `gorm:"index:idx_stake_registration_delegation_credential,priority:1;not null;default:0"`
	PoolKeyHash   []byte `gorm:"index;size:28"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
	DepositAmount types.Uint64
}

func (StakeRegistrationDelegation) TableName() string {
	return "stake_registration_delegation"
}

type StakeVoteDelegation struct {
	StakingKey    []byte `gorm:"index:idx_stake_vote_delegation_credential,priority:2;size:28"`
	CredentialTag uint8  `gorm:"index:idx_stake_vote_delegation_credential,priority:1;not null;default:0"`
	PoolKeyHash   []byte `gorm:"index;size:28"`
	Drep          []byte `gorm:"index;size:28"`
	DrepType      uint64 `gorm:"default:0"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
}

func (StakeVoteDelegation) TableName() string {
	return "stake_vote_delegation"
}

type StakeVoteRegistrationDelegation struct {
	StakingKey    []byte `gorm:"index:idx_stake_vote_registration_delegation_credential,priority:2;size:28"`
	CredentialTag uint8  `gorm:"index:idx_stake_vote_registration_delegation_credential,priority:1;not null;default:0"`
	PoolKeyHash   []byte `gorm:"index;size:28"`
	Drep          []byte `gorm:"index;size:28"`
	DrepType      uint64 `gorm:"default:0"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
	DepositAmount types.Uint64
}

func (StakeVoteRegistrationDelegation) TableName() string {
	return "stake_vote_registration_delegation"
}

type VoteDelegation struct {
	StakingKey    []byte `gorm:"index:idx_vote_delegation_credential,priority:2;size:28"`
	CredentialTag uint8  `gorm:"index:idx_vote_delegation_credential,priority:1;not null;default:0"`
	Drep          []byte `gorm:"index;size:28"`
	DrepType      uint64 `gorm:"default:0"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
}

func (VoteDelegation) TableName() string {
	return "vote_delegation"
}

type VoteRegistrationDelegation struct {
	StakingKey    []byte `gorm:"index:idx_vote_registration_delegation_credential,priority:2;size:28"`
	CredentialTag uint8  `gorm:"index:idx_vote_registration_delegation_credential,priority:1;not null;default:0"`
	Drep          []byte `gorm:"index;size:28"`
	DrepType      uint64 `gorm:"default:0"`
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
	DepositAmount types.Uint64
}

func (VoteRegistrationDelegation) TableName() string {
	return "vote_registration_delegation"
}
