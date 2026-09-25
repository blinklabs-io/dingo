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

package migrations

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// migrationSQL contains immutable, versioned migration resources.
//
//go:embed v*/*/*.sql
var migrationSQL embed.FS

const (
	initialSchemaRelease                          = "v1alpha1"
	leiosKeySchemaRelease                         = "leios-key-registration"
	tokenRegistrySchemaRelease                    = "token-registry-metadata"
	accountBaselineSchemaRelease                  = "account-import-baseline"
	leiosSnapshotKeySchemaRelease                 = "leios-snapshot-keys"
	governanceRatificationHistorySchemaRelease    = "governance-ratification-history"
	accountDepositSchemaRelease                   = "account-import-deposit"
	committeeCredentialTagsSchemaRelease          = "committee-credential-tags"
	committeeTermStartPresenceSchemaRelease       = "committee-term-start-presence"
	rewardSeedFailureSchemaRelease                = "reward-seed-failure"
	importedPoolBlockCountSchemaRelease           = "imported-pool-block-count"
	poolDepositHeldSchemaRelease                  = "pool-registration-deposit-held"
	pointerAddressStakeSchemaRelease              = "pointer-address-stake"
	collateralAssociationSchemaRelease            = "collateral-transaction-associations"
	rewardStakeVersionRestampSchemaRelease        = "reward-stake-calculation-version-restamp"
	governanceProposalOptionalAnchorSchemaRelease = "governance-proposal-optional-anchor"
	governanceProposalDroppedSchemaRelease        = "governance-proposal-dropped-epoch"
	rewardSnapshotExcludedStakeSchemaRelease      = "reward-snapshot-excluded-active-stake"
	committeeZeroQuorumSchemaRelease              = "committee-zero-quorum"
	assetNameHexColumnDropSchemaRelease           = "asset-name-hex-column-drop"
	alonzoPParamsUnitSchemaRelease                = "alonzo-pparams-unit-provenance"
	assetAmountFingerprintIndexDropSchemaRelease  = "asset-amount-fingerprint-index-drop"
	governanceVoteHistorySchemaRelease            = "governance-vote-history"
	drepExpiryHistorySchemaRelease                = "drep-expiry-history"
	drepDormancyStateSchemaRelease                = "drep-dormancy-state"
	drepDelegatorStateSchemaRelease               = "drep-delegator-state"
)

// alonzoEraID is the pparams.era_id value Alonzo rows carry. A migration is a
// frozen historical artifact, so it holds its own copy rather than importing
// gouroboros' alonzo.EraIdAlonzo: the two must agree, and a future upstream
// renumbering must not silently reclassify rows this backfill already read.
const alonzoEraID = 4

// schemaVersions names every migration in ascending version order.
var schemaVersions = []struct {
	Version int
	Name    string
	Dir     string
}{
	{Version: 1, Name: initialSchemaRelease, Dir: "v1"},
	{Version: 2, Name: leiosKeySchemaRelease, Dir: "v2"},
	{Version: 3, Name: tokenRegistrySchemaRelease, Dir: "v3"},
	{Version: 4, Name: accountBaselineSchemaRelease, Dir: "v4"},
	{Version: 5, Name: leiosSnapshotKeySchemaRelease, Dir: "v5"},
	{
		Version: 6,
		Name:    governanceRatificationHistorySchemaRelease,
		Dir:     "v6",
	},
	{Version: 7, Name: accountDepositSchemaRelease, Dir: "v7"},
	{Version: 8, Name: committeeCredentialTagsSchemaRelease, Dir: "v8"},
	{
		Version: 9,
		Name:    committeeTermStartPresenceSchemaRelease,
		Dir:     "v9",
	},
	{Version: 10, Name: rewardSeedFailureSchemaRelease, Dir: "v10"},
	{Version: 11, Name: importedPoolBlockCountSchemaRelease, Dir: "v11"},
	{Version: 12, Name: poolDepositHeldSchemaRelease, Dir: "v12"},
	{Version: 13, Name: pointerAddressStakeSchemaRelease, Dir: "v13"},
	{Version: 14, Name: collateralAssociationSchemaRelease, Dir: "v14"},
	{
		Version: 15,
		Name:    rewardStakeVersionRestampSchemaRelease,
		Dir:     "v15",
	},
	{
		Version: 16,
		Name:    governanceProposalOptionalAnchorSchemaRelease,
		Dir:     "v16",
	},
	{
		Version: 17,
		Name:    governanceProposalDroppedSchemaRelease,
		Dir:     "v17",
	},
	{
		Version: 18,
		Name:    rewardSnapshotExcludedStakeSchemaRelease,
		Dir:     "v18",
	},
	{
		Version: 19,
		Name:    assetNameHexColumnDropSchemaRelease,
		Dir:     "v19",
	},
	{
		Version: 20,
		Name:    alonzoPParamsUnitSchemaRelease,
		Dir:     "v20",
	},
	{
		Version: 21,
		Name:    assetAmountFingerprintIndexDropSchemaRelease,
		Dir:     "v21",
	},
	{
		Version: 22,
		Name:    governanceVoteHistorySchemaRelease,
		Dir:     "v22",
	},
	{
		Version: 23,
		Name:    committeeZeroQuorumSchemaRelease,
		Dir:     "v23",
	},
	{Version: 24, Name: drepExpiryHistorySchemaRelease, Dir: "v24"},
	{Version: 25, Name: drepDormancyStateSchemaRelease, Dir: "v25"},
	{Version: 26, Name: drepDelegatorStateSchemaRelease, Dir: "v26"},
}

// SQLiteRegistry returns the checked-in SQLite migration registry.
func SQLiteRegistry() ([]Migration, error) {
	return registryForDialect("sqlite")
}

// PostgresRegistry and MySQLRegistry expose the same v1alpha1 schema contract
// with backend-native type and identity syntax. Keeping the migration
// registry shared prevents the three providers from drifting at the schema
// boundary while allowing each engine to execute its own DDL.
func PostgresRegistry() ([]Migration, error) {
	return registryForDialect("postgres")
}

func MySQLRegistry() ([]Migration, error) {
	return registryForDialect("mysql")
}

func registryForDialect(dialect string) ([]Migration, error) {
	loaded := make([]SQL, 0, len(schemaVersions))
	// Every CREATE TABLE lives in the initial version, and MySQL's translation
	// reads them to learn which columns are blobs. A later version indexing an
	// existing blob column carries no CREATE TABLE of its own, so it is
	// translated against the schema as a whole rather than against its own
	// statements -- otherwise MySQL would be handed a BLOB key with no prefix
	// length, which it rejects.
	var wholeSchema []string
	for _, version := range schemaVersions {
		expand, err := loadSQL(version.Dir + "/sqlite/expand.sql")
		if err != nil {
			return nil, err
		}
		contract, err := loadOptionalSQL(version.Dir + "/sqlite/contract.sql")
		if err != nil {
			return nil, err
		}
		loaded = append(loaded, SQL{Expand: expand, Contract: contract})
		wholeSchema = append(wholeSchema, expand...)
		wholeSchema = append(wholeSchema, contract...)
	}

	ret := make([]Migration, 0, len(schemaVersions))
	for index, version := range schemaVersions {
		sqlForDialect := loaded[index]
		if dialect != "sqlite" {
			nativeExpand, nativeExpandErr := loadOptionalSQL(
				version.Dir + "/" + dialect + "/expand.sql",
			)
			nativeContract, nativeContractErr := loadOptionalSQL(
				version.Dir + "/" + dialect + "/contract.sql",
			)
			if nativeExpandErr != nil {
				return nil, nativeExpandErr
			}
			if nativeContractErr != nil {
				return nil, nativeContractErr
			}
			if nativeExpand != nil || nativeContract != nil {
				sqlForDialect = SQL{
					Expand:   nativeExpand,
					Contract: nativeContract,
				}
				ret = append(ret, Migration{
					Version:          version.Version,
					Name:             version.Name,
					BackfillRevision: "none",
					SQL: map[string]SQL{
						dialect: sqlForDialect,
					},
				})
				continue
			}
			sqlForDialect.Expand = translateSchemaSQLInSchema(
				loaded[index].Expand,
				dialect,
				wholeSchema,
			)
			sqlForDialect.Contract = translateSchemaSQLInSchema(
				loaded[index].Contract,
				dialect,
				wholeSchema,
			)
		}
		migration := Migration{
			Version:          version.Version,
			Name:             version.Name,
			BackfillRevision: "none",
			SQL: map[string]SQL{
				dialect: sqlForDialect,
			},
		}
		if version.Name == committeeTermStartPresenceSchemaRelease {
			migration.BackfillRevision = "1"
			migration.Backfill = committeeTermStartBackfill
		}
		if version.Name == poolDepositHeldSchemaRelease {
			migration.BackfillRevision = "1"
			migration.Backfill = poolDepositHeldBackfill
		}
		if version.Name == rewardStakeVersionRestampSchemaRelease {
			migration.BackfillRevision = "1"
			migration.Backfill = rewardStakeVersionRestampBackfill
		}
		if version.Name == governanceProposalDroppedSchemaRelease {
			migration.BackfillRevision = "1"
			migration.Backfill = governanceProposalDroppedBackfill
		}
		if version.Name == alonzoPParamsUnitSchemaRelease {
			migration.BackfillRevision = "1"
			migration.Backfill = alonzoPParamsUnitBackfill
		}
		ret = append(ret, migration)
	}
	return ret, nil
}

func alonzoPParamsUnitBackfill(
	ctx context.Context,
	batch Batch,
) (BatchResult, error) {
	var existing string
	err := batch.Tx.QueryRowContext(
		ctx,
		batch.Rebind(`SELECT value FROM node_settings_gate WHERE name = ?`),
		nodesettings.AlonzoPParamsUnitGateName,
	).Scan(&existing)
	switch {
	case err == nil:
		return BatchResult{Done: true}, nil
	case !errors.Is(err, sql.ErrNoRows):
		return BatchResult{}, fmt.Errorf(
			"read Alonzo protocol-parameter unit marker: %w",
			err,
		)
	}

	var alonzoRows int64
	if err := batch.Tx.QueryRowContext(
		ctx,
		batch.Rebind(`SELECT COUNT(*) FROM pparams WHERE era_id = ?`),
		alonzoEraID,
	).Scan(&alonzoRows); err != nil {
		return BatchResult{}, fmt.Errorf(
			"classify persisted Alonzo protocol parameters: %w",
			err,
		)
	}
	unit := nodesettings.AlonzoPParamsUnitWordV1
	if alonzoRows > 0 {
		unit = nodesettings.AlonzoPParamsUnitLegacyByteV0
	}
	if _, err := batch.Tx.ExecContext(
		ctx,
		batch.Rebind(`INSERT INTO node_settings_gate (
    name, value, recorded_epoch, recorded_slot
) VALUES (?, ?, 0, 0)`),
		nodesettings.AlonzoPParamsUnitGateName,
		unit,
	); err != nil {
		return BatchResult{}, fmt.Errorf(
			"record Alonzo protocol-parameter unit marker: %w",
			err,
		)
	}
	return BatchResult{Rows: 1, Done: true}, nil
}

// governanceProposalDroppedBackfill records every proposal this database
// already expired as also already dropped, stamping the drop at the expiry it
// was refunded at.
//
// Before v17 the epoch tick refunded an expired proposal's deposit in the
// same tick that marked it expired, so on an upgraded database every
// `expired_epoch` row has had its deposit returned. The new drop step selects
// on `dropped_epoch IS NULL`, which without this backfill matches all of
// them and refunds each a second time at the first boundary after the
// upgrade. Stamping dropped_epoch/dropped_slot from expired_epoch/
// expired_slot also keeps rollback consistent, since
// DeleteGovernanceProposalsAfterSlot clears both by the same slot bound.
//
// The proposal ID cursor makes each batch independently resumable, and the
// NOT EXISTS predicate makes replay non-destructive.
func governanceProposalDroppedBackfill(
	ctx context.Context,
	batch Batch,
) (BatchResult, error) {
	lastID := int64(0)
	if batch.Cursor != "" {
		parsed, err := strconv.ParseInt(batch.Cursor, 10, 64)
		if err != nil {
			return BatchResult{}, fmt.Errorf(
				"parse governance proposal drop backfill cursor: %w",
				err,
			)
		}
		lastID = parsed
	}
	rows, err := batch.Tx.QueryContext(ctx, batch.Rebind(`
SELECT id, expired_epoch, expired_slot FROM governance_proposal
WHERE id > ? AND expired_epoch IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM governance_proposal_drop
    WHERE governance_proposal_drop.proposal_id = governance_proposal.id
  )
ORDER BY id LIMIT ?`), lastID, batch.Limit)
	if err != nil {
		return BatchResult{}, err
	}
	defer rows.Close()
	type droppedRow struct {
		id    int64
		epoch sql.NullInt64
		slot  sql.NullInt64
	}
	var pending []droppedRow
	for rows.Next() {
		var row droppedRow
		if err := rows.Scan(&row.id, &row.epoch, &row.slot); err != nil {
			return BatchResult{}, err
		}
		pending = append(pending, row)
	}
	if err := rows.Err(); err != nil {
		return BatchResult{}, err
	}
	if len(pending) == 0 {
		return BatchResult{Cursor: batch.Cursor, Done: true}, nil
	}
	for _, row := range pending {
		if _, err := batch.Tx.ExecContext(ctx, batch.Rebind(`
INSERT INTO governance_proposal_drop (
    proposal_id, dropped_epoch, dropped_slot
) VALUES (?, ?, ?)`), row.id, row.epoch, row.slot); err != nil {
			return BatchResult{}, err
		}
	}
	return BatchResult{
		Cursor: strconv.FormatInt(pending[len(pending)-1].id, 10),
		Rows:   int64(len(pending)),
	}, nil
}

type poolDepositPosition struct {
	slot, blockIndex, certIndex int64
}

type poolDepositRegistration struct {
	id              int64
	position        poolDepositPosition
	historyComplete bool
	held            sql.NullString
	amount          sql.NullString
}

type poolDepositRetirement struct {
	position        poolDepositPosition
	historyComplete bool
	epoch           int64
}

func poolDepositPositionBeforeOrEqual(a, b poolDepositPosition) bool {
	if a.slot != b.slot {
		return a.slot < b.slot
	}
	if a.blockIndex != b.blockIndex {
		return a.blockIndex < b.blockIndex
	}
	return a.certIndex <= b.certIndex
}

// poolDepositHeldBackfill reconstructs psDeposits from the persisted
// registration and retirement history. A registration after a completed reap
// starts a new deposit cycle; all other registrations carry the preceding
// cycle's held amount. The pool ID cursor makes each batch independently
// resumable, and the NULL predicate makes replay non-destructive.
func poolDepositHeldBackfill(
	ctx context.Context,
	batch Batch,
) (BatchResult, error) {
	lastID := int64(0)
	if batch.Cursor != "" {
		parsed, err := strconv.ParseInt(batch.Cursor, 10, 64)
		if err != nil {
			return BatchResult{}, fmt.Errorf(
				"parse pool deposit backfill cursor: %w",
				err,
			)
		}
		lastID = parsed
	}
	rows, err := batch.Tx.QueryContext(ctx, batch.Rebind(
		"SELECT id FROM pool WHERE id > ? ORDER BY id LIMIT ?",
	), lastID, batch.Limit)
	if err != nil {
		return BatchResult{}, err
	}
	defer rows.Close()
	var poolIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return BatchResult{}, err
		}
		poolIDs = append(poolIDs, id)
	}
	if err := rows.Err(); err != nil {
		return BatchResult{}, err
	}
	if len(poolIDs) == 0 {
		return BatchResult{Cursor: batch.Cursor, Done: true}, nil
	}
	for _, poolID := range poolIDs {
		if err := backfillPoolDeposits(ctx, batch, poolID); err != nil {
			return BatchResult{}, err
		}
	}
	return BatchResult{
		Cursor: strconv.FormatInt(poolIDs[len(poolIDs)-1], 10),
		Rows:   int64(len(poolIDs)),
	}, nil
}

func backfillPoolDeposits(
	ctx context.Context,
	batch Batch,
	poolID int64,
) error {
	regs, err := poolDepositRegistrations(ctx, batch, poolID)
	if err != nil {
		return err
	}
	rets, err := poolDepositRetirements(ctx, batch, poolID)
	if err != nil {
		return err
	}
	var previous poolDepositRegistration
	var havePrevious bool
	for _, reg := range regs {
		if reg.held.Valid {
			if _, err := parsePoolDeposit(reg.held.String); err != nil {
				return fmt.Errorf(
					"pool %d registration %d: %w",
					poolID,
					reg.id,
					err,
				)
			}
			previous = reg
			havePrevious = true
			continue
		}
		var held *uint64
		unknownPrevious := false
		if !havePrevious {
			held, err = parsePoolDepositValue(reg.amount)
			unknownPrevious = held == nil
		} else {
			held, err = previousHeld(previous)
		}
		if err != nil {
			return fmt.Errorf(
				"pool %d registration %d: %w",
				poolID,
				reg.id,
				err,
			)
		}
		if havePrevious {
			retirement, found := latestPoolRetirement(rets, reg.position)
			if found &&
				poolDepositPositionBeforeOrEqual(
					previous.position,
					retirement.position,
				) {
				epoch, resolved, epochErr := poolDepositEpochAtSlot(
					ctx,
					batch,
					reg.position.slot,
				)
				if epochErr != nil {
					return epochErr
				}
				if !resolved {
					return fmt.Errorf(
						"resync required: cannot reconstruct pool %d registration %d without epoch history at slot %d",
						poolID,
						reg.id,
						reg.position.slot,
					)
				}
				if epoch >= retirement.epoch {
					held, err = parsePoolDepositValue(reg.amount)
				} else {
					held, err = previousHeld(previous)
				}
			}
		}
		if err != nil {
			return fmt.Errorf(
				"pool %d registration %d: %w",
				poolID,
				reg.id,
				err,
			)
		}
		if held == nil {
			previous = reg
			havePrevious = true
			continue
		}
		if _, err := batch.Tx.ExecContext(ctx, batch.Rebind(
			"UPDATE pool_registration SET deposit_held = ? WHERE id = ? AND deposit_held IS NULL",
		), strconv.FormatUint(*held, 10), reg.id); err != nil {
			return fmt.Errorf(
				"pool %d registration %d: write held deposit: %w",
				poolID,
				reg.id,
				err,
			)
		}
		if !unknownPrevious {
			reg.held = sql.NullString{
				String: strconv.FormatUint(*held, 10),
				Valid:  true,
			}
		}
		previous = reg
		havePrevious = true
	}
	return nil
}

func poolDepositRegistrations(
	ctx context.Context,
	batch Batch,
	poolID int64,
) ([]poolDepositRegistration, error) {
	rows, err := batch.Tx.QueryContext(ctx, batch.Rebind(`
SELECT pr.id, pr.added_slot, COALESCE(t.block_index, 0),
       COALESCE(c.cert_index, 0),
       CASE WHEN pr.certificate_id IS NULL OR pr.certificate_id = 0
            OR (c.id IS NOT NULL AND t.id IS NOT NULL) THEN 1 ELSE 0 END,
       pr.deposit_held, pr.deposit_amount
FROM pool_registration pr
LEFT JOIN certs c ON c.id = pr.certificate_id
LEFT JOIN "transaction" t ON t.id = c.transaction_id
WHERE pr.pool_id = ?
ORDER BY pr.added_slot,
         CASE WHEN pr.certificate_id IS NULL OR pr.certificate_id = 0 THEN 1 ELSE 0 END,
         COALESCE(t.block_index, 0), COALESCE(c.cert_index, 0), pr.id`), poolID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ret []poolDepositRegistration
	for rows.Next() {
		var row poolDepositRegistration
		if err := rows.Scan(
			&row.id,
			&row.position.slot,
			&row.position.blockIndex,
			&row.position.certIndex,
			&row.historyComplete,
			&row.held,
			&row.amount,
		); err != nil {
			return nil, err
		}
		if !row.historyComplete {
			return nil, fmt.Errorf(
				"resync required: incomplete certificate history for pool %d registration %d",
				poolID,
				row.id,
			)
		}
		ret = append(ret, row)
	}
	return ret, rows.Err()
}

func poolDepositRetirements(
	ctx context.Context,
	batch Batch,
	poolID int64,
) ([]poolDepositRetirement, error) {
	rows, err := batch.Tx.QueryContext(ctx, batch.Rebind(`
SELECT rt.added_slot, COALESCE(t.block_index, 0), COALESCE(c.cert_index, 0),
       CASE WHEN rt.certificate_id IS NULL OR rt.certificate_id = 0
            OR (c.id IS NOT NULL AND t.id IS NOT NULL) THEN 1 ELSE 0 END,
       rt.epoch
FROM pool_retirement rt
LEFT JOIN certs c ON c.id = rt.certificate_id
LEFT JOIN "transaction" t ON t.id = c.transaction_id
WHERE rt.pool_id = ?
	ORDER BY rt.added_slot, COALESCE(t.block_index, 0),
		CASE WHEN rt.certificate_id = 0 THEN 1 ELSE 0 END,
		COALESCE(c.cert_index, 0)`), poolID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ret []poolDepositRetirement
	for rows.Next() {
		var row poolDepositRetirement
		if err := rows.Scan(
			&row.position.slot,
			&row.position.blockIndex,
			&row.position.certIndex,
			&row.historyComplete,
			&row.epoch,
		); err != nil {
			return nil, err
		}
		if !row.historyComplete {
			return nil, fmt.Errorf(
				"resync required: incomplete certificate history for pool %d retirement at slot %d",
				poolID,
				row.position.slot,
			)
		}
		ret = append(ret, row)
	}
	return ret, rows.Err()
}

func latestPoolRetirement(
	retirements []poolDepositRetirement,
	at poolDepositPosition,
) (poolDepositRetirement, bool) {
	var latest poolDepositRetirement
	found := false
	for _, retirement := range retirements {
		if poolDepositPositionBeforeOrEqual(retirement.position, at) {
			latest, found = retirement, true
		}
	}
	return latest, found
}

func poolDepositEpochAtSlot(
	ctx context.Context,
	batch Batch,
	slot int64,
) (int64, bool, error) {
	var epoch, start, length sql.NullInt64
	err := batch.Tx.QueryRowContext(ctx, batch.Rebind(`
SELECT epoch_id, start_slot, length_in_slots FROM epoch
WHERE start_slot <= ? ORDER BY start_slot DESC LIMIT 1`), slot).Scan(&epoch, &start, &length)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf(
			"resolve epoch for pool deposit backfill at slot %d: %w",
			slot,
			err,
		)
	}
	if !epoch.Valid || !start.Valid || !length.Valid ||
		slot >= start.Int64+length.Int64 {
		return 0, false, nil
	}
	return epoch.Int64, true, nil
}

func parsePoolDeposit(value string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid deposit %q: %w", value, err)
	}
	return parsed, nil
}

func parsePoolDepositValue(value sql.NullString) (*uint64, error) {
	if !value.Valid || value.String == "" {
		return nil, nil
	}
	parsed, err := parsePoolDeposit(value.String)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func previousHeld(reg poolDepositRegistration) (*uint64, error) {
	if !reg.held.Valid {
		return parsePoolDepositValue(reg.amount)
	}
	parsed, err := parsePoolDeposit(reg.held.String)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

// committeeTermStartBackfill is deliberately data-driven rather than a
// migration SQL statement. The expand phase can therefore be retried after a
// process dies immediately after adding the column; rows are selected by ID
// and each committed batch advances the durable migration cursor.
func committeeTermStartBackfill(
	ctx context.Context,
	batch Batch,
) (BatchResult, error) {
	lastID := int64(0)
	if batch.Cursor != "" {
		parsed, err := strconv.ParseInt(batch.Cursor, 10, 64)
		if err != nil {
			return BatchResult{}, fmt.Errorf(
				"parse committee backfill cursor: %w",
				err,
			)
		}
		lastID = parsed
	}
	rows, err := batch.Tx.QueryContext(ctx,
		batch.Rebind(
			"SELECT id FROM committee_member WHERE id > ? AND NOT term_start_slot_set ORDER BY id LIMIT ?",
		),
		lastID, batch.Limit,
	)
	if err != nil {
		return BatchResult{}, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return BatchResult{}, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return BatchResult{}, err
	}
	if len(ids) == 0 {
		return BatchResult{Cursor: batch.Cursor, Done: true}, nil
	}
	for _, id := range ids {
		if _, err := batch.Tx.ExecContext(ctx,
			batch.Rebind(
				"UPDATE committee_member SET term_start_slot_set = TRUE WHERE id = ?",
			),
			id,
		); err != nil {
			return BatchResult{}, err
		}
	}
	return BatchResult{
		Cursor: strconv.FormatInt(ids[len(ids)-1], 10),
		Rows:   int64(len(ids)),
	}, nil
}

const (
	restampCursorPoolPhase   = "P"
	restampCursorRewardPhase = "R"
)

// rewardStakeVersionRestampBackfill re-stamps snapshot rows a prior
// RewardStakeCalculationVersion bump left behind, in two phases encoded in
// the cursor ("P:<id>" then "R:<id>"), so upgrading in place only forces a
// rebootstrap for the epochs the version bump actually changed (dingo #4026).
//
// pool_stake_snapshot's stored values never depended on calculation version
// -- see the TotalActiveStake comment in ledger/snapshot/rotation.go -- so
// every stale row there is re-stamped unconditionally. reward_snapshot's
// mark-type TotalActiveStake did change (a version-1 row understates it for
// any epoch that excluded a pool from reward-input distribution); a stale
// row is only re-stamped when it already agrees with the epoch's
// independently-recorded epoch_summary.total_active_stake, which never
// depended on calculation version either. A row that disagrees is left
// stale on purpose: StaleConsensusStakeSnapshotsExist still fails closed on
// it, because reconstructing the correct value requires replaying that
// epoch's stake distribution, which this offline, row-local backfill cannot
// do.
func rewardStakeVersionRestampBackfill(
	ctx context.Context,
	batch Batch,
) (BatchResult, error) {
	phase, lastID, err := parseRestampCursor(batch.Cursor)
	if err != nil {
		return BatchResult{}, err
	}
	if phase == restampCursorPoolPhase {
		lastID, rowCount, done, err := restampPoolStakeSnapshotBatch(
			ctx, batch, lastID,
		)
		if err != nil {
			return BatchResult{}, err
		}
		if !done {
			return BatchResult{
				Cursor: formatRestampCursor(restampCursorPoolPhase, lastID),
				Rows:   rowCount,
			}, nil
		}
		// Pool phase exhausted; hand off to the reward phase from the start.
		return BatchResult{
			Cursor: formatRestampCursor(restampCursorRewardPhase, 0),
			Rows:   rowCount,
		}, nil
	}
	lastID, rowCount, done, err := restampRewardSnapshotBatch(
		ctx, batch, lastID,
	)
	if err != nil {
		return BatchResult{}, err
	}
	return BatchResult{
		Cursor: formatRestampCursor(restampCursorRewardPhase, lastID),
		Rows:   rowCount,
		Done:   done,
	}, nil
}

func parseRestampCursor(cursor string) (string, int64, error) {
	if cursor == "" {
		return restampCursorPoolPhase, 0, nil
	}
	phase, idPart, ok := strings.Cut(cursor, ":")
	if !ok {
		return "", 0, fmt.Errorf(
			"parse reward stake version restamp cursor: %q",
			cursor,
		)
	}
	id, err := strconv.ParseInt(idPart, 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf(
			"parse reward stake version restamp cursor: %w",
			err,
		)
	}
	return phase, id, nil
}

func formatRestampCursor(phase string, lastID int64) string {
	return phase + ":" + strconv.FormatInt(lastID, 10)
}

// restampPoolStakeSnapshotBatch re-stamps up to one batch of stale
// pool_stake_snapshot rows. Their stored totals never depended on
// calculation version, so every stale row found is safe to re-stamp.
func restampPoolStakeSnapshotBatch(
	ctx context.Context,
	batch Batch,
	lastID int64,
) (int64, int64, bool, error) {
	rows, err := batch.Tx.QueryContext(ctx, batch.Rebind(
		"SELECT id FROM pool_stake_snapshot WHERE id > ? AND calculation_version <> ? ORDER BY id LIMIT ?",
	), lastID, models.RewardStakeCalculationVersion, batch.Limit)
	if err != nil {
		return 0, 0, false, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return 0, 0, false, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return 0, 0, false, err
	}
	if len(ids) == 0 {
		return lastID, 0, true, nil
	}
	for _, id := range ids {
		if _, err := batch.Tx.ExecContext(ctx,
			batch.Rebind(
				"UPDATE pool_stake_snapshot SET calculation_version = ? WHERE id = ?",
			),
			models.RewardStakeCalculationVersion, id,
		); err != nil {
			return 0, 0, false, err
		}
	}
	return ids[len(ids)-1], int64(len(ids)), false, nil
}

// restampRewardSnapshotBatch re-stamps up to one batch of stale mark-type
// reward_snapshot rows whose total_active_stake already agrees with the same
// epoch's epoch_summary.total_active_stake -- the value that never depended
// on calculation version. A row that disagrees names an epoch the version
// bump actually changed and is left stale; StaleConsensusStakeSnapshotsExist
// still fails closed on it.
func restampRewardSnapshotBatch(
	ctx context.Context,
	batch Batch,
	lastID int64,
) (int64, int64, bool, error) {
	rows, err := batch.Tx.QueryContext(ctx, batch.Rebind(`
SELECT reward_snapshot.id, reward_snapshot.total_active_stake,
       epoch_summary.total_active_stake
FROM reward_snapshot
LEFT JOIN epoch_summary ON epoch_summary.epoch = reward_snapshot.epoch
WHERE reward_snapshot.id > ?
  AND reward_snapshot.snapshot_type = 'mark'
  AND reward_snapshot.calculation_version <> ?
ORDER BY reward_snapshot.id LIMIT ?`),
		lastID, models.RewardStakeCalculationVersion, batch.Limit,
	)
	if err != nil {
		return 0, 0, false, err
	}
	defer rows.Close()
	var (
		seenIDs []int64
		safeIDs []int64
	)
	for rows.Next() {
		var id int64
		var rewardTotal string
		var epochTotal sql.NullString
		if err := rows.Scan(&id, &rewardTotal, &epochTotal); err != nil {
			return 0, 0, false, err
		}
		seenIDs = append(seenIDs, id)
		if epochTotal.Valid && epochTotal.String == rewardTotal {
			safeIDs = append(safeIDs, id)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, false, err
	}
	if len(seenIDs) == 0 {
		return lastID, 0, true, nil
	}
	for _, id := range safeIDs {
		if _, err := batch.Tx.ExecContext(ctx,
			batch.Rebind(
				"UPDATE reward_snapshot SET calculation_version = ? WHERE id = ?",
			),
			models.RewardStakeCalculationVersion, id,
		); err != nil {
			return 0, 0, false, err
		}
	}
	return seenIDs[len(seenIDs)-1], int64(len(seenIDs)), false, nil
}

var (
	autoIncrementType = regexp.MustCompile(
		`(?i)integer PRIMARY KEY AUTOINCREMENT`,
	)
	wordType = func(word string) *regexp.Regexp {
		return regexp.MustCompile(`(?i)\b` + word + `\b`)
	}
	mysqlIndexPattern = regexp.MustCompile(
		`(?is)^(CREATE\s+(?:UNIQUE\s+)?INDEX\s+.*?\s+ON\s+` +
			"`?([a-zA-Z0-9_]+)`?" + `\s*)\(([^)]*)\)$`,
	)
	mysqlBlobColumnPattern = regexp.MustCompile(
		"(?i)`([^`]+)`\\s+(?:blob|text)\\b",
	)
	mysqlForeignKeyPattern = regexp.MustCompile(
		"(?i)FOREIGN\\s+KEY\\s*\\(`([^`]+)`\\)\\s+REFERENCES\\s+`([^`]+)`\\s*\\(`([^`]+)`\\)",
	)
	mysqlInlineKeyPattern = regexp.MustCompile(
		`(?is)(PRIMARY\s+KEY|UNIQUE(?:\s+KEY)?)\s*\(([^)]*)\)`,
	)
	mysqlTextDefaultPattern = regexp.MustCompile(
		`(?i)\btext(\s+NOT\s+NULL)?\s+DEFAULT\s+'0'`,
	)
)

// translateSchemaSQLInSchema translates statements for a dialect, deriving
// MySQL's column typing from schemaStatements rather than from the statements
// being translated. The two differ for any migration that alters a table it
// did not create; see registryForDialect.
func translateSchemaSQLInSchema(
	statements []string,
	dialect string,
	schemaStatements []string,
) []string {
	translated := make([]string, len(statements))
	mysqlBlobColumns := make(map[string]map[string]struct{})
	mysqlForeignKeyColumns := make(map[string]map[string]struct{})
	if dialect == "mysql" {
		for _, statement := range schemaStatements {
			if !strings.HasPrefix(strings.ToUpper(statement), "CREATE TABLE") {
				continue
			}
			table := schemaTableName(statement)
			if table == "" {
				continue
			}
			columns := make(map[string]struct{})
			for _, match := range mysqlBlobColumnPattern.FindAllStringSubmatch(statement, -1) {
				columns[match[1]] = struct{}{}
			}
			mysqlBlobColumns[table] = columns
			for _, match := range mysqlForeignKeyPattern.FindAllStringSubmatch(statement, -1) {
				addColumn(mysqlForeignKeyColumns, table, match[1])
				addColumn(mysqlForeignKeyColumns, match[2], match[3])
			}
		}
		for table, columns := range mysqlForeignKeyColumns {
			for column := range columns {
				delete(mysqlBlobColumns[table], column)
			}
		}
	}
	for index, statement := range statements {
		value := statement
		switch dialect {
		case "postgres":
			value = strings.ReplaceAll(value, "`", `"`)
			value = autoIncrementType.ReplaceAllString(
				value,
				"BIGSERIAL PRIMARY KEY",
			)
			// SQLite INTEGER is used for metadata IDs and foreign-key columns.
			// Widen all integer columns so PostgreSQL foreign keys retain matching
			// BIGINT types after the AUTOINCREMENT rewrite above.
			value = wordType("integer").ReplaceAllString(value, "BIGINT")
			value = wordType("blob").ReplaceAllString(value, "BYTEA")
			value = wordType("datetime").ReplaceAllString(value, "TIMESTAMPTZ")
			value = wordType("numeric").ReplaceAllString(value, "BOOLEAN")
			value = strings.ReplaceAll(value, `DEFAULT "0"`, "DEFAULT '0'")
		case "mysql":
			value = autoIncrementType.ReplaceAllString(
				value,
				"BIGINT AUTO_INCREMENT PRIMARY KEY",
			)
			value = wordType("integer").ReplaceAllString(value, "BIGINT")
			value = wordType("numeric").ReplaceAllString(value, "BOOLEAN")
			value = strings.ReplaceAll(value, `DEFAULT "0"`, "DEFAULT '0'")
			value = mysqlTextDefaultPattern.ReplaceAllString(
				value,
				"VARCHAR(255)$1 DEFAULT '0'",
			)
			value = strings.ReplaceAll(
				value,
				"CREATE INDEX IF NOT EXISTS",
				"CREATE INDEX",
			)
			value = strings.ReplaceAll(
				value,
				"CREATE UNIQUE INDEX IF NOT EXISTS",
				"CREATE UNIQUE INDEX",
			)
			value = strings.ReplaceAll(
				value,
				"DROP INDEX IF EXISTS `idx_committee_member_cold_cred_hash`",
				"DROP INDEX `idx_committee_member_cold_cred_hash` ON `committee_member`",
			)
			value = strings.ReplaceAll(
				value,
				"DROP INDEX IF EXISTS `idx_asset_name_hex`",
				"DROP INDEX `idx_asset_name_hex` ON `asset`",
			)
			value = strings.ReplaceAll(
				value,
				"DROP INDEX IF EXISTS `idx_asset_amount`",
				"DROP INDEX `idx_asset_amount` ON `asset`",
			)
			value = strings.ReplaceAll(
				value,
				"DROP INDEX IF EXISTS `idx_asset_fingerprint`",
				"DROP INDEX `idx_asset_fingerprint` ON `asset`",
			)
			if strings.HasPrefix(strings.ToUpper(statement), "CREATE TABLE") {
				for column := range mysqlForeignKeyColumns[schemaTableName(statement)] {
					value = replaceMySQLBlobType(value, column)
				}
			}
			value = translateMySQLInlineKeys(
				value,
				schemaTableName(statement),
				mysqlBlobColumns,
			)
			value = translateMySQLIndexColumns(value, mysqlBlobColumns)
		}
		translated[index] = value
	}
	return translated
}

func addColumn(columns map[string]map[string]struct{}, table, column string) {
	if columns[table] == nil {
		columns[table] = make(map[string]struct{})
	}
	columns[table][column] = struct{}{}
}

func replaceMySQLBlobType(statement, column string) string {
	pattern := regexp.MustCompile(
		"(?i)(`" + regexp.QuoteMeta(column) + "`\\s+)blob\\b",
	)
	return pattern.ReplaceAllString(statement, "${1}VARBINARY(255)")
}

func translateMySQLInlineKeys(
	statement string,
	table string,
	blobColumns map[string]map[string]struct{},
) string {
	return mysqlInlineKeyPattern.ReplaceAllStringFunc(
		statement,
		func(value string) string {
			match := mysqlInlineKeyPattern.FindStringSubmatch(value)
			columns := strings.Split(match[2], ",")
			for index, column := range columns {
				trimmed := strings.TrimSpace(column)
				name := strings.Trim(trimmed, "`")
				if _, ok := blobColumns[table][name]; ok {
					columns[index] = trimmed + "(255)"
				}
			}
			return match[1] + " (" + strings.Join(columns, ",") + ")"
		},
	)
}

func schemaTableName(statement string) string {
	parts := strings.Fields(statement)
	if len(parts) < 6 {
		return ""
	}
	return strings.Trim(parts[5], "`")
}

func translateMySQLIndexColumns(
	statement string,
	blobColumns map[string]map[string]struct{},
) string {
	match := mysqlIndexPattern.FindStringSubmatch(statement)
	if len(match) != 4 {
		return statement
	}
	columns := strings.Split(match[3], ",")
	for index, column := range columns {
		trimmed := strings.TrimSpace(column)
		name := strings.Trim(trimmed, "`")
		if _, ok := blobColumns[match[2]][name]; !ok {
			continue
		}
		columns[index] = trimmed + "(255)"
	}
	return match[1] + "(" + strings.Join(columns, ",") + ")"
}

// loadOptionalSQL loads a migration resource that a version need not ship,
// returning no statements when the file is absent. Only a missing file is
// tolerated: an unreadable or unparseable one is still an error.
//
// Existence is tested with fs.Stat rather than Open so the probe does not open
// a handle the caller then has to remember to close.
func loadOptionalSQL(path string) ([]string, error) {
	if _, err := fs.Stat(migrationSQL, path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read embedded migration %s: %w", path, err)
	}
	return loadSQL(path)
}

func loadSQL(path string) ([]string, error) {
	content, err := migrationSQL.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read embedded migration %s: %w", path, err)
	}
	// The embedded resources carry whatever bytes the working tree held at
	// build time, so a CRLF checkout would otherwise change the checksum
	// recorded in schema_migrations and make a database written by one build
	// report drift against another.
	normalized := strings.ReplaceAll(string(content), "\r\n", "\n")
	statements, err := splitSQL(normalized)
	if err != nil {
		return nil, fmt.Errorf("parse embedded migration %s: %w", path, err)
	}
	return statements, nil
}

// splitSQL is intentionally small and strict. Migration resources may contain
// semicolons in quoted values and comments, but not backend client directives.
func splitSQL(content string) ([]string, error) {
	var (
		statements   []string
		current      []byte
		quote        byte
		lineComment  bool
		blockComment bool
	)
	flush := func() {
		statement := strings.TrimSpace(string(current))
		current = nil
		if statement != "" {
			statements = append(statements, statement)
		}
	}
	for idx := 0; idx < len(content); idx++ {
		character := content[idx]
		if lineComment {
			if character == '\n' {
				lineComment = false
				current = append(current, character)
			}
			continue
		}
		if blockComment {
			if character == '*' && idx+1 < len(content) &&
				content[idx+1] == '/' {
				blockComment = false
				idx++
			}
			continue
		}
		if quote != 0 {
			current = append(current, character)
			if character == quote {
				if idx+1 < len(content) && content[idx+1] == quote {
					current = append(current, content[idx+1])
					idx++
				} else {
					quote = 0
				}
			}
			continue
		}
		if character == '-' && idx+1 < len(content) && content[idx+1] == '-' {
			appendCommentSpace(&current)
			lineComment = true
			idx++
			continue
		}
		if character == '/' && idx+1 < len(content) && content[idx+1] == '*' {
			appendCommentSpace(&current)
			blockComment = true
			idx++
			continue
		}
		switch character {
		case '\'', '"', '`':
			quote = character
			current = append(current, character)
		case ';':
			flush()
		default:
			current = append(current, character)
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated %q string", quote)
	}
	if blockComment {
		return nil, errors.New("unterminated block comment")
	}
	flush()
	return statements, nil
}

func appendCommentSpace(current *[]byte) {
	if len(*current) == 0 {
		return
	}
	last := (*current)[len(*current)-1]
	if last != ' ' && last != '\n' && last != '\r' && last != '\t' {
		*current = append(*current, ' ')
	}
}
