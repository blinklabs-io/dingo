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

//nolint:gosec // SQL INTEGER mappings preserve the existing unsigned domain API.
package sqlstore

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

const sqliteUtxoColumns = "transaction_id, collateral_return_for_tx_id, " +
	"tx_id, payment_key, staking_key, credential_tag, datum_hash, " +
	"spent_at_tx_id, referenced_by_tx_id, collateral_by_tx_id, id, " +
	"added_slot, deleted_slot, amount, output_idx, payment_script"

const qualifiedSQLiteUtxoColumns = "utxo.transaction_id, " +
	"utxo.collateral_return_for_tx_id, utxo.tx_id, utxo.payment_key, " +
	"utxo.staking_key, utxo.credential_tag, utxo.datum_hash, " +
	"utxo.spent_at_tx_id, utxo.referenced_by_tx_id, " +
	"utxo.collateral_by_tx_id, utxo.id, utxo.added_slot, " +
	"utxo.deleted_slot, utxo.amount, utxo.output_idx, " +
	"utxo.payment_script"

func (s *Store) CreateUtxo(txn types.Txn, utxo *models.Utxo) error {
	if utxo == nil {
		return errors.New("create UTxO: UTxO is nil")
	}
	err := s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			q := s.operationalQueries(db)
			params, err := createUtxoParams(utxo)
			if err != nil {
				return err
			}
			id, err := q.CreateUtxo(ctx, params)
			if err != nil {
				return err
			}
			utxo.ID = uint(id)
			// A pointer address carries its stake reference as a
			// position rather than a credential, so it lives in
			// utxo_pointer rather than in a utxo column. Writing the
			// utxo row alone would silently drop it -- the same
			// omission insertUtxoModel avoids on the block-apply path.
			if err := persistUtxoPointer(
				ctx, db, id, utxo.Pointer,
			); err != nil {
				return err
			}
			for i := range utxo.Assets {
				asset := &utxo.Assets[i]
				asset.UtxoID = utxo.ID
				assetID, err := q.CreateAsset(
					ctx,
					sqlitequery.CreateAssetParams{
						Name:        asset.Name,
						NameHex:     asset.NameHex,
						PolicyID:    asset.PolicyId,
						Fingerprint: asset.Fingerprint,
						UtxoID: sql.NullInt64{
							Int64: id,
							Valid: true,
						},
						Amount: sql.NullString{
							String: decimalUint64(asset.Amount),
							Valid:  true,
						},
					},
				)
				if err != nil {
					return err
				}
				asset.ID = uint(assetID)
			}
			return s.refreshRewardLiveStakeAggregate(
				ctx,
				db,
				models.NewStakeCredentialRef(
					utxo.CredentialTag,
					utxo.StakingKey,
				),
				utxo.AddedSlot,
			)
		},
	)
	if err != nil {
		return fmt.Errorf("create UTxO: %w", err)
	}
	return nil
}

func (s *Store) DeleteUtxo(
	utxoID models.UtxoId,
	txn types.Txn,
) error {
	return s.DeleteUtxos([]models.UtxoId{utxoID}, txn)
}

func (s *Store) DeleteUtxos(
	utxoIDs []models.UtxoId,
	txn types.Txn,
) error {
	if len(utxoIDs) == 0 {
		return nil
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			slot, err := currentTipSlot(ctx, db)
			if err != nil {
				return err
			}
			refs, err := queryUtxoStakeRefs(ctx, db, utxoIDs, true)
			if err != nil {
				return err
			}
			chunkSize := s.dialect.ParameterLimit() / 2
			for start := 0; start < len(utxoIDs); start += chunkSize {
				end := min(start+chunkSize, len(utxoIDs))
				predicate, args := utxoIDPredicate(utxoIDs[start:end])
				if _, err := db.ExecContext(
					ctx,
					s.dialect.Rebind("DELETE FROM utxo WHERE "+predicate),
					args...,
				); err != nil {
					return err
				}
			}
			return s.refreshRewardLiveStakeRefs(ctx, db, refs, slot)
		},
	)
}

func (s *Store) DeleteUtxosAfterSlot(
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
			refs, err := queryStakeRefs(
				ctx,
				db,
				"SELECT DISTINCT credential_tag, staking_key FROM utxo "+
					"WHERE added_slot > ?",
				slotValue,
			)
			if err != nil {
				return err
			}
			if _, err := db.ExecContext(
				ctx,
				"DELETE FROM utxo WHERE added_slot > ?",
				slotValue,
			); err != nil {
				return err
			}
			return s.refreshRewardLiveStakeRefs(ctx, db, refs, slot)
		},
	)
}

func (s *Store) SetUtxoDeletedAtSlot(
	input ledger.TransactionInput,
	slot uint64,
	spenderTxHash []byte,
	txn types.Txn,
) error {
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	txID := input.Id().Bytes()
	outputIndex := input.Index()
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			result, err := db.ExecContext(ctx, `
UPDATE utxo
SET deleted_slot = ?, spent_at_tx_id = ?
WHERE tx_id = ? AND output_idx = ?
  AND spent_at_tx_id IS NULL
  AND (deleted_slot = 0 OR deleted_slot = ?)`,
				slotValue,
				spenderTxHash,
				txID,
				outputIndex,
				slotValue,
			)
			if err != nil {
				return err
			}
			affected, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if affected != 1 {
				var count int
				if err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM utxo WHERE tx_id = ? AND output_idx = ?`,
					txID,
					outputIndex,
				).Scan(&count); err != nil {
					return err
				}
				if count == 0 {
					return fmt.Errorf(
						"%w: %x:%d",
						types.ErrUtxoNotFound,
						txID,
						outputIndex,
					)
				}
				return fmt.Errorf(
					"%w: %x:%d",
					types.ErrUtxoConflict,
					txID,
					outputIndex,
				)
			}
			refs, err := queryUtxoStakeRefs(
				ctx,
				db,
				[]models.UtxoId{{Hash: txID, Idx: outputIndex}},
				false,
			)
			if err != nil {
				return err
			}
			return s.refreshRewardLiveStakeRefs(ctx, db, refs, slot)
		},
	)
}

func (s *Store) SetUtxosNotDeletedAfterSlot(
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
			refs, err := queryStakeRefs(
				ctx,
				db,
				"SELECT DISTINCT credential_tag, staking_key FROM utxo "+
					"WHERE deleted_slot > ?",
				slotValue,
			)
			if err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, `
UPDATE utxo
SET deleted_slot = 0, spent_at_tx_id = NULL
WHERE deleted_slot > ?`,
				slotValue,
			); err != nil {
				return err
			}
			return s.refreshRewardLiveStakeRefs(ctx, db, refs, slot)
		},
	)
}

func (s *Store) MarkUtxosDeletedAtSlot(
	txn types.Txn,
	refs []types.UtxoKey,
	atSlot uint64,
) error {
	if len(refs) == 0 {
		return nil
	}
	slot, err := checkedInt64(atSlot)
	if err != nil {
		return err
	}
	ids := make([]models.UtxoId, len(refs))
	for i := range refs {
		ids[i] = models.UtxoId{
			Hash: refs[i].TxId,
			Idx:  refs[i].OutputIdx,
		}
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			stakeRefs, err := queryUtxoStakeRefs(ctx, db, ids, false)
			if err != nil {
				return err
			}
			chunkSize := (s.dialect.ParameterLimit() - 1) / 2
			for start := 0; start < len(ids); start += chunkSize {
				end := min(start+chunkSize, len(ids))
				predicate, args := utxoIDPredicate(ids[start:end])
				args = append([]any{slot}, args...)
				if _, err := db.ExecContext(
					ctx,
					s.dialect.Rebind(
						"UPDATE utxo SET deleted_slot = ? "+
							"WHERE deleted_slot = 0 AND ("+predicate+")",
					),
					args...,
				); err != nil {
					return err
				}
			}
			return s.refreshRewardLiveStakeRefs(ctx, db, stakeRefs, atSlot)
		},
	)
}

func (s *Store) AddUtxos(
	utxos []models.UtxoSlot,
	txn types.Txn,
) error {
	if len(utxos) == 0 {
		return nil
	}
	items := make([]models.Utxo, len(utxos))
	for i := range utxos {
		item, err := models.UtxoLedgerToModel(utxos[i].Utxo, utxos[i].Slot)
		if err != nil {
			return fmt.Errorf(
				"convert utxo %d: %w",
				utxos[i].Utxo.Id.Index(),
				err,
			)
		}
		items[i] = item
	}
	return s.importUtxos(items, txn, false)
}

func (s *Store) ImportUtxos(
	utxos []models.Utxo,
	txn types.Txn,
) error {
	return s.importUtxos(utxos, txn, true)
}

func (s *Store) GetUtxoBalanceByAddress(
	address lcommon.Address,
	mode models.UtxoAddressMatchMode,
	txn types.Txn,
) (models.AddressBalance, error) {
	var ret models.AddressBalance
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return ret, fmt.Errorf(
			"resolve DB for utxo balance by address: %w",
			err,
		)
	}
	var predicates []string
	var args []any
	if err := models.AppendUtxoAddressOrBranchMode(
		&predicates,
		&args,
		address,
		mode,
	); err != nil {
		return ret, fmt.Errorf("utxo balance by address: %w", err)
	}
	if len(predicates) == 0 {
		return ret, nil
	}
	addressPredicate := "(" + strings.Join(predicates, " OR ") + ")"
	rows, err := db.QueryContext(ctx, s.dialect.Rebind(`
SELECT amount
FROM utxo
WHERE deleted_slot = 0 AND `+addressPredicate), args...)
	if err != nil {
		return ret, fmt.Errorf("sum utxo balance by address: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		ret.UtxoCount++
		var raw sql.NullString
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return ret, fmt.Errorf("scan utxo balance by address: %w", err)
		}
		if raw.Valid && raw.String != "" {
			amount, err := parseUint64("utxo amount", raw.String)
			if err != nil {
				rows.Close()
				return ret, err
			}
			if ^uint64(0)-ret.Lovelace < amount {
				rows.Close()
				return ret, errors.New("utxo balance overflow")
			}
			ret.Lovelace += amount
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ret, fmt.Errorf("sum utxo balance by address: %w", err)
	}
	if err := rows.Close(); err != nil {
		return ret, fmt.Errorf("sum utxo balance by address: %w", err)
	}
	if ret.UtxoCount == 0 {
		return ret, nil
	}
	rows, err = db.QueryContext(
		ctx,
		s.dialect.Rebind(`
SELECT asset.policy_id, asset.name, asset.amount
FROM utxo
JOIN asset ON asset.utxo_id = utxo.id
WHERE utxo.deleted_slot = 0 AND `+addressPredicate+`
ORDER BY asset.policy_id, asset.name`),
		args...,
	)
	if err != nil {
		return ret, fmt.Errorf(
			"sum utxo asset balances by address: %w",
			err,
		)
	}
	defer rows.Close()
	type assetKey struct{ policy, name string }
	assetIndexes := make(map[assetKey]int)
	for rows.Next() {
		var asset models.AssetBalance
		var raw sql.NullString
		if err := rows.Scan(
			&asset.PolicyId,
			&asset.Name,
			&raw,
		); err != nil {
			return ret, err
		}
		if !raw.Valid || raw.String == "" {
			continue
		}
		amount, err := parseUint64("asset amount", raw.String)
		if err != nil {
			return ret, err
		}
		key := assetKey{
			policy: string(asset.PolicyId),
			name:   string(asset.Name),
		}
		idx, ok := assetIndexes[key]
		if !ok {
			asset.Amount = amount
			assetIndexes[key] = len(ret.Assets)
			ret.Assets = append(ret.Assets, asset)
			continue
		}
		if ^uint64(0)-ret.Assets[idx].Amount < amount {
			return ret, errors.New("asset balance overflow")
		}
		ret.Assets[idx].Amount += amount
	}
	return ret, rows.Err()
}

func (s *Store) importUtxos(
	utxos []models.Utxo,
	txn types.Txn,
	hydrateProvenance bool,
) error {
	if len(utxos) == 0 {
		return nil
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			q := s.operationalQueries(db)
			refs := make(map[string]models.StakeCredentialRef)
			slots := make(map[string]uint64)
			for i := range utxos {
				item := utxos[i]
				params, err := createUtxoParams(&item)
				if err != nil {
					return err
				}
				id, err := q.CreateUtxoIfAbsent(
					ctx,
					sqlitequery.CreateUtxoIfAbsentParams(params),
				)
				if errors.Is(err, sql.ErrNoRows) {
					id, err = q.GetUtxoIDByRef(
						ctx,
						sqlitequery.GetUtxoIDByRefParams{
							TxID: item.TxId,
							OutputIdx: validInt64(
								int64(item.OutputIdx),
							),
						},
					)
					if err != nil {
						return fmt.Errorf(
							"fetch imported UTxO ID: %w",
							err,
						)
					}
					if hydrateProvenance {
						if err := hydrateImportedUtxo(
							ctx,
							db,
							&item,
						); err != nil {
							return err
						}
					}
				} else if err != nil {
					return fmt.Errorf("import UTxO: %w", err)
				}
				// See CreateUtxo: the pointer position is a separate
				// row, and persistUtxoPointer converges, so an output
				// already imported keeps the same position.
				if err := persistUtxoPointer(
					ctx, db, id, item.Pointer,
				); err != nil {
					return err
				}
				for j := range item.Assets {
					asset := item.Assets[j]
					if err := q.ImportAsset(
						ctx,
						sqlitequery.ImportAssetParams{
							Name:        asset.Name,
							NameHex:     asset.NameHex,
							PolicyID:    asset.PolicyId,
							Fingerprint: asset.Fingerprint,
							UtxoID:      validInt64(id),
							Amount: validString(
								decimalUint64(asset.Amount),
							),
						},
					); err != nil {
						return fmt.Errorf(
							"import UTxO asset: %w",
							err,
						)
					}
				}
				ref := models.NewStakeCredentialRef(
					item.CredentialTag,
					item.StakingKey,
				)
				if len(ref.Key) > 0 {
					refs[ref.MapKey()] = ref
					if item.AddedSlot > slots[ref.MapKey()] {
						slots[ref.MapKey()] = item.AddedSlot
					}
				}
			}
			for key, ref := range refs {
				if err := s.refreshRewardLiveStakeAggregate(
					ctx,
					db,
					ref,
					slots[key],
				); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func hydrateImportedUtxo(
	ctx context.Context,
	db queryer,
	utxo *models.Utxo,
) error {
	addedSlot, err := checkedInt64(utxo.AddedSlot)
	if err != nil {
		return err
	}
	if utxo.TransactionID != nil {
		if _, err := db.ExecContext(ctx, `
UPDATE utxo
SET transaction_id = ?, added_slot = ?
WHERE tx_id = ? AND output_idx = ? AND transaction_id IS NULL`,
			*utxo.TransactionID,
			addedSlot,
			utxo.TxId,
			utxo.OutputIdx,
		); err != nil {
			return fmt.Errorf("hydrate UTxO transaction provenance: %w", err)
		}
	}
	if utxo.CollateralReturnForTxID != nil {
		if _, err := db.ExecContext(ctx, `
UPDATE utxo
SET collateral_return_for_tx_id = ?, added_slot = ?
WHERE tx_id = ? AND output_idx = ?
  AND collateral_return_for_tx_id IS NULL`,
			*utxo.CollateralReturnForTxID,
			addedSlot,
			utxo.TxId,
			utxo.OutputIdx,
		); err != nil {
			return fmt.Errorf(
				"hydrate UTxO collateral-return provenance: %w",
				err,
			)
		}
	}
	return nil
}

func (s *Store) refreshRewardLiveStakeRefs(
	ctx context.Context,
	db queryer,
	refs []models.StakeCredentialRef,
	slot uint64,
) error {
	for i := range refs {
		if err := s.refreshRewardLiveStakeAggregate(ctx, db, refs[i], slot); err != nil {
			return err
		}
	}
	return nil
}

func currentTipSlot(ctx context.Context, db queryer) (uint64, error) {
	var slot sql.NullInt64
	err := db.QueryRowContext(
		ctx,
		"SELECT slot FROM tip WHERE id = 1",
	).Scan(&slot)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return uint64(slot.Int64), nil
}

func queryUtxoStakeRefs(
	ctx context.Context,
	db queryer,
	ids []models.UtxoId,
	liveOnly bool,
) ([]models.StakeCredentialRef, error) {
	ret := []models.StakeCredentialRef{}
	if len(ids) == 0 {
		return ret, nil
	}
	seen := make(map[string]struct{})
	// Two bind variables per reference; 400 keeps this portable to SQLite's
	// conservative 999-parameter configuration.
	for start := 0; start < len(ids); start += 400 {
		end := min(start+400, len(ids))
		predicate, args := utxoIDPredicate(ids[start:end])
		query := "SELECT DISTINCT credential_tag, staking_key FROM utxo WHERE (" +
			predicate + ")"
		if liveOnly {
			query += " AND deleted_slot = 0"
		}
		rows, err := queryStakeRefs(ctx, db, query, args...)
		if err != nil {
			return nil, err
		}
		for _, ref := range rows {
			if _, ok := seen[ref.MapKey()]; ok {
				continue
			}
			seen[ref.MapKey()] = struct{}{}
			ret = append(ret, ref)
		}
	}
	return ret, nil
}

func queryStakeRefs(
	ctx context.Context,
	db queryer,
	query string,
	args ...any,
) ([]models.StakeCredentialRef, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := []models.StakeCredentialRef{}
	for rows.Next() {
		var tag int64
		var key []byte
		if err := rows.Scan(&tag, &key); err != nil {
			return nil, err
		}
		if len(key) == 0 {
			continue
		}
		ret = append(ret, models.NewStakeCredentialRef(uint8(tag), key))
	}
	return ret, rows.Err()
}

func utxoIDPredicate(ids []models.UtxoId) (string, []any) {
	parts := make([]string, len(ids))
	args := make([]any, 0, len(ids)*2)
	for i := range ids {
		parts[i] = "(tx_id = ? AND output_idx = ?)"
		args = append(args, ids[i].Hash, ids[i].Idx)
	}
	return strings.Join(parts, " OR "), args
}

func (s *Store) GetUtxo(
	txID []byte,
	index uint32,
	txn types.Txn,
) (*models.Utxo, error) {
	return s.getUtxo(txID, index, txn, false)
}

func (s *Store) GetUtxoIncludingSpent(
	txID []byte,
	index uint32,
	txn types.Txn,
) (*models.Utxo, error) {
	return s.getUtxo(txID, index, txn, true)
}

func (s *Store) getUtxo(
	txID []byte,
	index uint32,
	txn types.Txn,
	includeSpent bool,
) (*models.Utxo, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	params := sqlitequery.GetLiveUtxoParams{
		TxID: txID,
		OutputIdx: sql.NullInt64{
			Int64: int64(index),
			Valid: true,
		},
	}
	var row sqlitequery.Utxo
	if includeSpent {
		row, err = q.GetUtxoIncludingSpent(
			ctx,
			sqlitequery.GetUtxoIncludingSpentParams(params),
		)
	} else {
		row, err = q.GetLiveUtxo(ctx, params)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get UTxO %x#%d: %w", txID, index, err)
	}
	ret, err := utxoFromSQLite(row)
	if err != nil {
		return nil, err
	}
	if err := s.loadUtxoAssets(ctx, db, []*models.Utxo{ret}); err != nil {
		return nil, err
	}
	return ret, nil
}

// GetUtxosByRefs retrieves multiple live UTxOs by their (tx_id, output_idx)
// references in a single batch. Refs with no matching live UTxO are simply
// absent from the result. A ref repeated in the input yields at most one
// row, keeping one-row-per-requested-ref semantics for callers.
func (s *Store) GetUtxosByRefs(
	refs []models.UtxoId,
	txn types.Txn,
) ([]models.Utxo, error) {
	ret := []models.Utxo{}
	refs = dedupeUtxoIDs(refs)
	// Two bind variables per reference; 400 keeps this portable to
	// SQLite's conservative 999-parameter configuration.
	for start := 0; start < len(refs); start += 400 {
		end := min(start+400, len(refs))
		predicate, args := utxoIDPredicate(refs[start:end])
		utxos, err := s.queryUtxosWithAssets(
			txn,
			"deleted_slot = 0 AND ("+predicate+")",
			args,
			"",
		)
		if err != nil {
			return nil, err
		}
		ret = append(ret, utxos...)
	}
	return ret, nil
}

// dedupeUtxoIDs returns ids with duplicate (Hash, Idx) pairs removed,
// preserving the order of first occurrence.
func dedupeUtxoIDs(ids []models.UtxoId) []models.UtxoId {
	seen := make(map[string]struct{}, len(ids))
	ret := make([]models.UtxoId, 0, len(ids))
	for _, id := range ids {
		key := fmt.Sprintf("%s:%d", id.Hash, id.Idx)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ret = append(ret, id)
	}
	return ret
}

func (s *Store) GetUtxosAddedAfterSlot(
	slot uint64,
	txn types.Txn,
) ([]models.Utxo, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return nil, err
	}
	rows, err := q.GetUtxosAddedAfterSlot(
		ctx,
		sql.NullInt64{Int64: sqlSlot, Valid: true},
	)
	if err != nil {
		return nil, err
	}
	return utxosFromSQLite(rows)
}

func (s *Store) GetLiveUtxosBySlot(
	slot uint64,
	txn types.Txn,
) ([]models.UtxoId, error) {
	return s.getUtxoRefsBySlot(slot, txn, true)
}

func (s *Store) GetUtxosBySlot(
	slot uint64,
	txn types.Txn,
) ([]models.UtxoId, error) {
	return s.getUtxoRefsBySlot(slot, txn, false)
}

func (s *Store) getUtxoRefsBySlot(
	slot uint64,
	txn types.Txn,
	liveOnly bool,
) ([]models.UtxoId, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return nil, err
	}
	arg := sql.NullInt64{Int64: sqlSlot, Valid: true}
	var rows []sqlitequery.GetLiveUtxoRefsBySlotRow
	if liveOnly {
		rows, err = q.GetLiveUtxoRefsBySlot(ctx, arg)
	} else {
		var all []sqlitequery.GetUtxoRefsBySlotRow
		all, err = q.GetUtxoRefsBySlot(ctx, arg)
		rows = make([]sqlitequery.GetLiveUtxoRefsBySlotRow, len(all))
		for i := range all {
			rows[i] = sqlitequery.GetLiveUtxoRefsBySlotRow(all[i])
		}
	}
	if err != nil {
		return nil, err
	}
	ret := make([]models.UtxoId, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, models.UtxoId{
			Hash: row.TxID,
			Idx:  uint32(row.OutputIdx.Int64),
		})
	}
	return ret, nil
}

func (s *Store) GetUtxosDeletedBeforeSlot(
	slot uint64,
	limit int,
	txn types.Txn,
) ([]models.Utxo, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return nil, err
	}
	sqlLimit := int64(-1)
	if limit > 0 {
		sqlLimit = int64(limit)
	}
	rows, err := q.GetUtxosDeletedBeforeSlot(
		ctx,
		sqlitequery.GetUtxosDeletedBeforeSlotParams{
			DeletedSlot: sql.NullInt64{Int64: sqlSlot, Valid: true},
			Limit:       sqlLimit,
		},
	)
	if err != nil {
		return nil, err
	}
	return utxosFromSQLite(rows)
}

// GetUtxosByAddress runs one OR-joined query per chunk of patterns -- a
// single statement covering every pattern can exceed the dialect's
// bound-parameter limit (e.g. SQLite's 999) or its OR-expression tree depth
// limit once enough addresses are requested. Chunking is bounded by both
// accumulated bind-argument count and branch count: a pattern whose
// exact-address hash decodes as the zero hash for both payment and staking
// (e.g. a Byron address with an all-zero payment hash) falls back to a
// zero-argument branch in AppendUtxoAddressPatternOrBranch, so
// argument-count alone would never chunk a run of such patterns before the
// expression tree overflowed. Candidates are deduplicated by (tx id, output
// index) across chunks -- a coarse, non-selective branch (e.g. the
// zero-argument fallback above) can return the same candidate rows from
// every chunk it appears in -- before assets are loaded once on the final
// deduplicated set, so asset-loading cost is bounded by the result size
// rather than chunk count times candidate-set size.
func (s *Store) GetUtxosByAddress(
	patterns []models.UtxoAddressPattern,
	txn types.Txn,
) ([]models.Utxo, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	limit := s.dialect.ParameterLimit()
	type utxoKey struct {
		txId string
		idx  uint32
	}
	seen := make(map[utxoKey]struct{})
	var ret []models.Utxo
	var branches []string
	var args []any
	runQuery := func() error {
		if len(branches) == 0 {
			return nil
		}
		utxos, err := s.queryUtxos(
			txn,
			"utxo.deleted_slot = 0 AND ("+strings.Join(branches, " OR ")+")",
			args,
			"",
		)
		if err != nil {
			return err
		}
		for i := range utxos {
			key := utxoKey{string(utxos[i].TxId), utxos[i].OutputIdx}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			ret = append(ret, utxos[i])
		}
		branches = nil
		args = nil
		return nil
	}
	for _, pattern := range patterns {
		var branchOrs []string
		var branchArgs []any
		if err := models.AppendUtxoAddressPatternOrBranch(
			&branchOrs,
			&branchArgs,
			pattern,
		); err != nil {
			return nil, err
		}
		if len(branches) > 0 &&
			(len(args)+len(branchArgs) > limit ||
				len(branches)+len(branchOrs) >= max(1, limit/2)) {
			if err := runQuery(); err != nil {
				return nil, err
			}
		}
		branches = append(branches, branchOrs...)
		args = append(args, branchArgs...)
	}
	if err := runQuery(); err != nil {
		return nil, err
	}
	pointers := make([]*models.Utxo, len(ret))
	for i := range ret {
		pointers[i] = &ret[i]
	}
	if err := s.loadUtxoAssets(ctx, db, pointers); err != nil {
		return nil, err
	}
	return ret, nil
}

// utxoOrderingPredicate builds the WHERE predicate shared by
// GetUtxosByAddressWithOrdering and CountUtxosByAddressWithOrdering. The
// returned predicate excludes any keyset (query.After) bound, which only
// GetUtxosByAddressWithOrdering applies.
func utxoOrderingPredicate(
	query *models.UtxoWithOrderingQuery,
) (string, []any, error) {
	predicate := "utxo.deleted_slot = 0"
	args := []any{}
	switch {
	case query.MatchAllAddresses:
	case len(query.AddressPatterns) == 0:
		predicate += " AND 1 = 0"
	default:
		branches := []string{}
		for _, pattern := range query.AddressPatterns {
			if err := models.AppendUtxoAddressPatternOrBranch(
				&branches,
				&args,
				pattern,
			); err != nil {
				return "", nil, err
			}
		}
		if len(branches) == 0 {
			predicate += " AND 1 = 0"
		} else {
			predicate += " AND (" + strings.Join(branches, " OR ") + ")"
		}
	}
	if query.FilterByAsset {
		if len(query.AssetPolicyID) == 0 {
			return "", nil, models.ErrEmptyAssetPolicyID
		}
		predicate += `
 AND EXISTS (
     SELECT 1 FROM asset
     WHERE asset.utxo_id = utxo.id AND asset.policy_id = ?`
		args = append(args, query.AssetPolicyID)
		if query.AssetName != nil {
			predicate += " AND asset.name = ?"
			args = append(args, query.AssetName)
		}
		predicate += ")"
	}
	return predicate, args, nil
}

func (s *Store) GetUtxosByAddressWithOrdering(
	query *models.UtxoWithOrderingQuery,
	txn types.Txn,
) ([]models.UtxoWithOrdering, error) {
	if query == nil {
		return nil, fmt.Errorf(
			"GetUtxosByAddressWithOrdering: %w",
			models.ErrNilUtxoWithOrderingQuery,
		)
	}
	if query.After != nil && query.Descending {
		return nil, fmt.Errorf(
			"GetUtxosByAddressWithOrdering: %w",
			models.ErrDescendingKeysetUnsupported,
		)
	}
	if query.After != nil && query.Offset > 0 {
		return nil, fmt.Errorf(
			"GetUtxosByAddressWithOrdering: %w",
			models.ErrOffsetKeysetUnsupported,
		)
	}
	if query.Offset > 0 &&
		models.RequiresExactAddressFilter(query.AddressPatterns) {
		return nil, fmt.Errorf(
			"GetUtxosByAddressWithOrdering: %w",
			models.ErrOffsetRequiresCoarseMatch,
		)
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	predicate, args, err := utxoOrderingPredicate(query)
	if err != nil {
		return nil, fmt.Errorf("GetUtxosByAddressWithOrdering: %w", err)
	}
	slotExpr := `COALESCE("transaction".slot, utxo.added_slot)`
	blockIndexExpr := `COALESCE("transaction".block_index, 0)`
	if query.After != nil {
		predicate += `
 AND (
     ` + slotExpr + ` > ?
     OR (` + slotExpr + ` = ? AND ` + blockIndexExpr + ` > ?)
     OR (` + slotExpr + ` = ? AND ` + blockIndexExpr + ` = ?
         AND utxo.output_idx > ?)
     OR (` + slotExpr + ` = ? AND ` + blockIndexExpr + ` = ?
         AND utxo.output_idx = ? AND utxo.tx_id > ?)
 )`
		args = append(
			args,
			query.After.Slot,
			query.After.Slot,
			query.After.BlockIndex,
			query.After.Slot,
			query.After.BlockIndex,
			query.After.OutputIdx,
			query.After.Slot,
			query.After.BlockIndex,
			query.After.OutputIdx,
			query.After.TxId,
		)
	}
	orderDir := "ASC"
	if query.Descending {
		orderDir = "DESC"
	}
	statement := `
SELECT ` + qualifiedSQLiteUtxoColumns + `,
       ` + slotExpr + `, ` + blockIndexExpr + `
FROM utxo
LEFT JOIN "transaction" ON utxo.transaction_id = "transaction".id
WHERE ` + predicate + `
ORDER BY ` + slotExpr + ` ` + orderDir + `, ` + blockIndexExpr + ` ` + orderDir + `,
         utxo.output_idx ` + orderDir + `, utxo.tx_id ` + orderDir
	statement, args = addLimitOffset(statement, args, query.Limit, query.Offset)
	rows, err := db.QueryContext(
		ctx,
		s.dialect.Rebind(statement),
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := []models.UtxoWithOrdering{}
	pointers := []*models.Utxo{}
	for rows.Next() {
		item, err := scanUtxoWithOrdering(rows)
		if err != nil {
			return nil, err
		}
		ret = append(ret, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if query.SkipAssets {
		return ret, nil
	}
	for i := range ret {
		pointers = append(pointers, &ret[i].Utxo)
	}
	if err := s.loadUtxoAssets(ctx, db, pointers); err != nil {
		return nil, err
	}
	return ret, nil
}

// CountUtxosByAddressWithOrdering returns the number of live UTxOs matching
// query's coarse SQL predicate (address patterns and asset filter), without
// materializing rows. It rejects a query whose address patterns require
// CBOR-based exact-address filtering (see RequiresExactAddressFilter):
// the coarse predicate alone over-matches address forms that share a
// payment/delegation credential (for example pointer addresses), so a count
// against it would not equal the exact-match total.
func (s *Store) CountUtxosByAddressWithOrdering(
	query *models.UtxoWithOrderingQuery,
	txn types.Txn,
) (int, error) {
	if query == nil {
		return 0, fmt.Errorf(
			"CountUtxosByAddressWithOrdering: %w",
			models.ErrNilUtxoWithOrderingQuery,
		)
	}
	if models.RequiresExactAddressFilter(query.AddressPatterns) {
		return 0, fmt.Errorf(
			"CountUtxosByAddressWithOrdering: %w",
			models.ErrExactAddressRequiresCbor,
		)
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	predicate, args, err := utxoOrderingPredicate(query)
	if err != nil {
		return 0, fmt.Errorf("CountUtxosByAddressWithOrdering: %w", err)
	}
	var count int
	err = db.QueryRowContext(
		ctx,
		s.dialect.Rebind(
			"SELECT COUNT(*) FROM utxo WHERE "+predicate,
		),
		args...,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count utxos by address: %w", err)
	}
	return count, nil
}

func (s *Store) GetUtxosByAddressAtSlot(
	pattern models.UtxoAddressPattern,
	slot uint64,
	txn types.Txn,
) ([]models.Utxo, error) {
	var predicates []string
	var args []any
	if err := models.AppendUtxoAddressPatternOrBranch(
		&predicates,
		&args,
		pattern,
	); err != nil {
		return nil, err
	}
	if len(predicates) == 0 {
		return nil, models.ErrEmptyUtxoAddressPattern
	}
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return nil, err
	}
	args = append(
		[]any{sqlSlot, sqlSlot},
		args...,
	)
	return s.queryUtxosWithAssets(
		txn,
		"utxo.added_slot <= ? AND "+
			"(utxo.deleted_slot = 0 OR utxo.deleted_slot > ?) AND "+
			predicates[0],
		args,
		"",
	)
}

func (s *Store) GetControlledAmountByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) (uint64, error) {
	if len(stakingKey) == 0 {
		return 0, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	return sumUint64Rows(ctx, db, s.dialect.Rebind(`
SELECT amount FROM utxo
WHERE credential_tag = ? AND staking_key = ? AND deleted_slot = 0`), credentialTag, stakingKey)
}

func (s *Store) GetUtxoPaymentScriptByCredential(
	credentialTag uint8,
	stakingKey []byte,
	paymentKeys [][]byte,
	txn types.Txn,
) (map[string]bool, error) {
	ret := make(map[string]bool, len(paymentKeys))
	if len(stakingKey) == 0 || len(paymentKeys) == 0 {
		return ret, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve read DB for payment script by stake credential: %w",
			err,
		)
	}
	paymentKeys = dedupeByteSlices(paymentKeys)
	chunkSize := max(1, s.dialect.ParameterLimit()-2)
	for start := 0; start < len(paymentKeys); start += chunkSize {
		end := min(start+chunkSize, len(paymentKeys))
		chunk := paymentKeys[start:end]
		err := func() error {
			args := make([]any, 0, len(chunk)+2)
			args = append(args, credentialTag, stakingKey)
			for _, key := range chunk {
				args = append(args, key)
			}
			rows, err := db.QueryContext(
				ctx,
				s.dialect.Rebind(`
SELECT DISTINCT payment_key, payment_script
FROM utxo
WHERE credential_tag = ? AND staking_key = ?
  AND payment_key IN (`+bindPlaceholders(len(chunk))+`)`),
				args...,
			)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var key []byte
				var script sql.NullBool
				if err := rows.Scan(&key, &script); err != nil {
					return err
				}
				ret[hex.EncodeToString(key)] = script.Bool
			}
			return rows.Err()
		}()
		if err != nil {
			return nil, fmt.Errorf(
				"get payment script by stake credential: %w",
				err,
			)
		}
	}
	return ret, nil
}

func (s *Store) GetScriptLockedSupply(txn types.Txn) (uint64, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	return sumUint64Rows(ctx, db, s.dialect.Rebind(`
SELECT amount FROM utxo
WHERE payment_script = TRUE AND deleted_slot = 0`))
}

func (s *Store) GetUtxosByAssets(
	policyID []byte,
	assetName []byte,
	txn types.Txn,
) ([]models.Utxo, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	var rows []sqlitequery.Utxo
	if assetName == nil {
		rows, err = q.GetLiveUtxosByAssetPolicy(
			ctx,
			policyID,
		)
	} else {
		rows, err = q.GetLiveUtxosByAsset(
			ctx,
			sqlitequery.GetLiveUtxosByAssetParams{
				PolicyID: policyID,
				Name:     assetName,
			},
		)
	}
	if err != nil {
		return nil, err
	}
	ret, err := utxosFromSQLite(rows)
	if err != nil {
		return nil, err
	}
	pointers := make([]*models.Utxo, len(ret))
	for i := range ret {
		pointers[i] = &ret[i]
	}
	if err := s.loadUtxoAssets(ctx, db, pointers); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Store) IterateLiveUtxos(
	txn types.Txn,
	fn func(*models.Utxo) error,
) error {
	if fn == nil {
		return errors.New("iterate live UTxOs: callback is nil")
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return err
	}
	rows, err := db.QueryContext(
		ctx,
		"SELECT "+sqliteUtxoColumns+" FROM utxo WHERE deleted_slot = 0",
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		row, err := scanSQLiteUtxo(rows)
		if err != nil {
			return err
		}
		model, err := utxoFromSQLite(row)
		if err != nil {
			return err
		}
		if err := fn(model); err != nil {
			return err
		}
	}
	return rows.Err()
}

// queryUtxos runs predicate/args against the utxo table and returns the
// matching rows without loading assets -- callers that need to deduplicate
// candidates across multiple queries (e.g. chunked GetUtxosByAddress) should
// use this and load assets once on the final deduplicated set, rather than
// paying the asset-load cost once per query.
func (s *Store) queryUtxos(
	txn types.Txn,
	predicate string,
	args []any,
	order string,
) ([]models.Utxo, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	query := "SELECT " + sqliteUtxoColumns + " FROM utxo WHERE " + predicate
	if order != "" {
		query += " ORDER BY " + order
	}
	rows, err := db.QueryContext(
		ctx,
		s.dialect.Rebind(query),
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := make([]models.Utxo, 0)
	for rows.Next() {
		row, err := scanSQLiteUtxo(rows)
		if err != nil {
			return nil, err
		}
		model, err := utxoFromSQLite(row)
		if err != nil {
			return nil, err
		}
		ret = append(ret, *model)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Store) queryUtxosWithAssets(
	txn types.Txn,
	predicate string,
	args []any,
	order string,
) ([]models.Utxo, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	ret, err := s.queryUtxos(txn, predicate, args, order)
	if err != nil {
		return nil, err
	}
	pointers := make([]*models.Utxo, len(ret))
	for i := range ret {
		pointers[i] = &ret[i]
	}
	if err := s.loadUtxoAssets(ctx, db, pointers); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Store) loadUtxoAssets(
	ctx context.Context,
	db queryer,
	utxos []*models.Utxo,
) error {
	if len(utxos) == 0 {
		return nil
	}
	return s.loadUtxoAssetsPointers(ctx, db, utxos)
}

func (s *Store) loadUtxoAssetsBatch(
	ctx context.Context,
	db queryer,
	groups ...map[string][]models.Utxo,
) error {
	pointers := make([]*models.Utxo, 0)
	for _, group := range groups {
		for _, items := range group {
			for i := range items {
				pointers = append(pointers, &items[i])
			}
		}
	}
	return s.loadUtxoAssetsPointers(ctx, db, pointers)
}

func (s *Store) loadUtxoAssetsPointers(
	ctx context.Context,
	db queryer,
	utxos []*models.Utxo,
) error {
	if len(utxos) == 0 {
		return nil
	}
	for _, utxo := range utxos {
		utxo.Assets = make([]models.Asset, 0)
	}
	// The same UTxO can be present in multiple association groups (for
	// example, when hydrating transactions by block/hash/address).  Keep the
	// fan-out map keyed by ID, but deduplicate IDs before chunking the query;
	// otherwise a repeated ID straddling two parameter chunks causes its asset
	// rows to be loaded twice and appended twice to every instance.
	ids := make([]uint, 0, len(utxos))
	byID := make(map[uint][]*models.Utxo, len(utxos))
	for _, utxo := range utxos {
		if _, exists := byID[utxo.ID]; !exists {
			ids = append(ids, utxo.ID)
		}
		byID[utxo.ID] = append(byID[utxo.ID], utxo)
	}
	for start := 0; start < len(ids); start += s.dialect.ParameterLimit() {
		end := min(start+s.dialect.ParameterLimit(), len(ids))
		args := make([]any, end-start)
		for i, id := range ids[start:end] {
			args[i] = id
		}
		rows, err := db.QueryContext(ctx, s.dialect.Rebind(
			"SELECT name, name_hex, policy_id, fingerprint, id, utxo_id, amount FROM asset WHERE utxo_id IN ("+bindPlaceholders(
				end-start,
			)+") ORDER BY id",
		), args...)
		if err != nil {
			return err
		}
		err = func() error {
			defer rows.Close()
			for rows.Next() {
				var name, nameHex, policyID, fingerprint []byte
				var id int64
				var utxoID sql.NullInt64
				var amount sql.NullString
				if err := rows.Scan(&name, &nameHex, &policyID, &fingerprint, &id, &utxoID, &amount); err != nil {
					return err
				}
				if !utxoID.Valid {
					continue
				}
				value := uint64(0)
				if amount.Valid {
					value, err = parseUint64("asset amount", amount.String)
					if err != nil {
						return err
					}
				}
				asset := models.Asset{
					Name:        name,
					NameHex:     nameHex,
					PolicyId:    policyID,
					Fingerprint: fingerprint,
					ID:          uint(id),
					UtxoID:      uint(utxoID.Int64),
					Amount:      types.Uint64(value),
				}
				for _, utxo := range byID[asset.UtxoID] {
					utxo.Assets = append(utxo.Assets, asset)
				}
			}
			return rows.Err()
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func utxosFromSQLite(rows []sqlitequery.Utxo) ([]models.Utxo, error) {
	ret := make([]models.Utxo, 0, len(rows))
	for _, row := range rows {
		utxo, err := utxoFromSQLite(row)
		if err != nil {
			return nil, err
		}
		ret = append(ret, *utxo)
	}
	return ret, nil
}

func utxoFromSQLite(row sqlitequery.Utxo) (*models.Utxo, error) {
	amount := uint64(0)
	var err error
	if row.Amount.Valid {
		amount, err = parseUint64("UTxO amount", row.Amount.String)
		if err != nil {
			return nil, err
		}
	}
	return &models.Utxo{
		TransactionID:           uintPointer(row.TransactionID),
		CollateralReturnForTxID: uintPointer(row.CollateralReturnForTxID),
		TxId:                    row.TxID,
		PaymentKey:              row.PaymentKey,
		StakingKey:              row.StakingKey,
		CredentialTag:           uint8(row.CredentialTag),
		DatumHash:               row.DatumHash,
		SpentAtTxId:             types.NullableHash(row.SpentAtTxID),
		ReferencedByTxId:        types.NullableHash(row.ReferencedByTxID),
		CollateralByTxId:        types.NullableHash(row.CollateralByTxID),
		ID:                      uint(row.ID),
		AddedSlot:               uint64(row.AddedSlot.Int64),
		DeletedSlot:             uint64(row.DeletedSlot.Int64),
		Amount:                  types.Uint64(amount),
		OutputIdx:               uint32(row.OutputIdx.Int64),
		PaymentScript:           row.PaymentScript.Bool,
	}, nil
}

func scanSQLiteUtxo(rows *sql.Rows) (sqlitequery.Utxo, error) {
	var row sqlitequery.Utxo
	err := rows.Scan(
		&row.TransactionID,
		&row.CollateralReturnForTxID,
		&row.TxID,
		&row.PaymentKey,
		&row.StakingKey,
		&row.CredentialTag,
		&row.DatumHash,
		&row.SpentAtTxID,
		&row.ReferencedByTxID,
		&row.CollateralByTxID,
		&row.ID,
		&row.AddedSlot,
		&row.DeletedSlot,
		&row.Amount,
		&row.OutputIdx,
		&row.PaymentScript,
	)
	return row, err
}

func scanUtxoWithOrdering(
	rows *sql.Rows,
) (models.UtxoWithOrdering, error) {
	var raw sqlitequery.Utxo
	var slot sql.NullInt64
	var blockIndex sql.NullInt64
	err := rows.Scan(
		&raw.TransactionID,
		&raw.CollateralReturnForTxID,
		&raw.TxID,
		&raw.PaymentKey,
		&raw.StakingKey,
		&raw.CredentialTag,
		&raw.DatumHash,
		&raw.SpentAtTxID,
		&raw.ReferencedByTxID,
		&raw.CollateralByTxID,
		&raw.ID,
		&raw.AddedSlot,
		&raw.DeletedSlot,
		&raw.Amount,
		&raw.OutputIdx,
		&raw.PaymentScript,
		&slot,
		&blockIndex,
	)
	if err != nil {
		return models.UtxoWithOrdering{}, err
	}
	utxo, err := utxoFromSQLite(raw)
	if err != nil {
		return models.UtxoWithOrdering{}, err
	}
	return models.UtxoWithOrdering{
		Utxo:         *utxo,
		TxSlot:       uint64(slot.Int64),
		TxBlockIndex: uint32(blockIndex.Int64),
	}, nil
}

func uintPointer(value sql.NullInt64) *uint {
	if !value.Valid {
		return nil
	}
	ret := uint(value.Int64)
	return &ret
}

func createUtxoParams(
	utxo *models.Utxo,
) (sqlitequery.CreateUtxoParams, error) {
	addedSlot, err := checkedInt64(utxo.AddedSlot)
	if err != nil {
		return sqlitequery.CreateUtxoParams{}, err
	}
	deletedSlot, err := checkedInt64(utxo.DeletedSlot)
	if err != nil {
		return sqlitequery.CreateUtxoParams{}, err
	}
	return sqlitequery.CreateUtxoParams{
		TransactionID:           nullableUint(utxo.TransactionID),
		CollateralReturnForTxID: nullableUint(utxo.CollateralReturnForTxID),
		TxID:                    utxo.TxId,
		PaymentKey:              utxo.PaymentKey,
		StakingKey:              utxo.StakingKey,
		CredentialTag:           int64(utxo.CredentialTag),
		DatumHash:               utxo.DatumHash,
		SpentAtTxID:             utxo.SpentAtTxId,
		ReferencedByTxID:        utxo.ReferencedByTxId,
		CollateralByTxID:        utxo.CollateralByTxId,
		AddedSlot: sql.NullInt64{
			Int64: addedSlot,
			Valid: true,
		},
		DeletedSlot: sql.NullInt64{
			Int64: deletedSlot,
			Valid: true,
		},
		Amount: sql.NullString{
			String: decimalUint64(utxo.Amount),
			Valid:  true,
		},
		OutputIdx: sql.NullInt64{
			Int64: int64(utxo.OutputIdx),
			Valid: true,
		},
		PaymentScript: sql.NullBool{
			Bool:  utxo.PaymentScript,
			Valid: true,
		},
	}, nil
}

func nullableUint(value *uint) sql.NullInt64 {
	if value == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(*value), Valid: true}
}
