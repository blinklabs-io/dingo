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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
)

// UpsertTokenRegistryEntries writes CIP-26 token registry properties, keyed by
// subject, and returns the number of rows written. Each entry replaces every
// property of an existing row for the same subject, so a property the upstream
// registry has dropped stops being served rather than surviving from an
// earlier sync.
//
// syncedAt stamps every written row with the timestamp of the snapshot being
// applied, so that PruneTokenRegistryEntriesBefore can afterwards identify
// rows the snapshot did not carry. All batches of one snapshot must pass the
// same value, or a later batch would make earlier ones look stale.
//
// Entries are written one statement at a time rather than as a single
// multi-row INSERT: a registry sync is an infrequent background pass whose
// latency nobody waits on, and a per-row statement keeps one malformed subject
// from failing the batch around it.
func (s *Store) UpsertTokenRegistryEntries(
	ctx context.Context,
	entries []models.TokenRegistryEntry,
	syncedAt time.Time,
	txn types.Txn,
) (int, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	ctx = nonNilContext(ctx)
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return 0, err
	}
	q := s.operationalQueries(db)
	now := syncedAt.UTC()
	written := 0
	for idx := range entries {
		entry := &entries[idx]
		subject := normalizeTokenRegistrySubject(entry.Subject)
		if subject == "" {
			return written, fmt.Errorf(
				"token registry entry %d has no subject",
				idx,
			)
		}
		params := sqlitequery.UpsertTokenRegistryEntryParams{
			Subject:     subject,
			Name:        nullableTrimmedString(entry.Name),
			Ticker:      nullableTrimmedString(entry.Ticker),
			Description: nullableTrimmedString(entry.Description),
			Url:         nullableTrimmedString(entry.URL),
			Logo:        nullableTrimmedString(entry.Logo),
			Decimals:    nullableDecimals(entry.Decimals),
			CreatedAt:   sql.NullTime{Time: now, Valid: true},
			UpdatedAt:   sql.NullTime{Time: now, Valid: true},
		}
		if err := q.UpsertTokenRegistryEntry(ctx, params); err != nil {
			return written, fmt.Errorf(
				"upsert token registry entry %s: %w",
				subject,
				err,
			)
		}
		written++
	}
	return written, nil
}

// GetTokenRegistryEntry returns the registry properties for a subject, or nil
// when the registry has nothing for it. An unknown subject is absence rather
// than an error: the API serves a null `metadata` field for it.
func (s *Store) GetTokenRegistryEntry(
	subject string,
	txn types.Txn,
) (*models.TokenRegistryEntry, error) {
	normalized := normalizeTokenRegistrySubject(subject)
	if normalized == "" {
		return nil, nil
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	row, err := q.GetTokenRegistryEntry(context.Background(), normalized)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get token registry entry: %w", err)
	}
	ret := tokenRegistryEntryFromSQLite(row)
	return &ret, nil
}

// PruneTokenRegistryEntriesBefore deletes registry rows last confirmed by a
// snapshot older than cutoff, returning the number removed. Callers pass the
// timestamp they gave UpsertTokenRegistryEntries for the snapshot just
// applied, which leaves that snapshot's own rows (stamped exactly at the
// cutoff) in place and removes everything it did not carry.
//
// This is what stops a subject the upstream registry has dropped, or one that
// lost every property, from being served forever by an upsert-only sync. It
// must run only after a snapshot has fully applied: pruning against a partial
// snapshot would delete live subjects the failed run never reached.
func (s *Store) PruneTokenRegistryEntriesBefore(
	ctx context.Context,
	cutoff time.Time,
	txn types.Txn,
) (int64, error) {
	ctx = nonNilContext(ctx)
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return 0, err
	}
	q := s.operationalQueries(db)
	pruned, err := q.PruneTokenRegistryEntriesStaleBefore(
		ctx,
		sql.NullTime{Time: cutoff.UTC(), Valid: true},
	)
	if err != nil {
		return 0, fmt.Errorf("prune token registry entries: %w", err)
	}
	return pruned, nil
}

// normalizeTokenRegistrySubject lower-cases and trims a subject so that a
// lookup built from raw on-chain bytes matches a subject the registry
// published in upper case. Returns "" for a subject that is blank.
func normalizeTokenRegistrySubject(subject string) string {
	return strings.ToLower(strings.TrimSpace(subject))
}

func nullableTrimmedString(value string) sql.NullString {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: trimmed, Valid: true}
}

func nullableDecimals(value *int) sql.NullInt64 {
	if value == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(*value), Valid: true}
}

func tokenRegistryEntryFromSQLite(
	row sqlitequery.GetTokenRegistryEntryRow,
) models.TokenRegistryEntry {
	ret := models.TokenRegistryEntry{
		Subject:     row.Subject,
		Name:        row.Name.String,
		Ticker:      row.Ticker.String,
		Description: row.Description.String,
		URL:         row.Url.String,
		Logo:        row.Logo.String,
	}
	if row.ID > 0 {
		ret.ID = uint(row.ID)
	}
	// A decimals value outside int range cannot have been written by
	// UpsertTokenRegistryEntries (the parser caps it well below), so a row
	// carrying one was written out of band; drop it rather than wrap.
	if row.Decimals.Valid &&
		row.Decimals.Int64 >= 0 &&
		row.Decimals.Int64 <= math.MaxInt32 {
		decimals := int(row.Decimals.Int64)
		ret.Decimals = &decimals
	}
	if row.CreatedAt.Valid {
		ret.CreatedAt = row.CreatedAt.Time
	}
	if row.UpdatedAt.Valid {
		ret.UpdatedAt = row.UpdatedAt.Time
	}
	return ret
}
