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
	"strconv"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
)

const (
	offchainFetchClaimLease           = 30 * time.Minute
	offchainPointerDiscoveryBatchSize = 64
	offchainPointerDiscoveryCursorKey = "offchain-metadata-discovery:"
)

type offchainPointerSource struct {
	sourceType string
	table      string
	urlColumn  string
	hashColumn string
}

var offchainPointerSources = []offchainPointerSource{
	{
		models.OffchainMetadataSourcePool,
		"pool_registration",
		"metadata_url",
		"metadata_hash",
	},
	{models.OffchainMetadataSourceDrep, "drep", "anchor_url", "anchor_hash"},
	{
		models.OffchainMetadataSourceDrepRegistration,
		"registration_drep",
		"anchor_url",
		"anchor_hash",
	},
	{
		models.OffchainMetadataSourceDrepUpdate,
		"update_drep",
		"anchor_url",
		"anchor_hash",
	},
	{
		models.OffchainMetadataSourceGovernanceProposal,
		"governance_proposal",
		"anchor_url",
		"anchor_hash",
	},
	{
		models.OffchainMetadataSourceGovernanceVote,
		"governance_vote",
		"anchor_url",
		"anchor_hash",
	},
	{
		models.OffchainMetadataSourceConstitution,
		"constitution",
		"anchor_url",
		"anchor_hash",
	},
	{
		models.OffchainMetadataSourceCommitteeResign,
		"resign_committee_cold",
		"anchor_url",
		"anchor_hash",
	},
}

func (s *Store) EnsureOffchainMetadataPointers(
	ctx context.Context,
	now time.Time,
	txn types.Txn,
) (int, error) {
	ctx = nonNilContext(ctx)
	// The txn-carried ctx is not used here: this method already takes an
	// explicit ctx from its caller (and is always invoked with txn == nil,
	// the autocommit path, so dbFromTxn's own ctx would only ever be
	// context.Background() -- using it would silently discard the ctx this
	// method was actually given).
	db, _, err := s.dbFromTxn(txn)
	if err != nil {
		return 0, err
	}
	q := s.operationalQueries(db)
	created := 0
	for _, source := range offchainPointerSources {
		cursorKey := offchainPointerDiscoveryCursorKey + source.sourceType
		rawCursor, err := q.GetSyncState(ctx, cursorKey)
		if errors.Is(err, sql.ErrNoRows) {
			rawCursor = ""
		} else if err != nil {
			return created, fmt.Errorf(
				"read off-chain metadata discovery cursor for %s: %w",
				source.table,
				err,
			)
		}
		var cursor int64
		if rawCursor != "" {
			cursor, err = strconv.ParseInt(rawCursor, 10, 64)
			if err != nil || cursor < 0 {
				return created, fmt.Errorf(
					"invalid off-chain metadata discovery cursor for %s: %q",
					source.table,
					rawCursor,
				)
			}
		}
		query := "SELECT id, " + source.urlColumn + ", " + source.hashColumn +
			" FROM " + source.table +
			" WHERE id > ? ORDER BY id LIMIT ?"
		rows, err := db.QueryContext(
			ctx,
			s.dialect.Rebind(query),
			cursor,
			offchainPointerDiscoveryBatchSize,
		)
		if err != nil {
			return created, fmt.Errorf(
				"query off-chain metadata pointers from %s: %w",
				source.table,
				err,
			)
		}
		type pointer struct {
			id   int64
			url  string
			hash []byte
		}
		pointers := make([]pointer, 0, offchainPointerDiscoveryBatchSize)
		for rows.Next() {
			var id int64
			var url sql.NullString
			var hash []byte
			if err := rows.Scan(&id, &url, &hash); err != nil {
				rows.Close()
				return created, err
			}
			pointers = append(
				pointers,
				pointer{id: id, url: url.String, hash: hash},
			)
		}
		if err := rows.Close(); err != nil {
			return created, err
		}
		if err := rows.Err(); err != nil {
			return created, err
		}
		for _, row := range pointers {
			cursor = row.id
			url := row.url
			hash := row.hash
			url = strings.TrimSpace(url)
			if url == "" || len(hash) != 32 {
				continue
			}
			createdAt := now
			affected, err := q.InsertOffchainMetadataPointer(
				ctx,
				sqlitequery.InsertOffchainMetadataPointerParams{
					CreatedAt: sql.NullTime{
						Time:  createdAt,
						Valid: true,
					},
					UpdatedAt: sql.NullTime{
						Time:  createdAt,
						Valid: true,
					},
					Url:        url,
					SourceType: source.sourceType,
					Status:     models.OffchainMetadataStatusPending,
					Hash:       hash,
					NextFetchAfter: sql.NullTime{
						Time:  now,
						Valid: true,
					},
				},
			)
			if err != nil {
				return created, fmt.Errorf(
					"insert off-chain metadata pointer from %s: %w",
					source.table,
					err,
				)
			}
			created += int(affected)
		}
		// A short page means the scan reached the end of the table. Wrap
		// the cursor so the next pass re-reads from the start: source rows
		// such as drep and governance_vote update their anchor in place and
		// keep their id, so a cursor that only moves forward would never
		// discover the new anchor. Inserts are idempotent.
		if len(pointers) < offchainPointerDiscoveryBatchSize {
			cursor = 0
		}
		if len(pointers) > 0 || rawCursor != strconv.FormatInt(cursor, 10) {
			if err := q.SetSyncState(
				ctx,
				sqlitequery.SetSyncStateParams{
					SyncKey: cursorKey,
					Value:   strconv.FormatInt(cursor, 10),
				},
			); err != nil {
				return created, fmt.Errorf(
					"save off-chain metadata discovery cursor for %s: %w",
					source.table,
					err,
				)
			}
		}
	}
	return created, nil
}

func (s *Store) GetOffchainMetadataFetchBatch(
	ctx context.Context,
	limit int,
	now time.Time,
	txn types.Txn,
) ([]models.OffchainMetadata, error) {
	ctx = nonNilContext(ctx)
	if limit <= 0 {
		limit = 1
	}
	// See EnsureOffchainMetadataPointers: the txn-carried ctx is discarded
	// in favor of this method's own explicit ctx parameter.
	db, _, err := s.dbFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	claimUntil := now.Add(offchainFetchClaimLease)
	ret := make([]models.OffchainMetadata, 0, limit)
	for len(ret) < limit {
		rows, err := q.GetOffchainMetadataFetchCandidates(
			ctx,
			sqlitequery.GetOffchainMetadataFetchCandidatesParams{
				NextFetchAfter: sql.NullTime{Time: now, Valid: true},
				Limit:          int64(limit - len(ret)),
			},
		)
		if err != nil {
			return nil, fmt.Errorf(
				"query off-chain metadata fetch batch: %w",
				err,
			)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			// MySQL/PostgreSQL timestamp columns have microsecond precision;
			// normalize the claim token before writing and retaining it so the
			// ownership predicate compares the exact persisted value.
			claimedAt := time.Now().UTC().Truncate(time.Microsecond)
			affected, err := q.ClaimOffchainMetadataFetch(
				ctx,
				sqlitequery.ClaimOffchainMetadataFetchParams{
					NextFetchAfter: sql.NullTime{
						Time:  claimUntil,
						Valid: true,
					},
					UpdatedAt: sql.NullTime{
						Time:  claimedAt,
						Valid: true,
					},
					ID: row.ID,
					NextFetchAfter_2: sql.NullTime{
						Time:  now,
						Valid: true,
					},
				},
			)
			if err != nil {
				return nil, fmt.Errorf(
					"claim off-chain metadata fetch row %d: %w",
					row.ID,
					err,
				)
			}
			if affected == 0 {
				continue
			}
			doc := offchainMetadataFromSQLite(row)
			doc.UpdatedAt = claimedAt
			nextFetchAfter := claimUntil
			doc.NextFetchAfter = &nextFetchAfter
			ret = append(ret, doc)
			if len(ret) == limit {
				break
			}
		}
	}
	return ret, nil
}

func (s *Store) SetOffchainMetadataFetchResult(
	ctx context.Context,
	doc *models.OffchainMetadata,
	txn types.Txn,
) error {
	if doc == nil || doc.ID == 0 {
		return errors.New("off-chain metadata fetch result missing row ID")
	}
	ctx = nonNilContext(ctx)
	// See EnsureOffchainMetadataPointers: the txn-carried ctx is discarded
	// in favor of this method's own explicit ctx parameter.
	db, _, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	q := s.operationalQueries(db)
	fetchAttempts, err := checkedInt64(uint64(doc.FetchAttempts))
	if err != nil {
		return err
	}
	httpStatus, err := checkedInt64(uint64(doc.LastHTTPStatus))
	if err != nil {
		return err
	}
	err = q.SetOffchainMetadataFetchResult(
		ctx,
		sqlitequery.SetOffchainMetadataFetchResultParams{
			Status: doc.Status,
			ContentType: sql.NullString{
				String: doc.ContentType,
				Valid:  true,
			},
			LastError:      sql.NullString{String: doc.LastError, Valid: true},
			BodyHash:       doc.BodyHash,
			Content:        doc.Content,
			FetchedAt:      nullableTime(doc.FetchedAt),
			NextFetchAfter: nullableTime(doc.NextFetchAfter),
			FetchAttempts: sql.NullInt64{
				Int64: fetchAttempts,
				Valid: true,
			},
			LastHttpStatus: sql.NullInt64{
				Int64: httpStatus,
				Valid: true,
			},
			UpdatedAt:   sql.NullTime{Time: time.Now(), Valid: true},
			ID:          int64(doc.ID),
			UpdatedAt_2: nullableTime(&doc.UpdatedAt),
		},
	)
	if err != nil {
		return fmt.Errorf("update off-chain metadata fetch result: %w", err)
	}
	return nil
}

func (s *Store) GetOffchainMetadata(
	sourceType string,
	url string,
	hash []byte,
	txn types.Txn,
) (*models.OffchainMetadata, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q := s.operationalQueries(db)
	row, err := q.GetOffchainMetadata(
		ctx,
		sqlitequery.GetOffchainMetadataParams{
			SourceType: sourceType,
			Url:        strings.TrimSpace(url),
			Hash:       hash,
		},
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get off-chain metadata: %w", err)
	}
	ret := offchainMetadataFromSQLite(row)
	return &ret, nil
}

func (s *Store) GetOffchainMetadataBatch(
	sourceType string,
	urls []string,
	txn types.Txn,
) ([]models.OffchainMetadata, error) {
	seen := make(map[string]struct{}, len(urls))
	unique := make([]string, 0, len(urls))
	for _, url := range urls {
		url = strings.TrimSpace(url)
		if url == "" {
			continue
		}
		if _, ok := seen[url]; ok {
			continue
		}
		seen[url] = struct{}{}
		unique = append(unique, url)
	}
	if len(unique) == 0 {
		return nil, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	chunkSize := max(1, s.dialect.ParameterLimit()-1)
	ret := make([]models.OffchainMetadata, 0)
	for start := 0; start < len(unique); start += chunkSize {
		end := min(start+chunkSize, len(unique))
		chunk := unique[start:end]
		args := make([]any, 0, len(chunk)+1)
		args = append(args, sourceType)
		for _, url := range chunk {
			args = append(args, url)
		}
		rows, err := db.QueryContext(
			ctx,
			s.dialect.Rebind(`
SELECT fetched_at, next_fetch_after, created_at, updated_at, url,
       source_type, status, content_type, last_error, hash, body_hash,
       content, id, fetch_attempts, last_http_status
FROM offchain_metadata
WHERE source_type = ? AND url IN (`+
				bindPlaceholders(len(chunk))+`)`),
			args...,
		)
		if err != nil {
			return nil, fmt.Errorf("get off-chain metadata batch: %w", err)
		}
		for rows.Next() {
			var row sqlitequery.OffchainMetadatum
			if err := rows.Scan(
				&row.FetchedAt,
				&row.NextFetchAfter,
				&row.CreatedAt,
				&row.UpdatedAt,
				&row.Url,
				&row.SourceType,
				&row.Status,
				&row.ContentType,
				&row.LastError,
				&row.Hash,
				&row.BodyHash,
				&row.Content,
				&row.ID,
				&row.FetchAttempts,
				&row.LastHttpStatus,
			); err != nil {
				rows.Close()
				return nil, err
			}
			ret = append(ret, offchainMetadataFromSQLite(row))
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

func offchainMetadataFromSQLite(
	row sqlitequery.OffchainMetadatum,
) models.OffchainMetadata {
	return models.OffchainMetadata{
		FetchedAt:      timePointer(row.FetchedAt),
		NextFetchAfter: timePointer(row.NextFetchAfter),
		CreatedAt:      row.CreatedAt.Time,
		UpdatedAt:      row.UpdatedAt.Time,
		URL:            row.Url,
		SourceType:     row.SourceType,
		Status:         row.Status,
		ContentType:    row.ContentType.String,
		LastError:      row.LastError.String,
		Hash:           row.Hash,
		BodyHash:       row.BodyHash,
		Content:        row.Content,
		ID:             uint(row.ID),
		FetchAttempts:  uint(row.FetchAttempts.Int64),
		LastHTTPStatus: uint(row.LastHttpStatus.Int64),
	}
}

func nullableTime(value *time.Time) sql.NullTime {
	if value == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: *value, Valid: true}
}

func timePointer(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	ret := value.Time
	return &ret
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
