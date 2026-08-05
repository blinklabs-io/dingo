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

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
)

func (s *Store) GetCommitTimestamp() (int64, error) {
	if err := s.ensureReady(); err != nil {
		return 0, err
	}
	queries, err := newManagementQueries(s.dialect.Name(), s.readDB)
	if err != nil {
		return 0, err
	}
	timestamp, err := queries.getCommitTimestamp(context.Background())
	err = normalizeNoRows(err)
	if errors.Is(err, metadata.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get commit timestamp: %w", err)
	}
	if !timestamp.Valid {
		return 0, nil
	}
	return timestamp.Int64, nil
}

func (s *Store) SetCommitTimestamp(
	timestamp int64,
	txn types.Txn,
) error {
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries, err := newManagementQueries(s.dialect.Name(), db)
	if err != nil {
		return err
	}
	if err := queries.setCommitTimestamp(
		context.Background(),
		sql.NullInt64{Int64: timestamp, Valid: true},
	); err != nil {
		return fmt.Errorf("set commit timestamp: %w", err)
	}
	return nil
}

func (s *Store) GetNodeSettings() (*types.NodeSettings, error) {
	if err := s.ensureReady(); err != nil {
		return nil, err
	}
	queries, err := newManagementQueries(s.dialect.Name(), s.readDB)
	if err != nil {
		return nil, err
	}
	storageMode, network, err := queries.getNodeSettings(
		context.Background(),
	)
	err = normalizeNoRows(err)
	if errors.Is(err, metadata.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get node settings: %w", err)
	}
	return &types.NodeSettings{
		StorageMode: storageMode,
		Network:     network,
	}, nil
}

func (s *Store) SetNodeSettings(settings *types.NodeSettings) error {
	if settings == nil {
		return errors.New("set node settings: settings are nil")
	}
	if err := s.ensureReady(); err != nil {
		return err
	}
	queries, err := newManagementQueries(s.dialect.Name(), s.writeDB)
	if err != nil {
		return err
	}
	_, err = queries.insertNodeSettings(
		context.Background(),
		settings.StorageMode,
		settings.Network,
	)
	if err != nil {
		return fmt.Errorf("set node settings: insert: %w", err)
	}
	// Do not use RowsAffected to decide whether the row was newly inserted.
	// MySQL's CLIENT_FOUND_ROWS mode reports one affected row for a duplicate
	// no-op upsert, which would skip the legacy empty-network backfill.  The
	// conditional UPDATE is harmless for a newly inserted row and preserves the
	// immutable storage mode for existing rows.
	if settings.Network == "" {
		return nil
	}
	if _, err := queries.backfillNodeSettingsNetwork(
		context.Background(),
		settings.Network,
		settings.StorageMode,
	); err != nil {
		return fmt.Errorf("set node settings: network backfill: %w", err)
	}
	return nil
}

func (s *Store) ensureReady() error {
	if !s.ready.Load() {
		return errors.New("sqlstore: store is not ready")
	}
	return nil
}

func normalizeNoRows(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return metadata.ErrNotFound
	}
	return err
}
