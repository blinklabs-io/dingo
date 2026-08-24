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
	"errors"
	"fmt"
	"sort"

	"github.com/blinklabs-io/dingo/database/nodesettings"
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
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	queries, err := newManagementQueries(s.dialect.Name(), db)
	if err != nil {
		return err
	}
	if err := queries.setCommitTimestamp(
		ctx,
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

// GetNodeSettingsGates returns the persisted node settings gate values,
// keyed by gate name. An empty result means no gates have been recorded yet.
func (s *Store) GetNodeSettingsGates() (nodesettings.Values, error) {
	if err := s.ensureReady(); err != nil {
		return nil, err
	}
	queries, err := newManagementQueries(s.dialect.Name(), s.readDB)
	if err != nil {
		return nil, err
	}
	gates, err := queries.getNodeSettingsGates(context.Background())
	if err != nil {
		return nil, fmt.Errorf("get node settings gates: %w", err)
	}
	return nodesettings.Values(gates), nil
}

// SetNodeSettingsGates persists gates, upserting one row per entry so a
// later call can overwrite an earlier one. recordedEpoch and recordedSlot
// are stamped on every row in this call; callers pass zero for both when
// the write happens before the first block. A nil or empty gates is a no-op.
func (s *Store) SetNodeSettingsGates(
	gates nodesettings.Values,
	recordedEpoch uint64,
	recordedSlot uint64,
) error {
	if len(gates) == 0 {
		return nil
	}
	if err := s.ensureReady(); err != nil {
		return err
	}
	queries, err := newManagementQueries(s.dialect.Name(), s.writeDB)
	if err != nil {
		return err
	}
	for name, value := range gates {
		if err := queries.upsertNodeSettingsGate(
			context.Background(),
			name,
			value,
			int64(recordedEpoch),
			int64(recordedSlot),
		); err != nil {
			return fmt.Errorf(
				"set node settings gates: upsert %q: %w",
				name,
				err,
			)
		}
	}
	return nil
}

// InsertNodeSettingsGateIfAbsent persists a single gate only if no row for
// name exists yet, reporting whether this call is what created it. Unlike
// SetNodeSettingsGates's unconditional upsert, this lets a caller detect a
// concurrent opener's first-ever write to the same gate (see
// commit_timestamp.go's evaluateAndPersistGates) instead of silently
// overwriting it -- the loser learns it lost and can re-evaluate against
// what is now actually persisted rather than assuming its own write landed.
func (s *Store) InsertNodeSettingsGateIfAbsent(
	name string,
	value string,
	recordedEpoch uint64,
	recordedSlot uint64,
) (bool, error) {
	if err := s.ensureReady(); err != nil {
		return false, err
	}
	queries, err := newManagementQueries(s.dialect.Name(), s.writeDB)
	if err != nil {
		return false, err
	}
	rows, err := queries.insertNodeSettingsGateIfAbsent(
		context.Background(),
		name,
		value,
		int64(recordedEpoch),
		int64(recordedSlot),
	)
	if err != nil {
		return false, fmt.Errorf(
			"insert node settings gate %q if absent: %w",
			name,
			err,
		)
	}
	return rows > 0, nil
}

var errNodeSettingsGateInitializationLost = errors.New(
	"node settings gate initialization lost race",
)

func isOnlyNodeSettingsGateInitializationRace(err error) bool {
	if errors.Is(err, errNodeSettingsGateInitializationLost) {
		var joined interface{ Unwrap() []error }
		if !errors.As(err, &joined) {
			return true
		}
		causes := joined.Unwrap()
		return len(causes) == 1 && errors.Is(
			causes[0], errNodeSettingsGateInitializationLost,
		)
	}
	return false
}

// InsertNodeSettingsGatesIfAbsent inserts a complete first-fill set in one
// transaction. A concurrent initializer may win the conditional insert for
// one or more names; in that case the transaction is rolled back so this
// method never leaves a partially initialized gate set behind.
func (s *Store) InsertNodeSettingsGatesIfAbsent(
	gates nodesettings.Values,
	recordedEpoch uint64,
	recordedSlot uint64,
) (bool, error) {
	if len(gates) == 0 {
		return false, nil
	}
	names := make([]string, 0, len(gates))
	for name := range gates {
		names = append(names, name)
	}
	sort.Strings(names)
	inserted := 0
	err := s.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			queries, err := newManagementQueries(s.dialect.Name(), db)
			if err != nil {
				return err
			}
			for _, name := range names {
				rows, err := queries.insertNodeSettingsGateIfAbsent(
					ctx,
					name,
					gates[name],
					int64(recordedEpoch),
					int64(recordedSlot),
				)
				if err != nil {
					return fmt.Errorf(
						"insert node settings gate %q if absent: %w",
						name,
						err,
					)
				}
				inserted += int(rows)
			}
			if inserted != len(names) {
				return errNodeSettingsGateInitializationLost
			}
			return nil
		},
	)
	if isOnlyNodeSettingsGateInitializationRace(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
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
