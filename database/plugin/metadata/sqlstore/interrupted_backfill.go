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
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
)

// beforeUpgrade runs the startup prerequisites of a schema upgrade.
func (s *Store) beforeUpgrade(ctx context.Context, conn *sql.Conn) error {
	// First, and without a write: the reward stake preflight below commits
	// certifications, and a store refused here must stay as its release
	// wrote it.
	if err := s.refuseInterruptedBackfill(ctx, conn); err != nil {
		return err
	}
	// A prior binary may have committed the current schema before its
	// later provenance gate failed, so this must also run at target.
	return s.preflightRewardStakeUpgrade(ctx, conn)
}

// refuseInterruptedBackfill reports metadata.ErrInterruptedBackfill for an
// API-mode store that records an unfinished metadata backfill and whose schema
// this binary would upgrade. Only a release before the storage-role layout
// writes that status, so a store with migrations pending is one that release
// still opens and can finish. A store already at this binary's schema is past
// that; the move to the layout refuses it instead.
func (s *Store) refuseInterruptedBackfill(
	ctx context.Context,
	conn *sql.Conn,
) error {
	tx, err := conn.BeginTx(ctx, s.dialect.BeginOptions(true))
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // The transaction only reads.
	// A PostgreSQL transaction is aborted by a read of a missing table.
	for _, table := range []string{"schema_migrations", "sync_state"} {
		exists, err := s.rewardUpgradeTableExists(ctx, tx, table)
		if err != nil {
			return fmt.Errorf("inspect %s: %w", table, err)
		}
		if !exists {
			return nil
		}
	}
	var status string
	err = tx.QueryRowContext(
		ctx,
		"SELECT value FROM sync_state WHERE sync_key = 'sync_status'",
	).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && status != "backfill") {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read sync status: %w", err)
	}
	var applied int
	if err := tx.QueryRowContext(
		ctx,
		s.dialect.Rebind(
			"SELECT COUNT(*) FROM schema_migrations WHERE phase = ?",
		),
		string(migrations.PhaseComplete),
	).Scan(&applied); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if applied >= len(s.migrations) {
		return nil
	}
	// A store that left API mode keeps no API history to finish.
	gates, err := s.rewardUpgradeTableExists(ctx, tx, "node_settings_gate")
	if err != nil {
		return fmt.Errorf("inspect node_settings_gate: %w", err)
	}
	if gates {
		var mode string
		err = tx.QueryRowContext(
			ctx,
			"SELECT value FROM node_settings_gate WHERE name = 'storage_mode'",
		).Scan(&mode)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read storage mode: %w", err)
		}
		if mode == "core" {
			return nil
		}
	}
	return metadata.ErrInterruptedBackfill
}
