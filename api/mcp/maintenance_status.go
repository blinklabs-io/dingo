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

package mcp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
)

func sqliteMaintenanceStatus(ctx context.Context, db *sql.DB) (string, error) {
	if db == nil {
		return "- **Metadata Index Readiness**: unknown (SQLite unavailable)\n", nil
	}
	rows, err := db.QueryContext(
		ctx,
		"SELECT type,name FROM sqlite_master WHERE type IN ('table','index')",
	)
	if err != nil {
		return "", fmt.Errorf("read metadata index catalog: %w", err)
	}
	defer rows.Close()
	indexes := make(map[string]bool)
	hasState := false
	for rows.Next() {
		var kind, name string
		if err := rows.Scan(&kind, &name); err != nil {
			return "", err
		}
		if kind == "index" {
			indexes[name] = true
		}
		if kind == "table" && name == "sync_state" {
			hasState = true
		}
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return "", err
	}
	if !hasState {
		return "- **Metadata Index Readiness**: unknown (maintenance metadata unavailable)\n", nil
	}
	var critical, lazy []string
	for _, index := range deferred.Manifest {
		if !indexes[index.Name] {
			if index.Critical {
				critical = append(critical, index.Name)
			} else {
				lazy = append(lazy, index.Name)
			}
		}
	}
	for _, index := range deferred.Retained {
		if !indexes[index.Name] {
			critical = append(critical, index.Name)
		}
	}
	stateRows, err := db.QueryContext(
		ctx,
		"SELECT sync_key,value FROM sync_state WHERE sync_key IN (?,?,?)",
		"sync_status",
		deferred.SyncStateKey,
		metadata.PlannerStatsBackfillSyncKey,
	)
	if err != nil {
		return "", fmt.Errorf("read metadata maintenance state: %w", err)
	}
	defer stateRows.Close()
	state := make(map[string]string)
	for stateRows.Next() {
		var key, value string
		if err := stateRows.Scan(&key, &value); err != nil {
			return "", err
		}
		state[key] = value
	}
	if err := stateRows.Err(); err != nil {
		return "", err
	}
	readiness := "ready"
	if len(critical) > 0 {
		readiness = "not ready; missing critical indexes: " + strings.Join(
			critical,
			", ",
		)
	}
	if state["sync_status"] != "" {
		readiness = "not ready; import state: " + formatUntrustedInline(
			state["sync_status"],
		)
	}
	maintenance := "complete"
	if len(lazy) > 0 || state[deferred.SyncStateKey] != "" {
		maintenance = fmt.Sprintf(
			"pending (%d background indexes missing)",
			len(lazy),
		)
	}
	stats := "not recorded"
	if state[metadata.PlannerStatsBackfillSyncKey] != "" {
		stats = "completion recorded"
	}
	return fmt.Sprintf(
		"- **Metadata Index Readiness**: %s\n- **Background Index Maintenance**: %s\n- **Post-backfill Statistics**: %s\n",
		readiness,
		maintenance,
		stats,
	), nil
}
