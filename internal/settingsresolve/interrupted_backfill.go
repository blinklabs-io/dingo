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

package settingsresolve

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/database/dbinfo"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/config"
	_ "modernc.org/sqlite" // the read opens the store without its provider
)

// ErrInterruptedBackfill is metadata.ErrInterruptedBackfill, which every
// provider's open also reports for such a store before it migrates the schema.
var ErrInterruptedBackfill = metadata.ErrInterruptedBackfill

// refuseInterruptedBackfill reports ErrInterruptedBackfill for a SQLite store
// in the data directory without opening it for writing, so the refusal leaves
// the file as v0.94.2 wrote it. A store it cannot read, or one of another
// provider, is left for the provider's open, which refuses the same state
// before it changes the schema.
func refuseInterruptedBackfill(cfg *config.Config) error {
	switch cfg.Plugins.Storage.Metadata.Provider {
	case "", "sqlite":
	default:
		return nil
	}
	// Moving the settings into the dbinfo file is the first open of the
	// store by this release, which upgrades its schema: v0.94.2 then
	// refuses the store and no backfill can be finished there, so the store
	// is not read again.
	if info, err := dbinfo.Read(cfg.DatabasePath); err == nil &&
		info.HoldsGates() {
		return nil
	}
	path := filepath.Join(cfg.DatabasePath, "metadata.sqlite")
	if _, err := os.Stat(path); err != nil {
		return nil
	}
	raw, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		return nil
	}
	defer raw.Close()
	var status string
	if err := raw.QueryRow(
		`SELECT value FROM sync_state WHERE sync_key = 'sync_status'`,
	).Scan(&status); err != nil || status != "backfill" {
		return nil
	}
	// A store that left API mode keeps no API history to finish.
	var mode string
	if err := raw.QueryRow(
		`SELECT value FROM node_settings_gate WHERE name = 'storage_mode'`,
	).Scan(&mode); err != nil {
		mode = ""
	}
	if mode == "core" {
		return nil
	}
	return fmt.Errorf("%s: %w", path, ErrInterruptedBackfill)
}
