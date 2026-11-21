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

package ledgerstate

import (
	"context"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// FamilyLoader is a live UTxO set that also holds the ledger-state families a
// database drained from its metadata store. An import into a database with
// drained families writes their rows to SQL as for any database, then moves
// them into the set through the loaders the set runs at startup, and deletes
// the SQL rows again.
type FamilyLoader interface {
	UtxoSetLoader
	// SetDrainedFamilies sets the families the set refuses to reload from
	// the metadata store.
	SetDrainedFamilies(drained nodesettings.FamilyMask)
	// LoadLedgerState loads into the set every family it lacks from ldb.
	LoadLedgerState(ctx context.Context, ldb ledgerdb.LedgerDB) error
}

// beginDrainedImport starts a drained import when the database drained
// families or has one pending and the loader can take them. finish reports
// that this call began it, so this call also finishes it; an outer caller
// that began it finishes it after its own writes.
func beginDrainedImport(
	ctx context.Context,
	cfg ImportConfig,
) (active, finish bool, err error) {
	if _, ok := cfg.UtxoSet.(FamilyLoader); !ok {
		return false, false, nil
	}
	already := cfg.Database.DrainedImportActive()
	began, err := cfg.Database.BeginDrainedImport(ctx)
	if err != nil {
		return false, false, err
	}
	return began, began && !already, nil
}

// FinishDrainedImport loads the families of a drained database into loader
// from the SQL rows its import wrote, then deletes those rows. It is safe to
// call again after an interruption: families the set already holds are not
// loaded again, and the deletes resume.
func FinishDrainedImport(
	ctx context.Context,
	db *database.Database,
	loader UtxoSetLoader,
) error {
	fl, ok := loader.(FamilyLoader)
	if !ok {
		return errors.New("the live UTxO set cannot load ledger-state families")
	}
	mask, pending, err := db.DrainedImportPending(ctx)
	if err != nil {
		return err
	}
	if !pending {
		return nil
	}
	return db.FinishDrainedImport(ctx, func(ctx context.Context) error {
		fl.SetDrainedFamilies(0)
		defer fl.SetDrainedFamilies(mask)
		if m, ok := loader.(interface{ SetMergeOverDrained(bool) }); ok {
			m.SetMergeOverDrained(true)
			defer m.SetMergeOverDrained(false)
		}
		if err := fl.LoadLedgerState(ctx, dbadapter.New(db)); err != nil {
			return fmt.Errorf("load ledger state into the set: %w", err)
		}
		return nil
	})
}
