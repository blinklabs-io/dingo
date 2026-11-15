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

package dingo

import (
	"context"
	"errors"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// drainStore adapts the database to statedrain.Store.
type drainStore struct {
	*database.Database
}

// publishingStore hands the drained mask to publish each time the drain
// latches a family, so the ledger stops falling back to rows that were just
// deleted.
type publishingStore struct {
	drainStore
	publish func(nodesettings.FamilyMask)
}

func (s publishingStore) RecordDrainedFamily(
	ctx context.Context,
	f nodesettings.Family,
) error {
	if err := s.drainStore.RecordDrainedFamily(ctx, f); err != nil {
		return err
	}
	mask, err := s.EffectiveDrainedFamilies(ctx)
	if err != nil {
		return err
	}
	s.publish(mask)
	return nil
}

// publishDrainedFamilies reads the latch and drain markers and gives them to
// the ledger and the live UTxO set. It runs before the reconcile, which can
// roll the SQL state back, so a family whose drain was interrupted is already
// served from the set; the drain then resumes its deletes.
func (n *Node) publishDrainedFamilies(ctx context.Context) error {
	mask, err := n.db.EffectiveDrainedFamilies(ctx)
	if err != nil {
		return err
	}
	n.setDrainedFamilies(mask)
	return nil
}

// prepareEpochCache loads the ledger's epoch cache before the ledger starts.
// A drained epochs family has its only copy in the live UTxO set, which holds
// the ledger's epochs only once reconciled to its tip, so that runs first.
func (n *Node) prepareEpochCache(ctx context.Context) error {
	if !n.utxoSetReconciled {
		mask, err := n.db.EffectiveDrainedFamilies(ctx)
		if err != nil {
			return err
		}
		if mask.Has(nodesettings.FamilyEpochs) {
			if err := n.reconcileUtxoSet(ctx); err != nil {
				return err
			}
		}
	}
	return n.ledgerState.PrepareEpochCacheForStartup(ctx)
}

func (n *Node) setDrainedFamilies(mask nodesettings.FamilyMask) {
	if n.ledgerState != nil {
		n.ledgerState.SetDrainedFamilies(mask)
	}
	if n.utxoSet != nil {
		n.utxoSet.SetDrainedFamilies(mask)
	}
}

func (s drainStore) SQLTip(context.Context) (ocommon.Point, error) {
	tip, err := s.GetTip(nil)
	if err != nil {
		return ocommon.Point{}, err
	}
	return tip.Point, nil
}

// DeleteUtxoRows deletes up to limit rows of the metadata store's utxo table;
// its children go with them.
func (s drainStore) DeleteUtxoRows(
	ctx context.Context,
	limit int,
) (int64, error) {
	return s.DeleteLedgerStateRows(ctx, "utxo", "id", limit)
}

func (s drainStore) GetSyncState(
	_ context.Context,
	key string,
) (string, error) {
	return s.Database.GetSyncState(key, nil)
}

func (s drainStore) SetSyncState(
	_ context.Context,
	key, value string,
) error {
	return s.Database.SetSyncState(key, value, nil)
}

func (s drainStore) DeleteSyncState(_ context.Context, key string) error {
	return s.DeleteFamilySyncState(key)
}

func (s drainStore) ListSyncStateKeys(
	_ context.Context,
	prefix string,
) ([]string, error) {
	return s.ListSyncStateKeysByPrefix(prefix, nil)
}

// statementGuard returns the guard a debug build installs on the metadata
// store's connections, nil in every other build.
func (n *Node) statementGuard() metadata.StatementGuard {
	if !drainGuardBuilt {
		return nil
	}
	if n.drainGuard == nil {
		n.drainGuard = statedrain.NewTripGuard()
	}
	return n.drainGuard
}

// drainLedgerState deletes the SQL rows of every family this release
// graduates, once the set is proven to hold them. It runs after the set is
// reconciled with the ledger and before the ledger applies a block.
func (n *Node) drainLedgerState(ctx context.Context) error {
	if !n.config.ledgerStateSetOnly {
		return nil
	}
	if n.utxoSet == nil {
		return errors.New(
			"ledgerStateSetOnly needs the live UTxO set, which is not open",
		)
	}
	env := statedrain.Env{
		Store: publishingStore{
			drainStore: drainStore{n.db},
			publish:    n.setDrainedFamilies,
		},
		Set:               n.utxoSet,
		VerifyBeforeDrain: n.config.ledgerStateVerifyBeforeDrain,
		Logger:            n.config.logger,
	}
	if n.statementGuard() != nil {
		env.Guard = n.drainGuard
	}
	return statedrain.Run(ctx, env, statedrain.Graduated(drainSource{
		Set: n.utxoSet, ldb: dbadapter.New(n.db),
	}))
}

// drainSource adapts the live UTxO set and the metadata store to
// statedrain.Source.
type drainSource struct {
	*utxoset.Set
	ldb ledgerdb.LedgerDB
}

func (s drainSource) Loaded(f nodesettings.Family) (bool, error) {
	return ledger.FamilyLoaded(f, s.Set)
}

func (s drainSource) Verify(
	ctx context.Context,
	f nodesettings.Family,
) error {
	return ledger.VerifyFamily(ctx, f, s.ldb, s.Set)
}
