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

package nodestorage

import (
	"context"
	"errors"
	"log/slog"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// DrainStore adapts the database to statedrain.Store.
type DrainStore struct {
	*database.Database
}

func (s DrainStore) SQLTip(context.Context) (ocommon.Point, error) {
	tip, err := s.GetTip(nil)
	if err != nil {
		return ocommon.Point{}, err
	}
	return tip.Point, nil
}

// DeleteUtxoRows deletes up to limit rows of the metadata store's utxo table;
// its children go with them.
func (s DrainStore) DeleteUtxoRows(
	ctx context.Context,
	limit int,
) (int64, error) {
	return s.DeleteLedgerStateRows(ctx, "utxo", "id", limit)
}

func (s DrainStore) GetSyncState(
	_ context.Context,
	key string,
) (string, error) {
	return s.Database.GetSyncState(key, nil)
}

func (s DrainStore) SetSyncState(
	_ context.Context,
	key, value string,
) error {
	return s.Database.SetSyncState(key, value, nil)
}

func (s DrainStore) DeleteSyncState(_ context.Context, key string) error {
	return s.DeleteFamilySyncState(key)
}

func (s DrainStore) ListSyncStateKeys(
	_ context.Context,
	prefix string,
) ([]string, error) {
	return s.ListSyncStateKeysByPrefix(prefix, nil)
}

// PublishingStore hands the drained mask to Publish each time the drain
// latches a family, so the ledger stops falling back to rows that were just
// deleted.
type PublishingStore struct {
	DrainStore
	Publish func(nodesettings.FamilyMask)
}

func (s PublishingStore) RecordDrainedFamily(
	ctx context.Context,
	f nodesettings.Family,
) error {
	if err := s.DrainStore.RecordDrainedFamily(ctx, f); err != nil {
		return err
	}
	mask, err := s.EffectiveDrainedFamilies(ctx)
	if err != nil {
		return err
	}
	s.Publish(mask)
	return nil
}

// DrainSource adapts the live UTxO set to statedrain.Source.
type DrainSource struct {
	*utxoset.Set
}

func (s DrainSource) Loaded(f nodesettings.Family) (bool, error) {
	return ledger.FamilyLoaded(f, s.Set)
}

// DrainLedgerState deletes the SQL rows of every family this release
// graduates, once the set is proven to hold them. It runs after the set is
// reconciled with the ledger and before the ledger writes its genesis or
// applies a block, so a new database never holds those families in SQL: its
// genesis state is written straight into the set.
func DrainLedgerState(
	ctx context.Context,
	db *database.Database,
	set *utxoset.Set,
	logger *slog.Logger,
	guard statedrain.Guard,
	publish func(nodesettings.FamilyMask),
) error {
	if set == nil {
		return errors.New("the live UTxO set is not open")
	}
	if publish == nil {
		publish = func(nodesettings.FamilyMask) {}
	}
	return statedrain.Run(
		ctx,
		statedrain.Env{
			Store:  PublishingStore{DrainStore: DrainStore{db}, Publish: publish},
			Set:    set,
			Guard:  guard,
			Logger: logger,
		},
		statedrain.Graduated(DrainSource{Set: set}),
	)
}
