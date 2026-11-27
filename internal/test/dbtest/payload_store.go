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

package dbtest

import (
	"context"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// PayloadStore is an API-mode store whose payload columns locate and resolve
// their payloads in an in-memory archive, standing in for the block stores. A
// Midnight write made with no transaction runs in one of its own, as the
// store's methods do, bound to the archive.
type PayloadStore struct {
	*sqlstore.Store
	Archive *testutil.PayloadArchive
}

// NewPayloadStore installs a fresh archive as store's payload resolver.
func NewPayloadStore(store *sqlstore.Store) *PayloadStore {
	archive := testutil.NewPayloadArchive()
	store.SetPayloadResolver(archive)
	return &PayloadStore{Store: store, Archive: archive}
}

// Transaction begins a transaction bound to the archive.
func (s *PayloadStore) Transaction(ctx context.Context) types.Txn {
	txn := s.Store.Transaction(ctx)
	if setter, ok := txn.(types.PayloadLocatorSetter); ok {
		setter.SetPayloadLocator(s.Archive)
	}
	return txn
}

func (s *PayloadStore) write(txn types.Txn, fn func(types.Txn) error) error {
	if txn != nil {
		return fn(txn)
	}
	own := s.Transaction(context.Background())
	defer own.Rollback() //nolint:errcheck
	if err := fn(own); err != nil {
		return err
	}
	return own.Commit()
}

func (s *PayloadStore) CreateMidnightRegistration(
	txn types.Txn,
	row *models.MidnightRegistration,
) error {
	return s.write(txn, func(t types.Txn) error {
		return s.Store.CreateMidnightRegistration(t, row)
	})
}

func (s *PayloadStore) CreateMidnightDeregistration(
	txn types.Txn,
	row *models.MidnightDeregistration,
) error {
	return s.write(txn, func(t types.Txn) error {
		return s.Store.CreateMidnightDeregistration(t, row)
	})
}

func (s *PayloadStore) InsertMidnightGovernanceDatum(
	txn types.Txn,
	row *models.MidnightGovernanceDatum,
) error {
	return s.write(txn, func(t types.Txn) error {
		return s.Store.InsertMidnightGovernanceDatum(t, row)
	})
}

func (s *PayloadStore) UpsertMidnightAriadneParams(
	txn types.Txn,
	row *models.MidnightAriadneParams,
) error {
	return s.write(txn, func(t types.Txn) error {
		return s.Store.UpsertMidnightAriadneParams(t, row)
	})
}

func (s *PayloadStore) CreateMidnightAriadneRollback(
	txn types.Txn,
	row *models.MidnightAriadneRollback,
) error {
	return s.write(txn, func(t types.Txn) error {
		return s.Store.CreateMidnightAriadneRollback(t, row)
	})
}

func (s *PayloadStore) CreateMidnightCandidateRemoval(
	txn types.Txn,
	row *models.MidnightCandidateRemoval,
) error {
	return s.write(txn, func(t types.Txn) error {
		return s.Store.CreateMidnightCandidateRemoval(t, row)
	})
}

func (s *PayloadStore) UpsertMidnightEpochCandidates(
	txn types.Txn,
	row *models.MidnightEpochCandidates,
) error {
	return s.write(txn, func(t types.Txn) error {
		return s.Store.UpsertMidnightEpochCandidates(t, row)
	})
}
