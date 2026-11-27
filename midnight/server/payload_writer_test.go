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

package server_test

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// payloadWriter writes Midnight datums into an IndexerDB whose store reads
// them back from an in-memory archive instead of a block store.
type payloadWriter struct {
	t  *testing.T
	db *database.Database
}

func midnightWriter(t *testing.T, db *database.Database) payloadWriter {
	t.Helper()
	return payloadWriter{t: t, db: db}
}

func (w payloadWriter) archive() *testutil.PayloadArchive {
	w.t.Helper()
	setter, ok := w.db.Metadata().(types.PayloadResolverSetter)
	require.True(w.t, ok)
	if archive, ok := archives.Load(w.db); ok {
		return archive.(*testutil.PayloadArchive)
	}
	archive := testutil.NewPayloadArchive()
	setter.SetPayloadResolver(archive)
	archives.Store(w.db, archive)
	return archive
}

func (w payloadWriter) write(fn func(types.Txn) error) error {
	w.t.Helper()
	txn := w.db.Metadata().Transaction(context.Background())
	defer txn.Rollback() //nolint:errcheck
	txn.(types.PayloadLocatorSetter).SetPayloadLocator(w.archive())
	if err := fn(txn); err != nil {
		return err
	}
	return txn.Commit()
}

func (w payloadWriter) InsertGovernanceDatum(
	row *models.MidnightGovernanceDatum,
) error {
	return w.write(func(txn types.Txn) error {
		return w.db.Metadata().InsertMidnightGovernanceDatum(txn, row)
	})
}

func (w payloadWriter) UpsertAriadneParams(
	row *models.MidnightAriadneParams,
) error {
	return w.write(func(txn types.Txn) error {
		return w.db.Metadata().UpsertMidnightAriadneParams(txn, row)
	})
}

func (w payloadWriter) UpsertEpochCandidates(
	row *models.MidnightEpochCandidates,
) error {
	return w.write(func(txn types.Txn) error {
		return w.db.Metadata().UpsertMidnightEpochCandidates(txn, row)
	})
}
