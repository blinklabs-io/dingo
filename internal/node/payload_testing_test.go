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

package node

import (
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

var testPayloadArchives sync.Map

// testPayloads returns the archive db's store reads payloads from in a test,
// installing it as the store's resolver on first use. Tests that write
// payloads without a block use it in place of the block stores.
func testPayloads(tb testing.TB, db *database.Database) *testutil.PayloadArchive {
	tb.Helper()
	if archive, ok := testPayloadArchives.Load(db); ok {
		return archive.(*testutil.PayloadArchive)
	}
	setter, ok := db.Metadata().(types.PayloadResolverSetter)
	require.True(tb, ok, "the metadata store takes a payload resolver")
	archive := testutil.NewPayloadArchive()
	setter.SetPayloadResolver(archive)
	testPayloadArchives.Store(db, archive)
	return archive
}

// bindTestPayloads makes writes through txn locate their payloads in db's
// test archive.
func bindTestPayloads(tb testing.TB, db *database.Database, txn *database.Txn) {
	tb.Helper()
	setter, ok := txn.Metadata().(types.PayloadLocatorSetter)
	require.True(tb, ok, "the metadata transaction takes a payload locator")
	setter.SetPayloadLocator(testPayloads(tb, db))
}

// setTestPParamUpdate stores one update proposal in a transaction of its own.
func setTestPParamUpdate(
	tb testing.TB,
	db *database.Database,
	genesis, update []byte,
	slot, epoch uint64,
) {
	tb.Helper()
	txn := db.Transaction(tb.Context(), true)
	defer txn.Rollback() //nolint:errcheck
	bindTestPayloads(tb, db, txn)
	require.NoError(tb, db.SetPParamUpdate(genesis, update, slot, epoch, txn))
	require.NoError(tb, txn.Commit())
}
