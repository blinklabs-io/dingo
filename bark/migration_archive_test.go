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

package bark

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// With an archive configured, a payload whose block expired locally is
// converted by fetching the block through the archive, and a read of the row
// proxies the same fetch and returns the bytes the released store held.
func TestMigratePayloadOffsetsFetchesExpiredBlocksThroughTheArchive(
	t *testing.T,
) {
	t.Parallel()
	block := archiveBlockFixtures(t, 2)[1]
	hash := block.Hash()
	cbor := block.Cbor()
	datum := cbor[len(cbor)/2 : len(cbor)/2+24]

	ledger := newTestDB(t)
	indexer, err := dbtest.NewIndexerDatabase(t, ledger, dbtest.IndexerOptions{})
	require.NoError(t, err)
	require.NoError(t, ledger.BlockCreate(models.Block{
		ID: 1, Slot: block.SlotNumber(), Number: block.BlockNumber(),
		Type: uint(gledger.BlockTypeConway), Hash: hash[:],
		PrevHash: block.PrevHash().Bytes(), Cbor: cbor,
	}, nil))
	_, err = ledger.PruneBlock(t.Context(), block.SlotNumber(), hash[:])
	require.NoError(t, err)

	raw, err := dbtest.RawSQLiteMetadata(t, indexer)
	require.NoError(t, err)
	datumHash := lcommon.Blake2b256Hash(datum).Bytes()
	_, err = raw.Exec(
		"INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, ?)",
		datumHash, datum, block.SlotNumber(),
	)
	require.NoError(t, err)

	blocks, configure := serveArchiveBlock(t, block)
	baseURL, fakeArch, httpClient := startFakeArchive(t, blocks)
	configure(fakeArch)
	indexer.SetBlobStore(
		newBarkBlobStoreForTest(t, indexer, baseURL, httpClient),
	)

	require.NoError(t, indexer.MigratePayloadOffsets(t.Context()))
	fetchedByMigration := fakeArch.fetchCalls
	require.NotZero(t, fetchedByMigration, "the migration fetched the block")

	var column []byte
	require.NoError(
		t, raw.QueryRow("SELECT raw_datum FROM datum").Scan(&column),
	)
	offset, err := types.DecodeCborOffset(column)
	require.NoError(t, err, "the column holds an offset, not a marker")
	require.Equal(t, hash[:], offset.BlockHash[:])

	got, err := indexer.GetDatum(t.Context(), datumHash, nil)
	require.NoError(t, err)
	require.Equal(t, datum, got.RawDatum)
	require.Greater(
		t, fakeArch.fetchCalls, fetchedByMigration,
		"the read proxied a fetch to the archive",
	)
}
