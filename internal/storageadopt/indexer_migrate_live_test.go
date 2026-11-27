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

package storageadopt

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

type liveOutputs map[string][]byte

func (l liveOutputs) LiveUtxoCbor(
	_ types.Txn,
	txID []byte,
	index uint32,
) ([]byte, bool, error) {
	cbor, ok := l[string(txID)+string(rune(index))]
	return cbor, ok, nil
}

// A v0.94.2 store whose block expired with no archive, and whose unspent
// output carries a datum, keeps that datum through the upgrade: the live
// output holds the bytes, so the column names them there instead of becoming an
// expired-payload marker, and the datum reads once the node's live set serves
// the output.
func TestMigrateToIndexerDBKeepsADatumOfAnUnspentOutputInAnExpiredBlock(
	t *testing.T,
) {
	t.Parallel()
	cfg, _ := seedAPIStore(t, "api", 5)
	m := &indexerMigration{cfg: cfg, logger: migrateTestLogger()}
	deps, err := m.dependencies("api")
	require.NoError(t, err)
	rt, err := internalplugins.OpenDatabase(
		t.Context(), m.databaseConfig("api", ""), m.selections(), deps,
	)
	require.NoError(t, err)
	db := rt.Database

	// The first block holds an output with an inline datum: a stretch of the
	// block's own bytes, the way an offset entry names an output.
	first, err := db.BlocksFromSlot(0).NextRaw()
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Greater(t, len(first.Cbor), 100)
	output := bytes.Clone(first.Cbor[20:90])
	datum := bytes.Clone(output[30:46])
	datumHash := lcommon.Blake2b256Hash(datum)
	txID := bytes.Repeat([]byte{0xa7}, 32)
	var blockHash [32]byte
	copy(blockHash[:], first.Hash)
	outOffset := database.CborOffset{
		BlockSlot: first.Slot, BlockHash: blockHash,
		ByteOffset: 20, ByteLength: uint32(len(output)),
	}
	blobTxn := db.BlobTxn(true)
	require.NoError(t, blobTxn.Do(func(txn *database.Txn) error {
		return db.Blob().SetUtxo(
			txn.Blob(), txID, 0, database.EncodeUtxoOffset(&outOffset),
		)
	}))
	mdTxn := db.MetadataTxn(t.Context(), true)
	require.NoError(t, mdTxn.Do(func(txn *database.Txn) error {
		return db.Metadata().CreateUtxo(txn.Metadata(), &models.Utxo{
			TxId: txID, Address: []byte{0xa, 0xb}, DatumHash: datumHash.Bytes(),
			PaymentKey: bytes.Repeat([]byte{0x11}, 28),
			AddedSlot:  first.Slot, Amount: types.Uint64(1),
		})
	}))
	// Expiry rewrites the output's entry as raw CBOR, as a released store did.
	_, err = db.PruneBlock(t.Context(), first.Slot, first.Hash)
	require.NoError(t, err)
	require.NoError(t, rt.Close(t.Context()))

	raw, err := sql.Open(
		"sqlite",
		"file:"+filepath.Join(cfg.DatabasePath, "metadata.sqlite")+
			"?_pragma=busy_timeout(30000)",
	)
	require.NoError(t, err)
	_, err = raw.Exec(
		"INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, ?)",
		datumHash.Bytes(), datum, first.Slot,
	)
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	require.NoError(t, MigrateToIndexerDB(
		t.Context(), cfg, migrateTestLogger(), nil,
	))

	coreDeps, err := m.dependencies("core")
	require.NoError(t, err)
	core, err := internalplugins.OpenDatabase(
		t.Context(), m.databaseConfig("core", "split"), m.selections(),
		coreDeps,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = core.Close(context.Background()) })
	host, err := internalplugins.NewHost()
	require.NoError(t, err)
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	idx, err := internalplugins.OpenIndexerDatabase(
		t.Context(), host, core.Database,
		m.databaseConfig("core", "split"), m.selections(), coreDeps,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = idx.Close() })
	idx.SetLiveOutputs(liveOutputs{string(txID) + string(rune(0)): output})

	got, err := idx.GetDatum(t.Context(), datumHash.Bytes(), nil)
	require.NoError(t, err, "the datum of an unspent output was dropped")
	require.Equal(t, datum, got.RawDatum)
}
