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

package blockfrost

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
)

// archiveBlocks gives every slot the same archive to locate payloads in, in
// place of the block stores.
type archiveBlocks struct{ archive *testutil.PayloadArchive }

func (b archiveBlocks) LocatorAtSlot(
	context.Context,
	uint64,
) (types.PayloadLocator, error) {
	return b.archive, nil
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	out, err := json.Marshal(v)
	require.NoError(t, err)
	return out
}

type payloadResponses struct {
	datumJSON, datumCBOR   string
	scriptJSON, scriptCBOR string
	metadataJSON           string
	metadataCBOR           string
}

func readPayloadResponses(
	t *testing.T,
	adapter *NodeAdapter,
	datumHash, scriptHash, txHash []byte,
) payloadResponses {
	t.Helper()
	ctx := context.Background()
	var out payloadResponses
	datumJSON, err := adapter.DatumJSON(ctx, datumHash)
	require.NoError(t, err)
	out.datumJSON = string(datumJSON)
	out.datumCBOR, err = adapter.DatumCBOR(ctx, datumHash)
	require.NoError(t, err)
	scriptJSON, err := adapter.ScriptJSON(ctx, scriptHash)
	require.NoError(t, err)
	out.scriptJSON = string(mustJSON(t, scriptJSON))
	scriptCBOR, err := adapter.ScriptCBOR(ctx, scriptHash)
	require.NoError(t, err)
	out.scriptCBOR = string(mustJSON(t, scriptCBOR))
	labels, err := adapter.TransactionMetadata(ctx, txHash)
	require.NoError(t, err)
	out.metadataJSON = string(mustJSON(t, labels))
	cborLabels, err := adapter.TransactionMetadataCBOR(ctx, txHash)
	require.NoError(t, err)
	out.metadataCBOR = string(mustJSON(t, cborLabels))
	return out
}

// A released store's datum, script and transaction metadata rows hold copies
// of their bytes. After they become block offsets, the datum, script and
// metadata endpoints answer exactly as they do over a store that was written
// with offsets.
func TestPayloadEndpointsAnswerTheSameAfterMigration(t *testing.T) {
	t.Parallel()
	datum, err := data.Encode(
		data.NewConstr(0, data.NewInteger(big.NewInt(42))),
	)
	require.NoError(t, err)
	datumHash := lcommon.Blake2b256Hash(datum).Bytes()
	script := []byte{0x45, 0x01, 0x02, 0x03, 0x04, 0x05}
	scriptHash := bytes.Repeat([]byte{0x02}, 28)
	metadataCbor, err := hex.DecodeString("a2016161" + "1902d1" + "a10163" + "74776f")
	require.NoError(t, err)
	label721, err := hex.DecodeString("a10163" + "74776f")
	require.NoError(t, err)
	txHash := bytes.Repeat([]byte{0x71}, 32)
	newTx := func() *models.Transaction {
		return &models.Transaction{
			Hash: txHash, BlockHash: bytes.Repeat([]byte{0xb1}, 32),
			Metadata: metadataCbor, Slot: 1, Valid: true,
		}
	}

	// Written with offsets, as the IndexerDB writer writes it.
	control, controlRaw, _ := newDBBackedAdapter(t)
	_, err = controlRaw.Exec(
		`INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, 1)`,
		datumHash, payloadColumn(t, controlRaw, datum),
	)
	require.NoError(t, err)
	insertScript(t, controlRaw, scriptHash, script, 2)
	controlTx := newTx()
	insertAdapterTransaction(t, controlRaw, controlTx)
	_, err = controlRaw.Exec(
		`INSERT INTO transaction_metadata_label (transaction_id, label, slot, cbor_value, json_value) VALUES (?, '1', 1, ?, '"a"'), (?, '721', 1, ?, NULL)`,
		controlTx.ID, payloadColumn(t, controlRaw, []byte{0x61, 0x61}),
		controlTx.ID, payloadColumn(t, controlRaw, label721),
	)
	require.NoError(t, err)

	// Written as the released store wrote it: copies in the columns.
	migrated, raw, db := newDBBackedAdapter(t)
	_, err = raw.Exec(
		`INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, 1)`,
		datumHash, datum,
	)
	require.NoError(t, err)
	_, err = raw.Exec(
		`INSERT INTO script (hash, content, created_slot, type) VALUES (?, ?, 1, 2)`,
		scriptHash, script,
	)
	require.NoError(t, err)
	released := newTx()
	released.Metadata = nil
	insertAdapterTransaction(t, raw, released)
	_, err = raw.Exec(
		`UPDATE applied_transaction SET metadata = ? WHERE tx_id = ?`,
		metadataCbor, released.ID,
	)
	require.NoError(t, err)
	_, err = raw.Exec(
		`INSERT INTO transaction_metadata_label (transaction_id, label, slot, cbor_value, json_value) VALUES (?, '1', 1, ?, '"a"'), (?, '721', 1, ?, NULL)`,
		released.ID, []byte{0x61, 0x61}, released.ID, label721,
	)
	require.NoError(t, err)

	archive, ok := fixtureArchives.Load(raw)
	require.True(t, ok)
	migrator, ok := db.Metadata().(metadata.PayloadColumnMigrator)
	require.True(t, ok)
	require.NoError(t, migrator.MigratePayloadColumns(
		t.Context(), archiveBlocks{archive: archive.(*testutil.PayloadArchive)},
	))

	want := readPayloadResponses(t, control, datumHash, scriptHash, txHash)
	require.NotEmpty(t, want.datumJSON)
	require.NotEmpty(t, want.metadataCBOR)
	got := readPayloadResponses(t, migrated, datumHash, scriptHash, txHash)
	require.Equal(t, want, got)
	require.Equal(t, hex.EncodeToString(datum), got.datumCBOR)
}
