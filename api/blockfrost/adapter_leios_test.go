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
	"database/sql"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// leiosTestBlockCbor builds a Dijkstra ranking block with an empty body whose
// header carries the body's hash, so decoding verifies it. The
// Leios header extension (leios_certified, then leios_announcement, null
// when announce is nil) is only reproduced from decoded bytes, so the header
// body array is assembled by hand.
func leiosTestBlockCbor(
	t *testing.T,
	number uint64,
	slot uint64,
	prevHash lcommon.Blake2b256,
	certified bool,
	announce *lcommon.Blake2b256,
) []byte {
	t.Helper()
	body := dijkstra.DijkstraBlockBody{}
	headerBody := babbage.BabbageBlockHeaderBody{
		BlockNumber:   number,
		Slot:          slot,
		PrevHash:      prevHash,
		BlockBodyHash: body.Hash(),
		VrfKey:        make([]byte, 32),
		VrfResult: lcommon.VrfResult{
			Output: make([]byte, 64),
			Proof:  make([]byte, 80),
		},
		OpCert: babbage.BabbageOpCert{
			HotVkey:   make([]byte, 32),
			Signature: make([]byte, 64),
		},
		ProtoVersion: babbage.BabbageProtoVersion{
			Major: dijkstra.MinProtocolVersionDijkstra,
		},
	}
	babbageBodyCbor, err := cbor.Encode(&headerBody)
	require.NoError(t, err)
	var bodyElems []cbor.RawMessage
	_, err = cbor.Decode(babbageBodyCbor, &bodyElems)
	require.NoError(t, err)
	certifiedCbor, err := cbor.Encode(certified)
	require.NoError(t, err)
	bodyElems = append(bodyElems, cbor.RawMessage(certifiedCbor))
	announcement := cbor.RawMessage{0xf6} // null: announces nothing
	if announce != nil {
		announcement, err = cbor.Encode([]any{announce.Bytes(), uint64(1)})
		require.NoError(t, err)
	}
	bodyElems = append(bodyElems, announcement)
	extendedBody, err := cbor.Encode(bodyElems)
	require.NoError(t, err)
	headerCbor, err := cbor.Encode([]any{
		cbor.RawMessage(extendedBody),
		make([]byte, 448),
	})
	require.NoError(t, err)
	emptyBody, err := cbor.Encode(body)
	require.NoError(t, err)
	blockCbor, err := cbor.Encode([]any{
		cbor.RawMessage(headerCbor),
		cbor.RawMessage(emptyBody),
	})
	require.NoError(t, err)
	return blockCbor
}

// storeLeiosTestBlock stores a Dijkstra block and returns it decoded.
func storeLeiosTestBlock(
	t *testing.T,
	db *database.Database,
	blockCbor []byte,
) lcommon.Block {
	t.Helper()
	stored := models.Block{Type: dijkstra.BlockTypeDijkstra, Cbor: blockCbor}
	decoded, err := stored.Decode()
	require.NoError(t, err)
	stored.Hash = decoded.Hash().Bytes()
	stored.PrevHash = decoded.PrevHash().Bytes()
	stored.Slot = decoded.SlotNumber()
	stored.Number = decoded.BlockNumber()
	stored.ID = stored.Number + database.BlockInitialIndex
	require.NoError(t, db.BlockCreate(stored, nil))
	return decoded
}

// leiosTestEndorserTx builds one endorser-block transaction as leios-fetch
// delivers it: a Dijkstra block_transaction [body, witnesses, aux, is_valid].
func leiosTestEndorserTx(
	t *testing.T,
	seed byte,
) (cbor.RawMessage, lcommon.Transaction) {
	t.Helper()
	bodyCbor, err := cbor.Encode(map[uint]any{
		0: cbor.Tag{
			Number:  258,
			Content: []any{[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)}},
		},
		1: []any{[]any{
			append([]byte{0x61}, make([]byte, 28)...),
			uint64(1_000_000),
		}},
		2: 200_000 + uint64(seed),
	})
	require.NoError(t, err)
	txCbor, err := cbor.Encode([]any{
		cbor.RawMessage(bodyCbor),
		map[uint]any{},
		nil,
		true,
	})
	require.NoError(t, err)
	tx, err := safedecode.DijkstraBlockTransaction(txCbor)
	require.NoError(t, err)
	return cbor.RawMessage(txCbor), tx
}

type leiosCertifyingFixture struct {
	adapter *NodeAdapter
	db      *database.Database
	raw     *sql.DB
	// certRB is the certifying ranking block: certified header, empty body.
	certRB lcommon.Block
	ebHash lcommon.Blake2b256
	ebSlot uint64
	ebRaw  []cbor.RawMessage
	// applied are the endorser transactions with rows under certRB, in
	// endorser-block order.
	applied []lcommon.Transaction
}

// newLeiosCertifyingFixture stores an announcing ranking block, the ranking
// block certifying its endorser block, and the certified endorser block's
// transaction list. The first two endorser transactions have rows under the
// certifying block, as applyEndorserBlock stores them; the third was already
// on chain, so the ledger skipped it and stored no row for it here.
func newLeiosCertifyingFixture(t *testing.T) *leiosCertifyingFixture {
	t.Helper()
	nodeConfig, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		cardano.EmbeddedConfigPath("mainnet"),
	)
	require.NoError(t, err)
	adapter, raw, db := newDBBackedAdapter(t, nodeConfig)

	var ebHash lcommon.Blake2b256
	copy(ebHash[:], fill32(0xeb))
	parent := storeLeiosTestBlock(t, db, leiosTestBlockCbor(
		t, 1, 100, lcommon.Blake2b256{}, false, &ebHash,
	))
	certRB := storeLeiosTestBlock(t, db, leiosTestBlockCbor(
		t, 2, 101, parent.Hash(), true, nil,
	))
	require.Empty(t, certRB.Transactions())

	rawA, txA := leiosTestEndorserTx(t, 0x5a)
	rawB, txB := leiosTestEndorserTx(t, 0x5b)
	rawSkipped, _ := leiosTestEndorserTx(t, 0x5c)
	ebRaw := []cbor.RawMessage{rawA, rawB, rawSkipped}
	for i, tx := range []lcommon.Transaction{txA, txB} {
		insertAdapterTransaction(t, raw, &models.Transaction{
			Hash:       tx.Hash().Bytes(),
			BlockHash:  certRB.Hash().Bytes(),
			BlockIndex: uint32(i), //nolint:gosec // test indexes
			Slot:       certRB.SlotNumber(),
			Valid:      true,
		})
	}
	// Block info converts the slot to an epoch and a time.
	require.NoError(t, db.SetEpoch(0, 0, nil, nil, nil, nil, 6, 1000, 1000, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(
			certRB.SlotNumber(),
			certRB.Hash().Bytes(),
		),
		BlockNumber: certRB.BlockNumber(),
	}, nil))
	require.NoError(t, adapter.ledgerState.PrepareEpochCacheForStartup(
		context.Background(),
	))
	return &leiosCertifyingFixture{
		adapter: adapter,
		db:      db,
		raw:     raw,
		certRB:  certRB,
		ebHash:  ebHash,
		ebSlot:  parent.SlotNumber(),
		ebRaw:   ebRaw,
		applied: []lcommon.Transaction{txA, txB},
	}
}

func (f *leiosCertifyingFixture) persistEndorserTxs(t *testing.T) {
	t.Helper()
	require.NoError(t, f.db.SetLeiosEBTxs(f.ebSlot, f.ebHash.Bytes(), f.ebRaw))
}

// A certifying ranking block carries no transactions in its body; its
// transactions are the certified endorser block's, stored under its hash.
// Blockfrost must report them as the block's transactions, as the
// node-to-client chain-sync server does by inlining them.
func TestCertifyingBlockReportsEndorserTransactions(t *testing.T) {
	t.Parallel()
	f := newLeiosCertifyingFixture(t)
	f.persistEndorserTxs(t)
	ctx := context.Background()
	id := f.certRB.Hash().String()
	want := make([]string, 0, len(f.applied))
	for _, tx := range f.applied {
		want = append(want, tx.Hash().String())
	}

	info, err := f.adapter.BlockByHashOrNumber(id)
	require.NoError(t, err)
	require.Equal(t, len(f.applied), info.TxCount)

	hashes, err := f.adapter.BlockTransactionHashes(ctx, id)
	require.NoError(t, err)
	require.Equal(t, want, hashes)

	cbors, err := f.adapter.BlockTransactionCBOR(ctx, id)
	require.NoError(t, err)
	require.Len(t, cbors, len(f.applied))
	for i, tx := range f.applied {
		require.Equal(t, want[i], cbors[i].TxHash)
		require.Equal(t, hex.EncodeToString(tx.Cbor()), cbors[i].CBOR)
	}

	for i, tx := range f.applied {
		row, block, decodedTx, err := f.adapter.decodedTransactionByHash(
			tx.Hash().Bytes(),
		)
		require.NoError(t, err)
		require.Equal(t, f.certRB.Hash().Bytes(), block.Hash)
		require.Equal(t, uint32(i), row.BlockIndex) //nolint:gosec // test indexes
		require.Equal(t, want[i], decodedTx.Hash().String())

		info, err := f.adapter.Transaction(tx.Hash().Bytes())
		require.NoError(t, err)
		require.Equal(t, id, info.Block)
		require.Equal(t, f.certRB.BlockNumber(), info.BlockHeight)
		require.Equal(t, uint32(i), info.Index) //nolint:gosec // test indexes
	}
}

// Without the persisted endorser-block transaction list (pruned under a
// non-zero Leios persistence retention) the block body is reported, as
// before, rather than failing the request.
func TestCertifyingBlockWithoutEndorserTxsFallsBackToBody(t *testing.T) {
	t.Parallel()
	f := newLeiosCertifyingFixture(t)
	ctx := context.Background()
	id := f.certRB.Hash().String()

	info, err := f.adapter.BlockByHashOrNumber(id)
	require.NoError(t, err)
	require.Zero(t, info.TxCount)

	hashes, err := f.adapter.BlockTransactionHashes(ctx, id)
	require.NoError(t, err)
	require.Empty(t, hashes)

	_, _, _, err = f.adapter.decodedTransactionByHash(
		f.applied[0].Hash().Bytes(),
	)
	require.ErrorIs(t, err, ErrTransactionNotFound)
}

// History imported without transaction rows (a Mithril restore) reports the
// block body even when the endorser transaction list is held.
func TestCertifyingBlockWithoutRowsFallsBackToBody(t *testing.T) {
	t.Parallel()
	f := newLeiosCertifyingFixture(t)
	f.persistEndorserTxs(t)
	_, err := f.raw.Exec(`DELETE FROM "transaction"`)
	require.NoError(t, err)

	info, err := f.adapter.BlockByHashOrNumber(f.certRB.Hash().String())
	require.NoError(t, err)
	require.Zero(t, info.TxCount)
}
