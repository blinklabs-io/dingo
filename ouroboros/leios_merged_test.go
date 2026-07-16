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

package ouroboros

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger"
	gouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	"github.com/stretchr/testify/require"
)

func mustCbor(t *testing.T, value any) cbor.RawMessage {
	t.Helper()
	data, err := cbor.Encode(value)
	require.NoError(t, err)
	return cbor.RawMessage(data)
}

func newTestOuroborosWithLeiosDB(t *testing.T) *Ouroboros {
	t.Helper()

	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2160}))

	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: cm,
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
	})
	require.NoError(t, err)

	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	o.LedgerState = ls
	return o
}

func testDijkstraBlockRaw(
	t *testing.T,
	idx int,
) (ocommon.Point, cbor.RawMessage) {
	t.Helper()
	blockBody := gdijkstra.DijkstraBlockBody{
		InvalidTransactions: []uint{},
		Transactions:        []gdijkstra.DijkstraTransaction{},
		LeiosCertificate: &gdijkstra.DijkstraLeiosCertificate{
			Signers:             []byte{0x01},
			AggregatedSignature: make([]byte, 48),
		},
	}
	block := gdijkstra.DijkstraBlock{
		BlockHeader: &gdijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					Slot:          uint64(idx),
					BlockBodyHash: blockBody.Hash(),
					VrfKey:        make([]byte, 32),
					VrfResult: lcommon.VrfResult{
						Output: []byte{},
						Proof:  make([]byte, 80),
					},
					OpCert: babbage.BabbageOpCert{
						HotVkey:   make([]byte, 32),
						Signature: make([]byte, 64),
					},
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: gdijkstra.MinProtocolVersionDijkstra,
					},
				},
				Signature: make([]byte, 448),
			},
		},
		BlockBody: blockBody,
	}
	raw, err := block.MarshalCBOR()
	require.NoError(t, err)
	decoded, err := gdijkstra.NewDijkstraBlockFromCbor(raw)
	require.NoError(t, err)
	hash := decoded.Hash()
	return ocommon.NewPoint(uint64(idx), hash.Bytes()), cbor.RawMessage(raw)
}

func testLeiosEndorserBlockRaw(
	t *testing.T,
	idx int,
) (ocommon.Point, cbor.RawMessage) {
	t.Helper()
	return testLeiosEndorserBlockRawWithRefs(t, idx, 1)
}

func testLeiosEndorserBlockRawWithRefs(
	t *testing.T,
	idx int,
	refCount int,
) (ocommon.Point, cbor.RawMessage) {
	t.Helper()
	refs := make([]lcommon.LeiosTransactionReference, refCount)
	for refIdx := range refs {
		var hashSeed [12]byte
		binary.BigEndian.PutUint64(hashSeed[:8], uint64(idx))
		binary.BigEndian.PutUint32(hashSeed[8:], uint32(refIdx))
		refs[refIdx] = lcommon.LeiosTransactionReference{
			TransactionHash: lcommon.Blake2b256Hash(hashSeed[:]),
			TransactionSize: uint16(refIdx + 1),
		}
	}
	block := lcommon.LeiosEndorserBlock{
		TransactionReferences: refs,
	}
	raw, err := cbor.Encode(&block)
	require.NoError(t, err)
	hash := lcommon.Blake2b256Hash(raw)
	return ocommon.NewPoint(uint64(idx), hash.Bytes()), cbor.RawMessage(raw)
}

func TestMergedLeiosRankingBlockCborIsNoopForDijkstra(t *testing.T) {
	_, blockRaw := testDijkstraBlockRaw(t, 1)

	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	got, ok, err := o.mergedLeiosRankingBlockCbor(blockRaw)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, []byte(blockRaw), got)
}

func TestLeiosTxsFromBitmapPreservesRequestedOrder(t *testing.T) {
	txs := []cbor.RawMessage{
		mustCbor(t, "tx0"),
		mustCbor(t, "tx1"),
		mustCbor(t, "tx2"),
		mustCbor(t, "tx3"),
	}

	// MSB-first (see leiosWindowNeededMask): txs 1 and 3 are bits 62 and 60.
	got := leiosTxsFromBitmap(txs, map[uint16]uint64{0: (1 << 62) | (1 << 60)})
	require.Equal(t, []cbor.RawMessage{txs[1], txs[3]}, got)
}

func TestLeiosFetchServerBlockTxsRejectsIncompleteCache(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 10, 2)

	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			[]cbor.RawMessage{mustCbor(t, "tx0")},
		),
	)

	msg, err := o.leiosfetchServerBlockTxsRequest(
		oleiosfetch.CallbackContext{},
		point,
		map[uint16]uint64{0: 0b11},
	)
	require.Error(t, err)
	require.Nil(t, msg)
	require.Contains(t, err.Error(), "txs not available")
}

func TestLeiosFetchServerBlockTxsRejectsOutOfRangeBitmap(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 10, 2)

	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			[]cbor.RawMessage{mustCbor(t, "tx0"), mustCbor(t, "tx1")},
		),
	)

	msg, err := o.leiosfetchServerBlockTxsRequest(
		oleiosfetch.CallbackContext{},
		point,
		map[uint16]uint64{0: 0b100},
	)
	require.Error(t, err)
	require.Nil(t, msg)
	require.Contains(t, err.Error(), "beyond")
}

func TestLeiosNotifyBlockTxsOfferCacheMissIsNonFatal(t *testing.T) {
	cm := connmanager.NewConnectionManager(connmanager.ConnectionManagerConfig{})
	conn, err := gouroboros.New()
	require.NoError(t, err)
	require.True(t, cm.AddConnection(conn, false, "127.0.0.1:1234"))
	defer func() {
		conn.ErrorChan() <- errors.New("test connection closed")
	}()

	o := NewOuroboros(OuroborosConfig{ConnManager: cm, EnableLeios: true})
	err = o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: conn.Id()},
		oleiosnotify.NewMsgBlockTxsOffer(
			ocommon.NewPoint(99, []byte{0xaa}),
		),
	)
	require.NoError(t, err)
}

// TestLeiosNotifyUnrecognizedMessageTypeLogsAndDoesNotError verifies that an
// unhandled leios-notify message type (e.g. MsgBlockAnnouncement, which this
// handler doesn't act on) is logged for observability rather than silently
// dropped with no trace, while still not tearing down the connection.
func TestLeiosNotifyUnrecognizedMessageTypeLogsAndDoesNotError(t *testing.T) {
	var logBuf bytes.Buffer
	cm := connmanager.NewConnectionManager(connmanager.ConnectionManagerConfig{})
	conn, err := gouroboros.New()
	require.NoError(t, err)
	require.True(t, cm.AddConnection(conn, false, "127.0.0.1:1234"))
	defer func() {
		conn.ErrorChan() <- errors.New("test connection closed")
	}()

	o := NewOuroboros(OuroborosConfig{
		ConnManager: cm,
		EnableLeios: true,
		Logger: slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
	})
	err = o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: conn.Id()},
		oleiosnotify.NewMsgBlockAnnouncement(cbor.RawMessage{0x00}),
	)
	require.NoError(t, err)
	require.Contains(
		t,
		logBuf.String(),
		"received unexpected leios-notify message type",
	)
}

var errLeiosEndorserBlockNotCached = errors.New("leios endorser block not cached")

func (o *Ouroboros) fetchCachedLeiosEndorserBlockTxs(
	point ocommon.Point,
) ([]cbor.RawMessage, error) {
	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	if !ok {
		return nil, fmt.Errorf(
			"%w: %d.%x",
			errLeiosEndorserBlockNotCached,
			point.Slot,
			point.Hash,
		)
	}
	// In gouroboros v0.180.0 the Leios aliases decode as Dijkstra blocks.
	// The current Dijkstra CDDL has no transaction-reference list, so there
	// is no extra BlockTxsRequest to make here.
	return cloneRawMessages(data.txsRaw), nil
}

func TestFetchCachedLeiosEndorserBlockTxsReturnsCompleteCacheWithoutFetch(
	t *testing.T,
) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 10)
	txRaw := mustCbor(t, "tx0")

	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			[]cbor.RawMessage{txRaw},
		),
	)

	got, err := o.fetchCachedLeiosEndorserBlockTxs(point)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, []cbor.RawMessage{txRaw}, got)

	got[0][0] ^= 0xff
	cached, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.Equal(t, txRaw, cached.txsRaw[0])
}

// Covers the historical-serving path: after the in-memory EB cache is gone,
// lookup reloads manifest+txs from blob storage and leios-fetch serves them.
func TestLeiosEndorserBlockLookupReloadsFromDBAndServesFetchRequests(
	t *testing.T,
) {
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 10, 2)
	txsRaw := []cbor.RawMessage{
		mustCbor(t, "tx0"),
		mustCbor(t, "tx1"),
	}

	o := newTestOuroborosWithLeiosDB(t)
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, txsRaw))

	// Endorser-block persistence is asynchronous: storeLeiosEndorserBlock
	// queues the blob write on a background writer. Drain it so the blob store
	// reflects the stored block before we force the DB-reload path by clearing
	// the in-memory cache below.
	o.StopLeiosPersistWriter()

	o.leiosMu.Lock()
	o.leiosEndorserBlocks = make(map[string]*leiosEndorserBlockData)
	o.leiosMu.Unlock()

	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.Equal(t, point.Slot, data.point.Slot)
	require.Equal(t, point.Hash, data.point.Hash)
	require.Equal(t, []byte(blockRaw), data.blockRaw)
	require.Equal(t, txsRaw, data.txsRaw)
	require.True(t, data.completeTxCache())

	o.leiosMu.Lock()
	o.leiosEndorserBlocks = make(map[string]*leiosEndorserBlockData)
	o.leiosMu.Unlock()

	blockResp, err := o.leiosfetchServerBlockRequest(
		oleiosfetch.CallbackContext{},
		point,
	)
	require.NoError(t, err)
	blockMsg, ok := blockResp.(*oleiosfetch.MsgBlock)
	require.True(t, ok)
	require.Equal(t, cbor.RawMessage(blockRaw), blockMsg.BlockRaw)

	o.leiosMu.Lock()
	o.leiosEndorserBlocks = make(map[string]*leiosEndorserBlockData)
	o.leiosMu.Unlock()

	txsResp, err := o.leiosfetchServerBlockTxsRequest(
		oleiosfetch.CallbackContext{},
		point,
		map[uint16]uint64{0: (1 << 63) | (1 << 62)},
	)
	require.NoError(t, err)
	txsMsg, ok := txsResp.(*oleiosfetch.MsgBlockTxs)
	require.True(t, ok)
	require.Equal(t, txsRaw, txsMsg.TxsRaw)
}

func TestStoreLeiosEndorserBlockRejectsPointHashMismatch(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 10)
	point.Hash[0] ^= 0xff

	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	err := o.storeLeiosEndorserBlock(point, blockRaw, nil)
	require.ErrorContains(
		t,
		err,
		"leios endorser block cache: point hash mismatch",
	)

	_, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.False(t, ok)
}

func TestLeiosEndorserBlockLookupExpiresStaleEntries(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	point, raw := testLeiosEndorserBlockRaw(t, 1)
	require.NoError(t, o.storeLeiosEndorserBlock(point, raw, nil))
	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)

	o.leiosMu.Lock()
	data.insertedAt = time.Now().Add(-leiosEndorserBlockCacheTTL - time.Second)
	o.leiosMu.Unlock()

	_, ok = o.lookupLeiosEndorserBlock(point.Hash)
	require.False(t, ok)

	o.leiosMu.RLock()
	cacheEntries := len(o.leiosEndorserBlocks)
	o.leiosMu.RUnlock()
	require.Zero(t, cacheEntries)
}

func TestLeiosEndorserBlockCachePrunesExpiredEntries(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	oldPoint, oldRaw := testLeiosEndorserBlockRaw(t, 1)
	require.NoError(t, o.storeLeiosEndorserBlock(oldPoint, oldRaw, nil))
	oldData, ok := o.lookupLeiosEndorserBlock(oldPoint.Hash)
	require.True(t, ok)

	o.leiosMu.Lock()
	oldData.insertedAt = time.Now().Add(-leiosEndorserBlockCacheTTL - time.Second)
	o.leiosMu.Unlock()

	newPoint, newRaw := testLeiosEndorserBlockRaw(t, 2)
	require.NoError(t, o.storeLeiosEndorserBlock(newPoint, newRaw, nil))

	_, ok = o.lookupLeiosEndorserBlock(oldPoint.Hash)
	require.False(t, ok)
	_, ok = o.lookupLeiosEndorserBlock(newPoint.Hash)
	require.True(t, ok)
}

func TestLeiosEndorserBlockCachePrunesBySize(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	var lastPoint ocommon.Point
	for idx := range leiosEndorserBlockCacheMaxEntries + 1 {
		point, raw := testLeiosEndorserBlockRaw(t, idx)
		require.NoError(t, o.storeLeiosEndorserBlock(point, raw, nil))
		lastPoint = point
	}

	o.leiosMu.RLock()
	cacheEntries := len(o.leiosEndorserBlocks)
	o.leiosMu.RUnlock()
	require.LessOrEqual(t, cacheEntries, leiosEndorserBlockCacheMaxEntries)
	_, ok := o.lookupLeiosEndorserBlock(lastPoint.Hash)
	require.True(t, ok)
}

// buildDijkstraLeiosBlockRaw assembles a Dijkstra block [header, block_body]
// whose header carries the 12-field Leios extension. The extension elements
// (ext) and the four-element block_body (bodyElems) are supplied as raw CBOR.
// The header is assembled directly because DijkstraBlockHeader.MarshalCBOR
// drops the extension for in-process-constructed headers.
func buildDijkstraLeiosBlockRaw(
	t *testing.T,
	slot uint64,
	prevHash []byte,
	ext []cbor.RawMessage,
	bodyElems []cbor.RawMessage,
) cbor.RawMessage {
	t.Helper()
	require.Len(t, bodyElems, 4)
	headerBody := babbage.BabbageBlockHeaderBody{
		Slot:          slot,
		PrevHash:      lcommon.NewBlake2b256(prevHash),
		BlockBodyHash: lcommon.NewBlake2b256(make([]byte, lcommon.Blake2b256Size)),
		VrfKey:        make([]byte, 32),
		VrfResult:     lcommon.VrfResult{Output: []byte{}, Proof: make([]byte, 80)},
		OpCert: babbage.BabbageOpCert{
			HotVkey:   make([]byte, 32),
			Signature: make([]byte, 64),
		},
		ProtoVersion: babbage.BabbageProtoVersion{
			Major: gdijkstra.MinProtocolVersionDijkstra,
		},
	}
	bodyCbor, err := cbor.Encode(&headerBody)
	require.NoError(t, err)
	var babbageElems []cbor.RawMessage
	_, err = cbor.Decode(bodyCbor, &babbageElems)
	require.NoError(t, err)
	headerBodyElems := append(babbageElems, ext...)
	headerBody12, err := cbor.Encode(headerBodyElems)
	require.NoError(t, err)
	kesSig, err := cbor.Encode(make([]byte, 448))
	require.NoError(t, err)
	headerRaw, err := cbor.Encode([]cbor.RawMessage{
		cbor.RawMessage(headerBody12), cbor.RawMessage(kesSig),
	})
	require.NoError(t, err)
	blockBodyRaw, err := cbor.Encode(bodyElems)
	require.NoError(t, err)
	blockRaw, err := cbor.Encode([]cbor.RawMessage{
		cbor.RawMessage(headerRaw), cbor.RawMessage(blockBodyRaw),
	})
	require.NoError(t, err)
	return cbor.RawMessage(blockRaw)
}

func testDijkstraCertRBBodyElems(t *testing.T) []cbor.RawMessage {
	t.Helper()
	return []cbor.RawMessage{
		mustCbor(t, []uint{}),            // invalid_transactions
		mustCbor(t, []cbor.RawMessage{}), // transactions (empty on a CertRB)
		mustCbor(t, []any{[]byte{0x01}, make([]byte, lcommon.LeiosBlsSignatureSize)}), // leios_cert
		mustCbor(t, nil), // peras_certificate
	}
}

// testDijkstraCertRBRaw builds a certifying ranking block: a 12-field header
// with leios_certified=true and no announcement, empty transaction segments,
// and a leios_certificate.
func testDijkstraCertRBRaw(
	t *testing.T,
	slot uint64,
	prevHash []byte,
) cbor.RawMessage {
	t.Helper()
	ext := []cbor.RawMessage{mustCbor(t, true), mustCbor(t, nil)}
	return buildDijkstraLeiosBlockRaw(
		t, slot, prevHash, ext, testDijkstraCertRBBodyElems(t),
	)
}

func testDijkstraTx(t *testing.T, seed byte) cbor.RawMessage {
	t.Helper()
	// A complete Dijkstra transaction: [transaction_body, witness_set, aux/nil].
	return mustCbor(t, []cbor.RawMessage{
		mustCbor(t, map[uint]any{2: 100_000 + uint64(seed)}),
		mustCbor(t, map[uint]any{}),
		mustCbor(t, nil),
	})
}

func TestSpliceEndorserTxsIntoDijkstraBlockFillsCertRB(t *testing.T) {
	certRB := testDijkstraCertRBRaw(t, 100, make([]byte, lcommon.Blake2b256Size))
	ebTxs := []cbor.RawMessage{testDijkstraTx(t, 1), testDijkstraTx(t, 2)}

	merged, err := spliceEndorserTxsIntoDijkstraBlock(certRB, ebTxs)
	require.NoError(t, err)

	// The header is preserved byte-for-byte so the served block's hash is
	// unchanged.
	var origTop, mergedTop []cbor.RawMessage
	_, err = cbor.Decode(certRB, &origTop)
	require.NoError(t, err)
	_, err = cbor.Decode(merged, &mergedTop)
	require.NoError(t, err)
	require.Len(t, mergedTop, 2)
	require.Equal(t, []byte(origTop[0]), []byte(mergedTop[0]))

	// The transaction segment now holds the endorser block's transactions; the
	// invalid, certificate, and peras segments are preserved.
	var origBody, mergedBody []cbor.RawMessage
	_, err = cbor.Decode(origTop[1], &origBody)
	require.NoError(t, err)
	_, err = cbor.Decode(mergedTop[1], &mergedBody)
	require.NoError(t, err)
	require.Len(t, mergedBody, 4)
	require.Equal(t, []byte(origBody[0]), []byte(mergedBody[0]))
	require.Equal(t, []byte(origBody[2]), []byte(mergedBody[2]))
	require.Equal(t, []byte(origBody[3]), []byte(mergedBody[3]))

	var mergedTxs []cbor.RawMessage
	_, err = cbor.Decode(mergedBody[1], &mergedTxs)
	require.NoError(t, err)
	require.Len(t, mergedTxs, 2)
	require.Equal(t, []byte(ebTxs[0]), []byte(mergedTxs[0]))
	require.Equal(t, []byte(ebTxs[1]), []byte(mergedTxs[1]))

	// The merged block deliberately has a stale body hash: the preserved header
	// still commits to the original empty body, so a full parse (which verifies
	// the body hash) rejects it. This is why the merge is node-to-client only,
	// where clients trust the node and do not re-verify the body hash.
	_, err = gdijkstra.NewDijkstraBlockFromCbor(merged)
	require.ErrorContains(t, err, "body hash")
}

func TestSpliceEndorserTxsRejectsBlockWithExistingTxs(t *testing.T) {
	ext := []cbor.RawMessage{mustCbor(t, true), mustCbor(t, nil)}
	body := testDijkstraCertRBBodyElems(t)
	body[1] = mustCbor(t, []cbor.RawMessage{testDijkstraTx(t, 9)}) // non-empty
	block := buildDijkstraLeiosBlockRaw(
		t, 101, make([]byte, lcommon.Blake2b256Size), ext, body,
	)
	_, err := spliceEndorserTxsIntoDijkstraBlock(
		block, []cbor.RawMessage{testDijkstraTx(t, 1)},
	)
	require.Error(t, err)
}

func TestSpliceEndorserTxsRejectsWrongShape(t *testing.T) {
	// A three-element top-level array is not a Dijkstra [header, block_body].
	notADijkstraBlock := mustCbor(t, []cbor.RawMessage{
		mustCbor(t, 1), mustCbor(t, 2), mustCbor(t, 3),
	})
	_, err := spliceEndorserTxsIntoDijkstraBlock(notADijkstraBlock, nil)
	require.Error(t, err)
}

func TestLeiosAnnouncementFromBlockCbor(t *testing.T) {
	ebHash := make([]byte, lcommon.Blake2b256Size)
	ebHash[0] = 0xAB
	announcement := mustCbor(t, []any{ebHash, uint64(4096)})
	ext := []cbor.RawMessage{mustCbor(t, false), announcement}
	announcing := buildDijkstraLeiosBlockRaw(
		t, 50, make([]byte, lcommon.Blake2b256Size), ext,
		testDijkstraCertRBBodyElems(t),
	)
	got, ok := leiosAnnouncementFromBlockCbor(announcing)
	require.True(t, ok)
	require.Equal(t, ebHash, got.Bytes())

	// A CertRB announces nothing.
	certRB := testDijkstraCertRBRaw(t, 51, make([]byte, lcommon.Blake2b256Size))
	_, ok = leiosAnnouncementFromBlockCbor(certRB)
	require.False(t, ok)
}

func TestResolveCertifiedEndorserTxsGuards(t *testing.T) {
	// A non-certifying Dijkstra block is never merged.
	_, blockRaw := testDijkstraBlockRaw(t, 1)
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	_, ok := o.resolveCertifiedEndorserTxs(blockRaw)
	require.False(t, ok)

	// A CertRB with no ledger state (so no parent to resolve) is served raw.
	certRB := testDijkstraCertRBRaw(t, 2, make([]byte, lcommon.Blake2b256Size))
	_, ok = o.resolveCertifiedEndorserTxs(certRB)
	require.False(t, ok)
}

func TestMergedLeiosRankingBlockCborServesRawForCertRBWithoutLedger(t *testing.T) {
	certRB := testDijkstraCertRBRaw(t, 3, make([]byte, lcommon.Blake2b256Size))
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	got, ok, err := o.mergedLeiosRankingBlockCbor(certRB)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, []byte(certRB), got)
}
