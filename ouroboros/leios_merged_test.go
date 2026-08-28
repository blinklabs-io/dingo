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
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	gouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type fakeLeiosAnnouncementLedger struct {
	currentSlot uint64
	slotTime    time.Time
	staleness   ledger.LeiosAnnouncementOCINStaleness
	err         error
	validated   int
}

func (f *fakeLeiosAnnouncementLedger) CurrentSlot() (uint64, error) {
	return f.currentSlot, nil
}

func (f *fakeLeiosAnnouncementLedger) SlotToTime(uint64) (time.Time, error) {
	return f.slotTime, nil
}

func (f *fakeLeiosAnnouncementLedger) ValidateLeiosAnnouncementHeader(
	gledger.BlockHeader,
) (ledger.LeiosAnnouncementOCINStaleness, error) {
	f.validated++
	return f.staleness, f.err
}

func mustCbor(t *testing.T, value any) cbor.RawMessage {
	t.Helper()
	data, err := cbor.Encode(value)
	require.NoError(t, err)
	return cbor.RawMessage(data)
}

func newTestOuroborosWithLeiosDB(t *testing.T) *Ouroboros {
	t.Helper()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2160}),
	)

	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: cm,
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
	})
	require.NoError(t, err)

	o := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LeiosAnnouncementLedger: ls,
	})
	o.ledgerState = ls
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

func testDijkstraAnnouncementHeaderRaw(t *testing.T) []byte {
	t.Helper()
	_, blockRaw := testDijkstraBlockRaw(t, 1)
	var components []cbor.RawMessage
	_, err := cbor.Decode(blockRaw, &components)
	require.NoError(t, err)
	require.Len(t, components, 2)

	var ebHash lcommon.Blake2b256
	ebHash[0] = 0xaa
	var headerTop []cbor.RawMessage
	_, err = cbor.Decode(components[0], &headerTop)
	require.NoError(t, err)
	require.Len(t, headerTop, 2)
	var headerBody []cbor.RawMessage
	_, err = cbor.Decode(headerTop[0], &headerBody)
	require.NoError(t, err)
	headerBody = append(
		headerBody,
		mustCbor(t, false),
		mustCbor(t, []any{ebHash.Bytes(), uint64(1234)}),
	)
	headerTop[0], err = cbor.Encode(headerBody)
	require.NoError(t, err)
	headerRaw, err := cbor.Encode(headerTop)
	require.NoError(t, err)
	return headerRaw
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

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
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

func TestLeiosFetchServerMissingDataUsesUnavailableErrors(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	point := ocommon.NewPoint(10, make([]byte, 32))

	msg, err := o.leiosfetchServerBlockRequest(
		oleiosfetch.CallbackContext{},
		point,
	)
	require.Nil(t, msg)
	require.ErrorIs(t, err, oleiosfetch.ErrBlockNotFound)

	msg, err = o.leiosfetchServerBlockTxsRequest(
		oleiosfetch.CallbackContext{},
		point,
		map[uint16]uint64{0: 1},
	)
	require.Nil(t, msg)
	require.ErrorIs(t, err, oleiosfetch.ErrBlockTxsNotFound)
}

func TestLeiosFetchServerBlockTxsRejectsIncompleteCache(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 10, 2)

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
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
	require.ErrorIs(t, err, oleiosfetch.ErrBlockTxsNotFound)
	require.Contains(t, err.Error(), "txs not available")
}

func TestLeiosFetchServerBlockTxsRejectsOutOfRangeBitmap(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 10, 2)

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
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
	require.NotErrorIs(t, err, oleiosfetch.ErrBlockNotFound)
	require.NotErrorIs(t, err, oleiosfetch.ErrBlockTxsNotFound)
	require.Contains(t, err.Error(), "beyond")
}

func TestLeiosNotifyBlockTxsOfferCacheMissIsNonFatal(t *testing.T) {
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	conn, err := gouroboros.New()
	require.NoError(t, err)
	require.True(t, cm.AddConnection(conn, false, "127.0.0.1:1234"))
	defer func() {
		conn.ErrorChan() <- errors.New("test connection closed")
	}()

	o := newOuroboros(OuroborosConfig{
		ConnManager:        cm,
		EnableLeios:        true,
		EnableLeiosTxFetch: true,
	})
	err = o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: conn.Id()},
		oleiosnotify.NewMsgBlockTxsOffer(
			ocommon.NewPoint(99, []byte{0xaa}),
		),
	)
	require.NoError(t, err)
}

func TestLeiosNotifyBlockAnnouncementIsConsumedAndDeduplicated(t *testing.T) {
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	conn, err := gouroboros.New()
	require.NoError(t, err)
	require.True(t, cm.AddConnection(conn, false, "127.0.0.1:1234"))
	defer func() {
		conn.ErrorChan() <- errors.New("test connection closed")
	}()

	_, blockRaw := testDijkstraBlockRaw(t, 1)
	var components []cbor.RawMessage
	_, err = cbor.Decode(blockRaw, &components)
	require.NoError(t, err)
	require.Len(t, components, 2)

	var ebHash lcommon.Blake2b256
	ebHash[0] = 0xaa
	var headerTop []cbor.RawMessage
	_, err = cbor.Decode(components[0], &headerTop)
	require.NoError(t, err)
	require.Len(t, headerTop, 2)
	var headerBody []cbor.RawMessage
	_, err = cbor.Decode(headerTop[0], &headerBody)
	require.NoError(t, err)
	headerBody = append(headerBody,
		mustCbor(t, false),
		mustCbor(t, []any{ebHash.Bytes(), uint64(1234)}),
	)
	headerTop[0], err = cbor.Encode(headerBody)
	require.NoError(t, err)
	headerRaw, err := cbor.Encode(headerTop)
	require.NoError(t, err)

	o := newOuroboros(OuroborosConfig{ConnManager: cm, EnableLeios: true})
	o.leiosEBLog.registerConn("test")
	err = o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: conn.Id()},
		oleiosnotify.NewMsgBlockAnnouncement(headerRaw),
	)
	require.NoError(t, err)
	err = o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: conn.Id()},
		oleiosnotify.NewMsgBlockAnnouncement(headerRaw),
	)
	require.NoError(t, err)
	entry, _ := o.leiosEBLog.next("test")
	require.Nil(t, entry)

	record := func(raw []byte) error {
		header, err := gdijkstra.NewDijkstraBlockHeaderFromCbor(raw)
		if err != nil {
			return err
		}
		ebHash, ebSize, ok := header.LeiosAnnouncement()
		if !ok {
			return errors.New("missing announcement")
		}
		return o.recordLeiosAnnouncement(
			raw,
			ebHash,
			ebSize,
			header,
			"test",
			true,
		)
	}
	require.NoError(t, record(headerRaw))
	require.NoError(t, record(headerRaw))
	entry, _ = o.leiosEBLog.next("test")
	require.NotNil(t, entry)
	require.Equal(t, headerRaw, entry.announcement)
	o.leiosEBLog.complete("test", true)
	entry, _ = o.leiosEBLog.next("test")
	require.Nil(t, entry)

	// A different ranking block may not change the established size for the
	// same endorser-block hash.
	headerBody[len(headerBody)-1] = mustCbor(
		t,
		[]any{ebHash.Bytes(), uint64(4321)},
	)
	headerTop[0], err = cbor.Encode(headerBody)
	require.NoError(t, err)
	headerRaw, err = cbor.Encode(headerTop)
	require.NoError(t, err)
	require.ErrorContains(t, record(headerRaw), "inconsistent")

	// A peer may announce at most two distinct ranking blocks for one
	// slot/issuer election. The third distinct message is suppressed even when
	// its endorser-block size is otherwise consistent.
	headerBody[len(headerBody)-1] = mustCbor(
		t,
		[]any{ebHash.Bytes(), uint64(1234)},
	)
	headerBody[0] = mustCbor(t, uint64(2))
	headerTop[0], err = cbor.Encode(headerBody)
	require.NoError(t, err)
	headerRaw, err = cbor.Encode(headerTop)
	require.NoError(t, err)
	require.NoError(t, record(headerRaw))
	headerBody[0] = mustCbor(t, uint64(3))
	headerTop[0], err = cbor.Encode(headerBody)
	require.NoError(t, err)
	headerRaw, err = cbor.Encode(headerTop)
	require.NoError(t, err)
	require.ErrorContains(t, record(headerRaw), "third distinct")
}

func TestLeiosNotifyAnnouncementOCINVerdictControlsDiffusion(t *testing.T) {
	tests := []struct {
		name        string
		staleness   ledger.LeiosAnnouncementOCINStaleness
		validateErr error
		wantRecord  bool
		wantRelay   bool
	}{
		{
			name:       "fresh announcement is processed and relayed",
			staleness:  ledger.LeiosAnnouncementFreshOCIN,
			wantRecord: true,
			wantRelay:  true,
		},
		{
			name:      "stale announcement is accepted and ignored",
			staleness: ledger.LeiosAnnouncementStaleOCIN,
		},
		{
			name:        "non OCIN validation failure keeps suppression behavior",
			validateErr: errors.New("invalid KES signature"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := connmanager.NewConnectionManager(
				connmanager.ConnectionManagerConfig{},
			)
			conn, err := gouroboros.New()
			require.NoError(t, err)
			require.True(t, cm.AddConnection(
				conn,
				false,
				"127.0.0.1:1234",
			))
			defer func() {
				conn.ErrorChan() <- errors.New("test connection closed")
			}()

			announcementLedger := &fakeLeiosAnnouncementLedger{
				currentSlot: 10,
				slotTime:    time.Now().Add(-time.Minute),
				staleness:   tt.staleness,
				err:         tt.validateErr,
			}
			o := newOuroboros(OuroborosConfig{
				ConnManager:             cm,
				EnableLeios:             true,
				LeiosAnnouncementLedger: announcementLedger,
			})
			o.leiosEBLog.registerConn("relay")

			err = o.leiosnotifyClientNotification(
				oleiosnotify.CallbackContext{ConnectionId: conn.Id()},
				oleiosnotify.NewMsgBlockAnnouncement(
					testDijkstraAnnouncementHeaderRaw(t),
				),
			)
			require.NoError(t, err,
				"accepted-ignore and existing suppression must keep the bearer usable")
			require.Equal(t, 1, announcementLedger.validated)
			require.Same(t, conn, cm.GetConnectionById(conn.Id()),
				"the announcement callback must not disconnect the peer")
			require.Equal(t, tt.wantRecord, len(o.leiosAnnouncements) == 1)
			entry, _ := o.leiosEBLog.next("relay")
			require.Equal(t, tt.wantRelay, entry != nil)
		})
	}
}

func TestAcceptLeiosAnnouncementRejectsWithoutLedgerState(t *testing.T) {
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	o.leiosDeferredAnnouncements["pending"] = leiosDeferredAnnouncement{
		raw: []byte("deferred"), source: "peer",
	}
	require.ErrorContains(
		t,
		o.acceptLeiosAnnouncement([]byte("not cbor"), "test"),
		"without announcement ledger",
	)
	require.Empty(t, o.leiosAnnouncements)
	require.Empty(t, o.leiosEBLog.items)
	_, stillDeferred := o.leiosDeferredAnnouncements["pending"]
	require.True(t, stillDeferred)
}

var errLeiosEndorserBlockNotCached = errors.New(
	"leios endorser block not cached",
)

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

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
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

func TestEndorserBlockTxHashesByHashReturnsManifestHashes(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 10, 2)
	block, err := lcommon.NewLeiosEndorserBlockFromCbor(blockRaw)
	require.NoError(t, err)

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		[]cbor.RawMessage{mustCbor(t, "tx0"), mustCbor(t, "tx1")},
	))

	got, ok := o.EndorserBlockTxHashesByHash(point.Hash)
	require.True(t, ok)
	require.Equal(t, []string{
		hex.EncodeToString(
			block.TransactionReferences[0].TransactionHash.Bytes(),
		),
		hex.EncodeToString(
			block.TransactionReferences[1].TransactionHash.Bytes(),
		),
	}, got)
}

// Covers the historical-serving path: after the in-memory EB cache is gone,
// lookup reloads manifest+txs from blob storage and leios-fetch serves them.
func TestLeiosEndorserBlockLookupReloadsFromDBAndServesFetchRequests(
	t *testing.T,
) {
	tx0, ref0 := testLeiosManifestTx(t, 0)
	tx1, ref1 := testLeiosManifestTx(t, 1)
	blockRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref0, ref1},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(
		10,
		lcommon.Blake2b256Hash(blockRaw).Bytes(),
	)
	txsRaw := []cbor.RawMessage{
		tx0,
		tx1,
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

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
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
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
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
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	oldPoint, oldRaw := testLeiosEndorserBlockRaw(t, 1)
	require.NoError(t, o.storeLeiosEndorserBlock(oldPoint, oldRaw, nil))
	oldData, ok := o.lookupLeiosEndorserBlock(oldPoint.Hash)
	require.True(t, ok)

	o.leiosMu.Lock()
	oldData.insertedAt = time.Now().
		Add(-leiosEndorserBlockCacheTTL - time.Second)
	o.leiosMu.Unlock()

	newPoint, newRaw := testLeiosEndorserBlockRaw(t, 2)
	require.NoError(t, o.storeLeiosEndorserBlock(newPoint, newRaw, nil))

	_, ok = o.lookupLeiosEndorserBlock(oldPoint.Hash)
	require.False(t, ok)
	_, ok = o.lookupLeiosEndorserBlock(newPoint.Hash)
	require.True(t, ok)
}

func TestLeiosEndorserBlockCachePrunesBySize(t *testing.T) {
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
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
		Slot:     slot,
		PrevHash: lcommon.NewBlake2b256(prevHash),
		BlockBodyHash: lcommon.NewBlake2b256(
			make([]byte, lcommon.Blake2b256Size),
		),
		VrfKey: make([]byte, 32),
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
		mustCbor(
			t,
			[]any{[]byte{0x01}, make([]byte, lcommon.LeiosBlsSignatureSize)},
		), // leios_cert
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

func testLeiosManifestTx(
	t *testing.T,
	seed byte,
) (cbor.RawMessage, lcommon.LeiosTransactionReference) {
	t.Helper()
	txCbor := testDijkstraTx(t, seed)
	var txElems []cbor.RawMessage
	bytesRead, err := cbor.Decode(txCbor, &txElems)
	require.NoError(t, err)
	require.Equal(t, len(txCbor), bytesRead)
	require.NotEmpty(t, txElems)
	wrapped, err := cbor.Encode([]byte(txCbor))
	require.NoError(t, err)
	return cbor.RawMessage(wrapped), lcommon.LeiosTransactionReference{
		TransactionHash: lcommon.Blake2b256Hash(txElems[0]),
		TransactionSize: uint16(len(txCbor)), //nolint:gosec // test fixture is small
	}
}

func TestValidateLeiosEndorserBlockTxsBindsManifestOrder(t *testing.T) {
	tx1, ref1 := testLeiosManifestTx(t, 1)
	tx2, ref2 := testLeiosManifestTx(t, 2)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref1, ref2},
	}.MarshalCBOR()
	require.NoError(t, err)

	require.NoError(t, validateLeiosEndorserBlockTxs(
		manifestRaw,
		[]cbor.RawMessage{tx1, tx2},
	))

	for name, txs := range map[string][]cbor.RawMessage{
		"substituted": {tx2, tx2},
		"reordered":   {tx2, tx1},
	} {
		t.Run(name, func(t *testing.T) {
			err := validateLeiosEndorserBlockTxs(manifestRaw, txs)
			require.ErrorContains(t, err, "endorser tx 0 body hash mismatch")
		})
	}
}

func TestValidateLeiosEndorserBlockTxsRejectsMalformedManifest(t *testing.T) {
	tx, ref := testLeiosManifestTx(t, 1)
	hashRaw, err := cbor.Encode(ref.TransactionHash)
	require.NoError(t, err)
	sizeRaw, err := cbor.Encode(uint16(ref.TransactionSize))
	require.NoError(t, err)

	duplicateManifest := append([]byte{0x81, 0xa2}, hashRaw...)
	duplicateManifest = append(duplicateManifest, sizeRaw...)
	duplicateManifest = append(duplicateManifest, hashRaw...)
	duplicateManifest = append(duplicateManifest, sizeRaw...)
	zeroSizeManifest := append([]byte{0x81, 0xa1}, hashRaw...)
	zeroSizeManifest = append(zeroSizeManifest, 0x00)

	for name, manifest := range map[string][]byte{
		"missing references":   {0x81, 0xa0},
		"duplicate references": duplicateManifest,
		"zero reference size":  zeroSizeManifest,
	} {
		t.Run(name, func(t *testing.T) {
			err := validateLeiosEndorserBlockTxs(manifest, []cbor.RawMessage{tx})
			require.Error(t, err)
		})
	}
}

func TestValidateLeiosEndorserBlockTxsRejectsWrongSizeAndMalformedBody(
	t *testing.T,
) {
	tx, ref := testLeiosManifestTx(t, 1)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)

	tests := map[string]cbor.RawMessage{
		"wrong size":     append(cbor.RawMessage(nil), tx...),
		"malformed body": {0xff},
		"trailing bytes": append(append(cbor.RawMessage(nil), tx...), 0x00),
	}
	for name, candidate := range tests {
		t.Run(name, func(t *testing.T) {
			manifest := manifestRaw
			candidateRef := ref
			if name == "wrong size" {
				candidateRef.TransactionSize++
				manifest, err = lcommon.LeiosEndorserBlock{
					TransactionReferences: []lcommon.LeiosTransactionReference{candidateRef},
				}.MarshalCBOR()
				require.NoError(t, err)
			}
			require.Error(t, validateLeiosEndorserBlockTxs(manifest, []cbor.RawMessage{candidate}))
		})
	}
}

type manifestTxRequester struct {
	txs []cbor.RawMessage
}

func (r manifestTxRequester) BlockTxsRequest(
	_ context.Context,
	point ocommon.Point,
	bitmaps map[uint16]uint64,
) (protocol.Message, error) {
	indices := leiosBitmapTxIndices(bitmaps)
	txs := make([]cbor.RawMessage, 0, len(indices))
	for _, index := range indices {
		if index >= 0 && index < len(r.txs) {
			txs = append(txs, r.txs[index])
		}
	}
	return oleiosfetch.NewMsgBlockTxsFull(point, bitmaps, txs), nil
}

type recordingManifestTxRequester struct {
	txs       []cbor.RawMessage
	requested []int
}

func (r *recordingManifestTxRequester) BlockTxsRequest(
	_ context.Context,
	point ocommon.Point,
	bitmaps map[uint16]uint64,
) (protocol.Message, error) {
	indices := leiosBitmapTxIndices(bitmaps)
	r.requested = append(r.requested, indices...)
	txs := make([]cbor.RawMessage, 0, len(indices))
	for _, index := range indices {
		if index >= 0 && index < len(r.txs) {
			txs = append(txs, r.txs[index])
		}
	}
	return oleiosfetch.NewMsgBlockTxsFull(point, bitmaps, txs), nil
}

func TestFetchLeiosEbTxsBatchedRefetchesMismatchedRetainedPartial(t *testing.T) {
	tx1, ref1 := testLeiosManifestTx(t, 1)
	tx2, ref2 := testLeiosManifestTx(t, 2)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref1, ref2},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(123, lcommon.Blake2b256Hash(manifestRaw).Bytes())
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(point, manifestRaw, nil))

	// Seed index 0 with the body for index 1. A resumed fetch must discard it
	// before computing its request bitmap, then replace it in the retained set.
	o.retainLeiosPartialTxs(point.Hash, []cbor.RawMessage{tx2, nil}, nil)
	requester := &recordingManifestTxRequester{
		txs: []cbor.RawMessage{tx1, tx2},
	}
	txs, err := o.fetchLeiosEbTxsBatched(requester, point, 2, manifestRaw)
	require.NoError(t, err)
	require.Contains(t, requester.requested, 0)
	require.NoError(t, validateLeiosEndorserBlockTxs(manifestRaw, txs))

	cached, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.Equal(t, 2, cached.partialTxCount())
	require.NoError(t, validateLeiosEndorserBlockTxs(
		manifestRaw,
		leiosCollectTxs(cached.partialTxs),
	))
}

func TestValidatedLeiosFetchRejectsMismatchBeforePartialRetention(t *testing.T) {
	tx1, ref1 := testLeiosManifestTx(t, 1)
	tx2, ref2 := testLeiosManifestTx(t, 2)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref1, ref2},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(
		123,
		lcommon.Blake2b256Hash(manifestRaw).Bytes(),
	)
	o := newOuroboros(OuroborosConfig{})
	require.NoError(t, o.storeLeiosEndorserBlock(point, manifestRaw, nil))

	_, err = o.fetchLeiosEbTxsBatched(
		manifestTxRequester{txs: []cbor.RawMessage{tx2, tx1}},
		point,
		2,
		manifestRaw,
	)
	require.ErrorContains(t, err, "endorser tx 0 body hash mismatch")
	cached, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.False(t, cached.completeTxCache())
	require.Zero(t, cached.partialTxCount())

	txs, err := o.fetchLeiosEbTxsBatched(
		manifestTxRequester{txs: []cbor.RawMessage{tx1, tx2}},
		point,
		2,
		manifestRaw,
	)
	require.NoError(t, err)
	require.NoError(t, validateLeiosEndorserBlockTxs(manifestRaw, txs))
	require.NoError(t, o.storeLeiosEndorserBlock(point, manifestRaw, txs))
	cached, ok = o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.True(t, cached.completeTxCache())
}

func TestLoadLeiosEBFromDBRejectsTransactionsThatMismatchManifest(t *testing.T) {
	_, ref := testLeiosManifestTx(t, 1)
	mismatchedTx, _ := testLeiosManifestTx(t, 2)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(
		123,
		lcommon.Blake2b256Hash(manifestRaw).Bytes(),
	)
	o := newTestOuroborosWithLeiosDB(t)
	db := o.leiosDatabase()
	require.NotNil(t, db)
	require.NoError(t, db.SetLeiosEB(
		point.Slot,
		point.Hash,
		manifestRaw,
		[]cbor.RawMessage{mismatchedTx},
	))

	cached, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok, "the valid manifest should remain available")
	require.False(
		t,
		cached.completeTxCache(),
		"mismatched persisted transactions must be refetched",
	)
	require.Empty(t, cached.txsRaw)
}

func TestLoadLeiosEBFromDBAcceptsTransactionsThatMatchManifest(t *testing.T) {
	validTx, ref := testLeiosManifestTx(t, 1)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(
		123,
		lcommon.Blake2b256Hash(manifestRaw).Bytes(),
	)
	o := newTestOuroborosWithLeiosDB(t)
	db := o.leiosDatabase()
	require.NotNil(t, db)
	require.NoError(t, db.SetLeiosEB(
		point.Slot,
		point.Hash,
		manifestRaw,
		[]cbor.RawMessage{validTx},
	))

	cached, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.True(t, cached.completeTxCache())
	require.Equal(t, []cbor.RawMessage{validTx}, cached.txsRaw)
}

func TestSpliceEndorserTxsIntoDijkstraBlockFillsCertRB(t *testing.T) {
	certRB := testDijkstraCertRBRaw(
		t,
		100,
		make([]byte, lcommon.Blake2b256Size),
	)
	ebTxs := []cbor.RawMessage{testDijkstraTx(t, 1), testDijkstraTx(t, 2)}

	merged, err := spliceEndorserTxsIntoDijkstraBlock(certRB, ebTxs)
	require.NoError(t, err)

	// The header is preserved byte-for-byte so the served block's hash is
	// unchanged.
	origTop := make([]cbor.RawMessage, 0)
	mergedTop := make([]cbor.RawMessage, 0)
	_, err = cbor.Decode(certRB, &origTop)
	require.NoError(t, err)
	_, err = cbor.Decode(merged, &mergedTop)
	require.NoError(t, err)
	require.Len(t, origTop, 2)
	require.Len(t, mergedTop, 2)
	require.Equal(t, []byte(origTop[0]), []byte(mergedTop[0]))

	// The transaction segment now holds the endorser block's transactions; the
	// invalid, certificate, and peras segments are preserved.
	origBody := make([]cbor.RawMessage, 0)
	mergedBody := make([]cbor.RawMessage, 0)
	_, err = cbor.Decode(origTop[1], &origBody)
	require.NoError(t, err)
	_, err = cbor.Decode(mergedTop[1], &mergedBody)
	require.NoError(t, err)
	require.Len(t, origBody, 4)
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

	// A certificate-only RB announces nothing.
	certRB := testDijkstraCertRBRaw(t, 51, make([]byte, lcommon.Blake2b256Size))
	_, ok = leiosAnnouncementFromBlockCbor(certRB)
	require.False(t, ok)

	// prototype-2026w29 also permits a CertRB to announce a new EB. The
	// announcement parser returns that current EB; certified-closure resolution
	// independently follows the parent block.
	combined := buildDijkstraLeiosBlockRaw(
		t,
		52,
		make([]byte, lcommon.Blake2b256Size),
		[]cbor.RawMessage{mustCbor(t, true), announcement},
		testDijkstraCertRBBodyElems(t),
	)
	got, ok = leiosAnnouncementFromBlockCbor(combined)
	require.True(t, ok)
	require.Equal(t, ebHash, got.Bytes())
}

func TestResolveCertifiedEndorserTxsGuards(t *testing.T) {
	// A non-certifying Dijkstra block is never merged.
	_, blockRaw := testDijkstraBlockRaw(t, 1)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	_, ok := o.resolveCertifiedEndorserTxs(blockRaw)
	require.False(t, ok)

	// A CertRB with no ledger state (so no parent to resolve) is served raw.
	certRB := testDijkstraCertRBRaw(t, 2, make([]byte, lcommon.Blake2b256Size))
	_, ok = o.resolveCertifiedEndorserTxs(certRB)
	require.False(t, ok)
}

func TestMergedLeiosRankingBlockCborServesRawForCertRBWithoutLedger(
	t *testing.T,
) {
	certRB := testDijkstraCertRBRaw(t, 3, make([]byte, lcommon.Blake2b256Size))
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	got, ok, err := o.mergedLeiosRankingBlockCbor(certRB)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, []byte(certRB), got)
}

func TestCertifiedEndorserBlockHashTriState(t *testing.T) {
	o := newOuroboros(OuroborosConfig{EnableLeios: true})

	// A non-certifying Dijkstra block is not a CertRB: certified=false.
	_, blockRaw := testDijkstraBlockRaw(t, 1)
	_, certified, resolved := o.certifiedEndorserBlockHash(blockRaw)
	require.False(t, certified)
	require.False(t, resolved)

	// A CertRB whose parent announcement cannot be resolved (no ledger state)
	// must report certified=true, resolved=false so the caller disconnects
	// instead of downgrading a certified block to the raw serve path.
	certRB := testDijkstraCertRBRaw(t, 2, make([]byte, lcommon.Blake2b256Size))
	_, certified, resolved = o.certifiedEndorserBlockHash(certRB)
	require.True(t, certified)
	require.False(t, resolved)
}

// A certified ranking block whose endorser reference cannot be resolved (parent
// missing / no announcement) must never be downgraded to the raw serve path; it
// returns an error so the caller closes the connection.
func TestServeLeiosRankingBlockCborDisconnectsOnUnresolvedCertifiedBlock(
	t *testing.T,
) {
	// No ledger state, so a CertRB's parent announcement cannot be resolved.
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	certRB := testDijkstraCertRBRaw(t, 5, make([]byte, lcommon.Blake2b256Size))
	block := models.Block{Cbor: certRB, Slot: 5, Hash: []byte{0x05}}

	got, err := o.serveLeiosRankingBlockCbor(block, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, errLeiosClosureUnresolved)
	require.Nil(t, got)
	// It must not have fallen through to serving the raw certified block.
	require.NotEqual(t, []byte(certRB), got)
}

func TestServeLeiosRankingBlockCborServesRawForNonCertifiedBlock(t *testing.T) {
	// A non-certifying Dijkstra block is served unchanged.
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	_, blockRaw := testDijkstraBlockRaw(t, 6)
	block := models.Block{Cbor: blockRaw, Slot: 6, Hash: []byte{0x06}}

	got, err := o.serveLeiosRankingBlockCbor(block, nil)
	require.NoError(t, err)
	require.Equal(t, []byte(blockRaw), got)
}

func TestWaitForLeiosEndorserClosureReturnsWhenAlreadyCached(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 10)

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			[]cbor.RawMessage{mustCbor(t, "tx0")},
		),
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.True(t, o.waitForLeiosEndorserClosure(ctx, point.Hash))
}

func TestWaitForLeiosEndorserClosureWakesOnStore(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 11)

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	result := make(chan bool, 1)
	go func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			2*time.Second,
		)
		defer cancel()
		result <- o.waitForLeiosEndorserClosure(ctx, point.Hash)
	}()

	// Register the waiter before storing so the store exercises the
	// wake-on-signal path rather than the already-cached fast path.
	testutil.WaitForCondition(
		t,
		func() bool {
			o.leiosMu.RLock()
			defer o.leiosMu.RUnlock()
			return len(o.leiosClosureWaiters[leiosBlockKey(point.Hash)]) > 0
		},
		2*time.Second,
		"closure waiter to register",
	)

	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			[]cbor.RawMessage{mustCbor(t, "tx0")},
		),
	)

	require.True(
		t,
		testutil.RequireReceive(
			t,
			result,
			3*time.Second,
			"closure wait to resolve after store",
		),
	)
}

func TestWaitForLeiosEndorserClosureTimesOutAndCleansUp(t *testing.T) {
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	ebHash := make([]byte, lcommon.Blake2b256Size)
	ebHash[0] = 0xbb

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.False(t, o.waitForLeiosEndorserClosure(ctx, ebHash))

	o.leiosMu.RLock()
	waiters := len(o.leiosClosureWaiters)
	o.leiosMu.RUnlock()
	require.Zero(t, waiters)
}

func TestAwaitMergedLeiosRankingBlockTimesOut(t *testing.T) {
	certRB := testDijkstraCertRBRaw(t, 42, make([]byte, lcommon.Blake2b256Size))
	var ebHash lcommon.Blake2b256
	ebHash[0] = 0xcc

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Millisecond,
	)
	defer cancel()
	merged, ok := o.awaitMergedLeiosRankingBlock(ctx, certRB, ebHash)
	require.False(t, ok)
	require.Nil(t, merged)
}

func TestLeiosCertRbMetricsRecordOutcomes(t *testing.T) {
	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{EnableLeios: true, PromRegistry: reg})
	require.NotNil(t, o.leiosMetrics)

	o.recordLeiosCertRbOutcome("merged")
	o.recordLeiosCertRbOutcome("merged_after_wait")
	o.recordLeiosCertRbOutcome("unresolved")
	o.recordLeiosCertRbOutcome("unresolved")
	o.recordLeiosCertRbWait("resolved", 100*time.Millisecond)
	o.recordLeiosCertRbWait("timeout", 3*time.Second)
	o.recordLeiosCertRbWait("cancelled", 5*time.Millisecond)

	require.Equal(t, float64(1), promtestutil.ToFloat64(
		o.leiosMetrics.certRbOutcomes.WithLabelValues("merged"),
	))
	require.Equal(t, float64(1), promtestutil.ToFloat64(
		o.leiosMetrics.certRbOutcomes.WithLabelValues("merged_after_wait"),
	))
	require.Equal(t, float64(2), promtestutil.ToFloat64(
		o.leiosMetrics.certRbOutcomes.WithLabelValues("unresolved"),
	))
	// One histogram series per wait outcome (resolved, timeout, cancelled).
	require.Equal(t, 3, promtestutil.CollectAndCount(
		o.leiosMetrics.certRbWaitSeconds,
	))
}

func TestLeiosCertRbMetricsNilSafe(t *testing.T) {
	// Without a PromRegistry, metrics are not initialized; recording must be
	// a no-op rather than panicking.
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.Nil(t, o.leiosMetrics)
	require.NotPanics(t, func() {
		o.recordLeiosCertRbOutcome("merged")
		o.recordLeiosCertRbWait("timeout", time.Second)
	})
}

func TestLeiosClosureWaitTimeoutPrecedence(t *testing.T) {
	// Explicit config override wins.
	o := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LeiosClosureWaitTimeout: 5 * time.Second,
	})
	require.Equal(t, 5*time.Second, o.leiosClosureWaitTimeout())

	// With no override and no ledger timing, the conservative default applies
	// (not a short constant).
	o = newOuroboros(OuroborosConfig{EnableLeios: true})
	require.Equal(
		t,
		defaultLeiosClosureWaitTimeout,
		o.leiosClosureWaitTimeout(),
	)
	require.GreaterOrEqual(t, defaultLeiosClosureWaitTimeout, 6*time.Second)
}

func TestServeLeiosCertRbWithWaitErrorsOnTimeout(t *testing.T) {
	// A certifying ranking block whose endorser closure never arrives must
	// surface an error (so the caller closes the connection) rather than
	// serving the raw, empty-transaction block.
	certRB := testDijkstraCertRBRaw(t, 77, make([]byte, lcommon.Blake2b256Size))
	var ebHash lcommon.Blake2b256
	ebHash[0] = 0xdd

	o := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LeiosClosureWaitTimeout: 20 * time.Millisecond,
	})
	block := models.Block{Cbor: certRB, Slot: 77, Hash: []byte{0x77}}
	got, err := o.serveLeiosCertRbWithWait(block, ebHash, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, errLeiosClosureUnresolved)
	require.Nil(t, got)
	require.Contains(t, err.Error(), "timeout")
}

// TestServeLeiosCertRbWithWaitCancelsOnConnectionDone reproduces issue #3514:
// the closure wait must not remain parked for the full wait window once the
// serving connection ends. It configures a wait window far longer than the
// test should ever take, closes connDone immediately, and requires the call
// to return well within the window with the defined unresolved-closure error.
func TestServeLeiosCertRbWithWaitCancelsOnConnectionDone(t *testing.T) {
	certRB := testDijkstraCertRBRaw(t, 78, make([]byte, lcommon.Blake2b256Size))
	var ebHash lcommon.Blake2b256
	ebHash[0] = 0xee

	o := newOuroboros(OuroborosConfig{
		EnableLeios: true,
		// Far longer than this test's own timeout budget: if connDone did not
		// bound the wait, the assertions below would time out first.
		LeiosClosureWaitTimeout: time.Hour,
	})
	block := models.Block{Cbor: certRB, Slot: 78, Hash: []byte{0x78}}

	connDone := make(chan struct{})
	close(connDone)

	done := make(chan struct{})
	var (
		got []byte
		err error
	)
	go func() {
		got, err = o.serveLeiosCertRbWithWait(block, ebHash, connDone)
		close(done)
	}()

	testutil.RequireReceive(
		t,
		done,
		2*time.Second,
		"closure wait to cancel promptly when the connection is already done",
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errLeiosClosureUnresolved)
	require.Nil(t, got)
	require.Contains(t, err.Error(), "cancelled")
}

// TestServeLeiosCertRbWithWaitCancelsWhenConnectionEndsMidWait covers the
// wait actually in progress (not already done) being woken by the connection
// ending, as distinct from the already-done case above and from a natural
// timeout.
func TestServeLeiosCertRbWithWaitCancelsWhenConnectionEndsMidWait(t *testing.T) {
	certRB := testDijkstraCertRBRaw(t, 79, make([]byte, lcommon.Blake2b256Size))
	var ebHash lcommon.Blake2b256
	ebHash[0] = 0xff

	o := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LeiosClosureWaitTimeout: time.Hour,
	})
	block := models.Block{Cbor: certRB, Slot: 79, Hash: []byte{0x79}}

	connDone := make(chan struct{})
	done := make(chan struct{})
	var (
		got []byte
		err error
	)
	go func() {
		got, err = o.serveLeiosCertRbWithWait(block, ebHash, connDone)
		close(done)
	}()

	// Let the wait actually register before ending the connection.
	testutil.WaitForCondition(
		t,
		func() bool {
			o.leiosMu.RLock()
			defer o.leiosMu.RUnlock()
			return len(o.leiosClosureWaiters[leiosBlockKey(ebHash.Bytes())]) > 0
		},
		2*time.Second,
		"closure waiter to register",
	)
	close(connDone)

	testutil.RequireReceive(
		t,
		done,
		2*time.Second,
		"closure wait to cancel promptly when the connection ends mid-wait",
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errLeiosClosureUnresolved)
	require.Nil(t, got)
	require.Contains(t, err.Error(), "cancelled")
}

// TestStoreLeiosEndorserBlockManifestDoesNotClobberCachedTxs reproduces the
// field failure where a producer repeatedly logged "certified Leios endorser
// block unavailable" for an endorser block whose transactions had already been
// fetched in full, minutes earlier, several times over.
//
// The relay offers every endorser block on every connection, so a
// manifest-only store (txsRaw nil, from the MsgBlockOffer handler) routinely
// lands AFTER the transactions have been fetched by some other connection.
// Replacing the cache entry then drops the transaction set, making a complete
// endorser block report itself unavailable again.
func TestStoreLeiosEndorserBlockManifestDoesNotClobberCachedTxs(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 2636557, 2)
	txsRaw := []cbor.RawMessage{
		mustCbor(t, "tx0"),
		mustCbor(t, "tx1"),
	}

	o := newOuroboros(OuroborosConfig{EnableLeios: true})

	// One connection delivers the manifest, another completes the txs.
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, txsRaw))
	_, gotTxs, ok := o.EndorserBlockTxsByHash(point.Hash)
	require.True(t, ok)
	require.Equal(t, txsRaw, gotTxs)

	// Every remaining connection's redundant manifest fetch must leave the
	// completed transaction set intact.
	for range 3 {
		require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))
		data, found := o.lookupLeiosEndorserBlock(point.Hash)
		require.True(t, found)
		require.True(
			t,
			data.completeTxCache(),
			"redundant manifest store dropped the cached endorser transactions",
		)
	}

	slot, gotTxs, ok := o.EndorserBlockTxsByHash(point.Hash)
	require.True(t, ok)
	require.Equal(t, point.Slot, slot)
	require.Equal(t, txsRaw, gotTxs)

	// The leios-fetch serving path must keep answering downstream peers too.
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

// TestStoreLeiosEndorserBlockKeepsLargerTxSet guards the general invariant:
// a store never shrinks a cached endorser block's transaction set, whichever
// caller supplies the smaller one.
func TestStoreLeiosEndorserBlockKeepsLargerTxSet(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 4242, 3)
	full := []cbor.RawMessage{
		mustCbor(t, "tx0"),
		mustCbor(t, "tx1"),
		mustCbor(t, "tx2"),
	}

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, full))
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, full[:1]))

	_, gotTxs, ok := o.EndorserBlockTxsByHash(point.Hash)
	require.True(t, ok)
	require.Equal(t, full, gotTxs)
}
