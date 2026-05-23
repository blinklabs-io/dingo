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
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	gouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gleios "github.com/blinklabs-io/gouroboros/ledger/leios"
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

func testTxRaw(
	t *testing.T,
	body cbor.RawMessage,
	witness cbor.RawMessage,
	valid bool,
	metadata cbor.RawMessage,
) cbor.RawMessage {
	t.Helper()
	return mustCbor(t, []any{
		body,
		witness,
		valid,
		metadata,
	})
}

func testEndorserBlockRaw(
	t *testing.T,
	refs []gleios.TxReference,
) cbor.RawMessage {
	t.Helper()
	block := &gleios.LeiosEndorserBlock{
		Body: &gleios.LeiosEndorserBlockBody{
			TxReferences: refs,
		},
	}
	data, err := block.MarshalCBOR()
	require.NoError(t, err)
	return cbor.RawMessage(data)
}

func testIndexedEndorserBlock(
	t *testing.T,
	idx int,
) (ocommon.Point, cbor.RawMessage) {
	t.Helper()
	var txHash lcommon.Blake2b256
	txHash[0] = byte(idx)
	txHash[1] = byte(idx >> 8)
	ebRaw := testEndorserBlockRaw(t, []gleios.TxReference{
		{TxHash: txHash, TxSize: uint16(idx + 1)},
	})
	ebHash := lcommon.Blake2b256Hash(ebRaw)
	return ocommon.NewPoint(uint64(idx), ebHash.Bytes()), ebRaw
}

func testCertifiedRankingBlockRaw(
	t *testing.T,
	ebHash lcommon.Blake2b256,
	baseBody cbor.RawMessage,
	baseWitness cbor.RawMessage,
	baseMetadata cbor.RawMessage,
) cbor.RawMessage {
	t.Helper()
	cert := lcommon.LeiosEbCertificate{
		ElectionId:        lcommon.Blake2b256{0x01},
		EndorserBlockHash: ebHash,
		AggregatedVoteSig: make([]byte, 48),
	}
	certRaw := mustCbor(t, cert)
	return mustCbor(t, []any{
		cbor.RawMessage{0x80}, // header, not inspected by merge logic
		[]cbor.RawMessage{baseBody},
		[]cbor.RawMessage{baseWitness},
		map[uint64]cbor.RawMessage{0: baseMetadata},
		[]uint64{},
		certRaw,
	})
}

func TestMergedLeiosRankingBlockCborAppendsEndorserTransactions(
	t *testing.T,
) {
	baseBody := mustCbor(t, map[uint64]uint64{0: 1})
	baseWitness := mustCbor(t, map[uint64]uint64{})
	baseMetadata := mustCbor(t, map[uint64]string{1: "base"})

	ebRefHash1 := lcommon.Blake2b256{0x11}
	ebRefHash2 := lcommon.Blake2b256{0x22}
	ebRaw := testEndorserBlockRaw(t, []gleios.TxReference{
		{TxHash: ebRefHash1, TxSize: 123},
		{TxHash: ebRefHash2, TxSize: 456},
	})
	ebHash := lcommon.Blake2b256Hash(ebRaw)
	blockRaw := testCertifiedRankingBlockRaw(
		t,
		ebHash,
		baseBody,
		baseWitness,
		baseMetadata,
	)

	ebBody1 := mustCbor(t, map[uint64]uint64{0: 2})
	ebWitness1 := mustCbor(t, map[uint64]uint64{1: 1})
	ebMetadata1 := mustCbor(t, map[uint64]string{2: "endorser"})
	ebBody2 := mustCbor(t, map[uint64]uint64{0: 3})
	ebWitness2 := mustCbor(t, map[uint64]uint64{2: 1})
	ebTxs := []cbor.RawMessage{
		testTxRaw(t, ebBody1, ebWitness1, true, ebMetadata1),
		testTxRaw(t, ebBody2, ebWitness2, false, cbor.RawMessage{0xf6}),
	}

	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			ocommon.NewPoint(10, ebHash.Bytes()),
			ebRaw,
			ebTxs,
		),
	)

	mergedRaw, ok, err := o.mergedLeiosRankingBlockCbor(blockRaw)
	require.NoError(t, err)
	require.True(t, ok)

	var mergedItems []cbor.RawMessage
	_, err = cbor.Decode(mergedRaw, &mergedItems)
	require.NoError(t, err)
	require.Len(t, mergedItems, 6)

	var bodies []cbor.RawMessage
	_, err = cbor.Decode(mergedItems[1], &bodies)
	require.NoError(t, err)
	require.Equal(t, []cbor.RawMessage{baseBody, ebBody1, ebBody2}, bodies)

	var witnesses []cbor.RawMessage
	_, err = cbor.Decode(mergedItems[2], &witnesses)
	require.NoError(t, err)
	require.Equal(
		t,
		[]cbor.RawMessage{baseWitness, ebWitness1, ebWitness2},
		witnesses,
	)

	var metadata map[uint64]cbor.RawMessage
	_, err = cbor.Decode(mergedItems[3], &metadata)
	require.NoError(t, err)
	require.Equal(t, baseMetadata, metadata[0])
	require.Equal(t, ebMetadata1, metadata[1])
	require.NotContains(t, metadata, uint64(2))

	var invalid []uint64
	_, err = cbor.Decode(mergedItems[4], &invalid)
	require.NoError(t, err)
	require.Equal(t, []uint64{2}, invalid)
}

func TestMergedLeiosRankingBlockCborFallsBackWithoutCachedTxs(t *testing.T) {
	ebHash := lcommon.Blake2b256{0xaa}
	blockRaw := testCertifiedRankingBlockRaw(
		t,
		ebHash,
		mustCbor(t, map[uint64]uint64{}),
		mustCbor(t, map[uint64]uint64{}),
		mustCbor(t, map[uint64]uint64{}),
	)

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

	got := leiosTxsFromBitmap(txs, map[uint16]uint64{0: 0b1010})
	require.Equal(t, []cbor.RawMessage{txs[1], txs[3]}, got)
}

func TestLeiosFetchServerBlockTxsRejectsIncompleteCache(t *testing.T) {
	txRefHash1 := lcommon.Blake2b256{0x11}
	txRefHash2 := lcommon.Blake2b256{0x22}
	ebRaw := testEndorserBlockRaw(t, []gleios.TxReference{
		{TxHash: txRefHash1, TxSize: 123},
		{TxHash: txRefHash2, TxSize: 456},
	})
	ebHash := lcommon.Blake2b256Hash(ebRaw)
	point := ocommon.NewPoint(10, ebHash.Bytes())

	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			ebRaw,
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
	require.Contains(t, err.Error(), "tx cache incomplete")
}

func TestLeiosFetchServerBlockTxsRejectsOutOfRangeBitmap(t *testing.T) {
	txRefHash1 := lcommon.Blake2b256{0x11}
	txRefHash2 := lcommon.Blake2b256{0x22}
	ebRaw := testEndorserBlockRaw(t, []gleios.TxReference{
		{TxHash: txRefHash1, TxSize: 123},
		{TxHash: txRefHash2, TxSize: 456},
	})
	ebHash := lcommon.Blake2b256Hash(ebRaw)
	point := ocommon.NewPoint(10, ebHash.Bytes())

	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			ebRaw,
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

func TestFetchCachedLeiosEndorserBlockTxsReturnsCompleteCacheWithoutFetch(
	t *testing.T,
) {
	txRefHash := lcommon.Blake2b256{0x11}
	ebRaw := testEndorserBlockRaw(t, []gleios.TxReference{
		{TxHash: txRefHash, TxSize: 123},
	})
	ebHash := lcommon.Blake2b256Hash(ebRaw)
	point := ocommon.NewPoint(10, ebHash.Bytes())
	txRaw := mustCbor(t, "tx0")

	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			ebRaw,
			[]cbor.RawMessage{txRaw},
		),
	)

	got, err := o.fetchCachedLeiosEndorserBlockTxs(nil, point)
	require.NoError(t, err)
	require.Equal(t, []cbor.RawMessage{txRaw}, got)

	got[0][0] ^= 0xff
	cached, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.Equal(t, txRaw, cached.txsRaw[0])
}

func TestLeiosEndorserBlockLookupExpiresStaleEntries(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	point, raw := testIndexedEndorserBlock(t, 1)
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
	oldPoint, oldRaw := testIndexedEndorserBlock(t, 1)
	require.NoError(t, o.storeLeiosEndorserBlock(oldPoint, oldRaw, nil))
	oldData, ok := o.lookupLeiosEndorserBlock(oldPoint.Hash)
	require.True(t, ok)

	o.leiosMu.Lock()
	oldData.insertedAt = time.Now().Add(-leiosEndorserBlockCacheTTL - time.Second)
	o.leiosMu.Unlock()

	newPoint, newRaw := testIndexedEndorserBlock(t, 2)
	require.NoError(t, o.storeLeiosEndorserBlock(newPoint, newRaw, nil))

	_, ok = o.lookupLeiosEndorserBlock(oldPoint.Hash)
	require.False(t, ok)
	_, ok = o.lookupLeiosEndorserBlock(newPoint.Hash)
	require.True(t, ok)
}

func TestLeiosEndorserBlockCachePrunesBySize(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	var lastPoint ocommon.Point
	for idx := 0; idx < leiosEndorserBlockCacheMaxEntries+1; idx++ {
		point, raw := testIndexedEndorserBlock(t, idx)
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
