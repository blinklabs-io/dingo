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
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
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

func testDijkstraBlockRaw(
	t *testing.T,
	idx int,
) (ocommon.Point, cbor.RawMessage) {
	t.Helper()
	blockBody := gdijkstra.DijkstraBlockBody{
		TransactionBodies:      []gdijkstra.DijkstraTransactionBody{},
		TransactionWitnessSets: []gdijkstra.DijkstraTransactionWitnessSet{},
		InvalidTransactions:    []uint{},
		LeiosCertificate:       &gdijkstra.DijkstraLeiosCertificate{},
		PerasCertificate:       &gdijkstra.DijkstraPerasCertificate{},
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
	require.Contains(t, err.Error(), "tx cache incomplete")
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
