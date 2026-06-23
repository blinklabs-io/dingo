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
	"slices"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	"github.com/stretchr/testify/require"
)

// cappingBlockTxsRequester simulates a relay that serves at most maxPerResp
// transactions per BlockTxsRequest — a prefix of the requested ascending
// indices, mirroring the prototype relay's per-message size cap. Each returned
// transaction's CBOR encodes its absolute index so callers can verify ordering.
// includeBitmaps toggles whether the response echoes the served bitmaps (the
// prototype's 4-element form) or omits them (forcing the prefix fallback).
type cappingBlockTxsRequester struct {
	maxPerResp     int // <= 0 means no cap (serve all requested)
	serveNothing   bool
	includeBitmaps bool
	calls          int
}

func (r *cappingBlockTxsRequester) BlockTxsRequest(
	point ocommon.Point,
	bitmaps map[uint16]uint64,
) (protocol.Message, error) {
	r.calls++
	requested := leiosBitmapTxIndices(bitmaps)
	slices.Sort(requested)
	n := len(requested)
	if r.serveNothing {
		n = 0
	} else if r.maxPerResp > 0 && n > r.maxPerResp {
		n = r.maxPerResp
	}
	served := map[uint16]uint64{}
	txs := make([]cbor.RawMessage, 0, n)
	for k := 0; k < n; k++ {
		idx := requested[k]
		served[uint16(idx/64)] |= 1 << uint(idx%64)
		enc, err := cbor.Encode(idx)
		if err != nil {
			return nil, err
		}
		txs = append(txs, cbor.RawMessage(enc))
	}
	if r.includeBitmaps {
		return leiosfetch.NewMsgBlockTxsFull(point, served, txs), nil
	}
	return leiosfetch.NewMsgBlockTxs(txs), nil
}

func requireTxsInIndexOrder(t *testing.T, txs []cbor.RawMessage, want int) {
	t.Helper()
	require.Len(t, txs, want)
	for i, raw := range txs {
		var idx int
		_, err := cbor.Decode(raw, &idx)
		require.NoError(t, err)
		require.Equalf(t, i, idx, "tx at position %d encodes index %d", i, idx)
	}
}

func TestFetchLeiosEbTxsBatchedReRequestsUntilComplete(t *testing.T) {
	o := &Ouroboros{}
	point := ocommon.Point{Slot: 100, Hash: []byte{0x01, 0x02}}
	// 639 txs across 10 windows; relay caps each response at 50, so most
	// windows need multiple rounds. Response echoes served bitmaps.
	txs, err := o.fetchLeiosEbTxsBatched(
		&cappingBlockTxsRequester{maxPerResp: 50, includeBitmaps: true},
		point,
		639,
	)
	require.NoError(t, err)
	requireTxsInIndexOrder(t, txs, 639)
}

func TestFetchLeiosEbTxsBatchedPrefixFallback(t *testing.T) {
	o := &Ouroboros{}
	point := ocommon.Point{Slot: 100, Hash: []byte{0x01, 0x02}}
	// Response omits bitmaps, so the fetch must assume a served prefix of the
	// requested ascending indices. Cap of 40 (< 64) forces re-requests.
	txs, err := o.fetchLeiosEbTxsBatched(
		&cappingBlockTxsRequester{maxPerResp: 40, includeBitmaps: false},
		point,
		116,
	)
	require.NoError(t, err)
	requireTxsInIndexOrder(t, txs, 116)
}

func TestFetchLeiosEbTxsBatchedFullResponse(t *testing.T) {
	o := &Ouroboros{}
	point := ocommon.Point{Slot: 1, Hash: []byte{0x09}}
	// No cap: every window served whole in one round.
	txs, err := o.fetchLeiosEbTxsBatched(
		&cappingBlockTxsRequester{maxPerResp: 0, includeBitmaps: true},
		point,
		200,
	)
	require.NoError(t, err)
	requireTxsInIndexOrder(t, txs, 200)
}

func TestFetchLeiosEbTxsBatchedNoProgressErrors(t *testing.T) {
	o := &Ouroboros{}
	point := ocommon.Point{Slot: 1, Hash: []byte{0x09}}
	// A relay that serves nothing must not loop forever; it returns an error
	// with whatever prefix was gathered (none here).
	txs, err := o.fetchLeiosEbTxsBatched(
		&cappingBlockTxsRequester{serveNothing: true, includeBitmaps: true},
		point,
		10,
	)
	require.Error(t, err)
	require.Empty(t, txs)
}

func TestFetchLeiosEbTxsBatchedRejectsUnrepresentableWindowCount(
	t *testing.T,
) {
	requester := &cappingBlockTxsRequester{}
	o := &Ouroboros{}
	point := ocommon.Point{Slot: 1, Hash: []byte{0x09}}

	txs, err := o.fetchLeiosEbTxsBatched(
		requester,
		point,
		leiosTxFetchWindowSize*leiosTxFetchMaxWindows+1,
	)
	require.Error(t, err)
	require.Nil(t, txs)
	require.Zero(t, requester.calls)
	require.Contains(t, err.Error(), "requires 65537 bitmap windows")
}

func TestLeiosBitmapTxIndices(t *testing.T) {
	// window 0 bits 0,1; window 2 bit 3 -> indices 0,1,131 ascending.
	got := leiosBitmapTxIndices(map[uint16]uint64{0: 0b11, 2: 1 << 3})
	require.Equal(t, []int{0, 1, 131}, got)
	require.Nil(t, leiosBitmapTxIndices(nil))
}

func TestLeiosWindowNeededMask(t *testing.T) {
	result := make([]cbor.RawMessage, 70)
	result[0] = cbor.RawMessage{0x00} // present
	result[2] = cbor.RawMessage{0x00} // present
	// window 0: bits 1 and 3..63 needed (0 and 2 present), capped at txCount 70.
	mask := leiosWindowNeededMask(result, 0, 70)
	require.Equal(t, uint64(0), mask&(1<<0))
	require.NotEqual(t, uint64(0), mask&(1<<1))
	require.Equal(t, uint64(0), mask&(1<<2))
	// window 1: only indices 64..69 exist (6 bits), all needed.
	require.Equal(t, uint64(0b111111), leiosWindowNeededMask(result, 1, 70))
}

func TestLeiosNeededBitmap(t *testing.T) {
	// 600 txs spans 10 windows (0..9); none fetched yet.
	result := make([]cbor.RawMessage, 600)
	// A batch is capped at maxWindows lowest-indexed windows.
	bm := leiosNeededBitmap(result, 600, leiosTxFetchWindowsPerRequest)
	require.Len(t, bm, leiosTxFetchWindowsPerRequest)
	for w := range uint16(leiosTxFetchWindowsPerRequest) {
		require.Contains(t, bm, w, "lowest windows selected first")
	}
	require.NotContains(t, bm, uint16(8))
	// Mark window 0 fully present; the batch then starts at window 1 and still
	// reaches one window past the previous cap.
	for i := range leiosTxFetchWindowSize {
		result[i] = cbor.RawMessage{0x00}
	}
	bm = leiosNeededBitmap(result, 600, leiosTxFetchWindowsPerRequest)
	require.NotContains(t, bm, uint16(0), "fully fetched window is skipped")
	require.Contains(t, bm, uint16(8))
	// Fewer remaining windows than the cap returns just those windows.
	require.Len(t, leiosNeededBitmap(result, 600, 100), 9)
}

// servingBlockTxsRequester serves every requested transaction in a single
// response (no per-message cap), echoing the served bitmap. It records the
// largest number of windows asked for in one request so a test can assert the
// fetch batches windows rather than requesting them one at a time.
type servingBlockTxsRequester struct {
	calls          int
	maxWindowsSeen int
}

func (r *servingBlockTxsRequester) BlockTxsRequest(
	point ocommon.Point,
	bitmaps map[uint16]uint64,
) (protocol.Message, error) {
	r.calls++
	if len(bitmaps) > r.maxWindowsSeen {
		r.maxWindowsSeen = len(bitmaps)
	}
	requested := leiosBitmapTxIndices(bitmaps)
	slices.Sort(requested)
	served := map[uint16]uint64{}
	txs := make([]cbor.RawMessage, 0, len(requested))
	for _, idx := range requested {
		served[uint16(idx/64)] |= 1 << uint(idx%64)
		enc, err := cbor.Encode(idx)
		if err != nil {
			return nil, err
		}
		txs = append(txs, cbor.RawMessage(enc))
	}
	return leiosfetch.NewMsgBlockTxsFull(point, served, txs), nil
}

func TestFetchLeiosEbTxsBatchedBatchesWindowsPerRequest(t *testing.T) {
	o := &Ouroboros{}
	point := ocommon.Point{Slot: 100, Hash: []byte{0x01}}
	// 600 txs = 10 windows. With up-to-8-windows-per-request and a relay that
	// serves the whole request, this completes in 2 rounds (8 + 2 windows),
	// not 10 — proving requests batch multiple windows.
	requester := &servingBlockTxsRequester{}
	txs, err := o.fetchLeiosEbTxsBatched(requester, point, 600)
	require.NoError(t, err)
	requireTxsInIndexOrder(t, txs, 600)
	require.Equal(t, 2, requester.calls)
	require.Equal(t, leiosTxFetchWindowsPerRequest, requester.maxWindowsSeen)
}
