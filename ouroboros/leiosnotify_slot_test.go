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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/stretchr/testify/require"
)

// TestLeiosBlockKeyDistinguishesSlots guards the composite cache/dedup key
// leiosEndorserBlocks and leiosFetchInProgress rely on: an entry (or an
// in-flight claim) for one occurrence of a hash must not collide with -- and
// so must not suppress or mask -- a legitimate, independent occurrence of the
// same content-addressed hash at a different slot (issue #3513).
func TestLeiosBlockKeyDistinguishesSlots(t *testing.T) {
	hash := []byte{0xaa, 0xbb}
	a := leiosBlockKey(10, hash)
	b := leiosBlockKey(11, hash)
	require.NotEqual(t, a, b)

	same := leiosBlockKey(10, hash)
	require.Equal(t, a, same)
}

// TestLeiosNotifyBlockOfferFetchesSameHashAtDifferentSlot is the offer-first
// regression from wolf31o2's review: MsgBlockOffer's cache-hit skip used to
// key solely on hash, so a hash already cached at one slot silently swallowed
// a genuine offer of the same content-addressed hash recurring at a different
// slot -- the manifest was never fetched (or reconciled against an
// announcement) for the new point at all. The fetch must still happen when
// the cached entry's slot does not match the offered point.
func TestLeiosNotifyBlockOfferFetchesSameHashAtDifferentSlot(t *testing.T) {
	staleSlot := uint64(100)
	offeredPoint, blockRaw := testLeiosEndorserBlockRaw(t, 200)

	conn, mockErrCh := newLeiosFetchConversation(
		t,
		append(
			leiosFetchHandshake(),
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockRequest,
			},
			ouroboros_mock.ConversationEntryOutput{
				ProtocolId: leiosfetch.ProtocolId,
				IsResponse: true,
				Messages: []protocol.Message{
					leiosfetch.NewMsgBlock(cbor.RawMessage(blockRaw)),
				},
			},
		),
	)
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	require.True(t, cm.AddConnection(conn, false, "peer"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})

	o := newOuroboros(OuroborosConfig{
		ConnManager: cm,
		EnableLeios: true,
	})
	// A different occurrence of the same hash is already cached under a
	// stale slot -- e.g. an earlier, unrelated replay of the same
	// content-addressed manifest.
	require.NoError(t, o.storeLeiosEndorserBlock(
		ocommon.NewPoint(staleSlot, offeredPoint.Hash),
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))

	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: conn.Id()},
		oleiosnotify.NewMsgBlockOffer(offeredPoint, uint64(len(blockRaw))),
	))

	// The mock connection only completes its conversation once the expected
	// BlockRequest/MsgBlock exchange actually happens. Under the pre-fix
	// hash-only skip, MsgBlockOffer returns immediately without ever
	// dispatching a fetch, and this never resolves.
	requireLeiosFetchConversationDone(t, mockErrCh)
}

// TestLeiosNotifyBlockTxsOfferFetchesSameHashAtDifferentSlot is the
// MsgBlockTxsOffer half of the same offer-first regression: both its
// top-level "already complete" skip and the dispatched fetch closure's own
// re-check of the same cache entry used to key solely on hash, so an offer
// for a genuinely different occurrence of a recurring hash was swallowed
// twice over -- once before the fetch was even dispatched, and again inside
// the dispatched closure itself, which independently treated the unrelated,
// differently-slotted cache hit as "nothing left to fetch."
func TestLeiosNotifyBlockTxsOfferFetchesSameHashAtDifferentSlot(t *testing.T) {
	staleSlot := uint64(300)
	offeredPoint, blockRaw := testLeiosEndorserBlockRaw(t, 400)

	conn, mockErrCh := newLeiosFetchConversation(
		t,
		append(
			leiosFetchHandshake(),
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockRequest,
			},
			ouroboros_mock.ConversationEntryOutput{
				ProtocolId: leiosfetch.ProtocolId,
				IsResponse: true,
				Messages: []protocol.Message{
					leiosfetch.NewMsgBlock(cbor.RawMessage(blockRaw)),
				},
			},
		),
	)
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	require.True(t, cm.AddConnection(conn, false, "peer"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})

	o := newOuroboros(OuroborosConfig{
		ConnManager:        cm,
		EnableLeios:        true,
		EnableLeiosTxFetch: true,
	})
	// A different, already-complete occurrence of the same hash is cached
	// under a stale slot -- complete (not just present), so the "nothing left
	// to fetch" skip is the one actually being exercised here.
	require.NoError(t, o.storeLeiosEndorserBlock(
		ocommon.NewPoint(staleSlot, offeredPoint.Hash),
		blockRaw,
		[]cbor.RawMessage{mustCbor(t, "tx0")},
		leiosStoreAuthoritative,
	))

	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: conn.Id()},
		oleiosnotify.NewMsgBlockTxsOffer(offeredPoint),
	))

	// Under the pre-fix hash-only checks, neither the top-level skip nor the
	// dispatched closure would ever issue this manifest re-fetch, and this
	// never resolves.
	requireLeiosFetchConversationDone(t, mockErrCh)
}
