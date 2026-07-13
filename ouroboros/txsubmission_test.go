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
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/mempool"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/txsubmission"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type txsubmissionTestValidator struct{}

func (txsubmissionTestValidator) ValidateTx(gledger.Transaction) error {
	return nil
}

func (txsubmissionTestValidator) ValidateTxWithOverlay(
	gledger.Transaction,
	map[string]struct{},
	map[string]lcommon.Utxo,
) error {
	return nil
}

// TestTxSubmissionClientRequestTxIds verifies empty, partial, and capped
// TxId responses when a peer asks what transactions this node can relay.
func TestTxSubmissionClientRequestTxIds(t *testing.T) {
	tests := []struct {
		name      string
		hashes    []string
		req       uint16
		wantCount int
	}{
		{
			name:      "empty response",
			req:       10,
			wantCount: 0,
		},
		{
			name: "partial response",
			hashes: []string{
				txsubmissionTestHash(1),
				txsubmissionTestHash(2),
			},
			req:       10,
			wantCount: 2,
		},
		{
			name: "full response",
			hashes: []string{
				txsubmissionTestHash(1),
				txsubmissionTestHash(2),
				txsubmissionTestHash(3),
			},
			req:       2,
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange a peer consumer with the test's available tx set.
			o, connId := newTxSubmissionTestOuroboros(t)
			o.Mempool.AddConsumer(connId)
			appendTxSubmissionTestTxs(t, o.Mempool, tt.hashes...)

			// Ask the handler for at most the peer-requested number of TxIds.
			ids, err := o.txsubmissionClientRequestTxIds(
				txsubmission.CallbackContext{ConnectionId: connId},
				false,
				0,
				tt.req,
			)

			// Verify the response count and metadata match the offered txs.
			require.NoError(t, err)
			require.Len(t, ids, tt.wantCount)
			for idx, id := range ids {
				require.Equal(t, uint16(6), id.TxId.EraId)
				require.Equal(
					t,
					uint32(len(txsubmissionTestBody(tt.hashes[idx]))),
					id.Size,
				)
				require.Equal(t, tt.hashes[idx], hex.EncodeToString(id.TxId.TxId[:]))
			}
		})
	}
}

// TestTxSubmissionClientRequestTxIdsClearsConsumerCacheOnAck verifies that
// peer acknowledgements discard previously advertised transaction bodies.
func TestTxSubmissionClientRequestTxIdsClearsConsumerCacheOnAck(t *testing.T) {
	// Arrange one cached transaction for a peer consumer.
	o, connId := newTxSubmissionTestOuroboros(t)
	o.Mempool.AddConsumer(connId)
	hash := txsubmissionTestHash(1)
	appendTxSubmissionTestTxs(t, o.Mempool, hash)
	ctx := txsubmission.CallbackContext{ConnectionId: connId}

	// First advertise the transaction so it is stored in the consumer cache.
	ids, err := o.txsubmissionClientRequestTxIds(ctx, false, 0, 1)
	require.NoError(t, err)
	require.Len(t, ids, 1)

	// Send an ack and zero request count to clear the advertised cache.
	ids, err = o.txsubmissionClientRequestTxIds(ctx, false, 1, 0)
	require.NoError(t, err)
	require.Empty(t, ids)

	// Verify the acknowledged transaction body can no longer be served.
	bodies, err := o.txsubmissionClientRequestTxs(ctx, []txsubmission.TxId{
		mustTxSubmissionTestTxId(t, hash),
	})
	require.NoError(t, err)
	require.Empty(t, bodies)
}

// TestTxSubmissionClientRequestTxs verifies that known cached TxIds return
// bodies while unknown or already-served TxIds are ignored.
func TestTxSubmissionClientRequestTxs(t *testing.T) {
	// Arrange one known tx and one unknown tx id for the peer request.
	o, connId := newTxSubmissionTestOuroboros(t)
	o.Mempool.AddConsumer(connId)
	knownHash := txsubmissionTestHash(1)
	unknownHash := txsubmissionTestHash(99)
	appendTxSubmissionTestTxs(t, o.Mempool, knownHash)
	ctx := txsubmission.CallbackContext{ConnectionId: connId}

	// Advertise the known tx first so RequestTxs can find it in cache.
	ids, err := o.txsubmissionClientRequestTxIds(ctx, false, 0, 1)
	require.NoError(t, err)
	require.Len(t, ids, 1)

	// Request both unknown and known ids; only the cached known tx is returned.
	bodies, err := o.txsubmissionClientRequestTxs(ctx, []txsubmission.TxId{
		mustTxSubmissionTestTxId(t, unknownHash),
		ids[0].TxId,
	})
	require.NoError(t, err)
	require.Equal(t, []txsubmission.TxBody{
		{
			EraId:  6,
			TxBody: txsubmissionTestBody(knownHash),
		},
	}, bodies)

	// Request the known id again to prove served txs are removed from cache.
	bodies, err = o.txsubmissionClientRequestTxs(ctx, []txsubmission.TxId{
		ids[0].TxId,
	})
	require.NoError(t, err)
	require.Empty(t, bodies)
}

// TestTxSubmissionClientRequestCallbacksMissingConsumer verifies that both
// client callbacks fail cleanly when no mempool consumer exists.
func TestTxSubmissionClientRequestCallbacksMissingConsumer(t *testing.T) {
	// Arrange a connection id without registering a mempool consumer.
	o, connId := newTxSubmissionTestOuroboros(t)
	ctx := txsubmission.CallbackContext{ConnectionId: connId}

	// RequestTxIds should fail cleanly instead of dereferencing nil state.
	ids, err := o.txsubmissionClientRequestTxIds(ctx, false, 0, 1)
	require.ErrorContains(t, err, "no mempool consumer")
	require.Nil(t, ids)

	// RequestTxs should report the same missing-consumer error.
	bodies, err := o.txsubmissionClientRequestTxs(ctx, []txsubmission.TxId{
		mustTxSubmissionTestTxId(t, txsubmissionTestHash(1)),
	})
	require.ErrorContains(t, err, "no mempool consumer")
	require.Nil(t, bodies)
}

// TestTxSubmissionClientRequestTxIdsMalformedCachedTxHash verifies malformed
// cached transaction hashes return errors instead of panicking.
func TestTxSubmissionClientRequestTxIdsMalformedCachedTxHash(t *testing.T) {
	// Arrange a cached tx with a hash that is valid hex but the wrong length.
	o, connId := newTxSubmissionTestOuroboros(t)
	o.Mempool.AddConsumer(connId)
	appendTxSubmissionTestTx(t, o.Mempool, &mempool.MempoolTransaction{
		Hash:     strings.Repeat("0", 62),
		Cbor:     []byte("bad-hash-body"),
		Type:     6,
		LastSeen: time.Now(),
	})

	// Verify the malformed hash is returned as an error, not a panic.
	require.NotPanics(t, func() {
		ids, err := o.txsubmissionClientRequestTxIds(
			txsubmission.CallbackContext{ConnectionId: connId},
			false,
			0,
			1,
		)
		require.ErrorContains(t, err, "unexpected tx hash length")
		require.Nil(t, ids)
	})
}

// TestTxSubmissionClientRequestTxIdsZeroRequestDoesNotAdvance verifies a
// zero-count peer request leaves the consumer positioned on the next tx.
func TestTxSubmissionClientRequestTxIdsZeroRequestDoesNotAdvance(t *testing.T) {
	// Arrange one available tx for the peer consumer.
	o, connId := newTxSubmissionTestOuroboros(t)
	o.Mempool.AddConsumer(connId)
	hash := txsubmissionTestHash(1)
	appendTxSubmissionTestTxs(t, o.Mempool, hash)
	ctx := txsubmission.CallbackContext{ConnectionId: connId}

	// A zero-count request should return nothing.
	ids, err := o.txsubmissionClientRequestTxIds(ctx, false, 0, 0)
	require.NoError(t, err)
	require.Empty(t, ids)

	// A later nonzero request should still see the same first tx.
	ids, err = o.txsubmissionClientRequestTxIds(ctx, false, 0, 1)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Equal(t, hash, hex.EncodeToString(ids[0].TxId.TxId[:]))
}

// TestTxSubmissionServerInitMissingConnectionReturnsCleanly verifies server
// init exits without error when the connection is already gone.
func TestTxSubmissionServerInitMissingConnectionReturnsCleanly(t *testing.T) {
	// Arrange an Ouroboros instance whose connection manager has no such peer.
	o, connId := newTxSubmissionTestOuroboros(t)

	// Start server init and let its background loop observe the missing peer.
	err := o.txsubmissionServerInit(
		txsubmission.CallbackContext{ConnectionId: connId},
	)

	// Missing connection during init should be treated as a clean exit.
	require.NoError(t, err)
}

// TestTxSubmissionConnectionClosedCleanup verifies connection close handling
// removes txsubmission consumer and rate-limiter state for that peer.
func TestTxSubmissionConnectionClosedCleanup(t *testing.T) {
	// Arrange per-peer mempool and rate-limiter state.
	o, connId := newTxSubmissionTestOuroboros(t)
	o.Mempool.AddConsumer(connId)
	o.txSubmissionRateLimiter = newTxSubmissionRateLimiter(1, 1)
	require.False(t, o.txSubmissionRateLimiter.Allow(connId, 2))

	// Deliver the same connection-close event used by normal node wiring.
	o.HandleConnClosedEvent(event.Event{
		Type: connmanager.ConnectionClosedEventType,
		Data: connmanager.ConnectionClosedEvent{
			ConnectionId: connId,
		},
	})

	// Verify both txsubmission state holders have forgotten the peer.
	require.Nil(t, o.Mempool.Consumer(connId))
	require.True(t, o.txSubmissionRateLimiter.Allow(connId, 1))
}

func newTxSubmissionTestOuroboros(
	t *testing.T,
) (*Ouroboros, ouroboros.ConnectionId) {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	m, err := mempool.NewMempool(mempool.MempoolConfig{
		Logger:          logger,
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       txsubmissionTestValidator{},
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, m.Stop(t.Context()))
	})

	o := NewOuroboros(OuroborosConfig{Logger: logger})
	o.ConnManager = connmanager.NewConnectionManager(connmanager.ConnectionManagerConfig{
		Logger: logger,
	})
	o.Mempool = m
	return o, txsubmissionTestConnId(t)
}

func txsubmissionTestConnId(t *testing.T) ouroboros.ConnectionId {
	t.Helper()
	localAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	require.NoError(t, err)
	remoteAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:3002")
	require.NoError(t, err)
	return ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}
}

func appendTxSubmissionTestTxs(
	t *testing.T,
	m *mempool.Mempool,
	hashes ...string,
) {
	t.Helper()
	for _, hash := range hashes {
		appendTxSubmissionTestTx(t, m, &mempool.MempoolTransaction{
			Hash:     hash,
			Cbor:     txsubmissionTestBody(hash),
			Type:     6,
			LastSeen: time.Now(),
		})
	}
}

func appendTxSubmissionTestTx(
	t *testing.T,
	m *mempool.Mempool,
	tx *mempool.MempoolTransaction,
) {
	t.Helper()
	m.Lock()
	defer m.Unlock()
	value := reflect.ValueOf(m).Elem().FieldByName("transactions")
	transactions := reflect.NewAt(
		value.Type(),
		unsafe.Pointer(value.UnsafeAddr()),
	).Elem()
	transactions.Set(reflect.Append(transactions, reflect.ValueOf(tx)))
}

func txsubmissionTestHash(idx int) string {
	return fmt.Sprintf("%064x", idx)
}

func txsubmissionTestBody(hash string) []byte {
	return []byte("tx-body-" + hash)
}

func mustTxSubmissionTestTxId(t *testing.T, hash string) txsubmission.TxId {
	t.Helper()
	bytes, err := hex.DecodeString(hash)
	require.NoError(t, err)
	require.Len(t, bytes, 32)
	var txId [32]byte
	copy(txId[:], bytes)
	return txsubmission.TxId{
		EraId: 6,
		TxId:  txId,
	}
}
