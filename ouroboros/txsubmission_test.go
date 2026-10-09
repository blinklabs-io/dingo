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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/utxoref"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/txsubmission"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// txsubmissionPanicReply builds a well-formed two-body reply whose sizes and
// hashes all match, so nothing but the decode can reject it.
func txsubmissionPanicReply(
	t *testing.T,
) ([]txsubmission.TxIdAndSize, []txsubmission.TxBody) {
	t.Helper()
	fixtures := txsubmissionTestFixtures(t)[:2]
	requested := make([]txsubmission.TxIdAndSize, 0, len(fixtures))
	returned := make([]txsubmission.TxBody, 0, len(fixtures))
	for _, fixture := range fixtures {
		requested = append(requested, txsubmission.TxIdAndSize{
			TxId: fixture.txId,
			Size: uint32(len(fixture.body)), // #nosec G115 -- real fixture
		})
		returned = append(returned, txsubmission.TxBody{
			EraId:  fixture.txId.EraId,
			TxBody: fixture.body,
		})
	}
	return requested, returned
}

// TestValidateTxsubmissionReplyContainsDecoderPanic covers the transaction-body
// other half: a peer
// body whose bytes panic the ledger decoder must be rejected as a decode
// failure, not unwound into the per-peer txsubmission goroutine, which has no
// recover above it and would take the node process down. Drop the containment
// and each subtest crashes the test binary rather than failing.
func TestValidateTxsubmissionReplyContainsDecoderPanic(t *testing.T) {
	t.Parallel()

	requested, returned := txsubmissionPanicReply(t)
	inner := errors.New("runtime error: index out of range [4] with length 2")
	tests := []struct {
		name    string
		panic   func()
		wrapped error
	}{
		{name: "string value", panic: func() { panic("cbor: bad header") }},
		{
			name:    "error value",
			panic:   func() { panic(inner) },
			wrapped: inner,
		},
		{name: "nil value", panic: func() { panic(nil) }},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			validated, err := validateTxsubmissionReply(
				requested,
				returned,
				func(uint, []byte) (gledger.Transaction, error) {
					testCase.panic()
					return nil, nil
				},
			)
			require.ErrorIs(t, err, safedecode.ErrDecodePanic)
			require.ErrorContains(
				t,
				err,
				"txsubmission reply transaction 0 decode failed",
			)
			if testCase.wrapped != nil {
				require.ErrorIs(t, err, testCase.wrapped)
			}
			// The reply is rejected outright: no body reaches admission, so
			// a panicking decode can never mark a peer's reply valid.
			require.Nil(t, validated)
		})
	}
}

// TestValidateTxsubmissionReplyPanicDropsWholeReply pins that a panic on one
// body discards the bodies that decoded before it. A partially valid batch
// from a peer that can crash the decoder is not trusted, matching how an
// ordinary decode failure is handled.
func TestValidateTxsubmissionReplyPanicDropsWholeReply(t *testing.T) {
	t.Parallel()

	requested, returned := txsubmissionPanicReply(t)
	var decoded int
	validated, err := validateTxsubmissionReply(
		requested,
		returned,
		func(txType uint, txCbor []byte) (gledger.Transaction, error) {
			decoded++
			if decoded == 2 {
				panic("cbor: bad header")
			}
			return gledger.NewTransactionFromCbor(txType, txCbor)
		},
	)
	require.ErrorIs(t, err, safedecode.ErrDecodePanic)
	require.ErrorContains(
		t,
		err,
		"txsubmission reply transaction 1 decode failed",
	)
	require.Nil(t, validated)
	require.Equal(t, 2, decoded)
}

// TestValidateTxsubmissionReplyNonPanickingFailuresUnchanged keeps the two
// outcomes that must not move: a valid reply still decodes, and a
// malformed-but-non-panicking body still produces the plain decode error it
// produced before, classified as an ordinary failure rather than a panic.
func TestValidateTxsubmissionReplyNonPanickingFailuresUnchanged(t *testing.T) {
	t.Parallel()

	requested, returned := txsubmissionPanicReply(t)

	t.Run("valid reply decodes", func(t *testing.T) {
		t.Parallel()
		validated, err := validateTxsubmissionReply(
			requested,
			returned,
			gledger.NewTransactionFromCbor,
		)
		require.NoError(t, err)
		require.Len(t, validated, len(returned))
	})

	t.Run("malformed body reports an ordinary decode error", func(t *testing.T) {
		t.Parallel()
		corrupt := []txsubmission.TxBody{{
			EraId:  returned[0].EraId,
			TxBody: []byte{0xff, 0xff, 0xff},
		}}
		want := []txsubmission.TxIdAndSize{{
			TxId: requested[0].TxId,
			Size: 3,
		}}
		validated, err := validateTxsubmissionReply(
			want,
			corrupt,
			gledger.NewTransactionFromCbor,
		)
		require.Error(t, err)
		require.ErrorContains(
			t,
			err,
			"txsubmission reply transaction 0 decode failed",
		)
		require.NotErrorIs(t, err, safedecode.ErrDecodePanic)
		require.Nil(t, validated)
	})
}

// txsubmissionRelayTestTxHex is a real, decodable Conway-era transaction.
// The server-init relay loop parses relayed bodies with
// ledger.NewTransactionFromCbor before admitting them to the mempool, so
// end-to-end relay tests need genuine CBOR rather than the placeholder
// bodies used by the callback-level tests above.
const txsubmissionRelayTestTxHex = "84a700818258200c07395aed88bdddc6de0518d1462dd0ec7e52e1e3a53599f7cdb24dc80237f8010181a20058390073a817bb425cbe179af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e3914f5bcca3a793011a02dc6c00021a001e84800b5820192d0c0c2c2320e843e080b5f91a9ca35155bc50f3ef3bfdbc72c1711b86367e0d818258203af629a5cd75f76d0cc21172e1193b85f199ca78e837c3965d77d7d6bc90206b0010a20058390073a817bb425cbe179af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e3914f5bcca3a793011a006acfc0111a002dc6c0a4008182582025fcacade3fffc096b53bdaf4c7d012bded303c9edbee686d24b372dae60aa1b58409da928a064ff9f795110bdcb8ab05d2a7a023dd15ebc42044f102ce366c0c9077024c7951c2d63584b7d2eea7bf1da4a7453bde4c99dd083889c1e2e2e3db804048119077a0581840000187b820a0a06814746010000222601f4f6"

const txsubmissionRelayTestTxWithValidityStartHex = "84a8081a02faf08000818258200c07395aed88bdddc6de0518d1462dd0ec7e52e1e3a53599f7cdb24dc80237f8010181a20058390073a817bb425cbe179af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e3914f5bcca3a793011a02dc6c00021a001e84800b5820192d0c0c2c2320e843e080b5f91a9ca35155bc50f3ef3bfdbc72c1711b86367e0d818258203af629a5cd75f76d0cc21172e1193b85f199ca78e837c3965d77d7d6bc90206b0010a20058390073a817bb425cbe179af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e3914f5bcca3a793011a006acfc0111a002dc6c0a4008182582025fcacade3fffc096b53bdaf4c7d012bded303c9edbee686d24b372dae60aa1b58409da928a064ff9f795110bdcb8ab05d2a7a023dd15ebc42044f102ce366c0c9077024c7951c2d63584b7d2eea7bf1da4a7453bde4c99dd083889c1e2e2e3db804048119077a0581840000187b820a0a06814746010000222601f4f6"

const txsubmissionRelayIssue1685TxHex = "84a500d901028282582004d97ebdeb064082639d67c8318ce069a35983bb05782d1327b004cca330ab5b008258204430e4bc2db0ef794c70b79851eecc332d8f77fb022c0d03ad24797f390ae54f000181825839005e7faca37d22d8753db699b104cbb2586f8787e17c116ff254ef0401e669129d1393c159b9b5a84d894271b5689910cc2e364ca05771988d1b0000000487a0103c021a0002d719031a0661906704d90102818a03581c7f4a5ac4b6a0f40cf07f989238d8e623315d80cc0602255b15c01eb3582025b400987b8e6d3f2d1913f7e7179611dc6563dc6731064de6b6dbe05114006e1b00000002540be4001a1908b100d81e82151901f4581de0e669129d1393c159b9b5a84d894271b5689910cc2e364ca05771988dd9010281581ce669129d1393c159b9b5a84d894271b5689910cc2e364ca05771988d818400190bb9444017f8d6f6827668747470733a2f2f6269742e6c792f34634e34374d31582086ed8edc5e20678c124d49dd1f6f6cb0b358797b71586f8a9db36bccf313f9eea100d9010283825820e61a0ef75ebcfba9569f2ef450d50320f376c36056f09f759d0e18ebf30a5ece5840c329a870e41de8e59b3ec872ec8d06f10e19c5dc436311e409827bf5792f86e75bb2c46785991563f42a03498c9c5342957efa15b348fffbd38f4fe64aef4f01825820942aaf02196ca16a79483b5862ff3d521e4c62c24dbc6aa495a360c101249de3584071ea7ed1740fbabe61f9c73f7306ef1ade9c2cf07a9d3c75d3ca130dd7e2078ea687cc326e7e790038580fdb3d9ec8e7e0edf70f5ff47527dd5ae0de6f5eca04825820eb2dbcf867f0611ca671a3ce89ae6c89a1a2eea96d6dcba82c607d4c9dbc489e5840f7e9a45d24cfbe8a7e7bc8200d84aa914cb51448873a41e0cf80aa641dd266490a0568b3039377fc5836d94320dc5c125f56352e0ad529f518035b4c2a313102f5f6"

const txsubmissionRelayTestEraId = 6 // Conway

const txsubmissionRelayTestNetworkMagic = 42

type txsubmissionTestValidator struct{}

func TestTxSubmissionRequestBatchFitsProtocolWindow(t *testing.T) {
	t.Parallel()

	require.LessOrEqual(
		t,
		txsubmissionRequestTxIdsCount,
		txsubmission.MaxUnackedTxIds,
		"Dingo's relay batch must fit the upstream outstanding-ID window",
	)
}

func (txsubmissionTestValidator) ValidateTx(gledger.Transaction) error {
	return nil
}

func (txsubmissionTestValidator) ValidateTxWithOverlay(
	gledger.Transaction,
	map[utxoref.Key]struct{},
	map[utxoref.Key]lcommon.Utxo,
) error {
	return nil
}

// txsubmissionSelectiveRejectingValidator rejects one transaction while
// allowing later offers from the same peer to exercise the relay pump.
type txsubmissionSelectiveRejectingValidator struct {
	rejectedHash string
}

func (v txsubmissionSelectiveRejectingValidator) ValidateTx(
	tx gledger.Transaction,
) error {
	if tx.Hash().String() == v.rejectedHash {
		return errors.New("txsubmissionSelectiveRejectingValidator: rejected")
	}
	return nil
}

func (v txsubmissionSelectiveRejectingValidator) ValidateTxWithOverlay(
	tx gledger.Transaction,
	_ map[utxoref.Key]struct{},
	_ map[utxoref.Key]lcommon.Utxo,
) error {
	return v.ValidateTx(tx)
}

type txsubmissionCorruptingConsumer struct {
	mempool.Consumer
	corruptHash string
	omitHash    string
	corruptAll  bool
}

func (c *txsubmissionCorruptingConsumer) GetTxFromCache(
	hash string,
) *mempool.MempoolTransaction {
	if hash == c.omitHash {
		return nil
	}
	tx := c.Consumer.GetTxFromCache(hash)
	if tx == nil || (!c.corruptAll && hash != c.corruptHash) {
		return tx
	}
	corrupted := *tx
	corrupted.Cbor = []byte{0xff}
	return &corrupted
}

type txsubmissionCorruptingService struct {
	mempool.Service
	corruptHash string
	omitHash    string
	corruptAll  bool
	mu          sync.Mutex
	consumers   map[ouroboros.ConnectionId]mempool.Consumer
}

// txsubmissionServiceWithoutHeadroom limits the wrapped service's method set
// to mempool.Service so the relay pump uses its batched-request path.
type txsubmissionServiceWithoutHeadroom struct {
	mempool.Service
}

func (s *txsubmissionCorruptingService) NewConsumer(
	connId ouroboros.ConnectionId,
) mempool.Consumer {
	consumer := s.Service.NewConsumer(connId)
	if consumer == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.consumers[connId]; existing != nil {
		return existing
	}
	wrapped := &txsubmissionCorruptingConsumer{
		Consumer:    consumer,
		corruptHash: s.corruptHash,
		omitHash:    s.omitHash,
		corruptAll:  s.corruptAll,
	}
	s.consumers[connId] = wrapped
	return wrapped
}

func (s *txsubmissionCorruptingService) FindConsumer(
	connId ouroboros.ConnectionId,
) mempool.Consumer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.consumers[connId]
}

func (s *txsubmissionCorruptingService) RemoveConsumer(
	connId ouroboros.ConnectionId,
) {
	s.mu.Lock()
	delete(s.consumers, connId)
	s.mu.Unlock()
	s.Service.RemoveConsumer(connId)
}

func TestRetryTxsubmissionAdmissionBoundsContention(t *testing.T) {
	t.Parallel()

	var addCalls int
	var waitCalls int
	var retryStreaks []int
	fullErr := &mempool.MempoolFullError{
		CurrentSize: 10,
		TxSize:      1,
		Capacity:    10,
	}

	err := retryTxsubmissionAdmission(
		func() error {
			addCalls++
			return fullErr
		},
		func() bool {
			waitCalls++
			return true
		},
		func(streak int) {
			retryStreaks = append(retryStreaks, streak)
		},
	)

	require.ErrorIs(t, err, errTxsubmissionAdmissionRetriesExhausted)
	require.ErrorAs(t, err, &fullErr)
	require.Equal(t, txsubmissionMaxAdmissionRetryStreak, addCalls)
	require.Equal(t, txsubmissionMaxAdmissionRetryStreak-1, waitCalls)
	require.Equal(t, []int{1, 2, 3}, retryStreaks)
}

func TestRetryTxsubmissionAdmissionSucceedsAfterContention(t *testing.T) {
	t.Parallel()

	var addCalls int
	var waitCalls int

	err := retryTxsubmissionAdmission(
		func() error {
			addCalls++
			if addCalls < txsubmissionMaxAdmissionRetryStreak {
				return &mempool.MempoolFullError{}
			}
			return nil
		},
		func() bool {
			waitCalls++
			return true
		},
		func(int) {},
	)

	require.NoError(t, err)
	require.Equal(t, txsubmissionMaxAdmissionRetryStreak, addCalls)
	require.Equal(t, txsubmissionMaxAdmissionRetryStreak-1, waitCalls)
}

// TestTxSubmissionClientRequestTxIds verifies empty, partial, and capped
// TxId responses when a peer asks what transactions this node can relay.
func TestTxSubmissionClientRequestTxIds(t *testing.T) {
	t.Parallel()

	fixtures := txsubmissionTestFixtures(t)
	tests := []struct {
		name      string
		txCount   int
		req       uint16
		wantCount int
	}{
		{
			name:      "empty response",
			req:       10,
			wantCount: 0,
		},
		{
			name:      "partial response",
			txCount:   2,
			req:       10,
			wantCount: 2,
		},
		{
			name:      "full response",
			txCount:   3,
			req:       2,
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange a peer consumer with the test's available tx set.
			o, connId := newTxSubmissionTestOuroboros(t)
			o.mempool.NewConsumer(connId)
			addTxSubmissionTestFixtures(t, o.mempool, fixtures[:tt.txCount]...)

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
				require.Equal(
					t,
					uint16(txsubmissionRelayTestEraId),
					id.TxId.EraId,
				)
				// The advertised size is the wrapped wire size, which is
				// what cardano-node advertises and what a peer reads off
				// the wire, not the unwrapped body length.
				require.Equal(
					t,
					uint32( // #nosec G115 -- test fixture
						len(
							txsubmissionWireEncodedItem(
								t,
								uint16(txsubmissionRelayTestEraId),
								fixtures[idx].body,
							),
						),
					),
					id.Size,
				)
				require.Equal(
					t,
					fixtures[idx].hash,
					hex.EncodeToString(id.TxId.TxId[:]),
				)
			}
		})
	}
}

// TestTxSubmissionClientRequestTxIdsClearsConsumerCacheOnAck verifies that
// peer acknowledgements discard previously advertised transaction bodies.
func TestTxSubmissionClientRequestTxIdsClearsConsumerCacheOnAck(t *testing.T) {
	t.Parallel()

	// Arrange one cached transaction for a peer consumer.
	fixture := txsubmissionTestFixtures(t)[0]
	o, connId := newTxSubmissionTestOuroboros(t)
	o.mempool.NewConsumer(connId)
	addTxSubmissionTestFixtures(t, o.mempool, fixture)
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
		fixture.txId,
	})
	require.NoError(t, err)
	require.Empty(t, bodies)
}

// TestTxSubmissionClientRequestTxIdsPartialAck verifies that acknowledging
// part of a batch of offered transactions discards only the acknowledged
// prefix, in the order the ids were offered, and preserves the bodies of ids
// the peer has not yet acknowledged so they remain available to retry.
// Regression test for
// the case where any nonzero ack
// cleared the entire offered cache and silently dropped unacknowledged
// bodies.
func TestTxSubmissionClientRequestTxIdsPartialAck(t *testing.T) {
	t.Parallel()

	// Arrange two cached transactions for a peer consumer.
	fixtures := txsubmissionTestFixtures(t)[:2]
	o, connId := newTxSubmissionTestOuroboros(t)
	o.mempool.NewConsumer(connId)
	addTxSubmissionTestFixtures(t, o.mempool, fixtures...)
	ctx := txsubmission.CallbackContext{ConnectionId: connId}

	// Advertise both transactions so both are stored in the consumer cache.
	ids, err := o.txsubmissionClientRequestTxIds(ctx, false, 0, 2)
	require.NoError(t, err)
	require.Len(t, ids, 2)

	// Acknowledge only the first (oldest) offered transaction; request no
	// additional ids in the same call to isolate the ack's effect.
	ids, err = o.txsubmissionClientRequestTxIds(ctx, false, 1, 0)
	require.NoError(t, err)
	require.Empty(t, ids)

	// The acknowledged transaction body can no longer be served.
	bodies, err := o.txsubmissionClientRequestTxs(ctx, []txsubmission.TxId{
		fixtures[0].txId,
	})
	require.NoError(t, err)
	require.Empty(t, bodies)

	// The unacknowledged transaction body is still cached and can be served.
	bodies, err = o.txsubmissionClientRequestTxs(ctx, []txsubmission.TxId{
		fixtures[1].txId,
	})
	require.NoError(t, err)
	require.Equal(t, []txsubmission.TxBody{
		{
			EraId:  txsubmissionRelayTestEraId,
			TxBody: fixtures[1].body,
		},
	}, bodies)
}

// TestTxSubmissionClientRequestTxIdsAckAcrossMixedBatches verifies that
// acknowledgements accumulate correctly across multiple RequestTxIds calls
// that mix acknowledging older offers with advertising new ones in the same
// call, matching the sliding FIFO window the TxSubmission protocol uses.
func TestTxSubmissionClientRequestTxIdsAckAcrossMixedBatches(t *testing.T) {
	t.Parallel()

	// Arrange three cached transactions for a peer consumer.
	fixtures := txsubmissionTestFixtures(t)
	o, connId := newTxSubmissionTestOuroboros(t)
	o.mempool.NewConsumer(connId)
	addTxSubmissionTestFixtures(t, o.mempool, fixtures...)
	ctx := txsubmission.CallbackContext{ConnectionId: connId}

	// Offer the first two transactions.
	ids, err := o.txsubmissionClientRequestTxIds(ctx, false, 0, 2)
	require.NoError(t, err)
	require.Len(t, ids, 2)

	// Acknowledge the first offer while requesting the third in the same
	// call, mirroring a peer that both confirms and asks for more at once.
	ids, err = o.txsubmissionClientRequestTxIds(ctx, false, 1, 1)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Equal(
		t,
		fixtures[2].hash,
		hex.EncodeToString(ids[0].TxId.TxId[:]),
	)

	// The first offer was acknowledged and its body is gone.
	bodies, err := o.txsubmissionClientRequestTxs(ctx, []txsubmission.TxId{
		fixtures[0].txId,
	})
	require.NoError(t, err)
	require.Empty(t, bodies)

	// The second and third offers are still unacknowledged and servable.
	bodies, err = o.txsubmissionClientRequestTxs(ctx, []txsubmission.TxId{
		fixtures[1].txId,
		fixtures[2].txId,
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []txsubmission.TxBody{
		{EraId: txsubmissionRelayTestEraId, TxBody: fixtures[1].body},
		{EraId: txsubmissionRelayTestEraId, TxBody: fixtures[2].body},
	}, bodies)
}

// TestTxSubmissionClientRequestTxs verifies that known cached TxIds return
// bodies while unknown or already-served TxIds are ignored.
func TestTxSubmissionClientRequestTxs(t *testing.T) {
	t.Parallel()

	// Arrange one known tx and one unknown tx id for the peer request.
	fixture := txsubmissionTestFixtures(t)[0]
	o, connId := newTxSubmissionTestOuroboros(t)
	o.mempool.NewConsumer(connId)
	unknownHash := txsubmissionTestHash(99)
	addTxSubmissionTestFixtures(t, o.mempool, fixture)
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
			EraId:  txsubmissionRelayTestEraId,
			TxBody: fixture.body,
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
	t.Parallel()

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

// TestTxSubmissionClientRequestTxsUnknownZeroTxId verifies malformed or
// impossible peer TxId requests return no bodies instead of panicking.
func TestTxSubmissionClientRequestTxsUnknownZeroTxId(t *testing.T) {
	t.Parallel()

	// Arrange a valid consumer without advertising any txs to its cache.
	o, connId := newTxSubmissionTestOuroboros(t)
	o.mempool.NewConsumer(connId)

	// Verify an all-zero TxId request is treated as a cache miss, not a panic.
	require.NotPanics(t, func() {
		bodies, err := o.txsubmissionClientRequestTxs(
			txsubmission.CallbackContext{ConnectionId: connId},
			[]txsubmission.TxId{{EraId: txsubmissionRelayTestEraId}},
		)
		require.NoError(t, err)
		require.Empty(t, bodies)
	})
}

// TestTxSubmissionClientRequestTxIdsZeroRequestDoesNotAdvance verifies a
// zero-count peer request leaves the consumer positioned on the next tx.
func TestTxSubmissionClientRequestTxIdsZeroRequestDoesNotAdvance(t *testing.T) {
	t.Parallel()

	// Arrange one available tx for the peer consumer.
	fixture := txsubmissionTestFixtures(t)[0]
	o, connId := newTxSubmissionTestOuroboros(t)
	o.mempool.NewConsumer(connId)
	addTxSubmissionTestFixtures(t, o.mempool, fixture)
	ctx := txsubmission.CallbackContext{ConnectionId: connId}

	// A zero-count request should return nothing.
	ids, err := o.txsubmissionClientRequestTxIds(ctx, false, 0, 0)
	require.NoError(t, err)
	require.Empty(t, ids)

	// A later nonzero request should still see the same first tx.
	ids, err = o.txsubmissionClientRequestTxIds(ctx, false, 0, 1)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Equal(t, fixture.hash, hex.EncodeToString(ids[0].TxId.TxId[:]))
}

// TestTxSubmissionServerInitMissingConnectionReturnsCleanly verifies server
// init exits without error when the connection is already gone.
func TestTxSubmissionServerInitMissingConnectionReturnsCleanly(t *testing.T) {
	t.Parallel()

	// Arrange an Ouroboros instance whose connection manager has no such peer.
	o, connId := newTxSubmissionTestOuroboros(t)

	// Start server init and let its background loop observe the missing peer.
	err := o.txsubmissionServerInit(
		txsubmission.CallbackContext{ConnectionId: connId},
	)

	// Missing connection during init should be treated as a clean exit.
	require.NoError(t, err)
}

func TestTxSubmissionClientStartMissingConnectionDoesNotAddConsumer(
	t *testing.T,
) {
	t.Parallel()

	o, connId := newTxSubmissionTestOuroboros(t)

	err := o.txsubmissionClientStart(connId)

	require.Error(t, err)
	require.ErrorContains(t, err, "failed to lookup connection ID")
	require.Nil(t, o.mempool.FindConsumer(connId))
}

func TestTxSubmissionClientStartIsIdempotent(t *testing.T) {
	t.Parallel()

	h := newTxSubmissionRelayHarness(t)
	defer h.close(t)
	connId := h.connB.Id()

	require.NoError(t, h.nodeB.txsubmissionClientStart(connId))
	first := h.mB.Consumer(connId)
	require.NotNil(t, first)

	require.NoError(t, h.nodeB.txsubmissionClientStart(connId))
	require.Same(t, first, h.mB.Consumer(connId))
}

// TestTxSubmissionConnectionClosedCleanup verifies connection close handling
// removes txsubmission consumer and rate-limiter state for that peer.
func TestTxSubmissionConnectionClosedCleanup(t *testing.T) {
	t.Parallel()

	// Arrange per-peer mempool and rate-limiter state.
	o, connId := newTxSubmissionTestOuroboros(t)
	o.mempool.NewConsumer(connId)
	o.txSubmissionRateLimiter = newTxSubmissionRateLimiter(1, 1)
	require.True(t, o.txSubmissionRateLimiter.Allow(connId, 1))
	require.False(t, o.txSubmissionRateLimiter.Allow(connId, 1))

	// Deliver the same connection-close event used by normal node wiring.
	o.HandleConnClosedEvent(event.Event{
		Type: connmanager.ConnectionClosedEventType,
		Data: connmanager.ConnectionClosedEvent{
			ConnectionId: connId,
		},
	})

	// Verify both txsubmission state holders have forgotten the peer.
	require.Nil(t, o.mempool.FindConsumer(connId))
	require.True(t, o.txSubmissionRateLimiter.Allow(connId, 1))
}

// newTxSubmissionTestOuroboros builds a lightweight Ouroboros/mempool pair
// for exercising the client-side TxSubmission callbacks directly. Optional
// mutateConfig funcs can override the default MempoolConfig, e.g. to set a
// short TTL for testing expiry behavior.
func newTxSubmissionTestOuroboros(
	t *testing.T,
	mutateConfig ...func(*mempool.MempoolConfig),
) (*Ouroboros, ouroboros.ConnectionId) {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	cfg := mempool.MempoolConfig{
		Logger:          logger,
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       txsubmissionTestValidator{},
		MempoolCapacity: 1024 * 1024,
	}
	for _, mutate := range mutateConfig {
		mutate(&cfg)
	}
	m, err := mempool.NewMempool(cfg)
	require.NoError(t, err)
	require.NoError(t, m.Start(t.Context()))
	t.Cleanup(func() {
		// context.Background, not t.Context: the latter is already
		// cancelled by the time Cleanup funcs run, which used to be masked
		// by Stop's ctx-deadline path always returning nil regardless of
		// whether workers actually drained.
		require.NoError(t, m.Stop(context.Background()))
	})

	o := newOuroboros(OuroborosConfig{Logger: logger})
	o.connManager = connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			Logger: logger,
		},
	)
	o.mempool = &mempool.FIFO{Mempool: m}
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

func txsubmissionTestHash(idx int) string {
	return fmt.Sprintf("%064x", idx)
}

type txsubmissionTestFixture struct {
	hash string
	body []byte
	txId txsubmission.TxId
}

func txsubmissionTestFixtures(t *testing.T) []txsubmissionTestFixture {
	t.Helper()
	hexFixtures := []string{
		txsubmissionRelayTestTxHex,
		txsubmissionRelayTestTxWithValidityStartHex,
		txsubmissionRelayIssue1685TxHex,
	}
	ret := make([]txsubmissionTestFixture, 0, len(hexFixtures))
	for _, txHex := range hexFixtures {
		txBytes, err := hex.DecodeString(txHex)
		require.NoError(t, err)
		tx, err := gledger.NewTransactionFromCbor(
			txsubmissionRelayTestEraId,
			txBytes,
		)
		require.NoError(t, err)
		txHash := tx.Hash().String()
		ret = append(ret, txsubmissionTestFixture{
			hash: txHash,
			body: txBytes,
			txId: mustTxSubmissionTestTxId(t, txHash),
		})
	}
	return ret
}

func TestValidateTxsubmissionReply(t *testing.T) {
	t.Parallel()

	fixtures := txsubmissionTestFixtures(t)[:2]
	requested := make([]txsubmission.TxIdAndSize, 0, len(fixtures))
	returned := make([]txsubmission.TxBody, 0, len(fixtures))
	for _, fixture := range fixtures {
		requested = append(requested, txsubmission.TxIdAndSize{
			TxId: fixture.txId,
			Size: uint32(len(fixture.body)), // #nosec G115 -- test fixture
		})
		returned = append(returned, txsubmission.TxBody{
			EraId:  fixture.txId.EraId,
			TxBody: fixture.body,
		})
	}

	t.Run("matching batch", func(t *testing.T) {
		validated, err := validateTxsubmissionReply(requested, returned, gledger.NewTransactionFromCbor)
		require.NoError(t, err)
		require.Len(t, validated, len(returned))
	})
	t.Run("ordered subset", func(t *testing.T) {
		validated, err := validateTxsubmissionReply(requested, returned[1:], gledger.NewTransactionFromCbor)
		require.NoError(t, err)
		require.Len(t, validated, 1)
		require.Equal(t, returned[1], validated[0].body)
	})
	t.Run("reference size discrepancy boundaries", func(t *testing.T) {
		bodySize := uint64(len(returned[0].TxBody))
		wireSize := txsubmissionWireSize(returned[0].EraId, len(returned[0].TxBody))
		tests := []struct {
			name       string
			advertised uint64
			shouldPass bool
		}{
			{name: "raw minus 32", advertised: bodySize - 32, shouldPass: true},
			{name: "raw minus 33", advertised: bodySize - 33, shouldPass: false},
			{name: "raw plus 32", advertised: bodySize + 32, shouldPass: true},
			// The raw +33 case remains accepted through the wrapped-wire
			// representation; its wire discrepancy is only +27 here.
			{name: "raw plus 40", advertised: bodySize + 40, shouldPass: false},
			{name: "wire minus 32", advertised: wireSize - 32, shouldPass: true},
			{name: "wire minus 40", advertised: wireSize - 40, shouldPass: false},
			{name: "wire plus 32", advertised: wireSize + 32, shouldPass: true},
			{name: "wire plus 33", advertised: wireSize + 33, shouldPass: false},
		}
		for _, testCase := range tests {
			t.Run(testCase.name, func(t *testing.T) {
				want := []txsubmission.TxIdAndSize{{
					TxId: fixtures[0].txId,
					Size: uint32(testCase.advertised), // #nosec G115 -- bounded fixture
				}}
				validated, err := validateTxsubmissionReply(want, returned[:1], gledger.NewTransactionFromCbor)
				if testCase.shouldPass {
					require.NoError(t, err)
					require.Len(t, validated, 1)
				} else {
					require.ErrorIs(t, err, errTxsubmissionReplySizeMismatch)
					require.Nil(t, validated)
				}
			})
		}
	})
	t.Run("multi-body discrepancy budget and omission", func(t *testing.T) {
		want := make([]txsubmission.TxIdAndSize, len(fixtures))
		for i, fixture := range fixtures {
			want[i] = txsubmission.TxIdAndSize{
				TxId: fixture.txId,
				Size: uint32(len(fixture.body) - 32), // #nosec G115 -- real fixtures
			}
		}
		validated, err := validateTxsubmissionReply(want, returned, gledger.NewTransactionFromCbor)
		require.NoError(t, err)
		require.Len(t, validated, len(returned))

		validated, err = validateTxsubmissionReply(want, returned[:1], gledger.NewTransactionFromCbor)
		require.NoError(t, err)
		require.Len(t, validated, 1)
	})
	t.Run("under-advertisement uses per-body tolerance", func(t *testing.T) {
		want := []txsubmission.TxIdAndSize{
			// Keep the aggregate request budget within tolerance while making
			// only the first body's under-advertisement invalid.
			{TxId: fixtures[0].txId, Size: uint32(len(fixtures[0].body) - 60)}, // #nosec G115 -- real fixture
			{TxId: fixtures[1].txId, Size: uint32(len(fixtures[1].body) - 4)},  // #nosec G115 -- real fixture
		}
		validated, err := validateTxsubmissionReply(want, returned, gledger.NewTransactionFromCbor)
		require.ErrorIs(t, err, errTxsubmissionReplySizeMismatch)
		require.Nil(t, validated)
	})
	t.Run("reordered reply preserves admission order", func(t *testing.T) {
		got := []txsubmission.TxBody{returned[1], returned[0]}
		validated, err := validateTxsubmissionReply(requested, got, gledger.NewTransactionFromCbor)
		require.NoError(t, err)
		require.Len(t, validated, 2)
		require.Equal(t, returned[0], validated[0].body)
		require.Equal(t, returned[1], validated[1].body)
		require.Equal(t, returned[1], got[0], "do not mutate the reply")
	})

	tests := []struct {
		name   string
		mutate func([]txsubmission.TxIdAndSize, []txsubmission.TxBody)
		match  string
	}{
		{
			name:  "count",
			match: "count exceeds request",
		},
		{
			name: "duplicate body",
			mutate: func(_ []txsubmission.TxIdAndSize, got []txsubmission.TxBody) {
				if len(got[0].TxBody) <= len(got[1].TxBody) {
					got[1] = got[0]
				} else {
					got[0] = got[1]
				}
			},
			match: "duplicate transaction",
		},
		{
			name: "era",
			mutate: func(want []txsubmission.TxIdAndSize, _ []txsubmission.TxBody) {
				want[0].TxId.EraId++
			},
			match: "era mismatch",
		},
		{
			name: "size",
			mutate: func(want []txsubmission.TxIdAndSize, _ []txsubmission.TxBody) {
				want[0].Size += 40
				want[1].Size--
			},
			match: "size mismatch",
		},
		{
			name: "hash",
			mutate: func(want []txsubmission.TxIdAndSize, _ []txsubmission.TxBody) {
				want[0].TxId.TxId[0] ^= 0xff
			},
			match: "hash mismatch",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := slices.Clone(requested)
			got := slices.Clone(returned)
			if tt.name == "count" {
				got = append(got, returned[0])
			}
			if tt.mutate != nil {
				tt.mutate(want, got)
			}
			validated, err := validateTxsubmissionReply(want, got, gledger.NewTransactionFromCbor)
			require.ErrorContains(t, err, tt.match)
			require.Nil(t, validated)
			if tt.name == "hash" {
				require.Contains(t, err.Error(), "received "+fixtures[0].hash)
			}
		})
	}
}

func TestValidateTxsubmissionReplyChecksByteBudgetBeforeDecode(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name      string
		sizes     []uint32
		bodies    [][]byte
		overLimit bool
	}{
		{
			name:      "single body exceeds budget",
			sizes:     []uint32{1},
			bodies:    [][]byte{make([]byte, 34)},
			overLimit: true,
		},
		{
			name:      "later body exceeds aggregate before first decode",
			sizes:     []uint32{1, 1},
			bodies:    [][]byte{{0xff}, make([]byte, 66)},
			overLimit: true,
		},
		{
			name:      "zero budget",
			sizes:     []uint32{0},
			bodies:    [][]byte{make([]byte, 33)},
			overLimit: true,
		},
		{
			name:   "exact budget still decodes",
			sizes:  []uint32{1},
			bodies: [][]byte{{0xff}},
		},
		{
			name:   "below budget still decodes",
			sizes:  []uint32{2},
			bodies: [][]byte{{0xff}},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			requested := make([]txsubmission.TxIdAndSize, len(testCase.sizes))
			returned := make([]txsubmission.TxBody, len(testCase.bodies))
			for index, size := range testCase.sizes {
				requested[index] = txsubmission.TxIdAndSize{
					TxId: txsubmission.TxId{EraId: txsubmissionRelayTestEraId},
					Size: size,
				}
				returned[index] = txsubmission.TxBody{
					EraId:  txsubmissionRelayTestEraId,
					TxBody: testCase.bodies[index],
				}
			}
			validated, err := validateTxsubmissionReply(requested, returned, gledger.NewTransactionFromCbor)
			require.Nil(t, validated)
			if testCase.overLimit {
				require.ErrorIs(t, err, errTxsubmissionReplySizeMismatch)
				require.ErrorContains(t, err, "exceeds byte limit")
			} else {
				require.ErrorContains(t, err, "decode failed")
				require.NotErrorIs(t, err, errTxsubmissionReplySizeMismatch)
			}
		})
	}
}

func addTxSubmissionTestFixtures(
	t *testing.T,
	m mempool.Service,
	fixtures ...txsubmissionTestFixture,
) {
	t.Helper()
	for _, fixture := range fixtures {
		require.NoError(
			t,
			m.AddTransaction(context.Background(), txsubmissionRelayTestEraId, fixture.body),
		)
	}
}

func mustTxSubmissionTestTxId(t *testing.T, hash string) txsubmission.TxId {
	t.Helper()
	bytes, err := hex.DecodeString(hash)
	require.NoError(t, err)
	require.Len(t, bytes, 32)
	var txId [32]byte
	copy(txId[:], bytes)
	return txsubmission.TxId{
		EraId: uint16(txsubmissionRelayTestEraId),
		TxId:  txId,
	}
}

// txSubmissionRelayHarness wires two real Ouroboros nodes together over a
// net.Pipe with the full NtN handshake and TxSubmission mini-protocol, so
// txsubmissionServerInit's background goroutine runs for real: node A's
// TxSubmission server pulls TxIds/Txs from node B's TxSubmission client and
// decodes/admits them into node A's own mempool. This exercises the relay
// loop itself, which the callback-level tests above cannot reach since
// ctx.Server is a concrete network-backed type.
type txSubmissionRelayHarness struct {
	nodeA *Ouroboros
	nodeB *Ouroboros
	connA *ouroboros.Connection
	connB *ouroboros.Connection
	cmA   *connmanager.ConnectionManager
	cmB   *connmanager.ConnectionManager
	mA    *mempool.Mempool
	mB    *mempool.Mempool
	busA  *event.EventBus
	busB  *event.EventBus
}

// newTxSubmissionRelayHarness intentionally does not register any
// t.Cleanup teardown: callers must close the harness themselves so tests
// that compare goroutine counts around the harness's lifetime observe a
// deterministic teardown point rather than one deferred until after the
// test function returns.
func newTxSubmissionRelayHarness(t *testing.T) *txSubmissionRelayHarness {
	return newTxSubmissionRelayHarnessWithOpts(
		t,
		txSubmissionRelayHarnessOpts{},
	)
}

// txSubmissionRelayHarnessOpts overrides the harness's defaults. Every
// field is optional; the zero value reproduces newTxSubmissionRelayHarness's
// original behavior (a shared discard logger and permissive validators on
// both nodes).
type txSubmissionRelayHarnessOpts struct {
	logger           *slog.Logger
	validatorA       mempool.TxValidator
	validatorB       mempool.TxValidator
	capacityA        int64
	dagA             bool
	corruptOfferHash string
	omitOfferHash    string
	corruptAllOffers bool
	batchRequestsA   bool
	// advertiseSizeDelta modifies the peer's TxId size announcements while
	// leaving the real body and relay path untouched.
	advertiseSizeDelta int64
	// transformReplyB runs after the real callback consumes requested cache
	// entries, so a wire-order change cannot alter cache lookup semantics.
	transformReplyB func([]txsubmission.TxBody)
	// promRegistryA installs protocol metrics on node A, whose
	// txsubmission server runs the relay pull loop under test.
	promRegistryA prometheus.Registerer
}

func newTxSubmissionRelayHarnessWithOpts(
	t *testing.T,
	opts txSubmissionRelayHarnessOpts,
) *txSubmissionRelayHarness {
	t.Helper()
	logger := opts.logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	validatorA := opts.validatorA
	if validatorA == nil {
		validatorA = txsubmissionTestValidator{}
	}
	validatorB := opts.validatorB
	if validatorB == nil {
		validatorB = txsubmissionTestValidator{}
	}

	capacityA := opts.capacityA
	if capacityA == 0 {
		capacityA = 1024 * 1024
	}
	// Without an EventBus a blocking NextTx returns at once, so node B would
	// answer a blocking RequestTxIds with no ids, a protocol violation.
	busA := event.NewEventBus(nil, logger)
	busB := event.NewEventBus(nil, logger)
	configA := mempool.MempoolConfig{
		Logger:          logger,
		EventBus:        busA,
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       validatorA,
		MempoolCapacity: capacityA,
	}
	var (
		mA           *mempool.Mempool
		nodeAMempool mempool.Service
		err          error
	)
	if opts.dagA {
		dag, dagErr := mempool.NewDAG(configA)
		require.NoError(t, dagErr)
		mA, nodeAMempool = dag.Mempool, dag
	} else {
		mA, err = mempool.NewMempool(configA)
		require.NoError(t, err)
		nodeAMempool = &mempool.FIFO{Mempool: mA}
	}
	if opts.batchRequestsA {
		nodeAMempool = &txsubmissionServiceWithoutHeadroom{
			Service: nodeAMempool,
		}
	}
	mB, err := mempool.NewMempool(mempool.MempoolConfig{
		Logger:          logger,
		EventBus:        busB,
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       validatorB,
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)

	cmA := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{Logger: logger},
	)
	cmB := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{Logger: logger},
	)

	nodeA := newOuroboros(OuroborosConfig{
		ConnManager:  cmA,
		Logger:       logger,
		PromRegistry: opts.promRegistryA,
	})
	nodeA.mempool = nodeAMempool
	nodeB := newOuroboros(OuroborosConfig{ConnManager: cmB, Logger: logger})
	nodeBMempool := mempool.Service(&mempool.FIFO{Mempool: mB})
	if opts.corruptOfferHash != "" || opts.omitOfferHash != "" ||
		opts.corruptAllOffers {
		nodeBMempool = &txsubmissionCorruptingService{
			Service:     nodeBMempool,
			corruptHash: opts.corruptOfferHash,
			omitHash:    opts.omitOfferHash,
			corruptAll:  opts.corruptAllOffers,
			consumers: make(
				map[ouroboros.ConnectionId]mempool.Consumer,
			),
		}
	}
	nodeB.mempool = nodeBMempool
	nodeBClientOpts := nodeB.txsubmissionClientConnOpts()
	if opts.advertiseSizeDelta != 0 {
		nodeBClientOpts = append(
			nodeBClientOpts,
			txsubmission.WithRequestTxIdsFunc(
				nodeB.instrumentTxsubmissionRequestTxIds(
					func(
						ctx txsubmission.CallbackContext,
						blocking bool,
						ack, req uint16,
					) ([]txsubmission.TxIdAndSize, error) {
						ids, err := nodeB.txsubmissionClientRequestTxIds(
							ctx,
							blocking,
							ack,
							req,
						)
						if err != nil {
							return nil, err
						}
						for index := range ids {
							adjusted := int64(ids[index].Size) + opts.advertiseSizeDelta
							if adjusted < 0 || adjusted > int64(^uint32(0)) {
								return nil, fmt.Errorf("test advertised size out of range: %d", adjusted)
							}
							ids[index].Size = uint32(adjusted)
						}
						return ids, nil
					},
				),
			),
		)
	}
	if opts.transformReplyB != nil {
		nodeBClientOpts = append(
			nodeBClientOpts,
			txsubmission.WithRequestTxsFunc(
				nodeB.instrumentTxsubmissionRequestTxs(func(
					ctx txsubmission.CallbackContext,
					ids []txsubmission.TxId,
				) ([]txsubmission.TxBody, error) {
					bodies, err := nodeB.txsubmissionClientRequestTxs(ctx, ids)
					if err == nil {
						opts.transformReplyB(bodies)
					}
					return bodies, err
				}),
			),
		)
	}

	serverPipe, clientPipe := net.Pipe()

	connACh := make(chan *ouroboros.Connection, 1)
	errACh := make(chan error, 1)
	go func() {
		conn, err := ouroboros.New(
			ouroboros.WithConnection(serverPipe),
			ouroboros.WithServer(true),
			ouroboros.WithNetworkMagic(txsubmissionRelayTestNetworkMagic),
			ouroboros.WithNodeToNode(true),
			ouroboros.WithFullDuplex(true),
			ouroboros.WithLogger(logger),
			ouroboros.WithTxSubmissionConfig(
				txsubmission.NewConfig(
					slices.Concat(
						nodeA.txsubmissionClientConnOpts(),
						nodeA.txsubmissionServerConnOpts(),
					)...,
				),
			),
		)
		if err != nil {
			errACh <- err
			return
		}
		connACh <- conn
	}()

	connB, err := ouroboros.New(
		ouroboros.WithConnection(clientPipe),
		ouroboros.WithNetworkMagic(txsubmissionRelayTestNetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithFullDuplex(true),
		ouroboros.WithLogger(logger),
		ouroboros.WithTxSubmissionConfig(
			txsubmission.NewConfig(
				slices.Concat(
					nodeBClientOpts,
					nodeB.txsubmissionServerConnOpts(),
				)...,
			),
		),
	)
	require.NoError(t, err)

	var connA *ouroboros.Connection
	select {
	case err := <-errACh:
		t.Fatalf("node A connection setup failed: %s", err)
	case connA = <-connACh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for node A connection setup")
	}

	require.True(
		t,
		cmA.AddConnection(connA, false, connA.Id().RemoteAddr.String()),
	)
	require.True(
		t,
		cmB.AddConnection(connB, true, connB.Id().RemoteAddr.String()),
	)

	return &txSubmissionRelayHarness{
		nodeA: nodeA,
		nodeB: nodeB,
		connA: connA,
		connB: connB,
		cmA:   cmA,
		cmB:   cmB,
		mA:    mA,
		mB:    mB,
		busA:  busA,
		busB:  busB,
	}
}

// close tears down both connections and their owning nodes synchronously,
// so callers can reliably observe goroutine counts settling afterward.
func (h *txSubmissionRelayHarness) close(t *testing.T) {
	t.Helper()
	_ = h.connA.Close()
	_ = h.connB.Close()
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = h.cmA.Stop(stopCtx)
	_ = h.cmB.Stop(stopCtx)
	_ = h.mA.Stop(context.Background())
	_ = h.mB.Stop(context.Background())
	h.busA.Close()
	h.busB.Close()
}

// requireSessionEnds waits for node A to drop the connection. gouroboros
// validates every reply body against the request and advertised sizes on
// both ends, so a mismatched reply is a protocol violation that ends the
// session rather than a reply for the relay loop to discard.
func (h *txSubmissionRelayHarness) requireSessionEnds(t *testing.T) {
	t.Helper()
	require.Eventually(
		t,
		func() bool {
			return h.cmA.GetConnectionById(h.connA.Id()) == nil
		},
		5*time.Second,
		10*time.Millisecond,
		"expected the mismatched reply to end the session",
	)
}

// TestTxSubmissionServerInitRelaysMempoolTransactionEndToEnd drives the real
// txsubmissionServerInit goroutine over an actual TxSubmission session: node
// B offers a real transaction from its mempool, node A's server pulls the
// TxIds then the TxBody, decodes the CBOR, and admits it to its own
// mempool. This is the happy-path relay loop that the direct callback tests
// cannot reach.
func TestTxSubmissionServerInitRelaysMempoolTransactionEndToEnd(t *testing.T) {
	t.Parallel()

	h := newTxSubmissionRelayHarness(t)
	defer h.close(t)

	txBytes, err := hex.DecodeString(txsubmissionRelayTestTxHex)
	require.NoError(t, err)
	require.NoError(t, h.mB.AddTransaction(context.Background(), txsubmissionRelayTestEraId, txBytes))
	wantTx, err := gledger.NewTransactionFromCbor(
		txsubmissionRelayTestEraId,
		txBytes,
	)
	require.NoError(t, err)

	// Mirrors txsubmissionClientStart's role in the real outbound-connection
	// flow: register a mempool consumer for the peer and tell it to start
	// asking us for our mempool contents, which triggers node A's Init
	// callback (txsubmissionServerInit) on the other end of the wire.
	require.NoError(t, h.nodeB.txsubmissionClientStart(h.connB.Id()))

	require.Eventually(
		t,
		func() bool {
			return len(h.mA.Transactions()) == 1
		},
		5*time.Second,
		10*time.Millisecond,
		"expected node B's transaction to be relayed into node A's mempool",
	)

	relayed := h.mA.Transactions()[0]
	require.Equal(t, wantTx.Hash().String(), relayed.Hash)
	require.Equal(t, txBytes, relayed.Cbor)
}

// The real TxSubmission client advertises a size 32 bytes below the wrapped
// wire size. The body is unchanged, so this reaches the production relay
// callback and verifies the reference V2 discrepancy is admitted end to end.
func TestTxSubmissionServerInitAcceptsReferenceSizeDiscrepancy(t *testing.T) {
	t.Parallel()

	h := newTxSubmissionRelayHarnessWithOpts(t, txSubmissionRelayHarnessOpts{
		batchRequestsA:     true,
		advertiseSizeDelta: -32,
	})
	defer h.close(t)
	fixture := txsubmissionTestFixtures(t)[0]
	addTxSubmissionTestFixtures(t, h.mB, fixture)
	require.NoError(t, h.nodeB.txsubmissionClientStart(h.connB.Id()))

	require.Eventually(
		t,
		func() bool { return len(h.mA.Transactions()) == 1 },
		5*time.Second,
		10*time.Millisecond,
		"expected the reference-tolerated transaction to be relayed",
	)
}

func TestTxSubmissionServerInitRejectsOutOfRangeAdvertisedSize(t *testing.T) {
	t.Parallel()

	h := newTxSubmissionRelayHarnessWithOpts(t, txSubmissionRelayHarnessOpts{
		advertiseSizeDelta: 40,
	})
	defer h.close(t)
	fixture := txsubmissionTestFixtures(t)[0]
	addTxSubmissionTestFixtures(t, h.mB, fixture)
	require.NoError(t, h.nodeB.txsubmissionClientStart(h.connB.Id()))

	h.requireSessionEnds(t)
	_, admitted := h.mA.GetTransaction(fixture.hash)
	require.False(t, admitted, "out-of-range advertised body was admitted")
}

func TestTxSubmissionDAGBackpressureResumesAfterRemoval(t *testing.T) {
	t.Parallel()

	fixtures := txsubmissionTestFixtures(t)
	seed := fixtures[2]
	offered := fixtures[0]
	capacity := int64(len(seed.body))
	for int64(len(seed.body)) >
		int64(float64(capacity)*mempool.DefaultRejectionWatermark) {
		capacity++
	}
	h := newTxSubmissionRelayHarnessWithOpts(t, txSubmissionRelayHarnessOpts{
		capacityA: capacity,
		dagA:      true,
	})
	defer h.close(t)

	require.NoError(
		t,
		h.mA.AddTransaction(context.Background(), txsubmissionRelayTestEraId, seed.body),
	)
	require.NoError(
		t,
		h.mB.AddTransaction(context.Background(), txsubmissionRelayTestEraId, offered.body),
	)
	require.NoError(t, h.nodeB.txsubmissionClientStart(h.connB.Id()))

	require.Never(t, func() bool {
		_, ok := h.mA.GetTransaction(offered.hash)
		return ok
	}, 200*time.Millisecond, 10*time.Millisecond)

	h.mA.RemoveTxsByHash([]string{seed.hash})
	require.Eventually(t, func() bool {
		_, ok := h.mA.GetTransaction(offered.hash)
		return ok
	}, 5*time.Second, 10*time.Millisecond)
}

// TestTxSubmissionServerInitExitsCleanlyOnPeerDisconnect verifies the
// server-init relay goroutine does not leak when the peer connection closes
// while it is parked in a blocking RequestTxIds call. The mempool is seeded
// with exactly one transaction so the loop completes one real round trip
// (proving the goroutine actually reached the blocking call again) before
// the connection is torn down.
//
// Goroutine counts are compared against a baseline captured before the
// harness is built, rather than using goleak, since goleak inspects the
// whole process and would also trip on unrelated pre-existing leaks
// elsewhere in this package's test suite.
// Not t.Parallel: runtime.NumGoroutine is a process-wide measurement that
// concurrent tests perturb.
func TestTxSubmissionServerInitExitsCleanlyOnPeerDisconnect(t *testing.T) {
	baseline := runtime.NumGoroutine()

	h := newTxSubmissionRelayHarness(t)

	txBytes, err := hex.DecodeString(txsubmissionRelayTestTxHex)
	require.NoError(t, err)
	require.NoError(t, h.mB.AddTransaction(context.Background(), txsubmissionRelayTestEraId, txBytes))

	require.NoError(t, h.nodeB.txsubmissionClientStart(h.connB.Id()))

	require.Eventually(
		t,
		func() bool {
			return len(h.mA.Transactions()) == 1
		},
		5*time.Second,
		10*time.Millisecond,
		"expected node B's transaction to be relayed before disconnect",
	)

	// Node B's mempool is now empty, so node A's relay goroutine is parked
	// in a blocking RequestTxIds call awaiting the next offer. Closing here
	// must unblock and exit that goroutine, along with every other
	// goroutine the harness spawned, rather than leaking any of them.
	h.close(t)

	require.Eventually(
		t,
		func() bool {
			return runtime.NumGoroutine() <= baseline+2
		},
		5*time.Second,
		20*time.Millisecond,
		"expected relay and connection goroutines to exit after peer disconnect",
	)
}

// TestTxSubmissionClientRequestTxsExpiredTransactionNotServed verifies that
// a transaction the mempool's own TTL has already expired -- before this
// peer ever advertised it via RequestTxIds -- is handled the same as an
// unknown TxId: an empty reply, not an error. The consumer cache only ever
// learns about a transaction when it is advertised, so a TxId that expired
// from the mempool beforehand must fall straight through to "not found"
// rather than erroring or panicking.
func TestTxSubmissionClientRequestTxsExpiredTransactionNotServed(t *testing.T) {
	t.Parallel()

	o, connId := newTxSubmissionTestOuroboros(
		t,
		func(cfg *mempool.MempoolConfig) {
			cfg.TransactionTTL = 10 * time.Millisecond
			cfg.CleanupInterval = 10 * time.Millisecond
		},
	)
	o.mempool.NewConsumer(connId)

	txBytes, err := hex.DecodeString(txsubmissionRelayTestTxHex)
	require.NoError(t, err)
	require.NoError(
		t,
		o.mempool.AddTransaction(context.Background(), txsubmissionRelayTestEraId, txBytes),
	)
	wantTx, err := gledger.NewTransactionFromCbor(
		txsubmissionRelayTestEraId,
		txBytes,
	)
	require.NoError(t, err)

	// Wait for the mempool's own TTL sweep to remove the transaction. It is
	// never requested via RequestTxIds first, so the consumer cache never
	// learns about it either -- exactly the "expired before offer" case.
	require.Eventually(
		t,
		func() bool {
			return len(o.mempool.Transactions()) == 0
		},
		5*time.Second,
		10*time.Millisecond,
		"expected transaction to expire from the mempool",
	)

	bodies, err := o.txsubmissionClientRequestTxs(
		txsubmission.CallbackContext{ConnectionId: connId},
		[]txsubmission.TxId{
			mustTxSubmissionTestTxId(t, wantTx.Hash().String()),
		},
	)
	require.NoError(t, err)
	require.Empty(t, bodies)
}

// TestTxSubmissionServerInitContinuesAfterMempoolRejection verifies a rejected
// transaction does not stop later offers on the same connection.
func TestTxSubmissionServerInitContinuesAfterMempoolRejection(
	t *testing.T,
) {
	t.Parallel()

	fixtures := txsubmissionTestFixtures(t)
	rejected := fixtures[0]
	accepted := fixtures[1]
	logBuf := &lockedBuffer{}
	logger := slog.New(
		slog.NewJSONHandler(
			logBuf,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)

	h := newTxSubmissionRelayHarnessWithOpts(t, txSubmissionRelayHarnessOpts{
		logger: logger,
		validatorA: txsubmissionSelectiveRejectingValidator{
			rejectedHash: rejected.hash,
		},
	})
	defer h.close(t)

	addTxSubmissionTestFixtures(t, h.mB, rejected)

	require.NoError(t, h.nodeB.txsubmissionClientStart(h.connB.Id()))

	require.Eventually(
		t,
		func() bool {
			return strings.Contains(logBuf.String(), "failed to add tx")
		},
		5*time.Second,
		10*time.Millisecond,
		"expected the mempool rejection to be logged",
	)
	require.Contains(t, logBuf.String(), rejected.hash)
	require.Contains(t, logBuf.String(), h.connA.Id().String())
	_, rejectedPresent := h.mA.GetTransaction(rejected.hash)
	require.False(t, rejectedPresent)

	// Offer the valid transaction only after the rejected item completed its
	// own round trip. This proves the same per-peer pump requests another batch
	// instead of merely processing a later item from the first batch.
	addTxSubmissionTestFixtures(t, h.mB, accepted)

	require.Eventually(
		t,
		func() bool {
			_, ok := h.mA.GetTransaction(accepted.hash)
			return ok
		},
		5*time.Second,
		10*time.Millisecond,
		"expected a valid transaction after a rejection to be processed on the same connection",
	)
}

// TestTxSubmissionServerInitRejectsMalformedReply verifies a reply body that
// does not match the requested id is never admitted.
func TestTxSubmissionServerInitRejectsMalformedReply(t *testing.T) {
	t.Parallel()

	malformed := txsubmissionTestFixtures(t)[0]
	h := newTxSubmissionRelayHarnessWithOpts(t, txSubmissionRelayHarnessOpts{
		corruptOfferHash: malformed.hash,
	})
	defer h.close(t)

	addTxSubmissionTestFixtures(t, h.mB, malformed)
	require.NoError(t, h.nodeB.txsubmissionClientStart(h.connB.Id()))

	h.requireSessionEnds(t)
	_, admitted := h.mA.GetTransaction(malformed.hash)
	require.False(t, admitted, "mismatched body was admitted")
}

// TestTxSubmissionServerInitRejectsBatchAtomically verifies a valid prefix is
// not admitted when a later body in the same reply is mismatched.
func TestTxSubmissionServerInitRejectsBatchAtomically(
	t *testing.T,
) {
	t.Parallel()

	fixtures := txsubmissionTestFixtures(t)
	omitted := fixtures[0]
	malformed := fixtures[1]
	h := newTxSubmissionRelayHarnessWithOpts(t, txSubmissionRelayHarnessOpts{
		corruptOfferHash: malformed.hash,
		batchRequestsA:   true,
	})
	defer h.close(t)

	addTxSubmissionTestFixtures(t, h.mB, omitted, malformed)
	require.NoError(t, h.nodeB.txsubmissionClientStart(h.connB.Id()))

	h.requireSessionEnds(t)
	_, admitted := h.mA.GetTransaction(omitted.hash)
	require.False(t, admitted, "valid prefix was partially admitted")
}

// TestTxSubmissionServerInitAcceptsOrderedSubset verifies a peer may omit a
// transaction that disappeared after advertisement while still returning a
// later requested transaction.
func TestTxSubmissionServerInitAcceptsOrderedSubset(t *testing.T) {
	t.Parallel()

	fixtures := txsubmissionTestFixtures(t)
	omitted := fixtures[0]
	returned := fixtures[1]
	h := newTxSubmissionRelayHarnessWithOpts(t, txSubmissionRelayHarnessOpts{
		omitOfferHash:  omitted.hash,
		batchRequestsA: true,
	})
	defer h.close(t)

	addTxSubmissionTestFixtures(t, h.mB, omitted, returned)
	require.NoError(t, h.nodeB.txsubmissionClientStart(h.connB.Id()))

	require.Eventually(
		t,
		func() bool {
			_, ok := h.mA.GetTransaction(returned.hash)
			return ok
		},
		5*time.Second,
		10*time.Millisecond,
		"expected later transaction from an ordered subset to be admitted",
	)
	_, admitted := h.mA.GetTransaction(omitted.hash)
	require.False(t, admitted)
}

// Observe the real FIFO after a peer reverses the two returned bodies.
// These existing fixtures are not claimed to be a parent/child transaction
// pair; the invariant is preservation of announcement order at admission.
func TestTxSubmissionServerInitRestoresOrderForReorderedReply(t *testing.T) {
	t.Parallel()
	fixtures := txsubmissionTestFixtures(t)[:2]
	replies := make(chan []txsubmission.TxBody, 1)
	logBuf := &lockedBuffer{}
	h := newTxSubmissionRelayHarnessWithOpts(t, txSubmissionRelayHarnessOpts{
		logger: slog.New(slog.NewJSONHandler(
			logBuf, &slog.HandlerOptions{Level: slog.LevelDebug},
		)),
		batchRequestsA: true,
		transformReplyB: func(bodies []txsubmission.TxBody) {
			slices.Reverse(bodies)
			if len(bodies) == 2 {
				select {
				case replies <- slices.Clone(bodies):
				default:
				}
			}
		},
	})
	defer h.close(t)
	addTxSubmissionTestFixtures(t, h.mB, fixtures...)
	require.NoError(t, h.nodeB.txsubmissionClientStart(h.connB.Id()))
	select {
	case reply := <-replies:
		require.Equal(t, fixtures[1].body, reply[0].TxBody)
		require.Equal(t, fixtures[0].body, reply[1].TxBody)
	case <-time.After(5 * time.Second):
		t.Fatalf("no reversed two-body reply: %s", logBuf.String())
	}
	require.Eventually(t, func() bool {
		return len(h.mA.Transactions()) == 2 || strings.Contains(
			logBuf.String(), "rejected mismatched txsubmission reply",
		)
	}, 5*time.Second, 10*time.Millisecond)
	require.NotContains(t, logBuf.String(),
		"rejected mismatched txsubmission reply")
	admitted := h.mA.Transactions()
	require.Len(t, admitted, 2)
	require.Equal(t, fixtures[0].body, admitted[0].Cbor)
	require.Equal(t, fixtures[1].body, admitted[1].Cbor)
}

// doubleHex returns the value a %x verb produces for an operand that
// implements fmt.Stringer with a hex String method: the hex encoding of the
// hex string, twice the intended length.
func doubleHex(t *testing.T, hexId string) string {
	t.Helper()
	encoded := hex.EncodeToString([]byte(hexId))
	require.Len(t, encoded, 2*len(hexId))
	return encoded
}

// txsubmissionLoggedMessages returns the "msg" field of every JSON log record
// in buf whose message starts with prefix.
func txsubmissionLoggedMessages(
	t *testing.T,
	buf string,
	prefix string,
) []string {
	t.Helper()
	var ret []string
	for line := range strings.SplitSeq(buf, "\n") {
		if line == "" {
			continue
		}
		var record struct {
			Msg string `json:"msg"`
		}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		if strings.HasPrefix(record.Msg, prefix) {
			ret = append(ret, record.Msg)
		}
	}
	return ret
}

// TestTxSubmissionServerInitRejectionLogsSingleHexTxId is the regression test
// for the mempool-rejection log line carrying a double-hex-encoded
// transaction id.
//
// tx.Hash() is an lcommon.Blake2b256, which implements fmt.Stringer with a hex
// String method, and fmt routes the x verb through String for such operands.
// Formatting the hash value itself with %x therefore hex-encoded its hex
// string, producing a 128-character id in the message prefix that matches no
// real transaction hash -- defeating the obvious use of the line, which is to
// grep for a transaction id seen on the wire or on chain. The 64-character id
// must appear in the message itself, not only inside the wrapped validation
// error further along the line.
func TestTxSubmissionServerInitRejectionLogsSingleHexTxId(t *testing.T) {
	fixtures := txsubmissionTestFixtures(t)
	rejected := fixtures[0]
	require.Len(t, rejected.hash, 64)

	logBuf := &lockedBuffer{}
	logger := slog.New(
		slog.NewJSONHandler(
			logBuf,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)

	h := newTxSubmissionRelayHarnessWithOpts(t, txSubmissionRelayHarnessOpts{
		logger: logger,
		validatorA: txsubmissionSelectiveRejectingValidator{
			rejectedHash: rejected.hash,
		},
	})
	defer h.close(t)

	addTxSubmissionTestFixtures(t, h.mB, rejected)
	require.NoError(t, h.nodeB.txsubmissionClientStart(h.connB.Id()))

	const prefix = "failed to add tx "
	var messages []string
	require.Eventually(
		t,
		func() bool {
			messages = txsubmissionLoggedMessages(t, logBuf.String(), prefix)
			return len(messages) > 0
		},
		5*time.Second,
		10*time.Millisecond,
		"expected the mempool rejection to be logged",
	)

	for _, msg := range messages {
		require.True(
			t,
			strings.HasPrefix(msg, prefix+rejected.hash+" to mempool: "),
			"rejection message must name the transaction by its 64-character id, got %q",
			msg,
		)
		require.NotContains(
			t,
			msg,
			doubleHex(t, rejected.hash),
			"transaction id must not be hex-encoded twice",
		)
	}
}

// TestValidateTxsubmissionReplyMismatchReportsSingleHexTxId covers the second
// %x-on-a-Stringer site in this file: the reply hash/order mismatch error also
// formatted the Blake2b256 value directly, so the id an operator would search
// for was double-hex encoded.
func TestValidateTxsubmissionReplyMismatchReportsSingleHexTxId(t *testing.T) {
	fixtures := txsubmissionTestFixtures(t)
	requested := []txsubmission.TxIdAndSize{{
		TxId: fixtures[0].txId,
		Size: uint32(len(fixtures[0].body)), // #nosec G115 -- test fixture
	}}
	returned := []txsubmission.TxBody{{
		EraId:  fixtures[1].txId.EraId,
		TxBody: fixtures[1].body,
	}}

	_, err := validateTxsubmissionReply(requested, returned, gledger.NewTransactionFromCbor)
	require.Error(t, err)
	require.Contains(t, err.Error(), "received "+fixtures[1].hash)
	require.NotContains(t, err.Error(), doubleHex(t, fixtures[1].hash))
}

// txsubmissionWireEncodedItem returns the bytes gouroboros puts on the wire
// for a single MsgReplyTxs item, [eraId, #6.24(txBody)]. The decode side
// keeps only the tag-24 payload, so this encoding is the only place the
// wrapper length is observable from Go.
func txsubmissionWireEncodedItem(
	t *testing.T,
	eraId uint16,
	body []byte,
) []byte {
	t.Helper()
	item := txsubmission.TxBody{EraId: eraId, TxBody: body}
	encoded, err := item.MarshalCBOR()
	require.NoError(t, err)
	return encoded
}

// TestTxsubmissionWireSizeMatchesWireEncoding checks the derived wire size
// against the real encoder across all four CBOR byte-string length header
// bands, which is what makes the observed delta 6 bytes for a 24..255 byte
// body and 7 bytes for a 256..65535 byte body.
func TestTxsubmissionWireSizeMatchesWireEncoding(t *testing.T) {
	t.Parallel()

	for _, bodyLen := range []int{
		0,     // empty
		1,     // length header 1 byte
		23,    // largest body with a 1-byte length header
		24,    // smallest body with a 2-byte length header
		255,   // largest body with a 2-byte length header
		256,   // smallest body with a 3-byte length header
		65535, // largest body with a 3-byte length header
		65536, // smallest body with a 5-byte length header
	} {
		for _, eraId := range []uint16{0, 6, 23, 24, 255} {
			t.Run(
				fmt.Sprintf("era%d/len%d", eraId, bodyLen),
				func(t *testing.T) {
					body := make([]byte, bodyLen)
					encoded := txsubmissionWireEncodedItem(t, eraId, body)
					require.Equal(
						t,
						uint64(len(encoded)),
						txsubmissionWireSize(eraId, bodyLen),
					)
				},
			)
		}
	}
}

// TestTxsubmissionWireSizeOverheadBands documents the exact per-band
// overhead observed against cardano-node peers.
func TestTxsubmissionWireSizeOverheadBands(t *testing.T) {
	t.Parallel()

	const conwayEraId = txsubmissionRelayTestEraId
	for _, tc := range []struct {
		bodyLen  int
		overhead uint64
	}{
		{bodyLen: 23, overhead: 5},
		{bodyLen: 24, overhead: 6},
		{bodyLen: 238, overhead: 6},
		{bodyLen: 255, overhead: 6},
		{bodyLen: 256, overhead: 7},
		{bodyLen: 2331, overhead: 7},
		{bodyLen: 65535, overhead: 7},
		{bodyLen: 65536, overhead: 9},
	} {
		t.Run(fmt.Sprintf("len%d", tc.bodyLen), func(t *testing.T) {
			require.Equal(
				t,
				uint64(tc.bodyLen)+tc.overhead,
				txsubmissionWireSize(conwayEraId, tc.bodyLen),
			)
		})
	}
}

// TestValidateTxsubmissionReplyAcceptsWireSizeAdvertisement is the
// regression test for the size-validation regression: a
// cardano-node peer advertises the wrapped wire size in MsgReplyTxIds while
// gouroboros hands Dingo only the unwrapped body, so an equality check
// against len(TxBody) rejects every batch such a peer offers.
func TestValidateTxsubmissionReplyAcceptsWireSizeAdvertisement(t *testing.T) {
	t.Parallel()

	fixtures := txsubmissionTestFixtures(t)
	requested := make([]txsubmission.TxIdAndSize, 0, len(fixtures))
	returned := make([]txsubmission.TxBody, 0, len(fixtures))
	for _, fixture := range fixtures {
		wireSize := len(
			txsubmissionWireEncodedItem(
				t,
				fixture.txId.EraId,
				fixture.body,
			),
		)
		require.Greater(t, wireSize, len(fixture.body))
		requested = append(requested, txsubmission.TxIdAndSize{
			TxId: fixture.txId,
			Size: uint32(wireSize), // #nosec G115 -- test fixture
		})
		returned = append(returned, txsubmission.TxBody{
			EraId:  fixture.txId.EraId,
			TxBody: fixture.body,
		})
	}

	validated, err := validateTxsubmissionReply(requested, returned, gledger.NewTransactionFromCbor)
	require.NoError(t, err)
	require.Len(t, validated, len(returned))
}

// TestValidateTxsubmissionReplyRejectsGenuineSizeMismatch verifies the
// wire-size allowance does not turn the size check into an unbounded range:
// only sizes within the reference discrepancy of the unwrapped body or exact
// derived wire size are accepted.
func TestValidateTxsubmissionReplyRejectsGenuineSizeMismatch(t *testing.T) {
	t.Parallel()

	fixture := txsubmissionTestFixtures(t)[0]
	returned := []txsubmission.TxBody{
		{EraId: fixture.txId.EraId, TxBody: fixture.body},
	}
	wireSize := uint32( // #nosec G115 -- test fixture
		len(
			txsubmissionWireEncodedItem(t, fixture.txId.EraId, fixture.body),
		),
	)
	bodySize := uint32(len(fixture.body)) // #nosec G115 -- test fixture
	for _, tc := range []struct {
		name  string
		size  uint32
		match string
	}{
		// A single body below the tolerance is rejected by the aggregate
		// budget before the per-body predicate is evaluated.
		{name: "beyond body tolerance below", size: bodySize - 33, match: "reply exceeds byte limit"},
		{name: "zero", size: 0, match: "size mismatch"},
		{name: "beyond body tolerance above", size: bodySize + 40, match: "size mismatch"},
		// This value is the same as bodySize-33 for this fixture, so the
		// aggregate budget rejects it before the per-body predicate runs.
		{name: "beyond wire tolerance below", size: wireSize - 40, match: "reply exceeds byte limit"},
		{name: "beyond wire tolerance above", size: wireSize + 33, match: "size mismatch"},
		{name: "double", size: bodySize * 2, match: "size mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requested := []txsubmission.TxIdAndSize{
				{TxId: fixture.txId, Size: tc.size},
			}
			validated, err := validateTxsubmissionReply(requested, returned, gledger.NewTransactionFromCbor)
			require.ErrorContains(t, err, tc.match)
			require.Nil(t, validated)
		})
	}
}

// TestTxSubmissionRelayAdmitsWireSizeAdvertisedTransaction drives the real
// pull loop end to end. Since Dingo's client now advertises the wrapped
// wire size, node B stands in for a cardano-node peer: node A must accept
// and admit the body and count the acceptance under the wire-size outcome.
func TestTxSubmissionRelayAdmitsWireSizeAdvertisedTransaction(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	h := newTxSubmissionRelayHarnessWithOpts(t, txSubmissionRelayHarnessOpts{
		promRegistryA: reg,
	})
	defer h.close(t)

	fixture := txsubmissionTestFixtures(t)[0]
	addTxSubmissionTestFixtures(t, h.mB, fixture)
	require.NoError(t, h.nodeB.txsubmissionClientStart(h.connB.Id()))

	require.Eventually(
		t,
		func() bool {
			_, ok := h.mA.GetTransaction(fixture.hash)
			return ok
		},
		5*time.Second,
		10*time.Millisecond,
		"expected a wire-size-advertised transaction to be admitted",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			h.nodeA.protocolMetrics.txsubmissionReplySizeMismatch.
				WithLabelValues(txsubmissionReplySizeAcceptedWire),
		),
	)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(
			h.nodeA.protocolMetrics.txsubmissionReplySizeMismatch.
				WithLabelValues(txsubmissionReplySizeRejected),
		),
	)
}

// TestTxsubmissionReplySizeMetricPreMaterialized verifies both outcomes are
// exported as zero before the first mismatch, so an alert on the counter
// does not have to tolerate a missing series.
func TestTxsubmissionReplySizeMetricPreMaterialized(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{PromRegistry: reg})
	families, err := reg.Gather()
	require.NoError(t, err)
	outcomes := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "dingo_txsubmission_reply_size_mismatch_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "outcome" {
					outcomes[label.GetValue()] = metric.GetCounter().
						GetValue()
				}
			}
		}
	}
	require.Equal(
		t,
		map[string]float64{
			txsubmissionReplySizeAcceptedWire: 0,
			txsubmissionReplySizeRejected:     0,
		},
		outcomes,
	)

	o.recordTxsubmissionReplySize(txsubmissionReplySizeRejected, 1)
	o.recordTxsubmissionReplySize(txsubmissionReplySizeAcceptedWire, 3)
	// A zero or negative count must not create spurious observations.
	o.recordTxsubmissionReplySize(txsubmissionReplySizeRejected, 0)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			o.protocolMetrics.txsubmissionReplySizeMismatch.
				WithLabelValues(txsubmissionReplySizeRejected),
		),
	)
	require.Equal(
		t,
		float64(3),
		testutil.ToFloat64(
			o.protocolMetrics.txsubmissionReplySizeMismatch.
				WithLabelValues(txsubmissionReplySizeAcceptedWire),
		),
	)
}

// TestRecordTxsubmissionReplySizeWithoutMetrics verifies the recorder is a
// no-op when metrics were never initialized.
func TestRecordTxsubmissionReplySizeWithoutMetrics(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{})
	require.Nil(t, o.protocolMetrics)
	require.NotPanics(t, func() {
		o.recordTxsubmissionReplySize(txsubmissionReplySizeRejected, 1)
	})
}

// TestValidateTxsubmissionReplyUndersizedAdvertisementIsCounted covers a
// peer advertising a size SMALLER than the body it returns. That case used
// to trip the aggregate byte-budget check before the per-body size check
// ran, so the reply was dropped without being classified or counted as a
// size mismatch.
func TestValidateTxsubmissionReplyUndersizedAdvertisementIsCounted(
	t *testing.T,
) {
	t.Parallel()

	fixture := txsubmissionTestFixtures(t)[0]
	returned := []txsubmission.TxBody{
		{EraId: fixture.txId.EraId, TxBody: fixture.body},
	}
	requested := []txsubmission.TxIdAndSize{
		{
			TxId: fixture.txId,
			Size: uint32(len(fixture.body)) - 33, // #nosec G115 -- fixture
		},
	}

	validated, err := validateTxsubmissionReply(requested, returned, gledger.NewTransactionFromCbor)
	require.Nil(t, validated)
	require.ErrorIs(t, err, errTxsubmissionReplySizeMismatch)
	// The operator needs both numbers and the era to tell an undersized
	// advertisement apart from a wrapper-size disagreement.
	require.ErrorContains(t, err, "advertised")
	require.ErrorContains(t, err, "body")
	require.ErrorContains(t, err, "wire")
	require.ErrorContains(t, err, "era")

	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{PromRegistry: reg})
	o.recordTxsubmissionReplyOutcome(validated, len(returned), err)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			o.protocolMetrics.txsubmissionReplySizeMismatch.
				WithLabelValues(txsubmissionReplySizeRejected),
		),
	)
}

// TestRecordTxsubmissionReplyOutcomeCountsBodies pins the unit of both
// outcomes: each counts reply BODIES, never replies. A three-body reply
// that is accepted adds three to accepted_wire_size, and a three-body
// reply dropped for a size mismatch adds three to rejected, because the
// whole reply is dropped.
func TestRecordTxsubmissionReplyOutcomeCountsBodies(t *testing.T) {
	t.Parallel()

	fixtures := txsubmissionTestFixtures(t)
	require.Len(t, fixtures, 3)
	requested := make([]txsubmission.TxIdAndSize, 0, len(fixtures))
	returned := make([]txsubmission.TxBody, 0, len(fixtures))
	for _, fixture := range fixtures {
		requested = append(requested, txsubmission.TxIdAndSize{
			TxId: fixture.txId,
			Size: uint32( // #nosec G115 -- test fixture
				len(
					txsubmissionWireEncodedItem(
						t,
						fixture.txId.EraId,
						fixture.body,
					),
				),
			),
		})
		returned = append(returned, txsubmission.TxBody{
			EraId:  fixture.txId.EraId,
			TxBody: fixture.body,
		})
	}

	t.Run("accepted counts three bodies", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		o := newOuroboros(OuroborosConfig{PromRegistry: reg})
		validated, err := validateTxsubmissionReply(requested, returned, gledger.NewTransactionFromCbor)
		require.NoError(t, err)
		require.Len(t, validated, 3)
		o.recordTxsubmissionReplyOutcome(validated, len(returned), err)
		require.Equal(
			t,
			float64(3),
			testutil.ToFloat64(
				o.protocolMetrics.txsubmissionReplySizeMismatch.
					WithLabelValues(txsubmissionReplySizeAcceptedWire),
			),
		)
		require.Equal(
			t,
			float64(0),
			testutil.ToFloat64(
				o.protocolMetrics.txsubmissionReplySizeMismatch.
					WithLabelValues(txsubmissionReplySizeRejected),
			),
		)
	})

	// One bad advertisement drops the whole three-body reply, so all three
	// bodies are counted as rejected regardless of which one was bad.
	for _, badIdx := range []int{0, 1, 2} {
		t.Run(
			fmt.Sprintf("rejected counts three bodies bad%d", badIdx),
			func(t *testing.T) {
				bad := make([]txsubmission.TxIdAndSize, len(requested))
				copy(bad, requested)
				bad[badIdx].Size += 40
				reg := prometheus.NewRegistry()
				o := newOuroboros(OuroborosConfig{PromRegistry: reg})
				validated, err := validateTxsubmissionReply(bad, returned, gledger.NewTransactionFromCbor)
				require.ErrorIs(t, err, errTxsubmissionReplySizeMismatch)
				o.recordTxsubmissionReplyOutcome(
					validated,
					len(returned),
					err,
				)
				require.Equal(
					t,
					float64(3),
					testutil.ToFloat64(
						o.protocolMetrics.txsubmissionReplySizeMismatch.
							WithLabelValues(
								txsubmissionReplySizeRejected,
							),
					),
				)
				// A dropped reply contributes nothing to the accepted
				// outcome, even when earlier bodies validated.
				require.Equal(
					t,
					float64(0),
					testutil.ToFloat64(
						o.protocolMetrics.txsubmissionReplySizeMismatch.
							WithLabelValues(
								txsubmissionReplySizeAcceptedWire,
							),
					),
				)
			},
		)
	}
}
