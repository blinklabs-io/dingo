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
	"errors"
	"fmt"
	"log/slog"
	"net"
	"testing"
	"time"

	gouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/connection"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	olocaltxsubmission "github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalTxSubmissionServerSubmitTx_NonByteContentReturnsError(
	t *testing.T,
) {
	t.Parallel()

	o := &Ouroboros{
		config: OuroborosConfig{
			Logger: slog.New(slog.DiscardHandler),
		},
	}
	ctx := olocaltxsubmission.CallbackContext{
		ConnectionId: connection.ConnectionId{
			LocalAddr:  &net.TCPAddr{},
			RemoteAddr: &net.TCPAddr{},
		},
	}
	tx := olocaltxsubmission.MsgSubmitTxTransaction{
		EraId: uint16(gledger.EraIdConway),
		Raw: cbor.Tag{
			Number:  24,
			Content: "not-bytes",
		},
	}

	require.NotPanics(t, func() {
		err := o.localtxsubmissionServerSubmitTx(ctx, tx)
		require.Error(t, err)
	})
}

func TestLocalTxSubmissionRejectReason_ConwayMempoolFailure(
	t *testing.T,
) {
	t.Parallel()

	err := newLocalTxSubmissionRejectReason(
		gledger.EraIdConway,
		errors.New("plain validation failure"),
	)
	reason, ok := err.(cborRejectReason)
	require.True(t, ok)

	wireBytes, marshalErr := reason.MarshalCBOR()
	require.NoError(t, marshalErr)
	var envelope []cbor.RawMessage
	_, err = cbor.Decode(wireBytes, &envelope)
	require.NoError(t, err)
	require.Len(t, envelope, 1)
	var eraFailure []cbor.RawMessage
	_, err = cbor.Decode(envelope[0], &eraFailure)
	require.NoError(t, err)
	require.Len(t, eraFailure, 2)
	var era uint16
	_, err = cbor.Decode(eraFailure[0], &era)
	require.NoError(t, err)
	assert.Equal(t, uint16(gledger.EraIdConway), era)
	var applyFailures []cbor.RawMessage
	_, err = cbor.Decode(eraFailure[1], &applyFailures)
	require.NoError(t, err)
	require.Len(t, applyFailures, 1)
	var failure []cbor.RawMessage
	_, err = cbor.Decode(applyFailures[0], &failure)
	require.NoError(t, err)
	require.Len(t, failure, 2)
	var failureType uint
	_, err = cbor.Decode(failure[0], &failureType)
	require.NoError(t, err)
	assert.Equal(t, uint(conwayLedgerMempoolFailure), failureType)
	var message string
	_, err = cbor.Decode(failure[1], &message)
	require.NoError(t, err)
	assert.Equal(t, "plain validation failure", message)
}

func TestLocalTxSubmissionRejectReason_UnsupportedGenericIsUnrepresentable(
	t *testing.T,
) {
	t.Parallel()
	for _, era := range []uint16{
		gledger.EraIdByron,
		gledger.EraIdShelley,
		gledger.EraIdAllegra,
		gledger.EraIdMary,
		gledger.EraIdAlonzo,
		gledger.EraIdBabbage,
		255,
		256,
		257,
		65535,
	} {
		t.Run(fmt.Sprint(era), func(t *testing.T) {
			err := newLocalTxSubmissionRejectReason(
				era,
				errors.New("plain validation failure"),
			)
			var reason cborRejectReason
			require.ErrorAs(t, err, &reason)
			_, marshalErr := reason.MarshalCBOR()
			require.ErrorContains(t, marshalErr, "not representable")
		})
	}
}

func TestLocalTxSubmissionRejectReason_DijkstraMempoolFailure(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		cause    error
		expected string
	}{
		"generic": {
			cause:    errors.New("generic rejection"),
			expected: "8182078182027167656e657269632072656a656374696f6e",
		},
		"empty inputs": {
			cause:    shelley.InputSetEmptyUtxoError{},
			expected: "818207818201820182008104",
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := newLocalTxSubmissionRejectReason(gledger.EraIdDijkstra, tc.cause)
			var reason cborRejectReason
			require.ErrorAs(t, err, &reason)
			wire, marshalErr := reason.MarshalCBOR()
			require.NoError(t, marshalErr)
			expected, decodeErr := hex.DecodeString(tc.expected)
			require.NoError(t, decodeErr)
			assert.Equal(t, expected, wire)
		})
	}
}

func TestLocalTxSubmissionRejectReason_InputSetEmptyIsStructured(t *testing.T) {
	t.Parallel()
	fixtures := map[uint16]string{
		gledger.EraIdShelley:  "81820181820082048103",
		gledger.EraIdAllegra:  "81820281820082048103",
		gledger.EraIdMary:     "81820381820082048103",
		gledger.EraIdAlonzo:   "818204818200820082048103",
		gledger.EraIdBabbage:  "818205818200820282018103",
		gledger.EraIdConway:   "81820681820082008104",
		gledger.EraIdDijkstra: "818207818201820182008104",
	}
	for era, fixture := range fixtures {
		t.Run(fmt.Sprint(era), func(t *testing.T) {
			t.Parallel()
			expectedWire, decodeErr := hex.DecodeString(fixture)
			require.NoError(t, decodeErr)
			for name, cause := range map[string]error{
				"value":           shelley.InputSetEmptyUtxoError{},
				"pointer":         &shelley.InputSetEmptyUtxoError{},
				"wrapped value":   fmt.Errorf("validate: %w", shelley.InputSetEmptyUtxoError{}),
				"wrapped pointer": fmt.Errorf("validate: %w", &shelley.InputSetEmptyUtxoError{}),
			} {
				t.Run(name, func(t *testing.T) {
					err := newLocalTxSubmissionRejectReason(era, cause)
					var reason cborRejectReason
					require.ErrorAs(t, err, &reason)
					wire, marshalErr := reason.MarshalCBOR()
					require.NoError(t, marshalErr)
					assert.Equal(t, expectedWire, wire)
					require.ErrorIs(t, err, cause)
				})
			}
		})
	}
}

func TestLocalTxSubmissionServer_EncodeErrorIsBounded(t *testing.T) {
	t.Parallel()

	// The protocol must surface a rejection encoding error instead of waiting
	// forever for a response that cannot be represented on the wire.
	opts, peer := newMuxerServerPeer(t)
	cfg := olocaltxsubmission.NewConfig(
		olocaltxsubmission.WithSubmitTxFunc(func(
			_ olocaltxsubmission.CallbackContext,
			_ olocaltxsubmission.MsgSubmitTxTransaction,
		) error {
			return newLocalTxSubmissionRejectReason(
				gledger.EraIdBabbage,
				errors.New("generic rejection"),
			)
		}),
	)
	server := olocaltxsubmission.NewServer(opts, &cfg)
	peer.start(t, server)
	peer.send(t, olocaltxsubmission.ProtocolId,
		olocaltxsubmission.NewMsgSubmitTx(gledger.EraIdBabbage, []byte{0x80}),
	)
	select {
	case err := <-peer.errChan:
		require.ErrorContains(t, err, "not representable")
	case <-time.After(2 * time.Second):
		t.Fatal("server did not report rejection encoding error")
	}
}

func TestLocalTxSubmissionConnection_EncodeErrorClosesPeer(t *testing.T) {
	t.Parallel()

	serverPipe, clientPipe := net.Pipe()
	t.Cleanup(func() {
		_ = serverPipe.Close()
		_ = clientPipe.Close()
	})

	logger := slog.New(slog.DiscardHandler)
	serverConnCh := make(chan *gouroboros.Connection, 1)
	serverErrCh := make(chan error, 1)
	serverProtocolErrCh := make(chan error, 1)
	go func() {
		serverConn, err := gouroboros.NewConnection(
			gouroboros.WithConnection(serverPipe),
			gouroboros.WithServer(true),
			gouroboros.WithNetworkMagic(42),
			gouroboros.WithLogger(logger),
			gouroboros.WithErrorChan(serverProtocolErrCh),
			gouroboros.WithLocalTxSubmissionConfig(
				olocaltxsubmission.NewConfig(
					olocaltxsubmission.WithSubmitTxFunc(func(
						olocaltxsubmission.CallbackContext,
						olocaltxsubmission.MsgSubmitTxTransaction,
					) error {
						return newLocalTxSubmissionRejectReason(
							gledger.EraIdBabbage,
							errors.New("generic rejection"),
						)
					}),
				),
			),
		)
		if err != nil {
			serverErrCh <- err
			return
		}
		serverConnCh <- serverConn
	}()

	clientConn, err := gouroboros.NewConnection(
		gouroboros.WithConnection(clientPipe),
		gouroboros.WithNetworkMagic(42),
		gouroboros.WithLogger(logger),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientConn.Close() })

	var serverConn *gouroboros.Connection
	select {
	case err := <-serverErrCh:
		t.Fatalf("server connection setup failed: %v", err)
	case serverConn = <-serverConnCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for server connection setup")
	}
	t.Cleanup(func() { _ = serverConn.Close() })

	// The server's rejection cannot be encoded for Babbage. A real connection
	// must close as a consequence of that protocol error, allowing the client's
	// blocking SubmitTx call to return instead of waiting forever.
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- clientConn.LocalTxSubmission().Client.SubmitTx(
			gledger.EraIdBabbage,
			[]byte{0x80},
		)
	}()
	select {
	case err := <-resultCh:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("SubmitTx did not return after rejection encoding error")
	}
	select {
	case err := <-serverProtocolErrCh:
		require.ErrorContains(t, err, "not representable")
	case <-time.After(2 * time.Second):
		t.Fatal("server did not report the rejection encoding cause")
	}
	shutdownCh := make(chan struct{})
	go func() {
		for range clientConn.ErrorChan() {
		}
		close(shutdownCh)
	}()
	select {
	case <-shutdownCh:
	case <-time.After(2 * time.Second):
		t.Fatal("client error channel did not close after peer shutdown")
	}
}

func TestLocalTxSubmissionServer_ConwayRejectThenAccept(t *testing.T) {
	t.Parallel()

	opts, peer := newMuxerServerPeer(t)
	var calls int
	cfg := olocaltxsubmission.NewConfig(
		olocaltxsubmission.WithSubmitTxFunc(func(
			_ olocaltxsubmission.CallbackContext,
			_ olocaltxsubmission.MsgSubmitTxTransaction,
		) error {
			calls++
			if calls == 1 {
				return newLocalTxSubmissionRejectReason(
					gledger.EraIdConway,
					errors.New("generic rejection"),
				)
			}
			return nil
		}),
	)
	server := olocaltxsubmission.NewServer(opts, &cfg)
	peer.start(t, server)
	peer.send(t, olocaltxsubmission.ProtocolId,
		olocaltxsubmission.NewMsgSubmitTx(gledger.EraIdConway, []byte{0x80}),
	)
	first := peer.readResponse(t, 2*time.Second)
	require.Equal(t, olocaltxsubmission.ProtocolId, first.ProtocolId&0x7fff)
	var firstItems []cbor.RawMessage
	_, err := cbor.Decode(first.Payload, &firstItems)
	require.NoError(t, err)
	require.Len(t, firstItems, 2)
	var firstType uint
	_, err = cbor.Decode(firstItems[0], &firstType)
	require.NoError(t, err)
	assert.Equal(t, uint(olocaltxsubmission.MessageTypeRejectTx), firstType)
	expectedReject, err := hex.DecodeString(
		"8182068182077167656e657269632072656a656374696f6e",
	)
	require.NoError(t, err)
	assert.Equal(t, expectedReject, []byte(firstItems[1]))

	peer.send(t, olocaltxsubmission.ProtocolId,
		olocaltxsubmission.NewMsgSubmitTx(gledger.EraIdConway, []byte{0x80}),
	)
	second := peer.readResponse(t, 2*time.Second)
	var secondItems []cbor.RawMessage
	_, err = cbor.Decode(second.Payload, &secondItems)
	require.NoError(t, err)
	var secondType uint
	_, err = cbor.Decode(secondItems[0], &secondType)
	require.NoError(t, err)
	assert.Equal(t, uint(olocaltxsubmission.MessageTypeAcceptTx), secondType)
}

func TestLocalTxSubmissionServer_DijkstraRejectThenAccept(t *testing.T) {
	t.Parallel()
	opts, peer := newMuxerServerPeer(t)
	var calls int
	cfg := olocaltxsubmission.NewConfig(
		olocaltxsubmission.WithSubmitTxFunc(func(
			_ olocaltxsubmission.CallbackContext,
			_ olocaltxsubmission.MsgSubmitTxTransaction,
		) error {
			calls++
			if calls == 1 {
				return newLocalTxSubmissionRejectReason(
					gledger.EraIdDijkstra, errors.New("generic rejection"),
				)
			}
			return nil
		}),
	)
	server := olocaltxsubmission.NewServer(opts, &cfg)
	peer.start(t, server)
	peer.send(t, olocaltxsubmission.ProtocolId,
		olocaltxsubmission.NewMsgSubmitTx(gledger.EraIdDijkstra, []byte{0x80}),
	)
	first := peer.readResponse(t, 2*time.Second)
	var firstItems []cbor.RawMessage
	_, err := cbor.Decode(first.Payload, &firstItems)
	require.NoError(t, err)
	require.Len(t, firstItems, 2)
	var firstType uint
	_, err = cbor.Decode(firstItems[0], &firstType)
	require.NoError(t, err)
	assert.Equal(t, uint(olocaltxsubmission.MessageTypeRejectTx), firstType)
	expectedReject, err := hex.DecodeString(
		"8182078182027167656e657269632072656a656374696f6e",
	)
	require.NoError(t, err)
	assert.Equal(t, expectedReject, []byte(firstItems[1]))

	peer.send(t, olocaltxsubmission.ProtocolId,
		olocaltxsubmission.NewMsgSubmitTx(gledger.EraIdDijkstra, []byte{0x80}),
	)
	second := peer.readResponse(t, 2*time.Second)
	var secondItems []cbor.RawMessage
	_, err = cbor.Decode(second.Payload, &secondItems)
	require.NoError(t, err)
	var secondType uint
	_, err = cbor.Decode(secondItems[0], &secondType)
	require.NoError(t, err)
	assert.Equal(t, uint(olocaltxsubmission.MessageTypeAcceptTx), secondType)
}

func TestLocalTxSubmissionRejectReason_PreservesTypedReason(t *testing.T) {
	t.Parallel()

	typed := &gledger.EraMismatch{
		OtherEra: gledger.EraInfo{
			Index: gledger.EraIdShelley,
			Name:  "Shelley",
		},
		LedgerEra: gledger.EraInfo{
			Index: gledger.EraIdByron,
			Name:  "Byron",
		},
	}
	wrapped := fmt.Errorf("validate transaction: %w", typed)

	err := newLocalTxSubmissionRejectReason(gledger.EraIdShelley, wrapped)
	assert.Same(t, wrapped, err)

	var reason cborRejectReason
	require.ErrorAs(t, err, &reason)
	require.NotNil(t, reason)

	wireBytes, marshalErr := reason.MarshalCBOR()
	require.NoError(t, marshalErr)

	decoded, decodeErr := gledger.NewTxSubmitErrorFromCbor(wireBytes)
	require.NoError(t, decodeErr)

	var eraMismatch *gledger.EraMismatch
	require.ErrorAs(t, decoded, &eraMismatch)
	require.NotNil(t, eraMismatch)
	assert.Equal(t, typed.OtherEra, eraMismatch.OtherEra)
	assert.Equal(t, typed.LedgerEra, eraMismatch.LedgerEra)
}

// Exercise the protocol callback and real mempool decode rejection. Unsupported
// eras have no ledger ApplyTxError representation and must fail encoding rather
// than emit a fabricated Conway error.
func TestLocalTxSubmissionServerSubmitTx_RejectsUnrepresentableEra(
	t *testing.T,
) {
	t.Parallel()

	for _, era := range []uint16{255, 256, 257, 65535} {
		t.Run(fmt.Sprint(era), func(t *testing.T) {
			o, connID := newTxSubmissionTestOuroboros(t)
			err := o.localtxsubmissionServerSubmitTx(
				olocaltxsubmission.CallbackContext{ConnectionId: connID},
				olocaltxsubmission.NewMsgSubmitTx(
					era,
					[]byte{0x80},
				).Transaction,
			)
			require.ErrorContains(t, err, "decode transaction")
			var reason cborRejectReason
			require.ErrorAs(t, err, &reason)
			_, err = reason.MarshalCBOR()
			require.ErrorContains(t, err, "not representable")
		})
	}
}
