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

	"github.com/blinklabs-io/dingo/mempool"
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
		var reason cborRejectReason
		require.NotErrorAs(t, err, &reason,
			"a framing fault must not be encoded as a ledger rejection")
	})
}

func TestLocalTxSubmissionRejectReason_ConwayGenericUsesMempoolFailure(
	t *testing.T,
) {
	t.Parallel()

	err := newLocalTxSubmissionRejectReason(
		gledger.EraIdConway,
		errors.New("generic rejection"),
	)
	var reason cborRejectReason
	require.ErrorAs(t, err, &reason)
	wire, marshalErr := reason.MarshalCBOR()
	require.NoError(t, marshalErr)
	expected, decodeErr := hex.DecodeString(
		"8182068182077167656e657269632072656a656374696f6e",
	)
	require.NoError(t, decodeErr)
	assert.Equal(t, expected, wire)
}

func TestLocalTxSubmissionRejectReason_NilCauseIsGeneric(t *testing.T) {
	t.Parallel()

	err := newLocalTxSubmissionRejectReason(gledger.EraIdConway, nil)
	require.Error(t, err)
	var reason cborRejectReason
	require.NotErrorAs(t, err, &reason)
}

func TestLocalTxSubmissionInfrastructureErrorsStayGeneric(t *testing.T) {
	t.Parallel()

	for _, err := range []error{
		mempool.ErrNilValidator,
		fmt.Errorf("wrapped: %w", mempool.ErrMempoolStopped),
		&mempool.MempoolFullError{CurrentSize: 10, TxSize: 2, Capacity: 10},
	} {
		require.True(t, isLocalTxSubmissionInfrastructureError(err))
		rejectErr := localTxSubmissionRejectReason(gledger.EraIdConway, err)
		require.EqualError(
			t,
			rejectErr,
			"local transaction submission unavailable",
		)
		var reason cborRejectReason
		require.NotErrorAs(t, rejectErr, &reason,
			"node faults must not masquerade as Conway ledger failures")
	}
	require.False(t, isLocalTxSubmissionInfrastructureError(
		errors.New("ledger validation failed"),
	))
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
		"empty inputs": {
			cause:    shelley.InputSetEmptyUtxoError{},
			expected: "818207818201820182008104",
		},
		"generic rejection": {
			cause:    errors.New("generic rejection"),
			expected: "8182078182027167656e657269632072656a656374696f6e",
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := newLocalTxSubmissionRejectReason(
				gledger.EraIdDijkstra,
				tc.cause,
			)
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

func TestLocalTxSubmissionRejectReason_GenericCauseSurvivesClientDecode(
	t *testing.T,
) {
	t.Parallel()

	for _, tc := range []struct {
		era        uint16
		failureTag int
	}{
		{era: gledger.EraIdConway, failureTag: conwayLedgerMempoolFailure},
		{era: gledger.EraIdDijkstra, failureTag: dijkstraMempoolFailure},
	} {
		reason := newLocalTxSubmissionRejectReason(
			tc.era,
			errors.New("generic rejection"),
		)
		var cborReason cborRejectReason
		require.ErrorAs(t, reason, &cborReason)
		wire, err := cborReason.MarshalCBOR()
		require.NoError(t, err)

		decoded, err := gledger.NewTxSubmitErrorFromCbor(wire)
		require.NoError(t, err)
		var validation *gledger.ShelleyTxValidationError
		require.ErrorAs(t, decoded, &validation)
		require.Len(t, validation.Err.Failures, 1)
		unknown, ok := validation.Err.Failures[0].(*gledger.UnknownApplyTxFailureError)
		require.True(t, ok)
		assert.Equal(t, uint8(tc.era), unknown.Era)
		assert.Equal(t, tc.failureTag, unknown.FailureType)
		assert.Contains(t, string(unknown.Cbor), "generic rejection",
			"the pinned client preserves the constructor payload as raw CBOR")
	}
}

func TestLocalTxSubmissionRejectReason_InputSetEmptyIsStructured(t *testing.T) {
	t.Parallel()
	fixtures := map[uint16]string{
		gledger.EraIdShelley: "81820181820082048103",
		gledger.EraIdAllegra: "81820281820082048103",
		gledger.EraIdMary:    "81820381820082048103",
		gledger.EraIdAlonzo:  "818204818200820082048103",
		gledger.EraIdBabbage: "818205818200820282018103",
		// Conway LEDGER uses UtxowFailure tag 1, unlike Shelley–Babbage tag 0.
		// cardano-ledger 2c33b4f858c0e62b300d121996a479f505d8c0e5,
		// eras/conway/impl/src/Cardano/Ledger/Conway/Rules/Ledger.hs.
		gledger.EraIdConway:   "81820681820182008104",
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
