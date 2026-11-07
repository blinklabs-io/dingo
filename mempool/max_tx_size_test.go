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

package mempool

import (
	"context"
	"encoding/hex"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// sizeLimitValidator reports a fixed MaxTxSize, or a per-era one from
// eraLimits when set, and counts how many transactions reach ledger
// validation.
type sizeLimitValidator struct {
	validated atomic.Int64
	eraLimits map[uint]uint
	maxSize   uint
	known     bool
}

func (v *sizeLimitValidator) MaxTxSize(txType uint) (uint, bool) {
	if v.eraLimits != nil {
		limit, ok := v.eraLimits[txType]
		return limit, ok
	}
	return v.maxSize, v.known
}

func (v *sizeLimitValidator) ValidateTx(gledger.Transaction) error {
	v.validated.Add(1)
	return nil
}

func (v *sizeLimitValidator) ValidateTxWithOverlay(
	gledger.Transaction,
	map[utxoref.Key]struct{},
	map[utxoref.Key]lcommon.Utxo,
	*utxoref.StateOverlay,
) error {
	v.validated.Add(1)
	return nil
}

func decodeTestTx(t *testing.T) ([]byte, int) {
	t.Helper()
	txBytes, err := hex.DecodeString(testTxHex)
	require.NoError(t, err)
	tx, err := safedecode.Transaction(uint(conway.EraIdConway), txBytes)
	require.NoError(t, err)
	size, err := lcommon.TxSize(tx)
	require.NoError(t, err)
	return txBytes, size
}

func joinCBOR(parts ...[]byte) []byte {
	var size int
	for _, part := range parts {
		size += len(part)
	}
	ret := make([]byte, 0, size)
	for _, part := range parts {
		ret = append(ret, part...)
	}
	return ret
}

func TestAddTransactionRejectsOversizedBeforeMutationGate(t *testing.T) {
	t.Parallel()
	txBytes, ledgerSize := decodeTestTx(t)
	v := &sizeLimitValidator{maxSize: uint(ledgerSize - 1), known: true}
	m := newTestMempoolWithValidator(t, v)
	defer m.Stop(context.Background())

	// Hold the gate: an oversized submission must be refused without waiting
	// for it.
	m.mutationMutex.Lock()
	gateHeld := true
	release := func() {
		if gateHeld {
			gateHeld = false
			m.mutationMutex.Unlock()
		}
	}
	defer release()

	errCh := make(chan error, 1)
	go func() {
		errCh <- m.AddTransaction(
			context.Background(),
			uint(conway.EraIdConway),
			txBytes,
		)
	}()
	err := testutil.RequireReceive(
		t, errCh, 5*time.Second,
		"oversized submission waited on the mutation gate",
	)
	release()

	var sizeErr shelley.MaxTxSizeUtxoError
	require.ErrorAs(t, err, &sizeErr)
	require.Equal(t, uint(ledgerSize), sizeErr.TxSize)
	require.Equal(t, uint(ledgerSize-1), sizeErr.MaxTxSize)
	require.Zero(t, v.validated.Load(), "ledger rules ran for oversized tx")
	require.Empty(t, m.Transactions())
}

func TestAddTransactionAcceptsTxAtMaxTxSize(t *testing.T) {
	t.Parallel()
	txBytes, ledgerSize := decodeTestTx(t)
	v := &sizeLimitValidator{maxSize: uint(ledgerSize), known: true}
	m := newTestMempoolWithValidator(t, v)
	defer m.Stop(context.Background())

	require.NoError(t, m.AddTransaction(
		context.Background(), uint(conway.EraIdConway), txBytes,
	))
	require.Equal(t, int64(1), v.validated.Load())
	require.Len(t, m.Transactions(), 1)
}

func TestAddTransactionAcceptsAlternateFramingAtMaxTxSize(t *testing.T) {
	t.Parallel()

	txBytes, ledgerSize := decodeTestTx(t)
	require.Equal(t, byte(0x84), txBytes[0])
	for _, testCase := range []struct {
		name string
		cbor []byte
	}{
		{
			name: "indefinite",
			cbor: append(
				append([]byte{0x9f}, txBytes[1:]...),
				0xff,
			),
		},
		{
			name: "wide",
			cbor: append([]byte{0x98, 0x04}, txBytes[1:]...),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			v := &sizeLimitValidator{maxSize: uint(ledgerSize), known: true}
			m := newTestMempoolWithValidator(t, v)
			defer m.Stop(context.Background())

			err := m.AddTransaction(
				context.Background(),
				uint(conway.EraIdConway),
				testCase.cbor,
			)
			if err != nil {
				t.Fatalf(
					"alternate framing must reach ledger admission at canonical size: %v",
					err,
				)
			}
			require.Equal(t, int64(1), v.validated.Load())
			require.Len(t, m.Transactions(), 1)
		})
	}
}

func TestAddTransactionDoesNotUndercountMalformedEnvelope(t *testing.T) {
	t.Parallel()

	txBytes, _ := decodeTestTx(t)
	var components []cbor.RawMessage
	bytesRead, err := cbor.Decode(txBytes, &components)
	require.NoError(t, err)
	require.Equal(t, len(txBytes), bytesRead)
	require.Len(t, components, 4)
	malformed := []byte{0x84}
	malformed = append(malformed, components[0]...)
	malformed = append(malformed, components[1]...)
	malformed = append(malformed, 0x00)
	malformed = append(malformed, components[3]...)

	v := &sizeLimitValidator{maxSize: uint(len(malformed) - 1), known: true}
	m := newTestMempoolWithValidator(t, v)
	defer m.Stop(context.Background())

	err = m.AddTransaction(
		context.Background(),
		uint(conway.EraIdConway),
		malformed,
	)
	var sizeErr shelley.MaxTxSizeUtxoError
	if !errors.As(err, &sizeErr) {
		t.Fatalf(
			"malformed envelope must be rejected at its full raw size: %v",
			err,
		)
	}
	require.Equal(t, uint(len(malformed)), sizeErr.TxSize)
	require.Zero(t, v.validated.Load())
	require.Empty(t, m.Transactions())
}

func TestAddTransactionDoesNotUndercountBooleanAuxiliaryData(t *testing.T) {
	t.Parallel()

	txBytes, _ := decodeTestTx(t)
	var components []cbor.RawMessage
	bytesRead, err := cbor.Decode(txBytes, &components)
	require.NoError(t, err)
	require.Equal(t, len(txBytes), bytesRead)
	require.Len(t, components, 4)

	for _, testCase := range []struct {
		name string
		cbor []byte
	}{
		{
			name: "wide true",
			cbor: joinCBOR(
				[]byte{0x98, 0x03}, components[0], components[1], []byte{0xf5},
			),
		},
		{
			name: "indefinite false",
			cbor: joinCBOR(
				[]byte{0x9f}, components[0], components[1], []byte{0xf4, 0xff},
			),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			v := &sizeLimitValidator{
				maxSize: uint(len(testCase.cbor) - 1),
				known:   true,
			}
			m := newTestMempoolWithValidator(t, v)
			defer m.Stop(context.Background())

			err := m.AddTransaction(
				context.Background(), uint(conway.EraIdConway), testCase.cbor,
			)
			var sizeErr shelley.MaxTxSizeUtxoError
			if !errors.As(err, &sizeErr) {
				t.Fatalf(
					"boolean auxiliary data must be rejected at its full raw size: %v",
					err,
				)
			}
			require.Equal(t, uint(len(testCase.cbor)), sizeErr.TxSize)
			require.Zero(t, v.validated.Load())
			require.Empty(t, m.Transactions())
		})
	}
}

func TestAddTransactionSkipsSizeCheckWhenLimitUnknown(t *testing.T) {
	t.Parallel()
	txBytes, _ := decodeTestTx(t)
	v := &sizeLimitValidator{maxSize: 1, known: false}
	m := newTestMempoolWithValidator(t, v)
	defer m.Stop(context.Background())

	require.NoError(t, m.AddTransaction(
		context.Background(), uint(conway.EraIdConway), txBytes,
	))
	require.Equal(t, int64(1), v.validated.Load())
}

func TestAddTransactionUsesCanonicalDijkstraSize(t *testing.T) {
	t.Parallel()

	mempool := testutil.DijkstraShapeMempoolTxBytes(
		t, testutil.ShapeTxBody(nil),
	)
	var components []cbor.RawMessage
	bytesRead, err := cbor.Decode(mempool, &components)
	require.NoError(t, err)
	require.Equal(t, len(mempool), bytesRead)
	require.Len(t, components, 3)
	canonicalSize := 1 + len(components[0]) + len(components[1]) +
		len(components[2])

	for _, testCase := range []struct {
		name string
		cbor []byte
	}{
		{name: "mempool definite", cbor: mempool},
		{
			name: "mempool indefinite",
			cbor: joinCBOR([]byte{0x9f}, mempool[1:], []byte{0xff}),
		},
		{
			name: "mempool wide header",
			cbor: append([]byte{0x98, 0x03}, mempool[1:]...),
		},
		{
			name: "legacy mempool definite",
			cbor: joinCBOR(
				[]byte{0x84}, components[0], components[1], []byte{0xf5},
				components[2],
			),
		},
		{
			name: "legacy mempool indefinite",
			cbor: joinCBOR(
				[]byte{0x9f}, components[0], components[1], []byte{0xf5},
				components[2], []byte{0xff},
			),
		},
		{
			name: "legacy mempool wide header",
			cbor: joinCBOR(
				[]byte{0x98, 0x04}, components[0], components[1], []byte{0xf5},
				components[2],
			),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			v := &sizeLimitValidator{maxSize: uint(canonicalSize), known: true}
			m := newTestMempoolWithValidator(t, v)
			defer m.Stop(context.Background())

			err := m.AddTransaction(
				context.Background(),
				uint(gledger.TxTypeDijkstra),
				testCase.cbor,
			)
			if err != nil {
				t.Fatalf(
					"Dijkstra framing must reach ledger admission at canonical size: %v",
					err,
				)
			}
			require.Equal(t, int64(1), v.validated.Load())
			require.Len(t, m.Transactions(), 1)
		})
	}

	t.Run("oversized", func(t *testing.T) {
		v := &sizeLimitValidator{maxSize: uint(canonicalSize - 1), known: true}
		m := newTestMempoolWithValidator(t, v)
		defer m.Stop(context.Background())

		err := m.AddTransaction(
			context.Background(), uint(gledger.TxTypeDijkstra), mempool,
		)
		var sizeErr shelley.MaxTxSizeUtxoError
		require.ErrorAs(t, err, &sizeErr)
		require.Equal(t, uint(canonicalSize), sizeErr.TxSize)
		require.Zero(t, v.validated.Load(), "ledger rules ran for oversized tx")
		require.Empty(t, m.Transactions())
	})
}

func TestAddTransactionDoesNotUndercountMalformedDijkstraEnvelope(
	t *testing.T,
) {
	t.Parallel()

	mempool := testutil.DijkstraShapeMempoolTxBytes(
		t, testutil.ShapeTxBody(nil),
	)
	var components []cbor.RawMessage
	bytesRead, err := cbor.Decode(mempool, &components)
	require.NoError(t, err)
	require.Equal(t, len(mempool), bytesRead)
	require.Len(t, components, 3)
	for _, testCase := range []struct {
		name string
		cbor []byte
	}{
		{
			name: "fourth non-boolean",
			cbor: joinCBOR(
				[]byte{0x84}, components[0], components[1], components[2],
				[]byte{0x00},
			),
		},
		{
			name: "wide three-element boolean",
			cbor: joinCBOR(
				[]byte{0x98, 0x03}, components[0], components[1], []byte{0xf5},
			),
		},
		{
			name: "indefinite three-element boolean",
			cbor: joinCBOR(
				[]byte{0x9f}, components[0], components[1], []byte{0xf4, 0xff},
			),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			v := &sizeLimitValidator{
				maxSize: uint(len(testCase.cbor) - 1),
				known:   true,
			}
			m := newTestMempoolWithValidator(t, v)
			defer m.Stop(context.Background())

			err := m.AddTransaction(
				context.Background(), uint(gledger.TxTypeDijkstra), testCase.cbor,
			)
			var sizeErr shelley.MaxTxSizeUtxoError
			if !errors.As(err, &sizeErr) {
				t.Fatalf(
					"malformed Dijkstra envelope must be rejected at its full raw size: %v",
					err,
				)
			}
			require.Equal(t, uint(len(testCase.cbor)), sizeErr.TxSize)
			require.Zero(t, v.validated.Load())
			require.Empty(t, m.Transactions())
		})
	}
}

// The limit is the one for the submitted era, not a single current limit.
func TestAddTransactionAppliesLimitOfSubmittedEra(t *testing.T) {
	t.Parallel()
	txBytes, ledgerSize := decodeTestTx(t)
	v := &sizeLimitValidator{eraLimits: map[uint]uint{
		uint(conway.EraIdConway):    uint(ledgerSize - 1),
		uint(gledger.TxTypeBabbage): uint(ledgerSize),
	}}
	m := newTestMempoolWithValidator(t, v)
	defer m.Stop(context.Background())

	err := m.AddTransaction(
		context.Background(), uint(conway.EraIdConway), txBytes,
	)
	var sizeErr shelley.MaxTxSizeUtxoError
	require.ErrorAs(t, err, &sizeErr)
	require.Equal(t, uint(ledgerSize-1), sizeErr.MaxTxSize)
	require.Zero(t, v.validated.Load(), "ledger rules ran for oversized tx")
}
