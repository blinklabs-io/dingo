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

package eras

import (
	"testing"

	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// indefiniteEnvelopeTx returns a transaction whose three components sit in an
// indefinite-length outer array, plus the canonical size the ledger measures.
// The wire form is two bytes longer than the canonical form: the IsValid flag
// and the break byte. common.TxSize only discounts the flag for a
// definite-length four-element array.
func indefiniteEnvelopeTx(
	t *testing.T,
) (cbor.RawMessage, uint64) {
	t.Helper()
	encode := func(v any) []byte {
		b, err := cbor.Encode(v)
		require.NoError(t, err)
		return b
	}
	build := func(fee uint64) ([]byte, uint64) {
		body := encode(map[uint]any{
			0: []any{[]any{testutil.ShapeHash(32, 0x11), uint64(0)}},
			1: []any{[]any{
				append([]byte{0x61}, testutil.ShapeHash(28, 0x22)...),
				uint64(1_000_000),
			}},
			2: fee,
		})
		wits := encode(map[uint]any{})
		aux := encode(nil)
		canonical := uint64(1 + len(body) + len(wits) + len(aux))
		return txSizeBytes(
			[]byte{0x9f}, body, wits, []byte{0xf5}, aux, []byte{0xff},
		), canonical
	}
	// A fee in 24..255 always encodes in two bytes, so the canonical size
	// is stable once the fee is chosen from that range.
	_, canonical := build(100)
	require.Less(t, canonical, uint64(256))
	require.Greater(t, canonical, uint64(23))
	txCbor, _ := build(canonical)
	return txCbor, canonical
}

// TestValidateTxUsesCanonicalSizeForFeeAndMaxSize pins that a transaction
// sitting exactly on the fee and size limits in canonical form is not
// rejected by the upstream rules, which measure the wire length of an
// indefinite-length envelope instead.
func TestValidateTxUsesCanonicalSizeForFeeAndMaxSize(t *testing.T) {
	cases := []struct {
		name     string
		validate func(*testing.T, []byte, uint64) error
	}{
		{
			name: "Alonzo",
			validate: func(t *testing.T, txCbor []byte, size uint64) error {
				tx, err := safedecode.Transaction(uint(alonzo.TxTypeAlonzo), txCbor)
				require.NoError(t, err)
				return ValidateTxAlonzo(
					tx, 1, newMockLedgerState(),
					&alonzo.AlonzoProtocolParameters{
						MinFeeA: 1, MaxTxSize: uint(size),
					},
				)
			},
		},
		{
			name: "Babbage",
			validate: func(t *testing.T, txCbor []byte, size uint64) error {
				tx, err := safedecode.Transaction(uint(babbage.TxTypeBabbage), txCbor)
				require.NoError(t, err)
				return ValidateTxBabbage(
					tx, 1, newMockLedgerState(),
					&babbage.BabbageProtocolParameters{
						MinFeeA: 1, MaxTxSize: uint(size),
					},
				)
			},
		},
		{
			name: "Conway",
			validate: func(t *testing.T, txCbor []byte, size uint64) error {
				tx, err := safedecode.Transaction(uint(conway.TxTypeConway), txCbor)
				require.NoError(t, err)
				return ValidateTxConway(
					tx, 1, newMockLedgerState(),
					&conway.ConwayProtocolParameters{
						MinFeeA: 1, MaxTxSize: uint(size),
					},
				)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			txCbor, size := indefiniteEnvelopeTx(t)
			err := tc.validate(t, txCbor, size)
			var tooLarge shelley.MaxTxSizeUtxoError
			assert.NotErrorAs(t, err, &tooLarge)
			var feeTooSmall shelley.FeeTooSmallUtxoError
			assert.NotErrorAs(t, err, &feeTooSmall)
		})
	}
}

// TestDijkstraMinFeeAndMaxSizeUseCanonicalSize pins the Dijkstra size and fee
// rules to TxSizeForFee for an indefinite-length envelope, whose wire length
// is one byte above its canonical size.
func TestDijkstraMinFeeAndMaxSizeUseCanonicalSize(t *testing.T) {
	const feePerByte = 1000
	var txCbor []byte
	var canonical uint64
	fee := uint64(feePerByte * 100)
	for range 4 {
		body, err := cbor.Encode(testutil.ShapeTxBody(map[uint]any{2: fee}))
		require.NoError(t, err)
		wits, err := cbor.Encode(map[uint]any{})
		require.NoError(t, err)
		aux, err := cbor.Encode(nil)
		require.NoError(t, err)
		canonical = uint64(1 + len(body) + len(wits) + len(aux))
		txCbor = txSizeBytes(
			[]byte{0x9f}, body, wits, aux, []byte{0xff},
		)
		fee = feePerByte * canonical
	}
	// The loop converges when the fee encodes in the width the size was
	// measured with, which the next two assertions confirm.
	tx := mustDijkstraTx(t, txCbor)
	require.Equal(t, canonical, TxSizeForFee(tx))
	require.Equal(t, fee, tx.Fee().Uint64())
	pp := &gdijkstra.DijkstraProtocolParameters{}
	pp.MinFeeA = feePerByte
	pp.MaxTxSize = uint(canonical)

	require.NoError(t, maxTxSizeRule(func(
		lcommon.ProtocolParameters,
	) (uint, bool) {
		return pp.MaxTxSize, true
	})(tx, 1, newMockLedgerState(), pp))
	require.NoError(t, validateDijkstraMinFee(tx, 1, newMockLedgerState(), pp))

	pp.MinFeeA = feePerByte + 1
	var feeTooSmall shelley.FeeTooSmallUtxoError
	require.ErrorAs(
		t,
		validateDijkstraMinFee(tx, 1, newMockLedgerState(), pp),
		&feeTooSmall,
	)
}

func mustDijkstraTx(t *testing.T, txCbor []byte) lcommon.Transaction {
	t.Helper()
	tx, err := safedecode.Transaction(uint(gdijkstra.TxTypeDijkstra), txCbor)
	require.NoError(t, err)
	return tx
}
