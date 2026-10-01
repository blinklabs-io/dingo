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
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

// dijkstraMetadataBatch encodes a batch with one child. A nil hash or nil aux
// omits that part; the top level carries its own metadata when topAux is set.
func dijkstraMetadataBatch(
	t *testing.T,
	childHash *lcommon.Blake2b256,
	childAux []byte,
	topAux []byte,
) *gdijkstra.DijkstraTransaction {
	t.Helper()
	childBodyMap := map[uint]any{0: []any{}, 1: []any{}}
	if childHash != nil {
		childBodyMap[7] = childHash.Bytes()
	}
	childBody, err := cbor.Encode(childBodyMap)
	require.NoError(t, err)
	var childAuxElem any
	if childAux != nil {
		childAuxElem = cbor.RawMessage(childAux)
	}
	child, err := cbor.Encode([]any{
		cbor.RawMessage(childBody), map[uint]any{}, childAuxElem,
	})
	require.NoError(t, err)
	topBodyMap := map[uint]any{
		0:  []any{},
		1:  []any{},
		2:  uint64(0),
		23: cbor.NewSetType([]cbor.RawMessage{child}, true),
	}
	var topAuxElem any
	if topAux != nil {
		topHash := lcommon.Blake2b256Hash(topAux)
		topBodyMap[7] = topHash.Bytes()
		topAuxElem = cbor.RawMessage(topAux)
	}
	topBody, err := cbor.Encode(topBodyMap)
	require.NoError(t, err)
	txCbor, err := cbor.Encode([]any{
		cbor.RawMessage(topBody), map[uint]any{}, true, topAuxElem,
	})
	require.NoError(t, err)
	tx, err := gdijkstra.NewDijkstraTransactionFromCbor(txCbor)
	require.NoError(t, err)
	return tx
}

// metadataRuleFailure reports whether err carries a rejection from the
// metadata rule, as opposed to another phase-1 rule the sparse fixture trips.
func metadataRuleFailure(err error) bool {
	var conflicting lcommon.ConflictingMetadataHashError
	var missingData lcommon.MissingTransactionMetadataError
	var missingHash lcommon.MissingTransactionAuxiliaryDataHashError
	return errors.As(err, &conflicting) ||
		errors.As(err, &missingData) ||
		errors.As(err, &missingHash) ||
		(err != nil && strings.Contains(err.Error(), "metadata text exceeds"))
}

func TestValidateTxDijkstraChecksChildMetadataAgainstChildBody(t *testing.T) {
	t.Parallel()
	childAux := []byte{0xa1, 0x00, 0x01}
	childHash := lcommon.Blake2b256Hash(childAux)
	topAux := []byte{0xa1, 0x00, 0x02}
	wrongHash := lcommon.Blake2b256{0xff}
	// Label 0 holding a 65-byte text exceeds the metadata text limit.
	longAux := append([]byte{0xa1, 0x00, 0x78, 0x41}, bytes.Repeat([]byte{'x'}, 65)...)
	longHash := lcommon.Blake2b256Hash(longAux)

	tests := []struct {
		name      string
		childHash *lcommon.Blake2b256
		childAux  []byte
		topAux    []byte
		reject    bool
	}{
		{"valid distinct child and top-level metadata", &childHash, childAux, topAux, false},
		{"valid child metadata without top-level metadata", &childHash, childAux, nil, false},
		{"child metadata without a hash", nil, childAux, nil, true},
		{"child hash without metadata", &childHash, nil, nil, true},
		{"child hash mismatching its metadata", &wrongHash, childAux, nil, true},
		{"top-level metadata does not satisfy child hash", &childHash, nil, childAux, true},
		{"malformed child metadata", &longHash, longAux, nil, true},
	}
	for _, tc := range tests {
		for _, valid := range []bool{true, false} {
			name := tc.name
			if !valid {
				name += " in a phase-2-invalid batch"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				tx := dijkstraMetadataBatch(t, tc.childHash, tc.childAux, tc.topAux)
				tx.TxIsValid = valid
				err := ValidateTxDijkstra(
					tx,
					0,
					newMockLedgerState(),
					&gdijkstra.DijkstraProtocolParameters{},
				)
				require.Equal(t, tc.reject, metadataRuleFailure(err), "err: %v", err)
			})
		}
	}
}
