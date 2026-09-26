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

package node

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/types"
	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// rawInvalidTxTestBody returns a Babbage-layout transaction body with one
// regular output and a collateral return. The fee only makes each body hash
// distinct.
func rawInvalidTxTestBody(t *testing.T, fee uint64) []byte {
	t.Helper()
	address := append([]byte{0x60}, bytes.Repeat([]byte{0x11}, 28)...)
	body, err := gcbor.Encode(map[uint64]any{
		0:  []any{},
		1:  []any{[]any{address, uint64(2_000_000)}},
		2:  fee,
		16: []any{address, uint64(3_000_000)},
	})
	require.NoError(t, err)
	return body
}

// rawInvalidTxTestBlock returns a block in the Alonzo-through-Conway layout
// [header, bodies, witnesses, auxiliary data, invalid_transactions].
func rawInvalidTxTestBlock(
	t *testing.T,
	bodies [][]byte,
	invalid []uint,
) []byte {
	t.Helper()
	rawBodies := make([]gcbor.RawMessage, len(bodies))
	witnesses := make([]any, len(bodies))
	for i, body := range bodies {
		rawBodies[i] = gcbor.RawMessage(body)
		witnesses[i] = map[uint64]any{}
	}
	if invalid == nil {
		invalid = []uint{}
	}
	block, err := gcbor.Encode([]any{
		uint64(0),
		rawBodies,
		witnesses,
		map[uint64]any{},
		invalid,
	})
	require.NoError(t, err)
	return block
}

// The raw-block copy has to read invalid_transactions the way the reference
// block decoder does: cardano-ledger's alignedValidFlags walks the list in
// wire order, so a descending index marks only the later transaction and a
// repeated index also marks the transaction after it. The expected flags are
// written out from that definition rather than computed.
func TestStoreRawBlockUtxoOffsetsAlignsInvalidTransactionIndexes(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		indexes []uint
		invalid [2]bool
	}{
		{name: "no invalid transactions", invalid: [2]bool{false, false}},
		{name: "second invalid", indexes: []uint{1}, invalid: [2]bool{false, true}},
		{name: "descending indexes", indexes: []uint{1, 0}, invalid: [2]bool{false, true}},
		{name: "repeated index", indexes: []uint{0, 0}, invalid: [2]bool{true, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bodies := [][]byte{
				rawInvalidTxTestBody(t, 1),
				rawInvalidTxTestBody(t, 2),
			}
			db := newTestDB(t)
			txn := db.BlobTxn(true)
			defer txn.Rollback() //nolint:errcheck

			stored, err := storeRawBlockUtxoOffsets(txn, chain.RawBlock{
				Slot: 7,
				Hash: bytes.Repeat([]byte{0x42}, 32),
				Cbor: rawInvalidTxTestBlock(t, bodies, tc.indexes),
				Type: gledger.BlockTypeBabbage,
			})
			require.NoError(t, err)
			require.Equal(t, len(bodies), stored)

			for i, body := range bodies {
				txHash := lcommon.Blake2b256Hash(body)
				// A valid transaction produces output 0; an invalid one
				// produces only its collateral return, at index
				// len(outputs) == 1.
				_, outputErr := db.Blob().GetUtxo(txn.Blob(), txHash[:], 0)
				_, returnErr := db.Blob().GetUtxo(txn.Blob(), txHash[:], 1)
				label := fmt.Sprintf("transaction %d", i)
				if tc.invalid[i] {
					require.ErrorIs(t, outputErr, types.ErrBlobKeyNotFound, label)
					require.NoError(t, returnErr, label)
				} else {
					require.NoError(t, outputErr, label)
					require.ErrorIs(t, returnErr, types.ErrBlobKeyNotFound, label)
				}
			}
		})
	}
}

// The reference block decoder rejects an invalid_transactions index outside
// the transaction list, so the raw copy must not store offsets for it.
func TestStoreRawBlockUtxoOffsetsRejectsOutOfRangeInvalidIndex(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	txn := db.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck

	_, err := storeRawBlockUtxoOffsets(txn, chain.RawBlock{
		Slot: 9,
		Hash: bytes.Repeat([]byte{0x42}, 32),
		Cbor: rawInvalidTxTestBlock(
			t,
			[][]byte{rawInvalidTxTestBody(t, 1), rawInvalidTxTestBody(t, 2)},
			[]uint{2},
		),
		Type: gledger.BlockTypeBabbage,
	})
	require.ErrorContains(t, err, "outside transaction list")
}
