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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// conwayAuxTxBody returns a Conway transaction body carrying auxiliary_data_hash
// (field 7) equal to the blake2b-256 of aux, so a decode failure cannot be a
// hash mismatch.
func conwayAuxTxBody(aux []byte) []byte {
	hash := lcommon.Blake2b256Hash(aux)
	// {0: [], 1: [], 2: 0, 7: h'<hash>'}
	body := []byte{0xa4, 0x00, 0x80, 0x01, 0x80, 0x02, 0x00, 0x07, 0x58, 0x20}
	return append(body, hash.Bytes()...)
}

// TestDecodeBlockfetchBlockConwayAuxiliaryData drives raw Conway block bytes
// through the production block-fetch decoder (decodeBlockfetchBlock) and
// asserts on the auxiliary-data rule that rejected the block, not merely on
// rejection, since an unrelated failure would also reject these blocks.
func TestDecodeBlockfetchBlockConwayAuxiliaryData(t *testing.T) {
	t.Parallel()

	metadataMap := []byte{0xa1, 0x01, 0x01}                // {1: 1}
	shelleyMaArray := []byte{0x82, 0xa1, 0x01, 0x01, 0x80} // [{1: 1}, []]
	taggedMetadata := []byte{0xd9, 0x01, 0x03, 0xa1, 0x00, 0xa1, 0x01, 0x01}
	taggedUnknownField := []byte{0xd9, 0x01, 0x03, 0xa1, 0x06, 0x80}
	taggedPlutusV4 := []byte{0xd9, 0x01, 0x03, 0xa1, 0x05, 0x80}
	shelleyMaOneElement := []byte{0x81, 0xa0}

	tests := []struct {
		name    string
		txCount int
		// auxByIndex maps transaction index to raw auxiliary data.
		auxByIndex map[uint][]byte
		wantErr    string
	}{
		{
			name:       "empty auxiliary-data map",
			txCount:    1,
			auxByIndex: nil,
		},
		{
			name:       "last valid index",
			txCount:    2,
			auxByIndex: map[uint][]byte{1: metadataMap},
		},
		{
			name:       "first invalid index",
			txCount:    2,
			auxByIndex: map[uint][]byte{2: metadataMap},
			wantErr:    "outside transaction list length 2",
		},
		{
			name:       "index with no transactions",
			txCount:    0,
			auxByIndex: map[uint][]byte{0: metadataMap},
			wantErr:    "outside transaction list length 0",
		},
		{
			name:       "shelley-ma array accepted",
			txCount:    1,
			auxByIndex: map[uint][]byte{0: shelleyMaArray},
		},
		{
			name:       "tagged map accepted",
			txCount:    1,
			auxByIndex: map[uint][]byte{0: taggedMetadata},
		},
		{
			name:       "shelley-ma array with one element",
			txCount:    1,
			auxByIndex: map[uint][]byte{0: shelleyMaOneElement},
			wantErr:    "must have 2 elements",
		},
		{
			name:       "tagged map unknown field",
			txCount:    1,
			auxByIndex: map[uint][]byte{0: taggedUnknownField},
			wantErr:    "unknown auxiliary-data field 6",
		},
		{
			name:       "plutus v4 scripts before dijkstra",
			txCount:    1,
			auxByIndex: map[uint][]byte{0: taggedPlutusV4},
			wantErr:    "Plutus V4 are not supported in this era",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var bodies, witnesses []byte
			bodies = append(bodies, 0x80+byte(tc.txCount))
			witnesses = append(witnesses, 0x80+byte(tc.txCount))
			for i := 0; i < tc.txCount; i++ {
				aux := tc.auxByIndex[uint(i)]
				if aux == nil {
					aux = metadataMap
				}
				bodies = append(bodies, conwayAuxTxBody(aux)...)
				witnesses = append(witnesses, 0xa0)
			}
			auxMap := []byte{0xa0 + byte(len(tc.auxByIndex))}
			for index := uint(0); index < 4; index++ {
				if aux, ok := tc.auxByIndex[index]; ok {
					auxMap = append(auxMap, byte(index))
					auxMap = append(auxMap, aux...)
				}
			}
			raw := testutil.BuildConwayBlockBytesFromComponents(
				t, 100, 1, bodies, witnesses, auxMap, []byte{0x80},
			)

			o := newOuroboros(OuroborosConfig{
				Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
				NetworkMagic: 764824073,
			})
			block, err := o.decodeBlockfetchBlock(gledger.BlockTypeConway, raw)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, block.Transactions(), tc.txCount)
		})
	}
}
