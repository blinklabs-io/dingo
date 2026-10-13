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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package database

import (
	"testing"

	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	fxcbor "github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
)

// emptyByronBlockCbor builds the minimal no-transaction block CBOR used by the
// existing empty-block indexer tests.
func emptyByronBlockCbor(t *testing.T) []byte {
	t.Helper()
	bodyCbor, err := fxcbor.Marshal([]gcbor.RawMessage{{0x00}, {0x01}})
	require.NoError(t, err)
	extraCbor, err := fxcbor.Marshal([]gcbor.RawMessage{{0x02}})
	require.NoError(t, err)
	blockCbor, err := fxcbor.Marshal([]gcbor.RawMessage{
		{0x80},
		gcbor.RawMessage(bodyCbor),
		gcbor.RawMessage(extraCbor),
	})
	require.NoError(t, err)
	return blockCbor
}

func TestBlockIndexerRejectsWrongLengthBlockHash(t *testing.T) {
	t.Parallel()
	blockCbor := emptyByronBlockCbor(t)
	testCases := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{name: "exact length accepted", size: 32},
		{name: "short hash rejected", size: 31, wantErr: true},
		{name: "long hash rejected", size: 33, wantErr: true},
		{name: "empty hash rejected", size: 0, wantErr: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			indexer := NewBlockIndexer(0, make([]byte, testCase.size))
			result, err := indexer.ComputeOffsets(
				blockCbor,
				&ledger.ByronEpochBoundaryBlock{},
			)
			if testCase.wantErr {
				require.Error(
					t,
					err,
					"a %d-byte block hash must not be zero-padded into the"+
						" offsets it stamps",
					testCase.size,
				)
				require.Nil(t, result)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result)
		})
	}
}
