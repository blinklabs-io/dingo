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

package ledger

import (
	"context"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/utxoref"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type boundedTestSet map[utxoref.Key][]byte

func (s boundedTestSet) ApplyUtxoChanges(*ocommon.Point, []UtxoEvent) error {
	return nil
}

func (s boundedTestSet) Get(txID []byte, index uint32) ([]byte, bool, error) {
	cbor, ok := s[utxoSetKey(txID, index)]
	return cbor, ok, nil
}

func (boundedTestSet) Sum([]byte) (uint64, error) {
	return 0, nil
}

func TestUtxosByRefsBoundedReadsTheSetAndEnforcesByteLimit(t *testing.T) {
	t.Parallel()
	first := models.UtxoId{Hash: boundedTestHash(1), Idx: 0}
	second := models.UtxoId{Hash: boundedTestHash(2), Idx: 1}
	firstCBOR := []byte("first")
	secondCBOR := []byte("second")
	set := boundedTestSet{
		utxoSetKey(first.Hash, first.Idx):   firstCBOR,
		utxoSetKey(second.Hash, second.Idx): secondCBOR,
	}
	ls := &LedgerState{config: LedgerStateConfig{UtxoSet: set}}

	// The first output is returned even when it alone exceeds the byte cap;
	// adding another output past the cap fails without returning partial data.
	got, err := ls.UtxosByRefsBounded(context.Background(), []models.UtxoId{first}, 1)
	require.NoError(t, err)
	require.Equal(t, []models.Utxo{{TxId: first.Hash, OutputIdx: first.Idx, Cbor: firstCBOR}}, got)
	_, err = ls.UtxosByRefsBounded(
		context.Background(), []models.UtxoId{first, second}, int64(len(firstCBOR)+len(secondCBOR)-1),
	)
	require.True(t, errors.Is(err, database.ErrUtxoBytesExceeded), "got %v", err)
}

func boundedTestHash(value byte) []byte {
	out := make([]byte, 32)
	for i := range out {
		out[i] = value
	}
	return out
}
