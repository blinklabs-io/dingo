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

package indexer

import (
	"bytes"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

type malformedCandidateStore struct {
	*testStore
}

func (s malformedCandidateStore) GetMidnightCandidates(
	ledger.Address,
	types.Txn,
) ([]models.Utxo, error) {
	return []models.Utxo{{
		TxId:      bytes.Repeat([]byte{0x01}, 31),
		OutputIdx: 2,
	}}, nil
}

// TestNewRejectsMalformedStoredCandidateTxID covers the candidate set the
// indexer restores at startup. Padded, a 31-byte id becomes the key of
// another transaction's output, which a later spend would then remove.
func TestNewRejectsMalformedStoredCandidateTxID(t *testing.T) {
	t.Parallel()
	_, err := New(Config{
		Metadata: malformedCandidateStore{setupTestStore(t)},
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		CommitteeCandidateAddress: testMappingAddr,
		SlotToEpoch: func(slot uint64) (uint64, error) {
			return slot / 100, nil
		},
	})
	require.ErrorContains(t, err, "midnight candidate output 2")
	require.ErrorContains(t, err, "invalid blake2b-256 hash")
}
