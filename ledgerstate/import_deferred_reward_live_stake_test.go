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

package ledgerstate

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

// inlineUTxOMap encodes the legacy inline UTxO map ImportLedgerState reads
// from RawLedgerState.UTxOData: CBOR map[TxIn]TxOut, with the TxIn a 34-byte
// binary key (32-byte hash plus big-endian output index) and the TxOut an
// [address, coin] array.
func inlineUTxOMap(
	tb testing.TB,
	addr []byte,
	amounts []uint64,
) cbor.RawMessage {
	tb.Helper()
	// A Go map cannot carry []byte keys, so build the CBOR map body by
	// hand: header, then key/value pairs in order.
	require.LessOrEqual(tb, len(amounts), 23, "short map header only")
	body := []byte{0xa0 | byte(len(amounts))}
	for i, amount := range amounts {
		txHash := bytes.Repeat([]byte{byte(0x40 + i)}, 32)
		key := append(append([]byte{}, txHash...), 0x00, 0x00)
		keyRaw, err := cbor.Encode(key)
		require.NoError(tb, err)
		valRaw, err := cbor.Encode([]any{addr, amount})
		require.NoError(tb, err)
		body = append(body, keyRaw...)
		body = append(body, valRaw...)
	}
	return cbor.RawMessage(body)
}

// TestImportLedgerStateRebuildsDeferredRewardLiveStake covers the invariant
// the deferred per-batch refresh depends on: importUTxOs skips the aggregate
// refresh on every batch, so ImportLedgerState's own
// RebuildRewardLiveStake is the only thing that leaves reward_live_stake
// correct. A return added between the two phases, or a rebuild removed,
// ships an empty aggregate on a bootstrapped node.
//
// It runs against the real metadata store, which implements
// deferredRewardLiveStakeImporter; the assertion below is meaningless
// against a store that does not, so that is checked first.
func TestImportLedgerStateRebuildsDeferredRewardLiveStake(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	_, ok := db.Metadata().(deferredRewardLiveStakeImporter)
	require.True(
		t, ok,
		"the metadata store must take the deferred import path for this "+
			"test to cover the rebuild it depends on",
	)

	stakeHash := bytes.Repeat([]byte{0x22}, 28)
	addr := buildShelleyAddr(
		0,
		1,
		bytes.Repeat([]byte{0x11}, 28),
		stakeHash,
	)
	nonce := make([]byte, 32)
	eraBounds := make([]EraBound, EraConway+1)

	require.NoError(t, ImportLedgerState(
		context.Background(),
		ImportConfig{
			Database: db,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			State: &RawLedgerState{
				UTxOData:            inlineUTxOMap(t, addr, []uint64{1_000_000, 2_000_000}),
				Epoch:               100,
				EraIndex:            EraConway,
				EraBounds:           eraBounds,
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &SnapshotTip{
					Slot:      1_000,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 1_000, nil
			},
		},
	))

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	var utxoStake string
	require.NoError(t, raw.QueryRow(
		"SELECT utxo_stake FROM reward_live_stake "+
			"WHERE credential_tag = 0 AND staking_key = ?",
		stakeHash,
	).Scan(&utxoStake))
	require.Equal(
		t, "3000000", utxoStake,
		"the post-import rebuild must aggregate every deferred batch",
	)
}
