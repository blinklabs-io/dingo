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

package database

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// indexedLiveSource is a live source that also holds a transaction index of
// the entries given, keyed by transaction hash.
type indexedLiveSource struct {
	fixedStakeLiveSource
	applied map[string]types.TxLocation
}

func (s indexedLiveSource) TxHashesAfterSlot(
	_ types.Txn, slot uint64,
) ([][]byte, error) {
	var hashes [][]byte
	for hash, loc := range s.applied {
		if loc.Slot > slot {
			hashes = append(hashes, []byte(hash))
		}
	}
	return hashes, nil
}

func (s indexedLiveSource) TxLocation(
	_ types.Txn, hash []byte,
) (types.TxLocation, bool, error) {
	loc, ok := s.applied[string(hash)]
	return loc, ok, nil
}

// A database whose LedgerDB holds the transaction index keeps no transaction
// row, so recovering an output's CBOR finds the producing block through the
// index, and refuses a transaction the index does not hold.
func TestResolveUtxoCborWithRecoveryFindsTheProducerInTheTransactionIndex(
	t *testing.T,
) {
	t.Parallel()
	candidate := findGapConsumeCandidateWithoutCertificates(t)
	require.NotEmpty(t, candidate.producers)
	producer := candidate.producers[0]
	produced := producer.tx.Produced()
	require.NotEmpty(t, produced)
	utxo := produced[0]
	txId := utxo.Id.Id().Bytes()
	outputIdx := utxo.Id.Index()

	for _, tc := range []struct {
		name    string
		indexed bool
	}{
		{"indexed", true},
		{"not indexed", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
			require.NoError(t, err)
			defer db.Close() //nolint:errcheck
			src := indexedLiveSource{applied: map[string]types.TxLocation{}}
			if tc.indexed {
				src.applied[string(txId)] = types.TxLocation{
					BlockHash: producer.point.Hash,
					Slot:      producer.point.Slot,
					Valid:     true,
				}
			}
			require.NoError(t, db.SetLiveUtxoSource(src))
			storeBlockOffsetsOnly(t, db, producer.block)

			// Without the output's offset and the transaction's own blob
			// entry, only the producing block can supply the CBOR.
			writeTxn := db.Transaction(context.Background(), true)
			t.Cleanup(writeTxn.Release)
			require.NoError(
				t, db.Blob().DeleteUtxo(writeTxn.Blob(), txId, outputIdx),
			)
			require.NoError(t, db.Blob().DeleteTx(writeTxn.Blob(), txId))
			require.NoError(t, writeTxn.Commit())

			recovered, err := db.ResolveUtxoCborWithRecovery(
				context.Background(), txId, outputIdx, nil,
			)
			if !tc.indexed {
				require.Error(t, err)
				require.Empty(t, recovered)
				return
			}
			require.NoError(t, err)
			require.Equal(t, []byte(utxo.Output.Cbor()), recovered)
		})
	}
}

// fixedNonceSource answers every block nonce read with nonce.
type fixedNonceSource struct{ nonce []byte }

func (s fixedNonceSource) BlockNonceAt(ocommon.Point) ([]byte, error) {
	return s.nonce, nil
}

func (fixedNonceSource) HasCheckpointAtOrBefore(uint64) (bool, error) {
	return true, nil
}

// The ledger's database keeps no transaction rows to name what a rollback
// drops, so the rollback deletes the blob offsets of the transactions the
// LedgerDB's index gained after the rollback slot, and no others.
func TestRollbackDropsIndexedTransactionBlobs(t *testing.T) {
	t.Parallel()
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	target := models.Block{
		ID: 1, Slot: 100, Hash: bytes.Repeat([]byte{0x15}, 32),
		Cbor: []byte{0x80}, Number: 1, Type: babbage.BlockTypeBabbage,
	}
	require.NoError(t, db.BlockCreate(target, nil))
	db.SetNonceSource(fixedNonceSource{nonce: bytes.Repeat([]byte{1}, 32)})
	kept := bytes.Repeat([]byte{0x21}, 32)
	dropped := bytes.Repeat([]byte{0x22}, 32)
	require.NoError(t, db.SetLiveUtxoSource(indexedLiveSource{
		applied: map[string]types.TxLocation{
			string(kept):    {Slot: 100},
			string(dropped): {Slot: 101},
		},
	}))
	writeTxn := db.Transaction(context.Background(), true)
	t.Cleanup(writeTxn.Release)
	for _, hash := range [][]byte{kept, dropped} {
		require.NoError(t, db.Blob().SetTx(writeTxn.Blob(), hash, []byte{1}))
	}
	require.NoError(t, writeTxn.Commit())

	_, _, err = db.TruncateAfterSlot(
		context.Background(),
		ocommon.Point{Slot: target.Slot, Hash: target.Hash},
		0,
		nil,
	)
	require.NoError(t, err)

	readTxn := db.BlobTxn(false)
	defer readTxn.Release()
	_, err = db.Blob().GetTx(readTxn.Blob(), kept)
	require.NoError(t, err)
	_, err = db.Blob().GetTx(readTxn.Blob(), dropped)
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
}
