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
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newBlobOrphanTestDB(store *mockBlobStore) *Database {
	return &Database{
		blob: store,
		logger: slog.New(
			slog.NewJSONHandler(
				io.Discard,
				&slog.HandlerOptions{Level: slog.LevelDebug},
			),
		),
	}
}

// TestDeleteUtxoBlobsReportsUnreachableObjects covers a blob delete that fails
// before the authoritative metadata row is removed.
//
// The metadata row is the source of truth, so once it is gone the blob can
// never be reached or named again: nothing knows it exists and no sweep
// reclaims it. Returning nil there reports a clean deletion to the caller and
// leaves the object to accumulate silently, so the failure has to reach the
// caller and be counted.
func TestDeleteUtxoBlobsReportsUnreachableObjects(t *testing.T) {
	store := &mockBlobStore{
		deleteUtxoErrs: map[string]error{
			"01:0": errors.New("blob store unavailable"),
		},
	}
	db := newBlobOrphanTestDB(store)
	before := BlobOrphanCount()

	err := deleteUtxoBlobs(db, []models.Utxo{
		{TxId: []byte{0x01}, OutputIdx: 0},
		{TxId: []byte{0x02}, OutputIdx: 0},
	}, nil)

	require.Error(t, err, "a failed blob delete must reach the caller")
	assert.ErrorIs(t, err, ErrBlobDeleteIncomplete)
	assert.Equal(t, uint64(1), BlobOrphanCount()-before,
		"the unreachable object must be counted for operators")
}

// TestDeleteUtxoBlobsCountsNoOrphansOnSuccess is the negative case: a clean
// run must not report an error or inflate the orphan counter, or the metric
// is useless as an alerting signal.
func TestDeleteUtxoBlobsCountsNoOrphansOnSuccess(t *testing.T) {
	store := &mockBlobStore{}
	db := newBlobOrphanTestDB(store)
	before := BlobOrphanCount()

	err := deleteUtxoBlobs(db, []models.Utxo{
		{TxId: []byte{0x03}, OutputIdx: 0},
	}, nil)

	require.NoError(t, err)
	assert.Zero(t, BlobOrphanCount()-before)
}

// TestDeleteTxBlobsReportsUnreachableObjects is the transaction-blob half of
// the same contract.
func TestDeleteTxBlobsReportsUnreachableObjects(t *testing.T) {
	store := &mockBlobStore{
		deleteTxErrs: map[string]error{
			string([]byte{0xAA}): errors.New("blob store unavailable"),
		},
	}
	db := newBlobOrphanTestDB(store)
	before := BlobOrphanCount()

	err := deleteTxBlobs(db, [][]byte{{0xAA}, {0xBB}}, nil)

	require.Error(t, err, "a failed blob delete must reach the caller")
	assert.ErrorIs(t, err, ErrBlobDeleteIncomplete)
	assert.Equal(t, uint64(1), BlobOrphanCount()-before)
}

// TestDeleteTxBlobsCountsNoOrphansOnSuccess is the negative case for tx blobs.
func TestDeleteTxBlobsCountsNoOrphansOnSuccess(t *testing.T) {
	store := &mockBlobStore{}
	db := newBlobOrphanTestDB(store)
	before := BlobOrphanCount()

	require.NoError(t, deleteTxBlobs(db, [][]byte{{0xCC}}, nil))
	assert.Zero(t, BlobOrphanCount()-before)
}

// TestDeleteUtxoBlobsWithoutBlobStoreIsReported covers a nil blob store. This
// already returned an error; the point is that the callers no longer discard
// it, so the condition is visible rather than a silent no-op.
func TestDeleteUtxoBlobsWithoutBlobStoreIsReported(t *testing.T) {
	db := newBlobOrphanTestDB(nil)
	db.blob = nil

	err := deleteUtxoBlobs(db, []models.Utxo{
		{TxId: []byte{0x04}, OutputIdx: 0},
	}, nil)
	require.Error(t, err)
}
