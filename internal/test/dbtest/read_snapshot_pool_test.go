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

package dbtest

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// TestReadSnapshotDoesNotHoldCommitBarrierForTheReadPool exercises
// database.NewReadSnapshotContext against a real SQLite metadata store with
// its read pool fully occupied.
//
// A read transaction holds one of the pool's connections until it is
// released, and an API request can hold its snapshot for the whole of a
// streamed response, so the pool can stay saturated for an unbounded time.
// Beginning the snapshot's read transaction while holding the commit barrier
// would therefore block construction of every read-write Txn -- block
// application included -- for exactly that long. The snapshot must instead
// wait for the pool outside the barrier.
func TestReadSnapshotDoesNotHoldCommitBarrierForTheReadPool(t *testing.T) {
	t.Parallel()

	db, err := NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)

	// Occupy every read connection, as concurrent streamed responses do.
	holders := make([]*database.Txn, 0, sqlite.DefaultMaxConnections)
	for range sqlite.DefaultMaxConnections {
		readTxn := db.Transaction(false)
		require.NotNil(t, readTxn.Metadata())
		holders = append(holders, readTxn)
	}

	started := make(chan struct{})
	snapshotDone := make(chan struct{})
	go func() {
		defer close(snapshotDone)
		close(started)
		txn, _, err := database.NewReadSnapshotContext(t.Context(), db)
		if err == nil {
			txn.Release()
		}
	}()
	<-started

	// The snapshot cannot complete while the pool is full, so every
	// iteration here runs against an outstanding snapshot caller. Each one
	// must still construct and commit promptly; before the read connection
	// was reserved ahead of the barrier, the first iteration blocked until
	// the readers below were released.
	const applies = 200
	for i := range applies {
		done := make(chan error, 1)
		go func() {
			writeTxn := db.Transaction(true)
			done <- writeTxn.Do(func(*database.Txn) error { return nil })
		}()
		require.NoError(t, testutil.RequireReceive(
			t,
			done,
			10*time.Second,
			"read-write transaction construction must not wait for the metadata read pool",
		))
		if i == 0 {
			select {
			case <-snapshotDone:
				t.Fatal(
					"read snapshot completed: the read pool was not saturated, so this case proves nothing",
				)
			default:
			}
		}
	}

	for _, holder := range holders {
		holder.Release()
	}
	testutil.RequireReceive(
		t,
		snapshotDone,
		30*time.Second,
		"read snapshot must complete once a read connection is free",
	)
}
