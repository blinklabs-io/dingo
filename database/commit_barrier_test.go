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
	"testing"
	"time"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// TestPauseCommitsBlocksNewReadWriteTxns proves the mechanism
// database/lifecycle.Snapshot relies on to keep its blob and metadata
// backup calls describing the same set of committed writes: a new
// read-write Txn must not even open while paused, and must open promptly
// once resumed.
//
// The barrier is held from Txn construction (not just around the eventual
// Commit) because the metadata plugin's write connection pool is sized to
// exactly one connection: an already-BEGUN-but-uncommitted transaction
// holds that connection regardless of whether Commit has been called. If
// the barrier only guarded Commit, PauseCommits could acquire its lock
// while such a transaction sat open, and Snapshot's metadata backup
// (VACUUM INTO, which needs that same connection) would deadlock against
// it — the open transaction can't reach Commit's release because
// PauseCommits already holds the exclusive side.
func TestPauseCommitsBlocksNewReadWriteTxns(t *testing.T) {
	db := openTestDB(t)

	resume := db.PauseCommits()

	started := make(chan struct{})
	opened := make(chan *Txn, 1)
	go func() {
		close(started)
		opened <- db.Transaction(true)
	}()
	testutil.RequireReceive(t, started, time.Second, "goroutine must start")

	testutil.RequireNoReceive(
		t,
		opened,
		150*time.Millisecond,
		"a new read-write Txn must not open while commits are paused",
	)

	resume()

	txn := testutil.RequireReceive(
		t,
		opened,
		time.Second,
		"a new read-write Txn must open once resumed",
	)
	require.NoError(t, db.BlockCreate(testIndexedBlock(10, 1, 0x01), txn))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 10},
		BlockNumber: 1,
	}, txn))
	require.NoError(t, txn.Commit())
}

// TestPauseCommitsAllowsConcurrentReads verifies PauseCommits only blocks
// new read-write transactions, not reads — a paused snapshot must not
// stall unrelated read-only query traffic against the same database.
func TestPauseCommitsAllowsConcurrentReads(t *testing.T) {
	db := openTestDB(t)
	require.NoError(t, db.BlockCreate(testIndexedBlock(10, 1, 0x01), nil))

	resume := db.PauseCommits()
	defer resume()

	done := make(chan *Txn, 1)
	go func() {
		done <- db.Transaction(false)
	}()

	txn := testutil.RequireReceive(
		t,
		done,
		time.Second,
		"a read-only Txn must open while commits are paused",
	)
	require.NoError(t, txn.Rollback())
}
