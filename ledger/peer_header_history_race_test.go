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
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// A blockfetch path requests a resync holding only chainsyncBlockfetchMutex.
// The request rejects the connection's buffered headers, which removes them
// from its header history, while the chainsync handler records another
// connection's header under chainsyncMutex. The two locks do not exclude each
// other, and the unguarded map killed the node with "concurrent map read and
// map write". Run with -race: the detector reports the pair without needing
// the accesses to overlap.
func TestBlockfetchResyncAndHeaderHistoryDoNotRace(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	recording := testChainsyncConnId(6410, 3001)
	resynced := testChainsyncConnId(6411, 3001)
	header := func(name string, slot uint64) ChainsyncEvent {
		hash := lcommon.NewBlake2b256(testHashBytes(name))
		return ChainsyncEvent{
			BlockHeader: mockHeader{hash: hash, slot: slot, blockNumber: slot},
			Point:       ocommon.NewPoint(slot, hash.Bytes()),
		}
	}

	for i := range 20 {
		slot := uint64(i + 1)
		buffered := header(fmt.Sprintf("buffered-%d", i), slot)
		buffered.ConnectionId = resynced
		ls.recordPeerHeaderHistory(buffered)
		ls.bufferedHeaderMutex.Lock()
		ls.bufferedHeaderEvents = map[string][]ChainsyncEvent{
			connIdKey(resynced): {buffered},
		}
		ls.bufferedHeaderMutex.Unlock()
		delivered := header(fmt.Sprintf("delivered-%d", i), slot)
		delivered.ConnectionId = recording

		var wg sync.WaitGroup
		wg.Go(func() {
			ls.chainsyncMutex.Lock()
			defer ls.chainsyncMutex.Unlock()
			ls.recordPeerHeaderHistory(delivered)
		})
		wg.Go(func() {
			ls.chainsyncBlockfetchMutex.Lock()
			defer ls.chainsyncBlockfetchMutex.Unlock()
			ls.requestChainsyncResync(resynced, "blockfetch restart failed", nil)
		})
		wg.Wait()
	}

	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()
	require.Nil(t, ls.peerHeaderHistory[connIdKey(resynced)],
		"the rejected headers must leave the resynced connection's history")
	require.Len(t, ls.peerHeaderHistory[connIdKey(recording)].order, 20)
}

// The same blockfetch-path resync clears the rollback loop history, a slice
// and a map of counts, while the chainsync handler records a rollback under
// chainsyncMutex. Run with -race.
func TestBlockfetchResyncAndRollbackHistoryDoNotRace(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	rollingBack := testChainsyncConnId(6412, 3001)
	resynced := testChainsyncConnId(6413, 3001)
	point := ocommon.NewPoint(7, testHashBytes("rollback-point"))
	now := time.Now()
	require.Equal(t, 1, ls.recordRollback(connIdKey(rollingBack), point, now))

	for range 20 {
		var wg sync.WaitGroup
		wg.Go(func() {
			ls.chainsyncMutex.Lock()
			defer ls.chainsyncMutex.Unlock()
			ls.recordRollback(connIdKey(rollingBack), point, now)
			ls.clearRollbackHistoryForPoint(point)
		})
		wg.Go(func() {
			ls.chainsyncBlockfetchMutex.Lock()
			defer ls.chainsyncBlockfetchMutex.Unlock()
			ls.requestChainsyncResync(resynced, "blockfetch restart failed", nil)
		})
		wg.Wait()
	}

	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()
	ls.clearRollbackHistory()
	require.Equal(t, 1, ls.recordRollback(connIdKey(rollingBack), point, now),
		"a cleared history must count the next rollback as the first")
}
