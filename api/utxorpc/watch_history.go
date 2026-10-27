// Copyright 2025 Blink Labs Software
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

package utxorpc

import (
	"sync"

	watch "github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch"
	"google.golang.org/protobuf/proto"
)

// Defaults for the WatchTx undo-history byte bounds on UtxorpcConfig.
const (
	DefaultMaxWatchHistoryBytes      = 16 << 20  // 16 MiB per stream
	DefaultMaxWatchHistoryBytesTotal = 256 << 20 // 256 MiB across streams
)

// watchTxHistoryBudget is the process-wide cap on the bytes WatchTx streams
// retain for rollback. Sizes are protobuf wire sizes, a lower bound on the
// heap the converted transactions occupy, so the limit is a proportional
// bound rather than an exact one.
type watchTxHistoryBudget struct {
	mu    sync.Mutex
	used  int64
	limit int64
}

func newWatchTxHistoryBudget(limit int64) *watchTxHistoryBudget {
	return &watchTxHistoryBudget{limit: limit}
}

// reserve takes n bytes if they fit under the limit; it never waits.
func (b *watchTxHistoryBudget) reserve(n int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.used+n > b.limit {
		return false
	}
	b.used += n
	return true
}

func (b *watchTxHistoryBudget) release(n int64) {
	if n == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.used = max(b.used-n, 0)
}

// watchTxHistoryTracker holds one stream's share of the budget. It evicts the
// oldest undo entries when a block-count, per-stream byte or process-wide
// byte limit is reached. An evicted entry costs nothing to correctness: a
// rollback that reaches past the retained history rebuilds the missing undo
// responses from persisted blocks, starting at the oldest retained entry's
// predecessor, or at the stream cursor when nothing is retained. The
// tracker is used by a single stream goroutine.
type watchTxHistoryTracker struct {
	budget    *watchTxHistoryBudget
	maxBlocks int
	maxBytes  int64
	held      int64
}

func newWatchTxHistoryTracker(
	budget *watchTxHistoryBudget,
	maxBlocks int,
	maxBytes int64,
) *watchTxHistoryTracker {
	return &watchTxHistoryTracker{
		budget:    budget,
		maxBlocks: maxBlocks,
		maxBytes:  maxBytes,
	}
}

func watchTxEntrySize(txs []*watch.AnyChainTx) int64 {
	var n int64
	for _, tx := range txs {
		n += int64(proto.Size(tx))
	}
	return n
}

// dropOldest evicts the oldest entry and returns its bytes to the budget.
func (t *watchTxHistoryTracker) dropOldest(
	history []watchTxHistoryEntry,
) []watchTxHistoryEntry {
	t.held -= history[0].size
	t.budget.release(history[0].size)
	return watchTxTrimHistory(history, len(history)-1)
}

// push retains entry as the newest undo entry, evicting older ones to stay
// within the limits. An entry that cannot fit even with an empty history is
// not retained; the history is then empty, which the rollback path treats as
// "rebuild from the stream cursor".
func (t *watchTxHistoryTracker) push(
	history []watchTxHistoryEntry,
	entry watchTxHistoryEntry,
) []watchTxHistoryEntry {
	entry.size = watchTxEntrySize(entry.appliedTxs)
	for len(history) > 0 &&
		(len(history) >= t.maxBlocks || t.held+entry.size > t.maxBytes) {
		history = t.dropOldest(history)
	}
	if entry.size > t.maxBytes {
		return history
	}
	for !t.budget.reserve(entry.size) {
		if len(history) == 0 {
			return history
		}
		history = t.dropOldest(history)
	}
	t.held += entry.size
	return append(history, entry)
}

// rolledBack returns the bytes of entries a rollback removed from history.
func (t *watchTxHistoryTracker) rolledBack(history []watchTxHistoryEntry) {
	var kept int64
	for i := range history {
		kept += history[i].size
	}
	t.budget.release(t.held - kept)
	t.held = kept
}

// close returns everything the stream still holds.
func (t *watchTxHistoryTracker) close() {
	t.budget.release(t.held)
	t.held = 0
}
