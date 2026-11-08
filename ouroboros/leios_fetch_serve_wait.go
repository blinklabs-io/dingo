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

package ouroboros

import (
	"sync"
	"time"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
)

const (
	leiosFetchServeTimeout      = 5 * time.Second
	leiosFetchServeMaxWaiters   = 64
	leiosFetchServePollInterval = 25 * time.Millisecond
)

type leiosManifestFetchClaim struct {
	done chan struct{}
	once sync.Once
}

func (o *Ouroboros) finishLeiosManifestFetch(key string, claim *leiosManifestFetchClaim) {
	o.leiosManifestFetchInProgress.CompareAndDelete(key, claim)
	claim.once.Do(func() { close(claim.done) })
}

func (o *Ouroboros) pendingLeiosServeWork(key string) (<-chan struct{}, <-chan struct{}, bool) {
	var manifestDone, txDone <-chan struct{}
	txPending := false
	if value, ok := o.leiosManifestFetchInProgress.Load(key); ok {
		if claim, ok := value.(*leiosManifestFetchClaim); ok && claim != nil {
			select {
			case <-claim.done:
			default:
				manifestDone = claim.done
			}
		}
	}
	o.leiosTxFetchClaimsMu.Lock()
	if claim := o.leiosTxFetchClaims[key]; claim != nil && claim.owner != nil {
		owner := claim.owner
		// The claim outlives an expired attempt until it is handed to the
		// next candidate or deleted, so it is still pending work.
		txPending = true
		if owner.ctx != nil && owner.ctx.Err() == nil && owner.attemptCtx != nil && owner.attemptCtx.Err() == nil {
			txDone = owner.attemptCtx.Done()
		}
	}
	o.leiosTxFetchClaimsMu.Unlock()
	return manifestDone, txDone, manifestDone != nil || txPending
}

// A validated announcement can reach a downstream peer before this relay's
// already-admitted fetch completes. LeiosFetch has no absence response: failing
// that request would tear down every mini-protocol on the shared bearer.
// Wait only for existing work on that exact announced occurrence, never fetch
// arbitrary peer-supplied points, and keep one deadline across owner handoffs.
func (o *Ouroboros) lookupLeiosFetchServingData(
	ctx oleiosfetch.CallbackContext,
	point ocommon.Point,
	ready func(*leiosEndorserBlockData) bool,
) (*leiosEndorserBlockData, bool) {
	lookup := func() (*leiosEndorserBlockData, bool) {
		data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
		return data, ok && (ready == nil || ready(data))
	}
	// Inside the wait only the in-memory cache is polled: a fetch publishes
	// into it, and a persistent read per tick would hammer the blob store.
	lookupCached := func() (*leiosEndorserBlockData, bool) {
		o.leiosMu.RLock()
		data := o.leiosEndorserBlocks[leiosBlockKey(point.Slot, point.Hash)]
		o.leiosMu.RUnlock()
		if data == nil || data.expired(time.Now()) || !data.hasManifest() {
			return nil, false
		}
		return data, ready == nil || ready(data)
	}
	if data, ok := lookup(); ok {
		return data, true
	}
	o.leiosAnnouncementsMu.Lock()
	known := o.leiosAnnouncementBindsSlotLocked(point.Hash, point.Slot)
	o.leiosAnnouncementsMu.Unlock()
	key := leiosBlockKey(point.Slot, point.Hash)
	if _, _, pending := o.pendingLeiosServeWork(key); !known || !pending || o.connManager == nil || ctx.Server == nil {
		return nil, false
	}
	conn := o.connManager.GetConnectionById(ctx.ConnectionId)
	if conn == nil || conn.LeiosFetch() == nil || conn.LeiosFetch().Server != ctx.Server ||
		conn.ChainSync() == nil || conn.ChainSync().Server == nil {
		return nil, false
	}
	if o.leiosFetchServeWaiting.Add(1) > leiosFetchServeMaxWaiters {
		o.leiosFetchServeWaiting.Add(-1)
		return nil, false
	}
	defer o.leiosFetchServeWaiting.Add(-1)
	// The same connection's chainsync owner is released by the node's existing
	// owner-aware close hook. Protocol.DoneChan cannot release its own callback.
	done, cancel := o.registerLeiosServeWaiter(ctx.ConnectionId, conn.ChainSync().Server, true)
	defer cancel()
	current := o.connManager.GetConnectionById(ctx.ConnectionId)
	if current != conn || current.LeiosFetch() == nil || current.LeiosFetch().Server != ctx.Server {
		return nil, false
	}
	timeout := leiosFetchServeTimeout
	if o.leiosFetchServeTimeoutOverride > 0 {
		timeout = o.leiosFetchServeTimeoutOverride
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	// Partial transaction batches are published independently of the owning
	// attempt's completion. Poll their immutable cache snapshots without adding
	// wakeups to the ledger's separate full-closure waiters.
	poll := time.NewTicker(leiosFetchServePollInterval)
	defer poll.Stop()
	for {
		select {
		case <-done:
			return nil, false
		case <-deadline.C:
			return nil, false
		default:
		}
		if data, ok := lookupCached(); ok {
			return data, true
		}
		manifestDone, txDone, pending := o.pendingLeiosServeWork(key)
		if !pending {
			// The work may have published and released its claim between
			// the cache read and the claim check.
			return lookup()
		}
		select {
		case <-done:
			return nil, false
		case <-deadline.C:
			return nil, false
		case <-manifestDone:
		case <-txDone:
		case <-poll.C:
		}
	}
}

func leiosRequestedTxsAvailable(data *leiosEndorserBlockData, bitmap map[uint16]uint64) bool {
	// Let the existing serving path reject an invalid bitmap immediately.
	if validateLeiosTxBitmap(data.txCount, bitmap) != nil {
		return true
	}
	for _, idx := range leiosBitmapTxIndices(bitmap) {
		if idx < len(data.txsRaw) && data.txsRaw[idx] != nil {
			continue
		}
		if idx < len(data.partialTxs) && data.partialTxs[idx] != nil {
			continue
		}
		return false
	}
	return true
}
