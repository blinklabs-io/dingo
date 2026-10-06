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

	ouroboros "github.com/blinklabs-io/gouroboros"
)

const (
	// blockfetchMaxRangesPerConnDefault bounds the BlockFetch ranges one
	// connection may have streaming at once. A compliant client has at most
	// one range outstanding, but the server's BatchDone is sent before the
	// range's reservation is released, so a client that immediately requests
	// its next range can briefly overlap its previous one. The remaining
	// headroom is for that overlap only.
	blockfetchMaxRangesPerConnDefault = 4
	// blockfetchMaxRangesGlobalDefault bounds the BlockFetch ranges streaming
	// across all connections. Each in-flight range holds a chain iterator and
	// a sender goroutine.
	blockfetchMaxRangesGlobalDefault = 256
)

// blockfetchRangeAdmission bounds the server-side BlockFetch range work, per
// connection and across the process. A reservation is taken before the range's
// iterator and sender goroutine exist, so a saturated server rejects a request
// without allocating either.
//
// A reservation is released only by the range that holds it, never by a
// connection-closed event: the sender's iterator stays live until the sender
// exits, and the event carries only a ConnectionId, which a replacement
// connection may already reuse. The sender checks connection shutdown between
// blocks, so a closed connection's reservations drain promptly.
type blockfetchRangeAdmission struct {
	mu         sync.Mutex
	maxPerConn int
	maxGlobal  int
	total      int
	conns      map[string]*blockfetchConnRanges
}

// blockfetchConnRanges is the in-flight range count of one connection.
type blockfetchConnRanges struct {
	active int
}

type blockfetchRangeAdmitResult int

const (
	blockfetchRangeAdmitted blockfetchRangeAdmitResult = iota
	blockfetchRangeConnSaturated
	blockfetchRangeGlobalSaturated
)

func newBlockfetchRangeAdmission(
	maxPerConn int,
	maxGlobal int,
) *blockfetchRangeAdmission {
	if maxPerConn <= 0 {
		maxPerConn = blockfetchMaxRangesPerConnDefault
	}
	if maxGlobal <= 0 {
		maxGlobal = blockfetchMaxRangesGlobalDefault
	}
	return &blockfetchRangeAdmission{
		maxPerConn: maxPerConn,
		maxGlobal:  maxGlobal,
		conns:      make(map[string]*blockfetchConnRanges),
	}
}

// reserve admits one range for connId. On success the returned release
// function must be called once the range is finished; it is idempotent.
// On rejection the release function is nil.
func (a *blockfetchRangeAdmission) reserve(
	connId ouroboros.ConnectionId,
) (func(), blockfetchRangeAdmitResult) {
	key := connIdKey(connId)
	a.mu.Lock()
	defer a.mu.Unlock()
	cr := a.conns[key]
	if cr != nil && cr.active >= a.maxPerConn {
		return nil, blockfetchRangeConnSaturated
	}
	if a.total >= a.maxGlobal {
		return nil, blockfetchRangeGlobalSaturated
	}
	if cr == nil {
		cr = &blockfetchConnRanges{}
		a.conns[key] = cr
	}
	cr.active++
	a.total++
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			cr.active--
			a.total--
			if cr.active == 0 && a.conns[key] == cr {
				delete(a.conns, key)
			}
		})
	}, blockfetchRangeAdmitted
}
