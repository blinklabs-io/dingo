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
	"container/list"
	"sync"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// statelessHeaderVerdictCacheLimit bounds the number of remembered verdicts.
const statelessHeaderVerdictCacheLimit = 32768

// statelessHeaderVerdictKey is a header hash followed by the epoch nonce it
// was verified against.
type statelessHeaderVerdictKey [2 * lcommon.Blake2b256Size]byte

// statelessHeaderVerdictCache remembers headers whose stateless crypto
// (VRF, KES, opcert signature and KES period) succeeded. That verdict is a
// function of the header bytes, which the header hash commits to, the epoch
// nonce, and Shelley genesis constants fixed for the lifetime of a
// LedgerState, so it is valid for every peer connection. Only successes are
// stored. The zero value is ready to use and safe for concurrent use.
type statelessHeaderVerdictCache struct {
	mu      sync.Mutex
	entries map[statelessHeaderVerdictKey]*list.Element
	order   list.List // front is most recently used
}

func newStatelessHeaderVerdictKey(
	hash []byte,
	nonce []byte,
) (statelessHeaderVerdictKey, bool) {
	var key statelessHeaderVerdictKey
	if len(hash) != lcommon.Blake2b256Size ||
		len(nonce) != lcommon.Blake2b256Size {
		return key, false
	}
	copy(key[:lcommon.Blake2b256Size], hash)
	copy(key[lcommon.Blake2b256Size:], nonce)
	return key, true
}

func (c *statelessHeaderVerdictCache) contains(
	hash []byte,
	nonce []byte,
) bool {
	key, ok := newStatelessHeaderVerdictKey(hash, nonce)
	if !ok {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, found := c.entries[key]
	if found {
		c.order.MoveToFront(elem)
	}
	return found
}

func (c *statelessHeaderVerdictCache) add(hash []byte, nonce []byte) {
	key, ok := newStatelessHeaderVerdictKey(hash, nonce)
	if !ok {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[statelessHeaderVerdictKey]*list.Element)
	}
	if elem, found := c.entries[key]; found {
		c.order.MoveToFront(elem)
		return
	}
	c.entries[key] = c.order.PushFront(key)
	for len(c.entries) > statelessHeaderVerdictCacheLimit {
		oldest := c.order.Back()
		delete(c.entries, oldest.Value.(statelessHeaderVerdictKey))
		c.order.Remove(oldest)
	}
}
