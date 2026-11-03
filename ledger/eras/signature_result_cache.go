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

package eras

import (
	"sync"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// SignatureResultCache remembers the outcome of verifying each
// transaction's vkey and bootstrap witness signatures, so a node that
// validates one transaction several times verifies its signatures once.
// Dingo validates a transaction when the mempool admits it, again on every
// chain update while it is pending, when it forges a block holding it, and
// when it applies a block holding it.
//
// Signature verification reads no ledger state, so its outcome is a function
// of the transaction alone. Entries are keyed by transaction ID, which every
// validation has already computed. The ID omits the witnesses, but gouroboros
// checks each result's binding to the transaction body and witness set before
// using it and verifies inline on a mismatch, so a result never stands in for
// another witness set's signatures and no entry needs invalidating. A body
// seen with a second witness set replaces the first one's entry.
//
// The cache holds two generations of at most signatureResultCacheGeneration
// entries each. A lookup that hits the older generation moves the entry to
// the newer one, and filling the newer one discards the older.
type SignatureResultCache struct {
	mu       sync.Mutex
	current  map[lcommon.Blake2b256]*lcommon.PreverifiedSignatures
	previous map[lcommon.Blake2b256]*lcommon.PreverifiedSignatures
}

// signatureResultCacheGeneration bounds each generation. An entry costs about
// 100 bytes, so the cache stays under a megabyte while covering a full
// mempool many times over.
const signatureResultCacheGeneration = 4096

// NewSignatureResultCache returns an empty cache ready for use.
func NewSignatureResultCache() *SignatureResultCache {
	return &SignatureResultCache{
		current: make(map[lcommon.Blake2b256]*lcommon.PreverifiedSignatures),
	}
}

// SignatureResultCacheProvider is implemented by LedgerState implementations
// that hold a SignatureResultCache spanning many validations (see
// ledger.LedgerView.SignatureResultCache). A LedgerState that does not
// implement it verifies every transaction's signatures.
type SignatureResultCacheProvider interface {
	SignatureResultCache() *SignatureResultCache
}

func signatureResultCacheFor(ls lcommon.LedgerState) *SignatureResultCache {
	provider, ok := stateCapability[SignatureResultCacheProvider](ls)
	if !ok {
		return nil
	}
	return provider.SignatureResultCache()
}

// preverifySignaturesFunc is a test seam: production always resolves to
// lcommon.PreverifySignatures. Tests substitute a counting wrapper to prove
// the cache verifies each transaction once.
var preverifySignaturesFunc = lcommon.PreverifySignatures

// Verify returns the outcome of verifying tx's vkey and bootstrap witness
// signatures, verifying them only when the cache has no outcome for tx. It
// returns nil for a Byron transaction, whose witnesses gouroboros always
// verifies inline, and a nil cache verifies without caching. It is safe to
// call concurrently.
func (c *SignatureResultCache) Verify(
	tx lcommon.Transaction,
) *lcommon.PreverifiedSignatures {
	if tx == nil {
		return nil
	}
	if _, byron := tx.(lcommon.ByronVKeyWitnessVerifier); byron {
		return nil
	}
	if c == nil {
		return preverifySignaturesFunc(tx)
	}
	key := tx.Hash()
	c.mu.Lock()
	result, ok := c.current[key]
	if !ok {
		if result, ok = c.previous[key]; ok {
			c.addLocked(key, result)
		}
	}
	c.mu.Unlock()
	if ok {
		return result
	}
	// Verify outside the lock: concurrent callers for one transaction may
	// both verify it, and both store the same outcome.
	result = preverifySignaturesFunc(tx)
	c.mu.Lock()
	c.addLocked(key, result)
	c.mu.Unlock()
	return result
}

func (c *SignatureResultCache) addLocked(
	key lcommon.Blake2b256,
	result *lcommon.PreverifiedSignatures,
) {
	if len(c.current) >= signatureResultCacheGeneration {
		c.previous = c.current
		c.current = make(
			map[lcommon.Blake2b256]*lcommon.PreverifiedSignatures,
			signatureResultCacheGeneration,
		)
	}
	c.current[key] = result
}
