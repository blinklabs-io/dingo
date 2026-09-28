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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package eras

import (
	"encoding/binary"
	"sync"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/plutigo/cek"
	"github.com/blinklabs-io/plutigo/lang"
)

// PlutusEvalContextCache holds one *cek.EvalContext per distinct (language
// version, protocol major version, cost-model parameter list,
// synthetic-V2-cost-model flag) combination.
//
// cek.EvalContext's own doc comment establishes the reuse guarantee this
// relies on: a built *cek.EvalContext is immutable and safe to share and
// reuse concurrently, across any number of goroutines and evaluations, for
// as long as the (LanguageVersion, ProtoVersion.Major, cost model parameter
// list) tuple that built it is unchanged. Since NewEvalContext is a pure
// function of exactly that tuple (plus the synthetic-V2 flag this cache also
// keys on -- see the field's call sites), reusing an entry across any two
// calls that share the full key is always correct, regardless of which
// protocol-parameter snapshot, era, or transaction either call came from.
// That is why this cache has no explicit invalidation: a governance-enacted
// cost-model change simply produces a new key rather than requiring the old
// one to be evicted, and distinct eras (including the previous-era pparams
// path era-boundary transactions use) that happen to share a key are, by
// construction, supposed to share the resulting context.
type PlutusEvalContextCache struct {
	mu      sync.Mutex
	entries map[string]*plutusEvalContextEntry
}

// NewPlutusEvalContextCache returns an empty cache ready for use.
func NewPlutusEvalContextCache() *PlutusEvalContextCache {
	return &PlutusEvalContextCache{
		entries: make(map[string]*plutusEvalContextEntry),
	}
}

// PlutusEvalContextCacheProvider is implemented by LedgerState implementations
// that hold a PlutusEvalContextCache spanning many redeemer evaluations (see
// ledger.LedgerView.PlutusEvalContextCache). Exported so implementers can
// assert conformance at compile time, mirroring MinPoolMarginProvider and
// CommitteeCredentialState above. A LedgerState that does not implement this
// -- including most unit-test stand-ins -- simply gets an uncached
// *cek.EvalContext per call via plutusEvalContext, identical to this cache's
// absence.
type PlutusEvalContextCacheProvider interface {
	PlutusEvalContextCache() *PlutusEvalContextCache
}

type plutusEvalContextEntry struct {
	once sync.Once
	ctx  *cek.EvalContext
	err  error
}

// newEvalContextFunc is a test seam: production always resolves to
// cek.NewEvalContext. Tests substitute a counting wrapper to prove
// construction happens at most once per distinct key even under concurrent
// callers (see TestPlutusEvalContextCacheBuildsOncePerKeyConcurrently).
var newEvalContextFunc = cek.NewEvalContext

// appendPlutusEvalContextKey appends the cache key to buf: a fixed-width
// header (the three language-version components, the protocol major version,
// and the synthetic-V2 flag) followed by every cost-model parameter as eight
// little-endian bytes. Every field is fixed width, so the encoding is
// injective without delimiters. The list must be reproduced exactly:
// plutigo's costModelFromList costs a parameter missing from a short list at
// maxBound, so lists differing only in length or in one element build
// different contexts, and a digest or prefix could conflate them. An exact
// key can only over-distinguish lists that build identical contexts (values
// past the name list are ignored), which costs a build, never a wrong
// context.
func appendPlutusEvalContextKey(
	buf []byte,
	version lang.LanguageVersion,
	protocolMajor uint,
	syntheticV2 bool,
	costModelParams []int64,
) []byte {
	for _, v := range version {
		buf = binary.LittleEndian.AppendUint32(buf, v)
	}
	buf = binary.LittleEndian.AppendUint64(buf, uint64(protocolMajor))
	if syntheticV2 {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	for _, p := range costModelParams {
		// #nosec G115 -- a bit-preserving reinterpretation, not arithmetic
		buf = binary.LittleEndian.AppendUint64(buf, uint64(p))
	}
	return buf
}

// get returns the shared *cek.EvalContext for the given key, building it via
// newEvalContextFunc at most once per distinct key even when called
// concurrently for the same key from multiple goroutines.
func (c *PlutusEvalContextCache) get(
	version lang.LanguageVersion,
	protoVersion cek.ProtoVersion,
	costModelParams []int64,
	syntheticV2 bool,
) (*cek.EvalContext, error) {
	// Sized for the largest current cost model (PlutusV3, a few hundred
	// parameters) so a hit encodes its key without a heap allocation; the
	// m[string(b)] lookup form does not copy the key.
	var keyBuf [4096]byte
	key := appendPlutusEvalContextKey(
		keyBuf[:0],
		version,
		protoVersion.Major,
		syntheticV2,
		costModelParams,
	)
	c.mu.Lock()
	entry, ok := c.entries[string(key)]
	if !ok {
		entry = &plutusEvalContextEntry{}
		c.entries[string(key)] = entry
	}
	c.mu.Unlock()
	entry.once.Do(func() {
		entry.ctx, entry.err = newEvalContextFunc(
			version,
			protoVersion,
			costModelParams,
		)
	})
	return entry.ctx, entry.err
}

// plutusEvalContext returns the *cek.EvalContext for the given key, using
// ls's PlutusEvalContextCache when it provides one and falling back to an
// uncached construction (identical to this cache's absence) otherwise.
func plutusEvalContext(
	ls lcommon.LedgerState,
	version lang.LanguageVersion,
	protoVersion cek.ProtoVersion,
	costModelParams []int64,
	syntheticV2 bool,
) (*cek.EvalContext, error) {
	if provider, ok := ls.(PlutusEvalContextCacheProvider); ok {
		if cache := provider.PlutusEvalContextCache(); cache != nil {
			return cache.get(
				version,
				protoVersion,
				costModelParams,
				syntheticV2,
			)
		}
	}
	return newEvalContextFunc(version, protoVersion, costModelParams)
}
