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
	"strconv"
	"strings"
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
	entries map[plutusEvalContextKey]*plutusEvalContextEntry
}

// NewPlutusEvalContextCache returns an empty cache ready for use.
func NewPlutusEvalContextCache() *PlutusEvalContextCache {
	return &PlutusEvalContextCache{
		entries: make(map[plutusEvalContextKey]*plutusEvalContextEntry),
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

type plutusEvalContextKey struct {
	version       lang.LanguageVersion
	protocolMajor uint
	syntheticV2   bool
	costModel     string
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

// encodeCostModelParams renders the full cost-model parameter list as an
// exact, collision-free map-key component. It must reproduce every element:
// plutigo's costModelFromList silently truncates a too-short list rather than
// erroring, so a digest or truncated encoding here could conflate two
// distinct lists that plutigo itself would treat differently. Each element is
// followed by a delimiter that cannot appear inside strconv.FormatInt's
// output, so no pair of distinct slices can render to the same string
// (concatenation without a delimiter can collide: [1, 23] and [12, 3] would
// both render "123").
func encodeCostModelParams(params []int64) string {
	var b strings.Builder
	for _, p := range params {
		b.WriteString(strconv.FormatInt(p, 10))
		b.WriteByte(',')
	}
	return b.String()
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
	key := plutusEvalContextKey{
		version:       version,
		protocolMajor: protoVersion.Major,
		syntheticV2:   syntheticV2,
		costModel:     encodeCostModelParams(costModelParams),
	}
	c.mu.Lock()
	entry, ok := c.entries[key]
	if !ok {
		entry = &plutusEvalContextEntry{}
		c.entries[key] = entry
	}
	c.mu.Unlock()
	entry.once.Do(func() {
		entry.ctx, entry.err = newEvalContextFunc(version, protoVersion, costModelParams)
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
			return cache.get(version, protoVersion, costModelParams, syntheticV2)
		}
	}
	return newEvalContextFunc(version, protoVersion, costModelParams)
}
