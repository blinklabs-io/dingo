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
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"sync"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

// Phase2ResultCache remembers transactions whose Plutus scripts all
// succeeded, so a node that validates one transaction several times runs its
// scripts once. Dingo validates a transaction when the mempool admits it,
// again whenever the chain moves while it is pending, when it forges a block
// holding it, and when it applies a block holding it; the last is on the path
// of block adoption.
//
// An entry is keyed by a digest of everything phase-2 evaluation reads (see
// conwayPhase2ResultKey): the transaction's bytes, the outputs its spent and
// reference inputs resolve to, the protocol version, the cost models, the
// transaction execution-unit limit, the synthetic-V2 flag, and the validity
// interval translated to POSIX time. Evaluation is a pure function of those,
// so an entry never needs invalidating: a change to any of them changes the
// key. Only success is cached, because a failure may come from the ledger
// state rather than from the scripts.
//
// The cache holds two generations of at most phase2ResultCacheGeneration
// keys each. A lookup that hits the older generation moves the key to the
// newer one, and filling the newer one discards the older, so the cache keeps
// at least the most recent phase2ResultCacheGeneration keys in use.
type Phase2ResultCache struct {
	mu       sync.Mutex
	current  map[phase2ResultKey]struct{}
	previous map[phase2ResultKey]struct{}
}

type phase2ResultKey [sha256.Size]byte

// phase2ResultCacheGeneration bounds each generation. A key costs about 50
// bytes, so the cache stays under half a megabyte while covering a full
// mempool many times over.
const phase2ResultCacheGeneration = 4096

// NewPhase2ResultCache returns an empty cache ready for use.
func NewPhase2ResultCache() *Phase2ResultCache {
	return &Phase2ResultCache{
		current: make(map[phase2ResultKey]struct{}),
	}
}

// Phase2ResultCacheProvider is implemented by LedgerState implementations
// that hold a Phase2ResultCache spanning many validations (see
// ledger.LedgerView.Phase2ResultCache). A LedgerState that does not
// implement it evaluates every transaction's scripts.
type Phase2ResultCacheProvider interface {
	Phase2ResultCache() *Phase2ResultCache
}

func phase2ResultCacheFor(ls lcommon.LedgerState) *Phase2ResultCache {
	provider, ok := stateCapability[Phase2ResultCacheProvider](ls)
	if !ok {
		return nil
	}
	return provider.Phase2ResultCache()
}

// contains reports whether key is cached. A nil cache holds nothing.
func (c *Phase2ResultCache) contains(key phase2ResultKey) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.current[key]; ok {
		return true
	}
	if _, ok := c.previous[key]; ok {
		c.addLocked(key)
		return true
	}
	return false
}

// add caches key. Adding to a nil cache does nothing.
func (c *Phase2ResultCache) add(key phase2ResultKey) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addLocked(key)
}

func (c *Phase2ResultCache) addLocked(key phase2ResultKey) {
	if _, ok := c.current[key]; ok {
		return
	}
	if len(c.current) >= phase2ResultCacheGeneration {
		c.previous = c.current
		c.current = make(
			map[phase2ResultKey]struct{},
			phase2ResultCacheGeneration,
		)
	}
	c.current[key] = struct{}{}
}

// phase2KeyDomainConway separates Conway keys from any other era's, whose
// evaluation rules differ for the same inputs.
const phase2KeyDomainConway = "dingo/phase2/conway/v1"

// conwayPhase2ResultKey returns the cache key for evaluating tx's scripts
// under Conway rules, and false when some input to evaluation has no stable
// encoding, in which case the result must not be cached.
//
// resolvedInputs are the spent and reference inputs, as the script context
// sees them. A transaction input names one output forever, so hashing the
// outputs is redundant in principle; it keeps the key correct even if a
// resolution path returned something else.
func conwayPhase2ResultKey(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	pp *conway.ConwayProtocolParameters,
	resolvedInputs []lcommon.Utxo,
	syntheticV2 bool,
) (phase2ResultKey, bool) {
	txCbor := tx.Cbor()
	if len(txCbor) == 0 {
		return phase2ResultKey{}, false
	}
	h := sha256.New()
	var buf [8]byte
	writeUint := func(v uint64) {
		binary.LittleEndian.PutUint64(buf[:], v)
		h.Write(buf[:])
	}
	writeBytes := func(b []byte) {
		writeUint(uint64(len(b)))
		h.Write(b)
	}
	writeBytes([]byte(phase2KeyDomainConway))
	writeBytes(txCbor)

	writeUint(uint64(len(resolvedInputs)))
	for _, utxo := range resolvedInputs {
		if utxo.Id == nil || utxo.Output == nil {
			return phase2ResultKey{}, false
		}
		outputCbor := utxo.Output.Cbor()
		if len(outputCbor) == 0 {
			return phase2ResultKey{}, false
		}
		id := utxo.Id.Id()
		writeBytes(id[:])
		writeUint(uint64(utxo.Id.Index()))
		writeBytes(outputCbor)
	}

	writeUint(uint64(pp.ProtocolVersion.Major))
	writeUint(uint64(pp.ProtocolVersion.Minor))
	// #nosec G115 -- a bit-preserving reinterpretation, not arithmetic
	writeUint(uint64(pp.MaxTxExUnits.Memory))
	// #nosec G115 -- a bit-preserving reinterpretation, not arithmetic
	writeUint(uint64(pp.MaxTxExUnits.Steps))
	if syntheticV2 {
		writeUint(1)
	} else {
		writeUint(0)
	}
	languages := make([]uint, 0, len(pp.CostModels))
	for language := range pp.CostModels {
		languages = append(languages, language)
	}
	slices.Sort(languages)
	writeUint(uint64(len(languages)))
	for _, language := range languages {
		params := pp.CostModels[language]
		writeUint(uint64(language))
		writeUint(uint64(len(params)))
		for _, p := range params {
			// #nosec G115 -- a bit-preserving reinterpretation, not arithmetic
			writeUint(uint64(p))
		}
	}

	// The script context carries the validity interval in POSIX time, which
	// depends on the era history as well as on the transaction.
	for _, bound := range []func(lcommon.TransactionBody) (uint64, bool){
		lcommon.TransactionValidityIntervalLowerBound,
		lcommon.TransactionValidityIntervalUpperBound,
	} {
		slot, present := bound(tx)
		if !present {
			writeUint(0)
			continue
		}
		t, err := ls.SlotToTime(slot)
		if err != nil {
			return phase2ResultKey{}, false
		}
		writeUint(1)
		// #nosec G115 -- a bit-preserving reinterpretation, not arithmetic
		writeUint(uint64(t.UnixMilli()))
	}

	var key phase2ResultKey
	h.Sum(key[:0])
	return key, true
}
