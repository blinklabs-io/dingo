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

package tracing

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Config selects which blocks are traced. Every node given the same Config
// makes the same decision for the same block, which is what lets their spans
// join one trace.
type Config struct {
	// SampleRatio is the fraction of blocks traced, from 0 to 1, decided on
	// the block's hash-derived trace ID exactly as the SDK's
	// TraceIDRatioBased sampler decides on a trace ID.
	SampleRatio float64
	// AlwaysSamplePools lists issuer key hashes (pool IDs) whose blocks are
	// traced regardless of SampleRatio.
	AlwaysSamplePools [][28]byte
}

type blockConfig struct {
	ratioBound uint64
	all        bool
	pools      map[[28]byte]struct{}
}

var (
	state   atomic.Pointer[blockConfig]
	nearTip atomic.Bool
	forced  = newForcedSet(forcedSetSize)
)

// forcedSetSize bounds the memory of pool-forced decisions. A rollback that
// reaches further back than this many forced blocks falls back to the ratio
// decision, which can only drop the rollback span, never misplace it.
const forcedSetSize = 4096

// Configure enables block tracing with cfg. Until it is called, StartBlock
// and StartTx return non-recording spans without touching the tracer.
func Configure(cfg Config) {
	bc := &blockConfig{
		all:   cfg.SampleRatio >= 1,
		pools: make(map[[28]byte]struct{}, len(cfg.AlwaysSamplePools)),
	}
	if cfg.SampleRatio > 0 && !bc.all {
		bc.ratioBound = uint64(cfg.SampleRatio * (1 << 63))
	}
	for _, p := range cfg.AlwaysSamplePools {
		bc.pools[p] = struct{}{}
	}
	state.Store(bc)
}

// Enabled reports whether block tracing is configured, so a caller can skip
// collecting blocks for spans that would never start.
func Enabled() bool {
	return state.Load() != nil
}

// Disable turns block tracing off again; StartBlock and StartTx return
// non-recording spans until the next Configure.
func Disable() {
	state.Store(nil)
}

// SetNearTip records whether the node is near the upstream tip. While it is
// not, only core stages (apply, commit, rollback, forge) are traced, so a
// sync from genesis produces a reduced span set per sampled block.
func SetNearTip(near bool) {
	nearTip.Store(near)
}

// Stage identifies a step in a block's life on one node.
type Stage uint8

const (
	StageHeaderReceived Stage = iota
	StageHeaderValidated
	StageBodyReceived
	StageApply
	StageCommit
	StageRollback
	StageForge
)

func (s Stage) core() bool {
	switch s {
	case StageApply, StageCommit, StageRollback, StageForge:
		return true
	case StageHeaderReceived, StageHeaderValidated, StageBodyReceived:
		return false
	}
	return false
}

// BlockTraceID returns the trace ID every node uses for the block with the
// given hash: its first 16 bytes, zero-padded when the hash is shorter.
func BlockTraceID(hash []byte) trace.TraceID {
	var id trace.TraceID
	copy(id[:], hash)
	return id
}

// blockRootSpanID is the span ID of the synthetic remote parent every stage
// span of a block hangs from. It is never exported, so backends show the
// stages of all nodes as siblings under one missing root.
func blockRootSpanID(hash []byte) trace.SpanID {
	var id trace.SpanID
	if len(hash) > 16 {
		copy(id[:], hash[16:])
	}
	if !id.IsValid() {
		id[7] = 1
	}
	return id
}

// BlockSampled reports whether the block with the given hash is traced.
// issuerKeyHash may be nil when the caller does not know the issuer, as for
// a rollback or a Byron block; a pool-forced decision made earlier for the
// same hash is then still honored.
func BlockSampled(hash []byte, issuerKeyHash []byte) bool {
	bc := state.Load()
	if bc == nil {
		return false
	}
	return bc.sampled(hash, issuerKeyHash)
}

func (bc *blockConfig) sampled(hash, issuerKeyHash []byte) bool {
	if bc.ratioSampled(BlockTraceID(hash)) {
		return true
	}
	if issuerKeyHash == nil {
		return forced.contains(hash)
	}
	if len(issuerKeyHash) == 28 {
		if _, ok := bc.pools[[28]byte(issuerKeyHash)]; ok {
			forced.add(hash)
			return true
		}
	}
	return false
}

// ratioSampled mirrors sdktrace.TraceIDRatioBased so a block-derived trace
// ID and an ordinary trace ID are judged by the same rule.
func (bc *blockConfig) ratioSampled(id trace.TraceID) bool {
	if bc.all {
		return true
	}
	return binary.BigEndian.Uint64(id[8:16])>>1 < bc.ratioBound
}

// StartBlock starts a span for one stage of a block's life, in the trace
// derived from the block's hash. The returned span is non-recording, and
// nothing is allocated, when block tracing is off, the block is not
// sampled, or the node is catching up and stage is not a core stage.
func StartBlock(
	ctx context.Context,
	stage Stage,
	name string,
	hash []byte,
	issuerKeyHash []byte,
	opts ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	bc := state.Load()
	if bc == nil || (!stage.core() && !nearTip.Load()) ||
		!bc.sampled(hash, issuerKeyHash) {
		return ctx, trace.SpanFromContext(context.Background())
	}
	return startUnder(ctx, name, hash, opts)
}

// StartHeader is StartBlock for a header or block. It reads the hash only
// once block tracing is on and the stage is wanted, and hashes the issuer
// key only when the pool list could change the decision, so the disabled
// and ratio-decided paths cost no hashing.
func StartHeader(
	ctx context.Context,
	stage Stage,
	name string,
	h lcommon.BlockHeader,
	opts ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	bc := state.Load()
	if bc == nil || (!stage.core() && !nearTip.Load()) {
		return ctx, trace.SpanFromContext(context.Background())
	}
	hash := h.Hash().Bytes()
	sampled := bc.ratioSampled(BlockTraceID(hash))
	if !sampled && len(bc.pools) > 0 {
		sampled = bc.sampled(hash, IssuerKeyHash(h))
	}
	if !sampled {
		return ctx, trace.SpanFromContext(context.Background())
	}
	return startUnder(ctx, name, hash, opts)
}

// StartTx starts a span in the trace derived from a transaction's hash,
// sampled by ratio alone.
func StartTx(
	ctx context.Context,
	name string,
	txHash []byte,
	opts ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	bc := state.Load()
	if bc == nil || !bc.ratioSampled(BlockTraceID(txHash)) {
		return ctx, trace.SpanFromContext(context.Background())
	}
	return startUnder(ctx, name, txHash, opts)
}

func startUnder(
	ctx context.Context,
	name string,
	hash []byte,
	opts []trace.SpanStartOption,
) (context.Context, trace.Span) {
	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    BlockTraceID(hash),
		SpanID:     blockRootSpanID(hash),
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	//nolint:spancheck // the caller ends the returned span
	return otel.Tracer(Name).Start(
		trace.ContextWithRemoteSpanContext(ctx, parent),
		name,
		opts...,
	)
}

// IssuerKeyHash returns the pool key hash of a header's issuer, or nil for a
// Byron header, whose issuer is not a stake pool.
func IssuerKeyHash(h lcommon.BlockHeader) []byte {
	if h.Era().Id == 0 {
		return nil
	}
	return h.IssuerVkey().Hash().Bytes()
}

// forcedSet remembers recent block hashes sampled only because of the pool
// list, in a fixed-size ring.
type forcedSet struct {
	mu   sync.Mutex
	set  map[string]struct{}
	ring []string
	next int
}

func newForcedSet(size int) *forcedSet {
	return &forcedSet{
		set:  make(map[string]struct{}, size),
		ring: make([]string, size),
	}
}

func (f *forcedSet) add(hash []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(hash)
	if _, ok := f.set[key]; ok {
		return
	}
	if old := f.ring[f.next]; old != "" {
		delete(f.set, old)
	}
	f.ring[f.next] = key
	f.set[key] = struct{}{}
	f.next = (f.next + 1) % len(f.ring)
}

func (f *forcedSet) contains(hash []byte) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.set[string(hash)]
	return ok
}
