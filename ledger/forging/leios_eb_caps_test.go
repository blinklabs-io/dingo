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

package forging

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestBuildLeiosEBRespectsRefCap bounds the endorser block independently of
// the clock. The deadline is the operative bound in normal operation, but a
// stalled or unavailable slot clock must not let manifest construction
// scale without limit with mempool depth.
func TestBuildLeiosEBRespectsRefCap(t *testing.T) {
	txs := leiosCandidateTxs(t, 10)
	ebCbor, _, bodies, err := buildLeiosEB(txs, leiosEBCaps{maxRefs: 4})
	require.NoError(t, err)
	require.Len(t, bodies, 4)

	eb, err := lcommon.NewLeiosEndorserBlockFromCbor(ebCbor)
	require.NoError(t, err)
	require.Len(t, eb.TransactionReferences, 4)
}

// TestBuildLeiosEBRespectsByteCap bounds total referenced payload rather
// than reference count, which is what actually has to cross the wire.
func TestBuildLeiosEBRespectsByteCap(t *testing.T) {
	txs := leiosCandidateTxs(t, 10)
	// Room for exactly three transactions.
	capBytes := uint64(3 * len(txs[0].Cbor))
	_, _, bodies, err := buildLeiosEB(txs, leiosEBCaps{maxBytes: capBytes})
	require.NoError(t, err)
	require.Len(t, bodies, 3)
}

// TestBuildLeiosEBZeroCapsAreUnlimited keeps the zero value meaning what it
// meant before caps existed.
func TestBuildLeiosEBZeroCapsAreUnlimited(t *testing.T) {
	txs := leiosCandidateTxs(t, 10)
	_, _, bodies, err := buildLeiosEB(txs, leiosEBCaps{})
	require.NoError(t, err)
	require.Len(t, bodies, 10)
}

// TestCheckAndForgeProductionAppliesEBRefCap follows the runtime path from
// the forger config into manifest construction.
func TestCheckAndForgeProductionAppliesEBRefCap(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	leiosCaster := &forgerTestLeiosCaster{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &ebTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Hour),
		},
		LeiosProduceChecker: &forgerTestLeiosChecker{allowed: true},
		LeiosEBBroadcaster:  leiosCaster,
		LeiosTxValidator:    &sessionMockTxValidator{},
		LeiosMempool: forgerTestMempoolProvider{
			txs: leiosCandidateTxs(t, 12),
		},
		ForgeEBMaxTxRefs: capPtr(5),
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, leiosCaster.txBodies, 5)
}

// TestBuildLeiosEBBoundsPreallocation keeps the cap a bound on work and
// memory, not only on the emitted manifest. Preallocating for the whole
// mempool would let a deep mempool dictate allocation even when the cap
// admits a handful of references.
func TestBuildLeiosEBBoundsPreallocation(t *testing.T) {
	txs := leiosCandidateTxs(t, 500)
	_, _, bodies, err := buildLeiosEB(txs, leiosEBCaps{maxRefs: 3})
	require.NoError(t, err)
	require.Len(t, bodies, 3)
	require.LessOrEqual(
		t,
		cap(bodies),
		16,
		"capacity must follow the cap, not the mempool depth",
	)
}

// TestNewBlockForgerAppliesEBCapDefaults covers the embedder path: a
// ForgerConfig that never mentions the caps must still get the backstop,
// rather than silently running uncapped because the zero value means
// "disabled".
func TestNewBlockForgerAppliesEBCapDefaults(t *testing.T) {
	forger := newCapDefaultsForger(t, nil, nil)
	require.Equal(t, uint64(defaultForgeEBMaxTxRefs), forger.forgeEBMaxTxRefs)
	require.Equal(t, uint64(defaultForgeEBMaxBytes), forger.forgeEBMaxBytes)
}

// TestNewBlockForgerHonoursExplicitZeroEBCaps is the other half: an
// explicit zero disables the cap and must not be overwritten by the
// default.
func TestNewBlockForgerHonoursExplicitZeroEBCaps(t *testing.T) {
	zero := uint64(0)
	forger := newCapDefaultsForger(t, &zero, &zero)
	require.Zero(t, forger.forgeEBMaxTxRefs)
	require.Zero(t, forger.forgeEBMaxBytes)
}

func capPtr(v uint64) *uint64 { return &v }

func newCapDefaultsForger(
	t *testing.T,
	refs *uint64,
	bytes *uint64,
) *BlockForger {
	t.Helper()
	block := newForgerTestBlock(10, 2)
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{block: block, cbor: block.cbor},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &ebTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Hour),
		},
		ForgeEBMaxTxRefs: refs,
		ForgeEBMaxBytes:  bytes,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger
}

// TestForgeEBCapDefaultsArePinned is the ledger/forging half of the
// drift guard. internal/config declares the same two numbers for its
// yaml/env defaults and cannot import this package, so both sides pin the
// literals; a change to one without the other fails here.
func TestForgeEBCapDefaultsArePinned(t *testing.T) {
	require.Equal(t, uint64(20000), defaultForgeEBMaxTxRefs)
	require.Equal(t, uint64(25165824), defaultForgeEBMaxBytes)
}
