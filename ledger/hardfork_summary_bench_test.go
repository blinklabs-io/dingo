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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// benchEraSpan describes a contiguous run of epochs for one era in the
// synthetic benchmark cache built by buildBenchEpochCache.
type benchEraSpan struct {
	eraID         uint
	slotLengthMs  uint
	lengthInSlots uint
	epochs        int
}

// mainnetLikeEraSpans approximates real mainnet/Preview era history up to
// Conway: a fixed, small number of epochs per pre-Conway era, matching how
// long each era actually ran before the next hard fork. Any benchmark cache
// size beyond this fixed prefix is appended entirely to Conway (the last
// span), which is what a real, still-syncing chain looks like: a handful of
// historical eras followed by one long-running current era.
var mainnetLikeEraSpans = []benchEraSpan{
	{eras.ByronEraDesc.Id, 20_000, 21_600, 74}, // Byron: ~5-day epochs
	{eras.ShelleyEraDesc.Id, 1_000, 432_000, 40},
	{eras.AllegraEraDesc.Id, 1_000, 432_000, 1},
	{eras.MaryEraDesc.Id, 1_000, 432_000, 15},
	{eras.AlonzoEraDesc.Id, 1_000, 432_000, 25},
	{eras.BabbageEraDesc.Id, 1_000, 432_000, 40},
	{eras.ConwayEraDesc.Id, 1_000, 432_000, -1}, // -1: fill remainder
}

// buildBenchEpochCache builds a synthetic, most-recent-last epoch cache of
// exactly n entries, spanning eras.Eras in order per mainnetLikeEraSpans. This
// gives the benchmark the same shape hardForkSummaryAnchoredAt sees on a real
// chain -- a handful of era transitions near the start, then a single current
// era covering the bulk of the epochs -- without depending on any live node.
func buildBenchEpochCache(n int) []models.Epoch {
	cache := make([]models.Epoch, 0, n)
	epochID := uint64(0)
	startSlot := uint64(0)
	for _, span := range mainnetLikeEraSpans {
		count := span.epochs
		if count < 0 || len(cache)+count > n {
			count = n - len(cache)
		}
		for range count {
			cache = append(cache, models.Epoch{
				EpochId:       epochID,
				StartSlot:     startSlot,
				SlotLength:    span.slotLengthMs,
				LengthInSlots: span.lengthInSlots,
				EraId:         span.eraID,
			})
			epochID++
			startSlot += uint64(span.lengthInSlots)
		}
		if len(cache) >= n {
			break
		}
	}
	return cache
}

// newBenchLedgerState builds a bare LedgerState with a published, n-entry
// synthetic epoch cache, ready for hardForkSummaryAnchoredAt / HardForkSummary
// benchmarking. The tip sits mid-way through the final cached epoch, and
// transitionInfo is left at its zero value (TransitionUnknown), matching a
// live node that is not near a known hard fork.
func newBenchLedgerState(tb testing.TB, n int) *LedgerState {
	tb.Helper()
	cache := buildBenchEpochCache(n)
	last := cache[len(cache)-1]
	tipSlot := last.StartSlot + uint64(last.LengthInSlots)/2

	ls := &LedgerState{
		epochCache: cache,
		currentEra: eras.EraDesc{
			Id:   last.EraId,
			Name: "bench",
		},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(tb),
		},
	}
	ls.publishSnapshotsLocked()
	return ls
}

// BenchmarkHardForkSummary_SmallCache measures HardForkSummary's per-call
// cost against a ~10-epoch cache -- the size implied by issue #2093's
// original (incorrect) "O(eras) ~= 7" cost assumption.
func BenchmarkHardForkSummary_SmallCache(b *testing.B) {
	ls := newBenchLedgerState(b, 10)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ls.HardForkSummary(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHardForkSummary_RealisticCache_700 measures HardForkSummary
// against a ~700-epoch cache, matching mainnet's real epoch count today
// (mainnet is at epoch 656 as of this writing).
func BenchmarkHardForkSummary_RealisticCache_700(b *testing.B) {
	ls := newBenchLedgerState(b, 700)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ls.HardForkSummary(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHardForkSummary_RealisticCache_1500 measures HardForkSummary
// against a ~1500-epoch cache, matching (and modestly exceeding) Preview's
// real epoch count today (Preview is at epoch 1424 as of this writing) to
// give the O(epoch-count) growth headroom to show.
func BenchmarkHardForkSummary_RealisticCache_1500(b *testing.B) {
	ls := newBenchLedgerState(b, 1500)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ls.HardForkSummary(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHardForkSummary_RealisticCache_1500_PerBlock simulates the real
// per-block calling pattern instead of a tight repeat: one publish per
// iteration (as ledgerProcessBlocksFromSource does once per applied batch),
// followed by one HardForkSummary call (as headerVerificationEpoch does once
// per verified header). Before the fix this differs from the tight-repeat
// benchmark only in the publish overhead, since every call rebuilds anyway;
// after the fix it demonstrates that per-block invalidation still costs one
// full rebuild per block -- the cache amortizes only the *within-generation*
// callers (multiple transactions, multiple SlotTimeConverter calls) sharing
// that one build, not cross-block calls.
func BenchmarkHardForkSummary_RealisticCache_1500_PerBlock(b *testing.B) {
	ls := newBenchLedgerState(b, 1500)
	b.ReportAllocs()
	for b.Loop() {
		ls.Lock()
		ls.publishSnapshotsLocked()
		ls.Unlock()
		if _, err := ls.HardForkSummary(); err != nil {
			b.Fatal(err)
		}
	}
}
