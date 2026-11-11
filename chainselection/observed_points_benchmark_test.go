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

package chainselection

import (
	"encoding/binary"
	"runtime"
	"testing"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

const (
	observedBenchK      = 1000
	observedBenchWindow = 5000
)

type observedBenchFixture struct {
	*eagernessDeliveryFixture
	next uint64
}

// observedBenchConn is built once: a ConnectionId holds address pointers, so
// building one per header would register a new peer for every header.
var observedBenchConn = corrConn(101)

func observedBenchHash(number uint64) []byte {
	hash := make([]byte, 32)
	binary.BigEndian.PutUint64(hash, number)
	return hash
}

func observedBenchUpdate(
	f *observedBenchFixture,
	number uint64,
) PeerTipUpdateEvent {
	tip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: number, Hash: observedBenchHash(number)},
		BlockNumber: number,
	}
	f.sequence++
	return PeerTipUpdateEvent{
		ConnectionId:                       observedBenchConn,
		AdmissionID:                        f.sequence,
		Tip:                                tip,
		ObservedTip:                        tip,
		ObservedPrevHash:                   observedBenchHash(number - 1),
		HeaderCryptoVerificationGeneration: f.generation.Load(),
	}
}

func newObservedBenchFixture(k, window uint64) *observedBenchFixture {
	f := &eagernessDeliveryFixture{}
	f.generation.Store(1)
	f.selector = NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         k,
		GenesisWindowSlots:    window,
		MinCorroboratingPeers: 1,
		CurrentHeaderCryptoVerificationGeneration: f.generation.Load,
		HeaderCryptoVerificationGenerationCurrent: func(g uint64) bool {
			return g == f.generation.Load()
		},
		PinHeaderCryptoVerificationGeneration: func(g uint64) (func(), bool) {
			return func() {}, g == f.generation.Load()
		},
	})
	return &observedBenchFixture{eagernessDeliveryFixture: f}
}

// deliverObservedBenchHeader runs one header through the per-header path:
// stage, record delivery, commit.
func deliverObservedBenchHeader(
	tb testing.TB,
	f *observedBenchFixture,
	number uint64,
) {
	tb.Helper()
	update := observedBenchUpdate(f, number)
	if d := f.selector.PreparePeerTipAdmissionDisposition(update); d != PeerTipUpdateAccepted {
		tb.Fatalf("prepare header %d: disposition %v", number, d)
	}
	f.selector.RecordDeliveredPeerTip(update)
	if !f.selector.CommitPeerTipAdmission(update) {
		tb.Fatalf("commit rejected header %d", number)
	}
}

// fill delivers headers until the point frontier is at capacity, then keeps
// going a little further so the oldest points are being trimmed.
func (f *observedBenchFixture) fill(tb testing.TB) {
	tb.Helper()
	capacity := f.selector.candidatePointCapacityLocked(true)
	for f.next < capacity+10 {
		f.next++
		deliverObservedBenchHeader(tb, f, f.next)
	}
}

func (f *observedBenchFixture) deliverNext(tb testing.TB) {
	tb.Helper()
	f.next++
	deliverObservedBenchHeader(tb, f, f.next)
}

// BenchmarkObservedPointFrontierPerHeader feeds a long run of consecutive
// headers through Genesis-mode staging, delivery recording and commit with the
// observed-point frontier already at capacity.
func BenchmarkObservedPointFrontierPerHeader(b *testing.B) {
	f := newObservedBenchFixture(observedBenchK, observedBenchWindow)
	f.fill(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		f.deliverNext(b)
	}
}

// measureObservedPointFrontierAllocs returns allocated objects and bytes per
// header. Bytes are needed as well as objects: a regression that copies the
// retained frontier into one new array per header allocates few objects.
func measureObservedPointFrontierAllocs(
	tb testing.TB,
	k, window uint64,
) (objects float64, bytes float64) {
	f := newObservedBenchFixture(k, window)
	f.fill(tb)
	const runs = 50
	objects = testing.AllocsPerRun(runs, func() { f.deliverNext(tb) })
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		f.deliverNext(tb)
	}
	runtime.ReadMemStats(&after)
	return objects, float64(after.TotalAlloc-before.TotalAlloc) / runs
}

// Not t.Parallel: testing.AllocsPerRun and runtime.MemStats are process-wide
// measurements.
func TestObservedPointFrontierAllocsDoNotScaleWithHistory(t *testing.T) {
	smallObjects, smallBytes := measureObservedPointFrontierAllocs(t, 20, 100)
	largeObjects, largeBytes := measureObservedPointFrontierAllocs(t, 1000, 5000)
	require.LessOrEqual(t, largeObjects, smallObjects+10,
		"per-header allocated objects must not grow with the retained observed points")
	require.LessOrEqual(t, largeBytes, 2*smallBytes+4096,
		"per-header allocated bytes must not grow with the retained observed points")
}
