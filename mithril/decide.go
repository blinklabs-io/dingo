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

package mithril

// MithrilHysteresis is how much faster than Ouroboros Mithril must have been
// measured before Decide prefers it. Without a margin, two sources measuring
// about the same rate would make a node switch back and forth every check.
const MithrilHysteresis = 1.25

// DecisionInput is what Decide weighs.
type DecisionInput struct {
	// LocalImmutable is the immutable file holding the local ledger tip.
	LocalImmutable uint64
	// Target is the aggregator's certified target. Only Target.Immutable is
	// read: LatestCertifiedTarget leaves Tip zero.
	Target CertifiedTarget
	// OuroborosBPS is the blocks per second Ouroboros applied over the last
	// check interval. Zero means unknown.
	OuroborosBPS float64
	// MithrilBPS is the blocks per second the last Mithril catch-up applied.
	// Zero means unknown.
	MithrilBPS float64
	// MinGapChunks is the smallest certified gap, in immutable files, worth
	// a catch-up. Zero is treated as one: any gap at all.
	MinGapChunks uint64
}

// Decide reports whether the node should catch up from Mithril rather than
// Ouroboros. The gap is Target.Immutable minus LocalImmutable. A gap below
// MinGapChunks never uses Mithril. A gap that is large enough uses it when
// either rate is unknown, and otherwise only when Mithril measured more than
// MithrilHysteresis times the Ouroboros rate.
func Decide(in DecisionInput) bool {
	minGap := max(in.MinGapChunks, 1)
	if in.Target.Immutable < in.LocalImmutable ||
		in.Target.Immutable-in.LocalImmutable < minGap {
		return false
	}
	if in.OuroborosBPS <= 0 || in.MithrilBPS <= 0 {
		return true
	}
	return in.MithrilBPS > MithrilHysteresis*in.OuroborosBPS
}

// ImmutableFileForSlot estimates the immutable file holding slot when every
// file spans chunkSlots slots, as the ImmutableDB lays out every Cardano
// network: the Byron epoch length, 10*k. ok is false when chunkSlots is zero,
// which gives no estimate.
func ImmutableFileForSlot(slot, chunkSlots uint64) (file uint64, ok bool) {
	if chunkSlots == 0 {
		return 0, false
	}
	return slot / chunkSlots, true
}
