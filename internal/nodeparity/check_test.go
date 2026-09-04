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

package nodeparity

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSandwichOK_TipsMatchAndHold covers the clean case: both nodes agree
// on a tip before the query and neither moves during it. sandwichOK must
// report this as trustworthy (ok=true) with no skip reason or detail, so a
// real comparison can proceed.
func TestSandwichOK_TipsMatchAndHold(t *testing.T) {
	tip := Tip{Slot: 100, Hash: "aa", BlockNumber: 10}
	ok, reason, detail := sandwichOK(tip, tip, tip, tip)
	assert.True(t, ok)
	assert.Empty(t, reason)
	assert.Empty(t, detail)
}

// TestSandwichOK_TipsNeverMatched covers the case where dingo and
// cardano-node were already on different tips before any query even ran.
// sandwichOK must refuse to proceed (a comparison built on two different
// starting points is meaningless) and report it with the SkipTipMismatch
// reason code, distinct from a mid-query advance.
func TestSandwichOK_TipsNeverMatched(t *testing.T) {
	dingo := Tip{Slot: 100, Hash: "aa"}
	cardano := Tip{Slot: 105, Hash: "bb"}
	ok, reason, detail := sandwichOK(dingo, cardano, dingo, cardano)
	assert.False(
		t,
		ok,
		"must not report a match when the two nodes never agreed on a tip",
	)
	assert.Equal(t, SkipTipMismatch, reason)
	assert.Contains(t, detail, "tips did not match")
}

// TestSandwichOK_DingoAdvancedDuringQuery covers the case where the two
// nodes agreed on a starting tip, but dingo's tip moved by the time of the
// re-check -- i.e. dingo produced or received a new block while its state
// was being queried. sandwichOK must discard the cycle (SkipTipAdvanced)
// rather than compare a dingo snapshot against a cardano-node snapshot
// that may no longer describe the same block.
func TestSandwichOK_DingoAdvancedDuringQuery(t *testing.T) {
	before := Tip{Slot: 100, Hash: "aa"}
	after := Tip{Slot: 101, Hash: "cc"}
	ok, reason, detail := sandwichOK(before, before, after, before)
	assert.False(
		t,
		ok,
		"must discard the cycle when dingo's tip moved mid-query",
	)
	assert.Equal(t, SkipTipAdvanced, reason)
	assert.Contains(t, detail, "advanced")
}

// TestSandwichOK_CardanoAdvancedDuringQuery is the mirror of
// TestSandwichOK_DingoAdvancedDuringQuery with cardano-node as the side
// that moved mid-query, confirming the discard applies symmetrically to
// either node, not just dingo.
func TestSandwichOK_CardanoAdvancedDuringQuery(t *testing.T) {
	before := Tip{Slot: 100, Hash: "aa"}
	after := Tip{Slot: 101, Hash: "cc"}
	ok, reason, detail := sandwichOK(before, before, before, after)
	assert.False(
		t,
		ok,
		"must discard the cycle when cardano-node's tip moved mid-query",
	)
	assert.Equal(t, SkipTipAdvanced, reason)
	assert.Contains(t, detail, "advanced")
}

// TestSandwichOK_SameHashDifferentSlotIsNotEqual guards Tip.Equal's
// definition of "same point on chain": slot and hash must both agree. A
// coincidental hash match at a different slot is not realistic on a real
// chain, but Tip.Equal must not treat it as equal regardless.
func TestSandwichOK_SameHashDifferentSlotIsNotEqual(t *testing.T) {
	a := Tip{Slot: 100, Hash: "aa"}
	b := Tip{Slot: 200, Hash: "aa"}
	assert.False(t, a.Equal(b))
}

// TestTip_Equal covers Tip.Equal directly: two tips with the same slot and
// hash are equal even if BlockNumber differs (it is not part of a tip's
// identity, only slot+hash are), and a tip with a different hash is not
// equal regardless of slot.
func TestTip_Equal(t *testing.T) {
	a := Tip{Slot: 100, Hash: "aa", BlockNumber: 5}
	b := Tip{
		Slot:        100,
		Hash:        "aa",
		BlockNumber: 999,
	} // BlockNumber not part of identity
	assert.True(t, a.Equal(b))

	c := Tip{Slot: 100, Hash: "bb"}
	assert.False(t, a.Equal(c))
}
