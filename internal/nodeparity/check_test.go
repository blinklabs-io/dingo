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
	"github.com/stretchr/testify/require"
)

// TestTipsAgree_Match covers the clean case: both nodes agree on a tip.
// tipsAgree must report this as trustworthy (ok=true) with no skip reason
// or detail, so a real comparison can proceed.
func TestTipsAgree_Match(t *testing.T) {
	t.Parallel()

	tip := Tip{Slot: 100, Hash: "aa", BlockNumber: 10}
	ok, reason, detail := tipsAgree(tip, tip)
	assert.True(t, ok)
	assert.Empty(t, reason)
	assert.Empty(t, detail)
}

// TestTipsAgree_Mismatch covers the case where dingo and cardano-node are
// on different tips. tipsAgree must refuse to proceed (there is no single
// point left to acquire on both connections) and report it with the
// SkipTipMismatch reason code.
func TestTipsAgree_Mismatch(t *testing.T) {
	t.Parallel()

	dingo := Tip{Slot: 100, Hash: "aa"}
	cardano := Tip{Slot: 105, Hash: "bb"}
	ok, reason, detail := tipsAgree(dingo, cardano)
	assert.False(
		t,
		ok,
		"must not report a match when the two nodes never agreed on a tip",
	)
	assert.Equal(t, SkipTipMismatch, reason)
	assert.Contains(t, detail, "tips did not match")
}

// TestTip_SameHashDifferentSlotIsNotEqual guards Tip.Equal's
// definition of "same point on chain": slot and hash must both agree. A
// coincidental hash match at a different slot is not realistic on a real
// chain, but Tip.Equal must not treat it as equal regardless.
func TestTip_SameHashDifferentSlotIsNotEqual(t *testing.T) {
	t.Parallel()

	a := Tip{Slot: 100, Hash: "aa"}
	b := Tip{Slot: 200, Hash: "aa"}
	assert.False(t, a.Equal(b))
}

// TestTip_Equal covers Tip.Equal directly: two tips with the same slot and
// hash are equal even if BlockNumber differs (it is not part of a tip's
// identity, only slot+hash are), and a tip with a different hash is not
// equal regardless of slot.
func TestTip_Equal(t *testing.T) {
	t.Parallel()

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

// TestTip_Point covers the conversion Check relies on to build the
// AcquireSpecificPoint argument from an agreed Tip: Slot passes through
// unchanged and Hash round-trips through hex decoding back to the same raw
// bytes.
func TestTip_Point(t *testing.T) {
	t.Parallel()

	tip := Tip{Slot: 12345, Hash: "aabbcc", BlockNumber: 7}
	point, err := tip.point()
	require.NoError(t, err)
	assert.Equal(t, uint64(12345), point.Slot)
	assert.Equal(t, []byte{0xaa, 0xbb, 0xcc}, point.Hash)
}

// TestTip_Point_InvalidHashErrors covers a malformed (non-hex) Hash: point
// must surface a decode error rather than silently producing a wrong or
// truncated byte slice that would then be sent to a real node as part of an
// AcquireSpecificPoint.
func TestTip_Point_InvalidHashErrors(t *testing.T) {
	t.Parallel()

	tip := Tip{Slot: 1, Hash: "not-hex"}
	_, err := tip.point()
	require.Error(t, err)
}
