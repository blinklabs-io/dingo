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
	"time"

	"github.com/stretchr/testify/assert"
)

// TestUTxOVerdict pins the fix for a Koios outage during tx_info
// reconstruction being reported as a false "utxo set match" instead of
// "not run" (human review, dingo#4319): utxoTaintedThisEpoch was closure
// state inside RunFromGenesis's 500+ line function literal, with nothing
// asserting on it directly. utxoVerdict is that decision, lifted out so it
// is directly testable: reverting its tainted case in place (returning
// utxoVerdictCompare instead) would make the first subtest below fail.
func TestUTxOVerdict(t *testing.T) {
	someRefs := UTxOSet{"deadbeef#0": "addr|100|||"}

	t.Run("tainted always wins, even with refs available", func(t *testing.T) {
		mode, err := utxoVerdict(true, someRefs)
		assert.Equal(t, utxoVerdictTainted, mode)
		assert.ErrorIs(t, err, errUTxOTainted)
	})

	t.Run("refs available and not tainted: compare", func(t *testing.T) {
		mode, err := utxoVerdict(false, someRefs)
		assert.Equal(t, utxoVerdictCompare, mode)
		assert.NoError(t, err)
	})

	t.Run("no refs, not tainted: no baseline", func(t *testing.T) {
		mode, err := utxoVerdict(false, nil)
		assert.Equal(t, utxoVerdictNoBaseline, mode)
		assert.NoError(t, err)
	})
}

// TestNextSessionRetryDelay pins RunFromGenesis's reconnect-loop backoff
// arithmetic: this is the fix for a from-genesis run that died outright,
// 129 clean epochs in and zero real mismatches, on a one-off transient
// "connection shutdown initiated: EOF" from currentEpochNo -- a session
// error unrelated to any real UTxO/stake/protocol-params divergence, which
// RunFromGenesis's reconnect loop now retries indefinitely (reconnecting
// and resuming from the last processed point) instead of treating as fatal.
func TestNextSessionRetryDelay(t *testing.T) {
	const (
		base = 1 * time.Second
		max  = 30 * time.Second
	)

	t.Run("no progress doubles the delay", func(t *testing.T) {
		sleepFor, next := nextSessionRetryDelay(base, false, base, max)
		assert.Equal(t, base, sleepFor,
			"the attempt that just failed should sleep for its own delay, "+
				"not the doubled one")
		assert.Equal(t, 2*base, next,
			"the following attempt should back off further")
	})

	t.Run("repeated failure without progress grows toward the cap", func(t *testing.T) {
		delay := base
		for range 10 {
			_, delay = nextSessionRetryDelay(delay, false, base, max)
		}
		assert.Equal(t, max, delay,
			"backoff must not exceed maxDelay however many times it doubles")
	})

	t.Run("progress resets the backoff to base", func(t *testing.T) {
		// A session that ran for a while (grown delay from prior failures)
		// but then made real progress before failing again must not keep
		// the grown delay -- an occasional hiccup in an otherwise-healthy
		// run should reconnect quickly, not slowly.
		grown := 16 * time.Second
		sleepFor, next := nextSessionRetryDelay(grown, true, base, max)
		assert.Equal(t, base, sleepFor,
			"progress must reset the delay actually slept for, not just "+
				"the following one")
		assert.Equal(t, 2*base, next)
	})

	t.Run("progress at the cap still resets to base", func(t *testing.T) {
		sleepFor, next := nextSessionRetryDelay(max, true, base, max)
		assert.Equal(t, base, sleepFor)
		assert.Equal(t, 2*base, next)
	})
}
