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
	"errors"
	"testing"
	"time"

	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
)

// TestResolveStartPoint pins from-genesis's resume-from-point behavior
// (dingo#1900 follow-up): a killed or restarted process has no on-disk
// checkpoint of its own, so from-genesis --at-slot/--at-hash lets a caller
// that already trusts a prior run's epochs resume from that point instead
// of Origin. Reverting resolveStartPoint to always return
// pcommon.NewPointOrigin() would make the second and third subtests fail.
func TestResolveStartPoint(t *testing.T) {
	t.Run("nil resumeFrom defaults to Origin", func(t *testing.T) {
		got, err := resolveStartPoint(nil)
		require.NoError(t, err)
		assert.Equal(t, pcommon.NewPointOrigin(), got)
	})

	t.Run("valid resumeFrom resolves to that exact point", func(t *testing.T) {
		got, err := resolveStartPoint(&Tip{
			Slot: 55784796,
			Hash: "e18e33065526668044300543396ef5259764dafe4159e7038b4148dd77c4bc91",
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(55784796), got.Slot)
		assert.NotEqual(t, pcommon.NewPointOrigin(), got)
	})

	t.Run("invalid hash is rejected immediately, not left to fail later", func(t *testing.T) {
		_, err := resolveStartPoint(&Tip{
			Slot: 100,
			Hash: "not-hex",
		})
		require.Error(t, err)
	})
}

// TestUTxOVerdict pins the fix for a Koios outage during tx_info
// reconstruction being reported as a false "utxo set match" instead of
// "not run": utxoTaintedThisEpoch was closure state inside RunFromGenesis's
// 500+ line function literal, with nothing asserting on it directly.
// utxoVerdict is that decision, lifted out so it is directly testable:
// reverting its tainted case in place (returning utxoVerdictCompare
// instead) would make the first subtest below fail.
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

// TestApplyTxInfoResults pins flushPendingTxInfos's actual failure
// decision -- the one that sets utxoTaintedThisEpoch -- not just
// utxoVerdict, which only reads that flag: TestUTxOVerdict alone doesn't
// prove a tx_info fetch failure is what makes utxoTaintedThisEpoch true in
// the first place. Reverting applyTxInfoResults to always return false (as
// if every chunk always succeeded) would make the "one chunk fails" subtest
// below fail.
func TestApplyTxInfoResults(t *testing.T) {
	noopLogf := func(string, ...any) {}

	t.Run("all chunks succeed: no failure, changes applied", func(t *testing.T) {
		refs := UTxOSet{"spent#0": "addr|100|||"}
		chunks := [][]string{{"tx1"}}
		results := [][]koiosparity.KoiosTxInfoItem{
			{{
				TxHash:  "tx1",
				Inputs:  []koiosparity.KoiosTxInfoUtxoRef{{TxHash: "spent", TxIndex: 0}},
				Outputs: []koiosparity.KoiosTxInfoOutput{{TxHash: "tx1", TxIndex: 0}},
			}},
		}
		errs := []error{nil}

		failed := applyTxInfoResults(refs, chunks, results, errs, noopLogf)
		assert.False(t, failed)
		_, stillPresent := refs["spent#0"]
		assert.False(t, stillPresent, "a successful chunk's spend must be applied")
		_, created := refs["tx1#0"]
		assert.True(t, created, "a successful chunk's new output must be applied")
	})

	t.Run("one chunk fails: reported failed, its own changes not applied, others still are", func(t *testing.T) {
		refs := UTxOSet{}
		chunks := [][]string{{"tx1"}, {"tx2"}}
		results := [][]koiosparity.KoiosTxInfoItem{
			nil, // tx1's chunk failed -- no results for it
			{{TxHash: "tx2", Outputs: []koiosparity.KoiosTxInfoOutput{{TxHash: "tx2", TxIndex: 0}}}},
		}
		errs := []error{errors.New("koios stalled"), nil}

		failed := applyTxInfoResults(refs, chunks, results, errs, noopLogf)
		assert.True(t, failed,
			"a single failed chunk must taint the whole flush, even if other chunks succeeded")
		_, created := refs["tx2#0"]
		assert.True(t, created, "an independently-succeeding chunk's changes must still be applied")
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
