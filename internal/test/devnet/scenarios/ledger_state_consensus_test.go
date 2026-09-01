//go:build devnet && devnet_conformance

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

package scenarios

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/devnet"
	"github.com/stretchr/testify/require"
)

// TestLedgerStateConsensus is Dingo's automated cross-node ledger-state
// comparison against cardano-node (blinklabs-io/dingo#1900): it samples
// dingo-producer's and cardano-producer's ledger state (current protocol
// parameters, stake distribution, and the whole UTxO set, normalized into a
// comparable form) at several points during the run and fails with a
// diagnostic listing every divergence found.
//
// Sampling, not true per-block comparison: Dingo's LocalStateQuery server
// (ouroboros/localstatequery.go) currently answers every Acquire against
// its live tip regardless of the requested point — it has no point-specific
// ledger view yet (tracked upstream as blinklabs-io/dingo#382). That means
// Acquire(specific historical point) cannot be used to pin an exact block on
// Dingo today, so a true block-by-block replay comparison isn't possible
// yet. Instead, each sample: (1) polls both nodes' chain tips over NtN until
// they report an identical slot and block hash, (2) immediately queries both
// nodes' LocalStateQuery (AcquireVolatileTip, i.e. "current tip"), and
// (3) re-polls both tips afterward to confirm neither node advanced during
// the query round trip, discarding and retrying the sample if it did. This
// still anchors every successful sample to one exact, agreed-upon block —
// it just doesn't visit every block, since finding a settled common tip and
// running three LocalStateQuery calls per node is far more expensive than a
// chain-tip poll. Revisit this once #382 lands: Acquire(point) would let
// this walk every block, not just periodic settled samples.
func TestLedgerStateConsensus(t *testing.T) {
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config from testnet.yaml")

	endpoints := devnet.LoadEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	dingoEP := h.DingoNode()
	cardanoEP, ok := h.ReferenceNode()
	require.True(t, ok, "conformance mode must have a reference node")

	h.WaitForAllNodesReady(60 * time.Second)

	dingoNtc := devnet.DingoProducerNtcAddr()
	cardanoNtc := devnet.CardanoProducerNtcAddr()

	initialTip, err := h.GetChainTip(dingoEP)
	require.NoError(t, err, "failed to get initial dingo-producer tip")

	const (
		samples             = 3
		slotsBetweenSamples = 15
	)
	sampleTimeout := time.Duration(slotsBetweenSamples)*cfg.SlotDuration() +
		cfg.ExpectedBlockTime()*10

	for i := 1; i <= samples; i++ {
		targetSlot := initialTip.SlotNumber + uint64(i*slotsBetweenSamples)
		h.WaitForNodeSlot(dingoEP, targetSlot, sampleTimeout)
		h.WaitForNodeSlot(cardanoEP, targetSlot, sampleTimeout)

		dingoState, cardanoState, tip := sampleLedgerStateAtStableTip(
			t, h, dingoEP, cardanoEP, dingoNtc, cardanoNtc, cfg.NetworkMagic,
			cfg.ExpectedBlockTime(),
		)

		diffs := devnet.DiffLedgerStates(dingoState, cardanoState)
		require.Empty(t, diffs,
			"ledger state diverged between dingo-producer and"+
				" cardano-producer at slot %d (block %d):\n%s",
			tip.SlotNumber, tip.BlockNumber, strings.Join(diffs, "\n"),
		)

		t.Logf(
			"ledger state matched at slot %d (block %d):"+
				" %d utxos, %d pools in stake distribution",
			tip.SlotNumber, tip.BlockNumber,
			len(dingoState.UTxOEntries), len(dingoState.StakeDistribution),
		)
	}
}

// sampleLedgerStateAtStableTip polls dingoEP and cardanoEP until they
// report an identical chain tip, queries both nodes' LocalStateQuery
// interfaces, and confirms neither node's tip moved during the query round
// trip. If either node advanced in the meantime, the two LocalStateQuery
// responses would reflect different blocks and any divergence found would
// be meaningless noise rather than a real conformance failure, so the
// sample is discarded and retried instead.
//
// Uses require.Eventually with harness.go's own 2*time.Second polling
// interval (see e.g. WaitForNodeSlot), budgeting the overall timeout off
// blockTime (cfg.ExpectedBlockTime()) rather than a fixed attempt count:
// transient tip divergence between two independently forging producers
// around block-adoption time is the normal case here, not a rare one, so a
// tight fixed-count retry with no backoff could exhaust its budget in well
// under one block interval on a legitimate run. Writing the result into
// the enclosing closure's variables from inside the condition function,
// then reading them only after require.Eventually returns, mirrors
// existing harness helpers -- testify's Eventually never runs the
// condition function twice concurrently (it waits for one tick's result
// before scheduling the next), so this needs no additional locking.
func sampleLedgerStateAtStableTip(
	t *testing.T,
	h *devnet.TestHarness,
	dingoEP, cardanoEP devnet.NodeEndpoint,
	dingoNtc, cardanoNtc string,
	magic uint32,
	blockTime time.Duration,
) (dingoState, cardanoState *devnet.LedgerState, tip devnet.ChainTip) {
	t.Helper()

	const pollInterval = 2 * time.Second
	timeout := blockTime * 20

	var (
		attempt                int
		resultDingo, resultRef *devnet.LedgerState
		resultTip              devnet.ChainTip
	)
	require.Eventually(t, func() bool {
		attempt++
		before, err := h.GetChainTip(dingoEP)
		if err != nil {
			t.Logf(
				"sampleLedgerStateAtStableTip: attempt %d: dingo-producer"+
					" tip: %v", attempt, err,
			)
			return false
		}
		beforeRef, err := h.GetChainTip(cardanoEP)
		if err != nil {
			t.Logf(
				"sampleLedgerStateAtStableTip: attempt %d: cardano-producer"+
					" tip: %v", attempt, err,
			)
			return false
		}
		if before.SlotNumber != beforeRef.SlotNumber ||
			!bytes.Equal(before.Hash, beforeRef.Hash) {
			t.Logf(
				"sampleLedgerStateAtStableTip: attempt %d: no common tip yet"+
					" (dingo-producer slot %d, cardano-producer slot %d)",
				attempt, before.SlotNumber, beforeRef.SlotNumber,
			)
			return false
		}

		ds, err := devnet.LedgerStateAtTip(dingoNtc, magic)
		if err != nil {
			t.Logf(
				"sampleLedgerStateAtStableTip: attempt %d: dingo-producer"+
					" ledger state query: %v", attempt, err,
			)
			return false
		}
		cs, err := devnet.LedgerStateAtTip(cardanoNtc, magic)
		if err != nil {
			t.Logf(
				"sampleLedgerStateAtStableTip: attempt %d: cardano-producer"+
					" ledger state query: %v", attempt, err,
			)
			return false
		}

		after, err := h.GetChainTip(dingoEP)
		if err != nil {
			t.Logf(
				"sampleLedgerStateAtStableTip: attempt %d: dingo-producer"+
					" re-check: %v", attempt, err,
			)
			return false
		}
		afterRef, err := h.GetChainTip(cardanoEP)
		if err != nil {
			t.Logf(
				"sampleLedgerStateAtStableTip: attempt %d: cardano-producer"+
					" re-check: %v", attempt, err,
			)
			return false
		}
		if !bytes.Equal(before.Hash, after.Hash) ||
			!bytes.Equal(beforeRef.Hash, afterRef.Hash) {
			t.Logf(
				"sampleLedgerStateAtStableTip: attempt %d: tip advanced"+
					" during the query round trip",
				attempt,
			)
			return false
		}

		resultDingo, resultRef, resultTip = ds, cs, before
		return true
	}, timeout, pollInterval,
		"dingo-producer and cardano-producer never settled on a stable"+
			" common tip within %s",
		timeout,
	)
	// require.Eventually calls t.FailNow() (halting this goroutine) rather
	// than returning when the condition never succeeds, so resultDingo and
	// resultRef are always set by the time execution reaches here -- this
	// check is for the static analyzer, not a runtime possibility.
	require.NotNil(t, resultDingo, "internal error: no dingo-producer result")
	require.NotNil(t, resultRef, "internal error: no cardano-producer result")

	return resultDingo, resultRef, resultTip
}
