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
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	csmock "github.com/blinklabs-io/ouroboros-mock/chainsync"
	mockfixtures "github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// preprodLikeSpans is preprod's era layout: four 21600-slot Byron epochs,
// then 432000-slot epochs from slot 86400 (epoch 4).
var preprodLikeSpans = []eraSpan{
	{startSlot: 0, startEpoch: 0, endSlot: 86400, epochSize: 21600},
	{startSlot: 86400, startEpoch: 4, unbounded: true, epochSize: 432000},
}

func TestEpochOf(t *testing.T) {
	t.Parallel()
	cases := []struct {
		slot  uint64
		epoch uint64
	}{
		{0, 0},
		{21599, 0},
		{21600, 1},
		{86399, 3},
		{86400, 4},
		{86400 + 431999, 4},
		{86400 + 432000, 5},
	}
	for _, c := range cases {
		got, ok := epochOf(preprodLikeSpans, c.slot)
		require.True(t, ok, "slot %d", c.slot)
		assert.Equal(t, c.epoch, got, "slot %d", c.slot)
	}
}

// TestEpochOracle_RefreshesPastBoundedEnd proves a slot past the cached
// history's bounded End refetches the history once, rather than falling
// back or answering from a stale span.
func TestEpochOracle_RefreshesPastBoundedEnd(t *testing.T) {
	t.Parallel()
	bounded := []eraSpan{
		preprodLikeSpans[0],
		{startSlot: 86400, startEpoch: 4, endSlot: 950400, epochSize: 432000},
	}
	fetches := 0
	oracle := newEpochOracle(func(context.Context) ([]eraSpan, error) {
		fetches++
		if fetches == 1 {
			return bounded, nil
		}
		return preprodLikeSpans, nil
	})
	ctx := t.Context()

	epoch, err := oracle.epoch(ctx, 950399)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), epoch)
	epoch, err = oracle.epoch(ctx, 100)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), epoch)
	assert.Equal(t, 1, fetches, "a covered slot must not refetch")

	epoch, err = oracle.epoch(ctx, 950400)
	require.NoError(t, err)
	assert.Equal(t, uint64(6), epoch)
	assert.Equal(t, 2, fetches)
}

// TestEpochOracle_FailedRefreshWaitsBeforeRetrying proves a failed fetch is
// reported to the caller, which falls back, and is not retried on every
// following block.
func TestEpochOracle_FailedRefreshWaitsBeforeRetrying(t *testing.T) {
	t.Parallel()
	fetchErr := errors.New("era history unavailable")
	fetches := 0
	oracle := newEpochOracle(func(context.Context) ([]eraSpan, error) {
		fetches++
		if fetches == 1 {
			return nil, fetchErr
		}
		return preprodLikeSpans, nil
	})
	now := time.Unix(0, 0)
	oracle.now = func() time.Time { return now }
	ctx := t.Context()

	_, err := oracle.epoch(ctx, 100)
	require.ErrorIs(t, err, fetchErr)
	_, err = oracle.epoch(ctx, 120)
	require.ErrorIs(t, err, errSlotNotInEraHistory)
	assert.Equal(t, 1, fetches)

	now = now.Add(eraHistoryRetryInterval)
	epoch, err := oracle.epoch(ctx, 86400)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), epoch)
	assert.Equal(t, 2, fetches)
}

// genesisRun runs RunFromGenesis against server until the test ends.
type genesisRun struct {
	results chan EpochResult
	logs    *logCollector
	cancel  context.CancelFunc
	done    chan error
}

func startGenesisRun(t *testing.T, server *genesisFakeServer) *genesisRun {
	t.Helper()
	const magic = 42
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preprod", "", koiosSrv.URL, true, true)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	run := &genesisRun{
		results: make(chan EpochResult, 16),
		logs:    newLogCollector(),
		cancel:  cancel,
		done:    make(chan error, 1),
	}
	go func() {
		run.done <- RunFromGenesis(
			ctx, listener.Addr().String(), "preprod", magic, koios, nil,
			func(r EpochResult) { run.results <- r },
			run.logs.logf, nil,
		)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-run.done:
			assert.True(t, err == nil || errors.Is(err, context.Canceled))
		case <-time.After(10 * time.Second):
			t.Error("RunFromGenesis did not exit after ctx cancellation")
		}
	})
	return run
}

func (r *genesisRun) recv(t *testing.T) EpochResult {
	t.Helper()
	select {
	case res := <-r.results:
		return res
	case <-time.After(20 * time.Second):
		t.Fatalf(
			"timed out waiting for an epoch result; logs: %v",
			r.logs.all(),
		)
		return EpochResult{}
	}
}

// TestRunFromGenesis_EraHistoryAvoidsPerBlockEpochQueries proves that with
// Dingo's era history available no block's epoch is resolved by acquiring
// its point: the only points acquired are the first block's UTxO baseline
// and the epoch boundaries being checked, each boundary once.
func TestRunFromGenesis_EraHistoryAvoidsPerBlockEpochQueries(t *testing.T) {
	t.Parallel()
	const blockCount = 6
	// Slots 100..200 step 20 under 50-slot epochs: 100,120,140 in epoch 2,
	// 160,180 in epoch 3, 200 in epoch 4.
	server := newGenesisFakeServer(t, blockCount)
	server.eraHistory = []eraSpan{
		{startSlot: 0, startEpoch: 0, unbounded: true, epochSize: 50},
	}
	run := startGenesisRun(t, server)

	for range blockCount {
		server.allowStep(t)
	}
	assert.Equal(t, uint64(3), run.recv(t).Epoch)
	assert.Equal(t, uint64(4), run.recv(t).Epoch)

	acquired, tipAcquires, epochNoQueries := server.queryCounts()
	assert.Zero(t, epochNoQueries, "no epoch may be asked of Dingo per point")
	assert.Positive(t, tipAcquires, "the era history is read at the tip")
	allowed := map[uint64]bool{
		server.chain.Points[0].Slot: true, // UTxO baseline
		server.chain.Points[3].Slot: true, // epoch 3 checks
		server.chain.Points[5].Slot: true, // epoch 4 checks
	}
	for _, slot := range acquired {
		assert.True(t, allowed[slot],
			"slot %d was acquired though it is neither the baseline nor an "+
				"epoch boundary; acquired: %v", slot, acquired)
	}
	assert.False(
		t,
		run.logs.waitFor("chainsync session ended", 200*time.Millisecond),
		"logs: %v",
		run.logs.all(),
	)
}

// buildByronToConwayChain builds Byron main blocks at byronSlots followed by
// Conway blocks at conwaySlots.
func buildByronToConwayChain(
	t *testing.T, byronSlots, conwaySlots []uint64,
) csmock.Chain {
	t.Helper()
	var chain csmock.Chain
	var prev ledger.Blake2b256
	add := func(eraID uint8, slot uint64, byronMain bool) {
		b := mockfixtures.NewBlockBuilder(ledger.GetEraById(eraID)).
			WithBlockNumber(uint64(len(chain.Blocks)) + 1).
			WithSlot(slot).
			WithPreviousHash(prev)
		if byronMain {
			b = b.WithByronMainBlock()
		}
		block, err := b.Build()
		require.NoError(t, err)
		chain.Blocks = append(chain.Blocks, block)
		chain.Points = append(chain.Points, csmock.PointOf(block))
		chain.Tips = append(chain.Tips, csmock.TipOf(block))
		prev = block.Hash()
	}
	for _, slot := range byronSlots {
		add(ledger.EraIdByron, slot, true)
	}
	for _, slot := range conwaySlots {
		add(ledger.EraIdConway, slot, false)
	}
	return chain
}

// TestRunFromGenesis_ByronSkippedWithoutAcquire starts from genesis on a
// preprod-shaped chain whose Byron points Dingo refuses as too old, except
// one early Byron point it answers for. Before the fix, that answer latched
// "acquirable", the next Byron refusal ended the session, and the reconnect
// resumed at the same point forever. Byron blocks must instead be skipped
// without acquiring their points, and the first Shelley-era epoch checked.
func TestRunFromGenesis_ByronSkippedWithoutAcquire(t *testing.T) {
	t.Parallel()
	byronSlots := []uint64{100, 21700, 43300, 64900}
	conwaySlots := []uint64{86500, 86600, 86400 + 432000 + 100}
	server := newGenesisFakeServer(t, 0)
	server.chain = buildByronToConwayChain(t, byronSlots, conwaySlots)
	server.initialRollbackToOrigin = true
	server.eraHistory = preprodLikeSpans
	server.epochBySlot = map[uint64]int{21700: 1}
	server.refuseAcquireFn = func(p pcommon.Point) bool {
		return p.Slot < 86400 && p.Slot != 21700
	}
	run := startGenesisRun(t, server)

	for range server.chain.Len() {
		server.allowStep(t)
	}
	assert.Equal(t, uint64(4), run.recv(t).Epoch,
		"the first Shelley-era epoch must be the first one checked")
	assert.Equal(t, uint64(5), run.recv(t).Epoch)
	assert.True(
		t,
		run.logs.waitFor("validation starts at slot 86500", 5*time.Second),
		"logs: %v",
		run.logs.all(),
	)
	assert.False(
		t,
		run.logs.waitFor("chainsync session ended", 200*time.Millisecond),
		"skipping Byron must not end the session; logs: %v",
		run.logs.all(),
	)

	acquired, _, epochNoQueries := server.queryCounts()
	assert.Zero(t, epochNoQueries)
	for _, slot := range acquired {
		// The first block still seeds the UTxO baseline, as before.
		if slot < 86400 {
			assert.Equal(t, byronSlots[0], slot,
				"a Byron point was acquired; acquired: %v", acquired)
		}
	}
	server.mu.Lock()
	sessions := server.sessions
	server.mu.Unlock()
	assert.Equal(t, 1, sessions)
}

// TestRunFromGenesis_EraHistoryUnavailableFallsBack proves that without an
// era history every epoch is still resolved, through Dingo's per-point
// GetEpochNo, and that the fallback is logged once rather than per block.
func TestRunFromGenesis_EraHistoryUnavailableFallsBack(t *testing.T) {
	t.Parallel()
	const blockCount = 4
	server := newGenesisFakeServer(t, blockCount)
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 1,
		server.chain.Points[1].Slot: 2,
		server.chain.Points[2].Slot: 2,
		server.chain.Points[3].Slot: 3,
	}
	run := startGenesisRun(t, server)

	for range blockCount {
		server.allowStep(t)
	}
	assert.Equal(t, uint64(2), run.recv(t).Epoch)
	assert.Equal(t, uint64(3), run.recv(t).Epoch)

	_, _, epochNoQueries := server.queryCounts()
	assert.GreaterOrEqual(t, epochNoQueries, blockCount)
	fallbacks := 0
	for _, line := range run.logs.all() {
		if strings.Contains(
			line,
			"resolving epochs by querying Dingo per point",
		) {
			fallbacks++
		}
	}
	assert.Equal(t, 1, fallbacks, "logs: %v", run.logs.all())
}
