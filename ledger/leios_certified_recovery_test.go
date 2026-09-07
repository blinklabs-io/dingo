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
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// leiosRecoveryProbe is a scripted EndorserBlockFetcher/EndorserBlockProvider
// pair standing in for the leios-fetch backfill. It records every fetch attempt
// and can be told to make the endorser block available on the Nth attempt, so
// the ledger's retry behavior is observable without a network.
type leiosRecoveryProbe struct {
	mu sync.Mutex
	// availableOnAttempt makes the endorser block available once this many
	// fetch attempts have been made; 0 means never.
	availableOnAttempt int
	attempts           int
	available          bool
	err                error
}

func (p *leiosRecoveryProbe) fetch(
	ctx context.Context,
	_ uint64,
	_ []byte,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attempts++
	if p.availableOnAttempt > 0 && p.attempts >= p.availableOnAttempt {
		p.available = true
		return nil
	}
	return p.err
}

func (p *leiosRecoveryProbe) provider(
	[]byte,
	uint64,
) ([]cbor.RawMessage, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return nil, p.available
}

func (p *leiosRecoveryProbe) attemptCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.attempts
}

// newLeiosRecoveryLedgerState wires a LedgerState around probe with the
// Haskell-conformant (Musashi) endorser-block path, which is the path on which a
// certified closure is mandatory.
func newLeiosRecoveryLedgerState(
	probe *leiosRecoveryProbe,
) *LedgerState {
	cfg := LedgerStateConfig{
		Logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		EndorserBlockProvider: probe.provider,
		EndorserBlockFetcher:  probe.fetch,
		// A zero wait disables the best-effort announcement window; the
		// mandatory certified closure must still be fetched.
		EndorserBlockWaitSlots: 0,
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)
	return ls
}

// TestEnsureReferencedEndorserBlocksRetriesUntilCertifiedEbArrives is the
// dingo #3552 recovery path: an endorser block that is unavailable on the first
// by-point attempt but arrives on a later one must let the chunk through.
//
// Before the fix the only retry was a whole pipeline restart -- the fetch made
// at most one attempt per pass, and on the zero-wait path it made none at all --
// so a fetch that failed for a transient reason (every leios-fetch connection
// busy serving another endorser block, or a replacement connection still being
// dialled) aborted the chunk and the ledger tip did not move.
func TestEnsureReferencedEndorserBlocksRetriesUntilCertifiedEbArrives(
	t *testing.T,
) {
	parent, certifier, _ := leiosTestCertifiedBlockPair(t)
	probe := &leiosRecoveryProbe{
		availableOnAttempt: 3,
		err: errors.New(
			"leios backfill: connection fetch already in progress",
		),
	}
	ls := newLeiosRecoveryLedgerState(probe)

	require.NoError(t, ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{parent, certifier},
	))
	require.Equal(
		t,
		3,
		probe.attemptCount(),
		"the certified endorser block must be retried, not attempted once",
	)
}

// TestEnsureReferencedEndorserBlocksBoundsCertifiedRetry covers the absence
// case: an endorser block no peer can serve must reach a bounded terminal
// outcome -- a definite error naming the endorser block AND the reason the fetch
// failed -- instead of retrying inside one pass forever. The pipeline's own
// escalating restart is what retries afterwards; the chunk itself gives up.
func TestEnsureReferencedEndorserBlocksBoundsCertifiedRetry(t *testing.T) {
	parent, certifier, ebHash := leiosTestCertifiedBlockPair(t)
	probe := &leiosRecoveryProbe{
		err: errors.New(
			"leios backfill: endorser block declined by every leios-fetch peer",
		),
	}
	ls := newLeiosRecoveryLedgerState(probe)

	err := ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{parent, certifier},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
	require.Contains(t, err.Error(), ebHash.String())
	require.Contains(
		t,
		err.Error(),
		"declined by every leios-fetch peer",
		"the fetch failure reason must reach the pipeline error; without it "+
			"a wedged node reports only that the EB is unavailable",
	)
	require.Equal(
		t,
		leiosCertifiedFetchAttempts,
		probe.attemptCount(),
		"the per-pass retry must be bounded",
	)
}

// TestEnsureReferencedEndorserBlocksCertifiedRetryHonoursContext verifies the
// bounded retry stops when its caller's context ends, so a shutdown or a
// pipeline restart is not delayed by a fetch loop.
func TestEnsureReferencedEndorserBlocksCertifiedRetryHonoursContext(
	t *testing.T,
) {
	parent, certifier, _ := leiosTestCertifiedBlockPair(t)
	probe := &leiosRecoveryProbe{err: errors.New("no peers")}
	ls := newLeiosRecoveryLedgerState(probe)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	start := time.Now()
	err := ls.ensureReferencedEndorserBlocks(
		ctx,
		[]gledger.Block{parent, certifier},
	)
	require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
	require.Less(
		t,
		time.Since(start),
		leiosCertifiedFetchRetryBase*leiosCertifiedFetchAttempts,
		"a cancelled context must not be waited out",
	)
}

// TestEnsureReferencedEndorserBlocksAvailableEbIsNotFetched is the healthy-sync
// case: a certified endorser block that is already available must not cost a
// single by-point fetch.
func TestEnsureReferencedEndorserBlocksAvailableEbIsNotFetched(t *testing.T) {
	parent, certifier, _ := leiosTestCertifiedBlockPair(t)
	probe := &leiosRecoveryProbe{available: true}
	ls := newLeiosRecoveryLedgerState(probe)

	require.NoError(t, ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{parent, certifier},
	))
	require.Zero(
		t,
		probe.attemptCount(),
		"an available certified endorser block must not be refetched",
	)
}

// TestLeiosBackfillFetchRequiredDedupsWithInFlightFetch verifies fetchRequired
// waits for a fetch another caller already has in flight for the same endorser
// block rather than starting a second one against the same connections.
func TestLeiosBackfillFetchRequiredDedupsWithInFlightFetch(t *testing.T) {
	probe := &leiosRecoveryProbe{}
	ls := newLeiosRecoveryLedgerState(probe)
	r := leiosEbRef{
		slot: 100,
		hash: lcommon.NewBlake2b256(leiosTestHash(0xD5)),
	}
	// Claim the in-flight marker the way spawn does, then release it once the
	// endorser block is available, as a completing fetch would.
	key := leiosEbRefKey(r)
	ls.leiosBackfill.inflight.Store(key, struct{}{})
	go func() {
		probe.mu.Lock()
		probe.available = true
		probe.mu.Unlock()
		ls.leiosBackfill.inflight.Delete(key)
	}()

	require.NoError(t, ls.leiosBackfill.fetchRequired(
		t.Context(),
		r,
		time.Millisecond,
	))
	require.Zero(
		t,
		probe.attemptCount(),
		"a second fetch must not be started for an in-flight endorser block",
	)
}

// TestCertifiedEndorserBlockRetryDelayEscalates verifies the pipeline's restart
// gap for an unavailable certified endorser block grows with the no-progress
// count instead of staying at a flat one second. A flat retry respun the chain
// reader, re-read the batch and re-decoded it once per second for as long as the
// endorser block stayed unavailable.
func TestCertifiedEndorserBlockRetryDelayEscalates(t *testing.T) {
	t.Parallel()
	require.Equal(
		t,
		certifiedEndorserBlockRetryDelay,
		certifiedEndorserBlockPipelineRetryDelay(0),
		"the first retry keeps the prompt floor",
	)
	require.Equal(
		t,
		certifiedEndorserBlockRetryDelay,
		certifiedEndorserBlockPipelineRetryDelay(1),
	)
	stuck, isStuck := ledgerPipelineBackoff(noProgressStuckThreshold)
	require.True(t, isStuck)
	require.Equal(
		t,
		stuck,
		certifiedEndorserBlockPipelineRetryDelay(noProgressStuckThreshold),
		"a stuck pipeline backs off instead of spinning at 1Hz",
	)
	require.Greater(
		t,
		certifiedEndorserBlockPipelineRetryDelay(noProgressStuckThreshold),
		certifiedEndorserBlockRetryDelay,
	)
}
