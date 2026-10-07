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
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestValidateDijkstraLeiosCertificateResolvesBatchParent(t *testing.T) {
	t.Parallel()

	parent, certifier, _ := leiosTestCertifiedBlockPair(t)
	certifier.BlockBody.LeiosCertificate = &dijkstra.DijkstraLeiosCertificate{
		Signers:             []byte{1},
		AggregatedSignature: make([]byte, 48),
	}
	var (
		gotEpoch  uint64
		gotParent []byte
	)
	ls := &LedgerState{
		config: LedgerStateConfig{
			ValidateLeiosCertificate: func(
				epoch uint64,
				parentHash, _, _ []byte,
			) error {
				gotEpoch = epoch
				gotParent = append([]byte(nil), parentHash...)
				return nil
			},
		},
	}
	ls.consensus.Store(&consensusSnapshot{
		epochCache: []models.Epoch{{
			EpochId:       5,
			StartSlot:     0,
			LengthInSlots: 200,
		}},
	})
	err := ls.validateDijkstraLeiosCertificate(certifier, map[string]leiosEbRef{
		string(parent.Hash().Bytes()): {slot: parent.SlotNumber()},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(5), gotEpoch)
	require.Equal(t, parent.Hash().Bytes(), gotParent)
}

func TestEnsureReferencedEndorserBlocksRejectsCertificateBeforeFetch(t *testing.T) {
	t.Parallel()

	parent, certifier, _ := leiosTestCertifiedBlockPair(t)
	certifier.BlockBody.LeiosCertificate = &dijkstra.DijkstraLeiosCertificate{
		Signers:             []byte{1},
		AggregatedSignature: make([]byte, 48),
	}
	probe := &leiosRecoveryProbe{err: errors.New("fetch must not run")}
	ls := newLeiosRecoveryLedgerState(probe)
	ls.consensus.Store(&consensusSnapshot{
		epochCache: []models.Epoch{{
			EpochId:       5,
			StartSlot:     0,
			LengthInSlots: 200,
		}},
	})
	ls.config.ValidateLeiosCertificate = func(
		uint64,
		[]byte,
		[]byte,
		[]byte,
	) error {
		return errors.New("invalid aggregate signature")
	}
	err := ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{parent, certifier},
	)
	require.ErrorContains(t, err, "invalid aggregate signature")
	require.Zero(t, probe.attemptCount(),
		"invalid certificates must be rejected before fetching certified data")
}

func TestEnsureReferencedEndorserBlocksRejectsHeaderOnlyCertificateBeforeFetch(
	t *testing.T,
) {
	t.Parallel()

	parent, certifier, _ := leiosTestCertifiedBlockPair(t)
	probe := &leiosRecoveryProbe{err: errors.New("fetch must not run")}
	ls := newLeiosRecoveryLedgerState(probe)

	err := ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{parent, certifier},
	)
	require.ErrorContains(t, err, "certificate body presence is false")
	require.Zero(t, probe.attemptCount(),
		"a certification header flag without its body certificate must not trigger a fetch")
}

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
// from-genesis certified-EB recovery path: an endorser block that is
// unavailable on the first by-point attempt but arrives on a later one must let
// the chunk through.
//
// Before the fix the only retry was a whole pipeline restart -- the fetch made
// at most one attempt per pass, and on the zero-wait path it made none at all --
// so a fetch that failed for a transient reason (every leios-fetch connection
// busy serving another endorser block, or a replacement connection still being
// dialled) aborted the chunk and the ledger tip did not move.
func TestEnsureReferencedEndorserBlocksRetriesUntilCertifiedEbArrives(
	t *testing.T,
) {
	t.Parallel()

	parent, certifier, _ := leiosTestCertifiedBlockPair(t)
	probe := &leiosRecoveryProbe{
		availableOnAttempt: 3,
		err: errors.New(
			"leios backfill: connection fetch already in progress",
		),
	}
	ls := newLeiosRecoveryLedgerState(probe)
	leiosTestEnableCertifiedBlock(t, ls, certifier)

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
	t.Parallel()

	parent, certifier, ebHash := leiosTestCertifiedBlockPair(t)
	probe := &leiosRecoveryProbe{
		err: errors.New(
			"leios backfill: endorser block declined by every leios-fetch peer",
		),
	}
	ls := newLeiosRecoveryLedgerState(probe)
	leiosTestEnableCertifiedBlock(t, ls, certifier)

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

// TestEnsureReferencedEndorserBlocksKeepsNoPeerCause verifies a certified
// fetch that failed for want of any connection reaches the pipeline still
// marked as such, since that is what keeps it out of the halt count.
func TestEnsureReferencedEndorserBlocksKeepsNoPeerCause(t *testing.T) {
	t.Parallel()

	parent, certifier, _ := leiosTestCertifiedBlockPair(t)
	probe := &leiosRecoveryProbe{
		err: fmt.Errorf("leios backfill: %w", ErrEndorserBlockFetchNoPeer),
	}
	ls := newLeiosRecoveryLedgerState(probe)
	leiosTestEnableCertifiedBlock(t, ls, certifier)

	err := ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{parent, certifier},
	)
	require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
	require.ErrorIs(t, err, ErrEndorserBlockFetchNoPeer)
}

// TestEnsureReferencedEndorserBlocksCertifiedRetryHonoursContext verifies the
// bounded retry stops when its caller's context ends, so a shutdown or a
// pipeline restart is not delayed by a fetch loop.
func TestEnsureReferencedEndorserBlocksCertifiedRetryHonoursContext(
	t *testing.T,
) {
	t.Parallel()

	parent, certifier, _ := leiosTestCertifiedBlockPair(t)
	probe := &leiosRecoveryProbe{err: errors.New("no peers")}
	ls := newLeiosRecoveryLedgerState(probe)
	leiosTestEnableCertifiedBlock(t, ls, certifier)

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
	t.Parallel()

	parent, certifier, _ := leiosTestCertifiedBlockPair(t)
	probe := &leiosRecoveryProbe{available: true}
	ls := newLeiosRecoveryLedgerState(probe)
	leiosTestEnableCertifiedBlock(t, ls, certifier)

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
	t.Parallel()

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

func certifiedEndorserBlockFetchFailure(cause error) error {
	return fmt.Errorf(
		"ensure referenced Leios endorser blocks: %w: slot 7, EB ab: last fetch attempt: %w",
		errCertifiedEndorserBlockUnavailable,
		cause,
	)
}

var (
	errNoFetchPeerForTest = fmt.Errorf(
		"leios backfill: %w",
		ErrEndorserBlockFetchNoPeer,
	)
	errFetchDeclinedForTest = errors.New(
		"leios backfill: endorser block declined by every leios-fetch peer",
	)
)

// TestLedgerProcessBlocksWaitsOutLeiosFetchPeerGap covers dingo#5026: while no
// leios-fetch connection exists, every certified endorser block fetch fails at
// once. Those restarts must not count toward the deterministic-halt threshold,
// or a few minutes without a peer stops the pipeline and it never resumes when
// peers return.
func TestLedgerProcessBlocksWaitsOutLeiosFetchPeerGap(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ls := newPipelineLoopLedger(t)
		peerGap := 3 * noProgressStuckThreshold
		attempts := 0
		var last time.Time
		start := time.Now()
		ls.ledgerProcessBlocksWithAttempt(
			t.Context(),
			func(context.Context) error {
				attempts++
				if attempts > 1 {
					require.GreaterOrEqual(
						t,
						time.Since(last),
						certifiedEndorserBlockRetryDelay,
						"a peer gap must not respin the pipeline faster than the endorser-block retry floor",
					)
				}
				last = time.Now()
				if attempts <= peerGap {
					return certifiedEndorserBlockFetchFailure(
						errNoFetchPeerForTest,
					)
				}
				// A connection is back and the endorser block applies.
				return nil
			},
		)
		require.Equal(
			t,
			peerGap+1,
			attempts,
			"the pipeline must keep retrying through a peer gap and resume when a connection appears",
		)
		require.Zero(
			t,
			promtestutil.ToFloat64(ls.metrics.pipelineHalted),
			"a peer gap must not halt the pipeline",
		)
		require.LessOrEqual(
			t,
			time.Since(start),
			time.Duration(peerGap)*noProgressBackoffMax,
			"retries during a peer gap stay bounded by the transient backoff cap",
		)
	})
}

// TestLedgerProcessBlocksHaltsOnUnavailableEndorserBlockAcrossPeerGap is the
// guard on the other side: a certified endorser block that connected peers do
// not serve still halts the pipeline at the threshold, and a peer gap in the
// middle neither resets nor adds to that count.
func TestLedgerProcessBlocksHaltsOnUnavailableEndorserBlockAcrossPeerGap(
	t *testing.T,
) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		before int
		gap    int
	}{
		{name: "no gap", before: 0, gap: 0},
		{name: "gap mid-count", before: 30, gap: 2 * noProgressStuckThreshold},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ls := newPipelineLoopLedger(t)
				attempts := 0
				ls.ledgerProcessBlocksWithAttempt(
					t.Context(),
					func(context.Context) error {
						attempts++
						if attempts > tc.before &&
							attempts <= tc.before+tc.gap {
							return certifiedEndorserBlockFetchFailure(
								errNoFetchPeerForTest,
							)
						}
						return certifiedEndorserBlockFetchFailure(
							errFetchDeclinedForTest,
						)
					},
				)
				// The first attempt only records the tip; each later
				// counted one adds one to the no-progress count.
				require.Equal(
					t,
					noProgressStuckThreshold+1+tc.gap,
					attempts,
					"only failures with a peer to ask may count toward the halt",
				)
				require.Equal(
					t,
					1.0,
					promtestutil.ToFloat64(ls.metrics.pipelineHalted),
					"an endorser block connected peers do not serve must still halt the pipeline",
				)
			})
		})
	}
}
