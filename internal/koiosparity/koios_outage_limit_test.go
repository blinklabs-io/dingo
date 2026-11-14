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

package koiosparity

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// switchableKoiosTransport fails every request with a transport error while
// down is set, the way an unreachable Koios does, and forwards it otherwise.
type switchableKoiosTransport struct {
	next http.RoundTripper
	down *atomic.Bool
}

func (t switchableKoiosTransport) RoundTrip(
	req *http.Request,
) (*http.Response, error) {
	if t.down.Load() {
		return nil, errors.New("connection refused")
	}
	return t.next.RoundTrip(req)
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type outageHarness struct {
	o    *Observer
	down atomic.Bool
	logs lockedBuffer

	mu     sync.Mutex
	fatals []error
}

// newOutageHarness builds an observer over a fake Koios that serves epoch 5
// with no pools, so a reachable Koios yields a reference_lag-only result and
// every run refetches the reference.
func newOutageHarness(
	t *testing.T,
	strict, accounts bool,
	limit int,
) *outageHarness {
	t.Helper()
	source, err := NewDatabaseSource(newTestDatabaseSourceDB(t))
	require.NoError(t, err)
	srv := newFakeKoiosServer(t, map[uint64]*fakeEpochRef{
		5: {activeStake: "1000000", treasury: "10", reserves: "20", fees: "30", endTimeUnix: time.Now().Add(-time.Hour).Unix()},
	})
	h := &outageHarness{}
	o, err := NewObserver(ObserverConfig{
		Network:                 "preview",
		CachePath:               filepath.Join(t.TempDir(), "cache.db"),
		Source:                  source,
		BaseURL:                 srv.URL,
		AllowInsecureHTTP:       true,
		AllowPrivateAddresses:   true,
		AccountsEnabled:         accounts,
		Strict:                  strict,
		GraceHours:              24,
		FetchRetryAttempts:      1,
		ErrorRetryDelay:         time.Hour,
		PoolHistoryRefetchDelay: time.Nanosecond,
		UnavailableRetryLimit:   limit,
		Logger: slog.New(slog.NewTextHandler(
			&h.logs, &slog.HandlerOptions{Level: slog.LevelError},
		)),
		FatalFunc: func(err error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.fatals = append(h.fatals, err)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, o.Stop(context.Background())) })
	o.koios.http.Transport = switchableKoiosTransport{
		next: o.koios.http.Transport,
		down: &h.down,
	}
	h.o = o
	return h
}

func (h *outageHarness) run(accounts bool, times int) {
	for range times {
		if accounts {
			h.o.processAccountEpoch(context.Background(), 5)
		} else {
			h.o.processEpoch(context.Background(), 5)
		}
	}
}

func (h *outageHarness) fatalErrs() []error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]error(nil), h.fatals...)
}

func forEachQueue(t *testing.T, fn func(t *testing.T, accounts bool)) {
	for name, accounts := range map[string]bool{
		"aggregate": false,
		"accounts":  true,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fn(t, accounts)
		})
	}
}

// Strict mode tolerates an unreachable Koios for limit-1 consecutive attempts
// and stops the node on the limit-th, naming the attempt count.
func TestStrictKoiosOutageIsFatalAtRetryLimit(t *testing.T) {
	t.Parallel()
	forEachQueue(t, func(t *testing.T, accounts bool) {
		const limit = 3
		h := newOutageHarness(t, true, accounts, limit)
		h.down.Store(true)

		h.run(accounts, limit-1)
		require.Empty(t, h.fatalErrs(),
			"an outage shorter than the limit must not stop the node")
		require.False(t, h.o.stopping())

		h.run(accounts, 1)
		fatals := h.fatalErrs()
		require.Len(t, fatals, 1)
		require.ErrorContains(t, fatals[0],
			"koios unreachable for 3 consecutive attempts over")
		require.True(t, h.o.stopping())
	})
}

// A Koios answer ends the outage, even one that is still reference lag: the
// next outage counts from zero rather than from where the previous one
// stopped. Pool A's history is unavailable on runs 1-2 and 4-6 and empty on
// run 3, so only run 6 completes three consecutive unavailable attempts.
func TestKoiosOutageCountResetsWhenKoiosAnswers(t *testing.T) {
	t.Parallel()
	down := func(run int) bool { return run != 3 }
	for name, accounts := range map[string]bool{
		"aggregate": false,
		"accounts":  true,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			below := runPoolRefScenario(t, poolRefScenario{
				historyA:              func(int32) bool { return false },
				historyDownA:          down,
				updatesA:              updatesRegisteredAt1,
				accounts:              accounts,
				runs:                  5,
				unavailableRetryLimit: 3,
			})
			require.Zero(t, below.fatal,
				"a Koios answer must reset the consecutive-failure count")

			at := runPoolRefScenario(t, poolRefScenario{
				historyA:              func(int32) bool { return false },
				historyDownA:          down,
				updatesA:              updatesRegisteredAt1,
				accounts:              accounts,
				runs:                  6,
				unavailableRetryLimit: 3,
			})
			require.Equal(t, int32(1), at.fatal,
				"three consecutive unavailable attempts after the reset")
		})
	}
}

// Koios answering without the epoch's pool_history is reference lag, which
// can last for days. It must never count toward the unavailability limit.
func TestKoiosReferenceLagNeverReachesRetryLimit(t *testing.T) {
	t.Parallel()
	t.Run("empty pool set", func(t *testing.T) {
		t.Parallel()
		forEachQueue(t, func(t *testing.T, accounts bool) {
			h := newOutageHarness(t, true, accounts, 1)
			h.run(accounts, 4)
			require.Empty(t, h.fatalErrs())
			require.False(t, h.o.stopping())
		})
	})
	t.Run("registered pool never published", func(t *testing.T) {
		t.Parallel()
		for name, accounts := range map[string]bool{
			"aggregate": false,
			"accounts":  true,
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				out := runPoolRefScenario(t, poolRefScenario{
					historyA:              func(int32) bool { return false },
					updatesA:              updatesRegisteredAt1,
					accounts:              accounts,
					runs:                  4,
					unavailableRetryLimit: 1,
				})
				require.Equal(t, StatusError, out.result.Status)
				require.Zero(t, out.fatal,
					"reference lag must not count as Koios unavailability")
			})
		}
	})
}

// Non-strict mode never stops the node. Past the limit it keeps the check
// queued for retry and logs, at ERROR, how long Koios has been unreachable.
func TestNonStrictKoiosOutagePastLimitLogsAndKeepsRetrying(t *testing.T) {
	t.Parallel()
	forEachQueue(t, func(t *testing.T, accounts bool) {
		const limit = 2
		h := newOutageHarness(t, false, accounts, limit)
		h.down.Store(true)

		h.run(accounts, limit+3)

		require.Empty(t, h.fatalErrs())
		require.False(t, h.o.stopping())
		logs := h.logs.String()
		require.Contains(t, logs, "level=ERROR")
		require.Contains(t, logs,
			"koios unreachable for 5 consecutive attempts over")
		retrying, err := h.o.cache.GetEpochsNeedingRetry("preview", accounts)
		require.NoError(t, err)
		require.Equal(t, []uint64{5}, retrying)
		h.o.mu.Lock()
		retry := h.o.retry
		if accounts {
			retry = h.o.retryAccounts
		}
		_, scheduled := retry[5]
		h.o.mu.Unlock()
		require.True(t, scheduled,
			"an exhausted outage must stay queued for retry in non-strict mode")
	})
}
