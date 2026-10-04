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

package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// scheduledJob names one periodic Store job and wires a counting callback
// into the Config fields that schedule it.
type scheduledJob struct {
	name string
	wire func(cfg *Config, every time.Duration, calls *atomic.Uint32)
}

var scheduledJobs = []scheduledJob{
	{
		name: "maintenance",
		wire: func(cfg *Config, every time.Duration, calls *atomic.Uint32) {
			cfg.Maintenance = func(context.Context) error {
				calls.Add(1)
				return nil
			}
			cfg.MaintenanceInterval = every
		},
	},
	{
		name: "vacuum",
		wire: func(cfg *Config, every time.Duration, calls *atomic.Uint32) {
			cfg.Vacuum = func(context.Context) error {
				calls.Add(1)
				return nil
			}
			cfg.VacuumInterval = every
		},
	},
}

func newScheduledStore(
	t *testing.T,
	job scheduledJob,
	every time.Duration,
	calls *atomic.Uint32,
) *Store {
	t.Helper()
	return newScheduledStoreWithLogger(t, job, every, calls, nil)
}

func newScheduledStoreWithLogger(
	t *testing.T,
	job scheduledJob,
	every time.Duration,
	calls *atomic.Uint32,
	logger *slog.Logger,
) *Store {
	t.Helper()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	cfg := Config{WriteDB: db, Dialect: SQLiteDialect(), Logger: logger}
	job.wire(&cfg, every, calls)
	store, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

// A job due while a ledger write holds a write-pool connection is postponed,
// not run: a full VACUUM or sweep on top of an active write path is what
// stalls block application.
func TestStorePostponesScheduledJobWhileWritePoolBusy(t *testing.T) {
	t.Parallel()
	for _, job := range scheduledJobs {
		t.Run(job.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Uint32
			store := newScheduledStore(t, job, time.Millisecond, &calls)
			writer, err := store.writeDB.Conn(t.Context())
			require.NoError(t, err)
			defer writer.Close()
			require.NoError(t, store.Start(t.Context()))

			require.Never(
				t,
				func() bool { return calls.Load() > 0 },
				300*time.Millisecond,
				10*time.Millisecond,
				"%s ran while the write pool was busy",
				job.name,
			)
			require.NoError(t, writer.Close())
			require.Eventually(
				t,
				func() bool { return calls.Load() > 0 },
				10*time.Second,
				10*time.Millisecond,
				"%s did not run once the write pool was quiet",
				job.name,
			)
		})
	}
}

// The delay before each run comes from Store.tickDelay rather than the bare
// interval, so a day-long interval cannot be phase-locked to process start.
func TestStoreSchedulesJobsWithTickDelay(t *testing.T) {
	t.Parallel()
	for _, job := range scheduledJobs {
		t.Run(job.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Uint32
			store := newScheduledStore(t, job, 24*time.Hour, &calls)
			store.tickDelay = func(time.Duration) time.Duration {
				return time.Millisecond
			}
			require.NoError(t, store.Start(t.Context()))
			require.Eventually(
				t,
				func() bool { return calls.Load() > 0 },
				10*time.Second,
				10*time.Millisecond,
				"%s ignored the scheduled tick delay",
				job.name,
			)
		})
	}
}

// A run postponed by a busy write pool waits for its retry delay, which also
// comes from Store.tickDelay: with a day-long interval the retry has to be
// the short busy-retry delay, not another interval.
func TestStoreRetriesPostponedJobWithTickDelay(t *testing.T) {
	t.Parallel()
	for _, job := range scheduledJobs {
		t.Run(job.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Uint32
			store := newScheduledStore(t, job, 24*time.Hour, &calls)
			store.tickDelay = func(time.Duration) time.Duration {
				return time.Millisecond
			}
			writer, err := store.writeDB.Conn(t.Context())
			require.NoError(t, err)
			defer writer.Close()
			require.NoError(t, store.Start(t.Context()))

			require.Never(
				t,
				func() bool { return calls.Load() > 0 },
				100*time.Millisecond,
				10*time.Millisecond,
				"%s ran while the write pool was busy",
				job.name,
			)
			require.NoError(t, writer.Close())
			require.Eventually(
				t,
				func() bool { return calls.Load() > 0 },
				10*time.Second,
				10*time.Millisecond,
				"%s was not retried after the write pool went quiet",
				job.name,
			)
		})
	}
}

func TestJitteredIntervalStaysWithinBoundsAndVaries(t *testing.T) {
	t.Parallel()
	const every = 24 * time.Hour
	// A store built by New must schedule with the jittered delay by default.
	store := newScheduledStore(t, scheduledJobs[0], every, &atomic.Uint32{})
	seen := map[time.Duration]struct{}{}
	for range 64 {
		delay := store.tickDelay(every)
		require.GreaterOrEqual(t, delay, every)
		require.LessOrEqual(t, delay, every+every/10)
		seen[delay] = struct{}{}
	}
	require.Greater(t, len(seen), 1, "interval must vary between runs")
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
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

// A job that keeps coming due while the write pool is busy would otherwise
// never run and never say so.
func TestStoreWarnsWhenJobKeepsBeingPostponed(t *testing.T) {
	t.Parallel()
	for _, job := range scheduledJobs {
		t.Run(job.name, func(t *testing.T) {
			t.Parallel()
			var out lockedBuffer
			var calls atomic.Uint32
			store := newScheduledStoreWithLogger(
				t, job, 24*time.Hour, &calls,
				slog.New(slog.NewTextHandler(&out, nil)),
			)
			store.tickDelay = func(time.Duration) time.Duration {
				return time.Millisecond
			}
			writer, err := store.writeDB.Conn(t.Context())
			require.NoError(t, err)
			defer writer.Close()
			require.NoError(t, store.Start(t.Context()))

			require.Eventually(
				t,
				func() bool {
					return strings.Contains(
						out.String(),
						"postponed while the write pool is busy",
					)
				},
				10*time.Second,
				10*time.Millisecond,
				"%s postponement went unreported",
				job.name,
			)
			require.Zero(t, calls.Load())
		})
	}
}
