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
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// poolMapStub replaces only the per-pool reward data of a RewardParitySource,
// so a test chooses exactly which pools Dingo holds for an epoch.
type poolMapStub struct {
	RewardParitySource
	pools map[string]*DingoPoolEpochData
}

func (s *poolMapStub) GetPoolEpochDataMap(
	context.Context,
	uint64,
	uint64,
) (map[string]*DingoPoolEpochData, error) {
	return s.pools, nil
}

const (
	refPoolKeyA = "2d4b292a16151000089cd462dfc6e21c0ac2ebdd373070fb0c511f12"
	refPoolKeyB = "3d4b292a16151000089cd462dfc6e21c0ac2ebdd373070fb0c511f12"
)

func refDingoPool() *DingoPoolEpochData {
	return &DingoPoolEpochData{
		StakePresent:   true,
		DelegatedStake: "5000000",
		DelegatorCount: 1,
		FixedCost:      "340000000",
		ParamsPresent:  true,
		BlocksProduced: 1,
	}
}

// poolRefScenario drives processEpoch for epoch 5 against a fake Koios that
// lists two pools. Pool B always answers /pool_history; pool A answers
// according to historyA, which receives the 1-based call number and reports
// whether to serve the row. updatesA is the per-pool /pool_updates body.
type poolRefScenario struct {
	historyA func(call int32) bool
	updatesA string
	// bulkUpdates, when set, is the all-pools /pool_updates body the fetch
	// builds its first-active skip map from.
	bulkUpdates string
	// accounts drives processAccountEpoch instead of processEpoch.
	accounts bool
}

type poolRefOutcome struct {
	result  *EpochCompareResult
	fatal   int32
	callsA  int32
	cachedA bool
}

func runPoolRefScenario(t *testing.T, sc poolRefScenario) poolRefOutcome {
	t.Helper()
	bechA, err := PoolKeyHashHexToBech32(refPoolKeyA)
	require.NoError(t, err)
	bechB, err := PoolKeyHashHexToBech32(refPoolKeyB)
	require.NoError(t, err)

	var callsA atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			q := r.URL.Query()
			switch r.URL.Path {
			case "/pool_list":
				_, _ = fmt.Fprintf(
					w,
					`[{"pool_id_bech32":%q},{"pool_id_bech32":%q}]`,
					bechA, bechB,
				)
			case "/pool_updates":
				if strings.Contains(q.Get("pool_id_bech32"), bechA) {
					_, _ = w.Write([]byte(sc.updatesA))
					return
				}
				if sc.bulkUpdates != "" {
					_, _ = w.Write([]byte(sc.bulkUpdates))
					return
				}
				_, _ = w.Write([]byte(`[]`))
			case "/epoch_info":
				_, _ = fmt.Fprintf(w, validEpochInfoTmpl, q.Get("_epoch_no"))
			case "/epoch_params":
				_, _ = fmt.Fprintf(w, validEpochParamsTmpl, q.Get("_epoch_no"))
			case "/totals":
				_, _ = fmt.Fprintf(w, validTotalsTmpl, q.Get("_epoch_no"))
			case "/pool_history":
				if q.Get("_pool_bech32") == bechA {
					if !sc.historyA(callsA.Add(1)) {
						_, _ = w.Write([]byte(`[]`))
						return
					}
				}
				_, _ = fmt.Fprintf(
					w,
					`[{"epoch_no":%s,"active_stake":"5000000","block_cnt":1,`+
						`"delegator_cnt":1,"fixed_cost":"340000000",`+
						`"pool_fees":"0","deleg_rewards":"0"}]`,
					q.Get("_epoch_no"),
				)
			default:
				_, _ = w.Write([]byte(`[]`))
			}
		}),
	)
	t.Cleanup(srv.Close)

	db := newTestDatabaseSourceDB(t)
	real, err := NewDatabaseSource(db)
	require.NoError(t, err)
	seedDingoEpochAggregate(t, real, 5, 12345, 1, 1, 1)
	source := &poolMapStub{
		RewardParitySource: real,
		pools: map[string]*DingoPoolEpochData{
			refPoolKeyA: refDingoPool(),
			refPoolKeyB: refDingoPool(),
		},
	}

	var fatal atomic.Int32
	var mu sync.Mutex
	var result *EpochCompareResult
	o, err := NewObserver(ObserverConfig{
		BaseURL:               srv.URL,
		AllowInsecureHTTP:     true,
		AllowPrivateAddresses: true,
		Network:               "preview",
		CachePath:             filepath.Join(t.TempDir(), "cache.db"),
		Source:                source,
		Strict:                true,
		AccountsEnabled:       sc.accounts,
		Logger:                slog.New(slog.DiscardHandler),
		FetchRetryAttempts:    3,
		FetchRetryDelay:       5 * time.Millisecond,
		FatalFunc:             func(error) { fatal.Add(1) },
		OnResult: func(res *EpochCompareResult) {
			mu.Lock()
			result = res
			mu.Unlock()
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = o.Stop(context.Background()) })

	if sc.accounts {
		o.processAccountEpoch(context.Background(), 5)
	} else {
		o.processEpoch(context.Background(), 5)
	}

	pools, err := o.cache.GetAllPoolsForEpoch("preview", 5)
	require.NoError(t, err)
	cachedA := false
	for _, p := range pools {
		if p.PoolBech32 == bechA {
			cachedA = true
		}
	}
	mu.Lock()
	defer mu.Unlock()
	require.NotNil(t, result, "processEpoch must report a result")
	return poolRefOutcome{
		result:  result,
		fatal:   fatal.Load(),
		callsA:  callsA.Load(),
		cachedA: cachedA,
	}
}

func hasCategory(res *EpochCompareResult, field, category string) bool {
	for _, m := range res.Mismatches {
		if m.Field == field && m.Category == category {
			return true
		}
	}
	return false
}

const updatesRegisteredAt1 = `[{"active_epoch_no":1,"retiring_epoch":null,"block_time":100}]`

// A pool whose first /pool_history answer is empty but which Koios
// registered must be fetched again, not reported as present only in Dingo.
func TestObserverEmptyPoolHistoryIsRefetchedBeforeClassifying(t *testing.T) {
	t.Parallel()
	out := runPoolRefScenario(t, poolRefScenario{
		historyA: func(call int32) bool { return call >= 2 },
		updatesA: updatesRegisteredAt1,
	})
	require.False(
		t,
		hasCategory(out.result, "pool_presence", CategoryPoolOnlyDingo),
		"a pool Koios publishes on re-request is not pool_only_dingo: %+v",
		out.result.Mismatches,
	)
	require.True(t, out.cachedA, "the refetched row must reach the cache")
	require.Equal(t, StatusPass, out.result.Status, "%+v", out.result.Mismatches)
	require.Zero(t, out.fatal)
}

// A pool Koios never registered stays a real disagreement.
func TestObserverPoolAbsentFromKoiosStillFails(t *testing.T) {
	t.Parallel()
	for name, updates := range map[string]string{
		"never registered": `[]`,
		"registered later": `[{"active_epoch_no":9,"retiring_epoch":null,"block_time":100}]`,
		"already retired": `[{"active_epoch_no":1,"retiring_epoch":null,"block_time":100},` +
			`{"active_epoch_no":null,"retiring_epoch":3,"block_time":200}]`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			out := runPoolRefScenario(t, poolRefScenario{
				historyA: func(int32) bool { return false },
				updatesA: updates,
			})
			require.True(
				t,
				hasCategory(out.result, "pool_presence", CategoryPoolOnlyDingo),
				"%+v", out.result.Mismatches,
			)
			require.Equal(t, StatusFail, out.result.Status)
			require.Equal(t, int32(1), out.fatal)
		})
	}
}

// A pool Koios registered but never publishes history for is an incomplete
// reference, which strict mode must not treat as a divergence.
func TestObserverPersistentlyEmptyPoolHistoryIsNonFatalReferenceError(
	t *testing.T,
) {
	t.Parallel()
	out := runPoolRefScenario(t, poolRefScenario{
		historyA: func(int32) bool { return false },
		updatesA: updatesRegisteredAt1,
	})
	require.False(
		t,
		hasCategory(out.result, "pool_presence", CategoryPoolOnlyDingo),
		"%+v", out.result.Mismatches,
	)
	require.Equal(t, StatusError, out.result.Status)
	require.Zero(t, out.fatal, "an incomplete reference must not halt strict mode")
}

// Koios's /pool_history row for epoch K is the stake snapshot taken one epoch
// earlier, so a pool retiring at epoch e still has rows at e and e+1. A pool
// retiring at or one epoch before the checked epoch (5) is still registered
// for that row, and an empty answer for it is an incomplete reference rather
// than pool_only_dingo.
func TestObserverPoolRetiringAtCheckedEpochIsNotAbsent(t *testing.T) {
	t.Parallel()
	for _, retiring := range []int{4, 5} {
		t.Run(fmt.Sprintf("retiring %d", retiring), func(t *testing.T) {
			t.Parallel()
			out := runPoolRefScenario(t, poolRefScenario{
				historyA: func(int32) bool { return false },
				updatesA: fmt.Sprintf(
					`[{"active_epoch_no":1,"retiring_epoch":null,"block_time":100},`+
						`{"active_epoch_no":null,"retiring_epoch":%d,"block_time":200}]`,
					retiring,
				),
			})
			require.False(
				t,
				hasCategory(out.result, "pool_presence", CategoryPoolOnlyDingo),
				"%+v", out.result.Mismatches,
			)
			require.Equal(t, StatusError, out.result.Status)
			require.Zero(t, out.fatal)
		})
	}
}

// The bulk /pool_updates scan can miss a pool's earliest certificate, which
// makes the fetch skip a pool Koios has history for. The comparison must not
// report that skipped pool as pool_only_dingo.
func TestObserverPoolSkippedByFirstActiveMapIsRefetched(t *testing.T) {
	t.Parallel()
	bechA, err := PoolKeyHashHexToBech32(refPoolKeyA)
	require.NoError(t, err)
	out := runPoolRefScenario(t, poolRefScenario{
		historyA: func(int32) bool { return true },
		updatesA: updatesRegisteredAt1,
		bulkUpdates: fmt.Sprintf(
			`[{"pool_id_bech32":%q,"active_epoch_no":9}]`, bechA,
		),
	})
	require.Zero(t, out.callsA-1, "the fetch skipped the pool; only the refetch asks")
	require.True(t, out.cachedA)
	require.Equal(t, StatusPass, out.result.Status, "%+v", out.result.Mismatches)
}

// The account queue compares pool presence too, and it can reach an epoch
// before the aggregate queue has reconciled the reference, so it must confirm
// a missing pool itself before strict mode can act on pool_only_dingo.
func TestObserverAccountQueueConfirmsMissingPool(t *testing.T) {
	t.Parallel()
	t.Run("published on re-request", func(t *testing.T) {
		t.Parallel()
		out := runPoolRefScenario(t, poolRefScenario{
			accounts: true,
			historyA: func(call int32) bool { return call >= 2 },
			updatesA: updatesRegisteredAt1,
		})
		require.False(
			t,
			hasCategory(out.result, "pool_presence", CategoryPoolOnlyDingo),
			"%+v", out.result.Mismatches,
		)
		require.True(t, out.cachedA, "the refetched row must reach the cache")
		require.Zero(t, out.fatal, "%+v", out.result.Mismatches)
	})
	t.Run("never registered", func(t *testing.T) {
		t.Parallel()
		out := runPoolRefScenario(t, poolRefScenario{
			accounts: true,
			historyA: func(int32) bool { return false },
			updatesA: `[]`,
		})
		require.True(
			t,
			hasCategory(out.result, "pool_presence", CategoryPoolOnlyDingo),
			"%+v", out.result.Mismatches,
		)
		require.Equal(t, int32(1), out.fatal)
	})
	t.Run("registered but never published", func(t *testing.T) {
		t.Parallel()
		out := runPoolRefScenario(t, poolRefScenario{
			accounts: true,
			historyA: func(int32) bool { return false },
			updatesA: updatesRegisteredAt1,
		})
		require.False(
			t,
			hasCategory(out.result, "pool_presence", CategoryPoolOnlyDingo),
			"%+v", out.result.Mismatches,
		)
		require.Equal(t, StatusError, out.result.Status)
		require.Zero(t, out.fatal, "an incomplete reference must not halt strict mode")
	})
}
