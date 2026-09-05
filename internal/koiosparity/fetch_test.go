// Copyright 2025 Blink Labs Software
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
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// withTestKoiosBaseURL points koiosBaseURLs["preview"] — the map NewKoiosClient
// resolves a network to its base URL through — at a test server for the
// duration of the test, restoring the original value afterward. This is the
// only seam available for exercising Fetch's dispatcher end-to-end against a
// fake Koios server, since NewKoiosClient's base URL isn't otherwise
// injectable from FetchConfig.
func withTestKoiosBaseURL(t *testing.T, url string) {
	t.Helper()
	orig := koiosBaseURLs["preview"]
	koiosBaseURLs["preview"] = url
	t.Cleanup(func() { koiosBaseURLs["preview"] = orig })
}

const validEpochInfoTmpl = `[{"epoch_no":%s,"era":"conway","out_sum":"100","fees":"10",` +
	`"tx_count":1,"blk_count":1,"start_time":1000,"end_time":2000,` +
	`"first_block_time":1000,"last_block_time":1999,"active_stake":"12345",` +
	`"total_rewards":"100","avg_blk_reward":"1"}]`

const validTotalsTmpl = `[{"epoch_no":%s,"treasury":"1","reserves":"1","fees":"1","reward":"1"}]`

// validEpochParamsTmpl serves the shared preview Babbage parameter fixture
// (see previewBabbageEpochParamsTmpl) for any requested epoch.
var validEpochParamsTmpl = previewBabbageEpochParamsTmpl

// TestFetchAbortsOnPermanentEpochInfoError guards against the bug where every
// Koios client error was wrapped as transient regardless of cause: a
// daily-quota/auth failure (permanent, non-retryable) must abort the whole
// Fetch run with a hard error instead of being recorded in FailedEpochs and
// letting the dispatcher continue through the remaining epoch range.
func TestFetchAbortsOnPermanentEpochInfoError(t *testing.T) {
	var epochInfoAttempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.URL.Path == "/tip":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[{"epoch_no":100}]`))
			case r.URL.Path == "/pool_list" || r.URL.Path == "/pool_updates":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[]`))
			case r.URL.Path == "/epoch_info":
				epochInfoAttempts.Add(1)
				epoch := r.URL.Query().Get("_epoch_no")
				if epoch == "12" {
					w.WriteHeader(http.StatusUnauthorized)
					_, _ = w.Write([]byte("invalid API key"))
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(w, validEpochInfoTmpl, epoch)
			case r.URL.Path == "/epoch_params":
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(
					w,
					validEpochParamsTmpl,
					r.URL.Query().Get("_epoch_no"),
				)
			case r.URL.Path == "/totals":
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(
					w,
					validTotalsTmpl,
					r.URL.Query().Get("_epoch_no"),
				)
			default:
				t.Errorf("unexpected request path %s", r.URL.Path)
				w.WriteHeader(http.StatusNotFound)
			}
		}),
	)
	defer srv.Close()
	withTestKoiosBaseURL(t, srv.URL)

	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	result, err := Fetch(context.Background(), FetchConfig{
		Network:      "preview",
		CachePath:    filepath.Join(t.TempDir(), "cache.db"),
		Concurrency:  1,
		FromEpoch:    10,
		ThroughEpoch: 15,
	}, slog.New(slog.DiscardHandler))
	require.Error(
		t,
		err,
		"a permanent client error must surface as a hard Fetch error",
	)
	require.Nil(t, result)
	require.True(t, strings.Contains(err.Error(), "epoch 12"))
	require.ErrorIs(t, err, ErrKoiosPermanent)
	require.Less(
		t,
		epochInfoAttempts.Load(),
		int32(6),
		"the epoch dispatcher must stop well short of attempting the full 6-epoch range",
	)
}

// TestFetchTransient503LandsInFailedEpochs guards against over-correcting:
// a transient failure that exhausts get()'s internal retries (not classified
// permanent) must remain an isolated, resumable per-epoch failure exactly as
// before — recorded in FailedEpochs with Fetch returning (result, nil) rather
// than aborting the whole run.
func TestFetchTransient503LandsInFailedEpochs(t *testing.T) {
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.URL.Path == "/tip":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[{"epoch_no":100}]`))
			case r.URL.Path == "/pool_list" || r.URL.Path == "/pool_updates":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[]`))
			case r.URL.Path == "/epoch_info":
				epoch := r.URL.Query().Get("_epoch_no")
				if epoch == "21" {
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = w.Write([]byte("no server available"))
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(w, validEpochInfoTmpl, epoch)
			case r.URL.Path == "/epoch_params":
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(
					w,
					validEpochParamsTmpl,
					r.URL.Query().Get("_epoch_no"),
				)
			case r.URL.Path == "/totals":
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(
					w,
					validTotalsTmpl,
					r.URL.Query().Get("_epoch_no"),
				)
			default:
				t.Errorf("unexpected request path %s", r.URL.Path)
				w.WriteHeader(http.StatusNotFound)
			}
		}),
	)
	defer srv.Close()
	withTestKoiosBaseURL(t, srv.URL)

	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	result, err := Fetch(context.Background(), FetchConfig{
		Network:      "preview",
		CachePath:    filepath.Join(t.TempDir(), "cache.db"),
		Concurrency:  3,
		FromEpoch:    20,
		ThroughEpoch: 22,
	}, slog.New(slog.DiscardHandler))
	require.NoError(
		t,
		err,
		"an exhausted transient failure must not abort the run",
	)
	require.NotNil(t, result)
	require.Equal(t, []uint64{21}, result.FailedEpochs)
	require.Equal(t, 2, result.EpochsFetched)
}

// TestFetchEpochStopsSchedulingPoolsAfterPermanentError guards against the
// pool loop continuing to schedule every remaining pool after the first
// permanent (quota/auth) error within an epoch — previously any pool error
// was treated as an isolated, ignorable-until-drained failure, so a hard
// auth/quota failure on the very first pool could still fan out requests for
// every other pool in the epoch before the epoch as a whole was marked
// failed.
func TestFetchEpochStopsSchedulingPoolsAfterPermanentError(t *testing.T) {
	const poolTotal = 20
	var poolHistoryAttempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.URL.Path == "/tip":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[{"epoch_no":100}]`))
			case r.URL.Path == "/pool_list":
				pools := make([]string, poolTotal)
				for i := range pools {
					pools[i] = fmt.Sprintf(`{"pool_id_bech32":"pool%d"}`, i)
				}
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(w, `[%s]`, strings.Join(pools, ","))
			case r.URL.Path == "/pool_updates":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[]`))
			case r.URL.Path == "/epoch_info":
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(
					w,
					validEpochInfoTmpl,
					r.URL.Query().Get("_epoch_no"),
				)
			case r.URL.Path == "/epoch_params":
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(
					w,
					validEpochParamsTmpl,
					r.URL.Query().Get("_epoch_no"),
				)
			case r.URL.Path == "/totals":
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(
					w,
					validTotalsTmpl,
					r.URL.Query().Get("_epoch_no"),
				)
			case r.URL.Path == "/pool_history":
				poolHistoryAttempts.Add(1)
				if r.URL.Query().Get("_pool_bech32") == "pool0" {
					w.WriteHeader(http.StatusUnauthorized)
					_, _ = w.Write([]byte("invalid API key"))
					return
				}
				// Block until the client cancels this request rather than
				// sleeping a fixed duration: every pool worker shares poolCtx, so
				// once pool0's failure calls poolCancel, the client aborts this
				// in-flight request and net/http cancels r.Context() in lockstep
				// — no arbitrary timing margin needed to hold this pool's
				// concurrency slot open until the dispatch loop observes
				// cancellation.
				<-r.Context().Done()
			default:
				t.Errorf("unexpected request path %s", r.URL.Path)
				w.WriteHeader(http.StatusNotFound)
			}
		}),
	)
	defer srv.Close()
	withTestKoiosBaseURL(t, srv.URL)

	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	result, err := Fetch(context.Background(), FetchConfig{
		Network:      "preview",
		CachePath:    filepath.Join(t.TempDir(), "cache.db"),
		Concurrency:  1,
		FromEpoch:    30,
		ThroughEpoch: 30,
	}, slog.New(slog.DiscardHandler))
	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrKoiosPermanent)
	require.Less(
		t,
		poolHistoryAttempts.Load(),
		int32(poolTotal),
		"scheduling must stop well short of every pool once one hits a permanent error",
	)
}

// TestFetchBackfillsAccountsForPreExistingCache is the fetch-side regression
// test for the account-coverage-blind epoch-selection bug found in review of
// #3097: a Dingo deployment that already ran koios-parity before #3097
// landed has epochs with fresh koios_epoch_info/koios_totals rows but no
// koios_account_coverage row at all. Turning on cfg.AccountsEnabled must
// still get that epoch's accounts backfilled — GetUncachedEpochs alone
// (keyed only on koios_epoch_info presence) would otherwise never re-select
// it, since its pool-level data already looks "fetched" forever.
//
// This also exercises the chosen fix shape: the pool-level fetchEpoch call
// (epoch_info/totals/pool_history) is skipped entirely for an epoch whose
// pool data is already fresh — only the account backfill runs — so
// /epoch_info and /totals must never be re-requested for this epoch.
func TestFetchBackfillsAccountsForPreExistingCache(t *testing.T) {
	const network = "preview"
	const epoch = uint64(50)
	const koiosAddr = "stake1uzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"

	var epochInfoCalls, totalsCalls atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.URL.Path == "/tip":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[{"epoch_no":100}]`))
			case r.URL.Path == "/pool_list" || r.URL.Path == "/pool_updates":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[]`))
			case r.URL.Path == "/epoch_info":
				epochInfoCalls.Add(1)
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(
					w,
					validEpochInfoTmpl,
					strconv.FormatUint(epoch, 10),
				)
			case r.URL.Path == "/epoch_params":
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(
					w,
					validEpochParamsTmpl,
					r.URL.Query().Get("_epoch_no"),
				)
			case r.URL.Path == "/totals":
				totalsCalls.Add(1)
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(
					w,
					validTotalsTmpl,
					strconv.FormatUint(epoch, 10),
				)
			case r.URL.Path == "/account_list":
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(w, `[{"stake_address":%q}]`, koiosAddr)
			case r.URL.Path == "/account_reward_history":
				var body struct {
					StakeAddresses []string `json:"_stake_addresses"`
					EpochNo        uint64   `json:"_epoch_no"`
				}
				// require.* must never run inside an HTTP handler
				// goroutine — a failure calls t.FailNow, valid only on the
				// goroutine running the test function, which would abort
				// the handler without writing a response and hang the
				// client until timeout. Use t.Errorf plus an explicit error
				// response instead.
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("decode request body: %v", err)
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				var sb strings.Builder
				sb.WriteByte('[')
				for i, addr := range body.StakeAddresses {
					if i > 0 {
						sb.WriteByte(',')
					}
					fmt.Fprintf(
						&sb,
						`{"stake_address":%q,"earned_epoch":%d,"amount":"1000000","type":"member"}`,
						addr,
						body.EpochNo,
					)
				}
				sb.WriteByte(']')
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(sb.String()))
			default:
				t.Errorf("unexpected request path %s", r.URL.Path)
				w.WriteHeader(http.StatusNotFound)
			}
		}),
	)
	defer srv.Close()
	withTestKoiosBaseURL(t, srv.URL)

	cachePath := filepath.Join(t.TempDir(), "cache.db")
	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)

	fetchedAt := time.Now().Add(-time.Hour).UTC()
	require.NoError(t, cache.CommitEpochData(KoiosEpochInfo{
		Network:      network,
		Epoch:        epoch,
		ActiveStake:  "12345",
		EpochEndTime: fetchedAt,
		FetchedAt:    fetchedAt,
	}, nil, &KoiosTotals{
		Treasury:  "1",
		Reserves:  "1",
		Fees:      "1",
		Reward:    "1",
		FetchedAt: fetchedAt,
	}))
	require.NoError(t, cache.UpsertCheckEpochStatus(CheckEpochStatus{
		Network:       network,
		Epoch:         epoch,
		LastCheckedAt: fetchedAt.Add(time.Minute),
		Status:        StatusPass,
	}))
	require.NoError(t, cache.Close())

	result, err := Fetch(context.Background(), FetchConfig{
		Network:         network,
		CachePath:       cachePath,
		Concurrency:     1,
		FromEpoch:       epoch,
		ThroughEpoch:    epoch,
		AccountsEnabled: true,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Empty(t, result.FailedEpochs)
	require.Equal(
		t,
		1,
		result.EpochsFetched,
		"the pre-existing epoch must be (re)fetched to backfill accounts",
	)
	require.Equal(
		t,
		int32(0),
		epochInfoCalls.Load(),
		"pool-level data was already fresh; /epoch_info must not be re-fetched just to backfill accounts",
	)
	require.Equal(
		t,
		int32(0),
		totalsCalls.Load(),
		"pool-level data was already fresh; /totals must not be re-fetched just to backfill accounts",
	)

	cache2, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache2.Close() //nolint:errcheck

	cov, err := cache2.GetAccountCoverage(network, epoch)
	require.NoError(t, err)
	require.True(t, cov.Complete)
	require.Equal(t, 1, cov.RequestedCount)

	rows, err := cache2.GetAccountRewardsForEpoch(network, epoch)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, koiosAddr, rows[0].StakeAddress)

	info, err := cache2.GetEpochInfo(network, epoch)
	require.NoError(t, err)
	require.WithinDuration(
		t,
		fetchedAt,
		info.FetchedAt,
		time.Second,
		"pool-level fetched_at must not be clobbered by an accounts-only backfill",
	)
}
