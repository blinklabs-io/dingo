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
	"errors"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// failingKoiosTransport fails every request whose path is in paths and
// forwards the rest. With status 0 the failure is a transport error, the way a
// Koios connection reset is; otherwise it is a response with status and body.
type failingKoiosTransport struct {
	next   http.RoundTripper
	paths  map[string]bool
	status int
	body   string
}

func (t failingKoiosTransport) RoundTrip(
	req *http.Request,
) (*http.Response, error) {
	if !t.paths[req.URL.Path] {
		return t.next.RoundTrip(req)
	}
	if t.status == 0 {
		return nil, errors.New("connection reset by peer")
	}
	return &http.Response{
		StatusCode: t.status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(t.body)),
		Request:    req,
	}, nil
}

// TestStrictFetchErrorFatalityFollowsProvenance drives processEpoch and
// processAccountEpoch through a real fetch. A Koios request that fails
// transiently must be retried without stopping the node, while a local cache
// write failure during the same fetch must stay fatal in strict mode.
func TestStrictFetchErrorFatalityFollowsProvenance(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		accounts  bool
		failPaths []string
		// failStatus and failBody, when set, make failPaths answer with that
		// response instead of a transport error.
		failStatus int
		failBody   string
		// failTable, when set, makes every insert into that cache table fail.
		failTable string
		// epoch defaults to 5, the one epoch the fake Koios serves; any other
		// epoch gets a 404, which the client reports as ErrKoiosPermanent.
		epoch uint64
		fatal bool
	}{
		{name: "aggregate/koios unavailable", failPaths: []string{"/pool_list", "/pool_updates", "/epoch_info", "/totals", "/epoch_params"}},
		{name: "aggregate/cache write", failTable: "koios_epoch_info", fatal: true},
		{name: "aggregate/koios permanent", epoch: 6, fatal: true},
		{name: "aggregate/koios 408 budget exhausted", failPaths: []string{"/pool_list"}, failStatus: http.StatusRequestTimeout},
		{name: "aggregate/koios daily quota", failPaths: []string{"/pool_list"}, failStatus: http.StatusTooManyRequests, failBody: "Exceeded Tier Limit"},
		{name: "accounts/koios unavailable", accounts: true, failPaths: []string{"/account_list"}},
		{name: "accounts/cache write", accounts: true, failTable: "koios_account_universe_state", fatal: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source, err := NewDatabaseSource(newTestDatabaseSourceDB(t))
			require.NoError(t, err)
			srv := newFakeKoiosServer(t, map[uint64]*fakeEpochRef{
				5: {activeStake: "1000000", treasury: "10", reserves: "20", fees: "30", endTimeUnix: time.Now().Add(-time.Hour).Unix()},
			})
			var mu sync.Mutex
			var fatals []error
			o, err := NewObserver(ObserverConfig{
				Network:               "preview",
				CachePath:             filepath.Join(t.TempDir(), "cache.db"),
				Source:                source,
				BaseURL:               srv.URL,
				AllowInsecureHTTP:     true,
				AllowPrivateAddresses: true,
				AccountsEnabled:       tc.accounts,
				Strict:                true,
				FetchRetryAttempts:    1,
				ErrorRetryDelay:       time.Hour,
				Logger:                slog.New(slog.DiscardHandler),
				FatalFunc: func(err error) {
					mu.Lock()
					defer mu.Unlock()
					fatals = append(fatals, err)
				},
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, o.Stop(context.Background())) })
			if len(tc.failPaths) > 0 {
				paths := make(map[string]bool, len(tc.failPaths))
				for _, p := range tc.failPaths {
					paths[p] = true
				}
				o.koios.http.Transport = failingKoiosTransport{
					next:   o.koios.http.Transport,
					paths:  paths,
					status: tc.failStatus,
					body:   tc.failBody,
				}
				o.koios.koios408MaxRetries = 1
				o.koios.koios408InitialBackoff = time.Millisecond
				o.koios.koios408MaxBackoff = time.Millisecond
			}
			if tc.failTable != "" {
				_, err := o.cache.db.Exec(
					"CREATE TRIGGER fail_insert BEFORE INSERT ON " +
						tc.failTable +
						" BEGIN SELECT RAISE(ABORT, 'disk I/O error'); END",
				)
				require.NoError(t, err)
			}

			epoch := tc.epoch
			if epoch == 0 {
				epoch = 5
			}
			if tc.accounts {
				o.processAccountEpoch(context.Background(), epoch)
			} else {
				o.processEpoch(context.Background(), epoch)
			}

			retrying, err := o.cache.GetEpochsNeedingRetry("preview", tc.accounts)
			require.NoError(t, err)
			require.Equal(t, []uint64{epoch}, retrying)
			mu.Lock()
			defer mu.Unlock()
			if tc.fatal {
				require.Len(t, fatals, 1, "only a transient Koios failure is non-fatal")
				require.True(t, o.stopping())
			} else {
				require.Empty(t, fatals,
					"an unavailable Koios reference must not stop the node")
				require.False(t, o.stopping())
			}
		})
	}
}
