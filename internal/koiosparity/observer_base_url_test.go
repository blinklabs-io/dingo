// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package koiosparity

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestObserverSendsRequestsToTheConfiguredBaseURL pins the hop from
// ObserverConfig.BaseURL to the client the observer actually queries.
//
// The existing base-URL tests construct a KoiosClient directly, and the
// observer tests reach their fake server through withTestKoiosBaseURL, which
// swaps the package-level default for the network rather than exercising the
// override. Neither shape covers NewObserver passing cfg.BaseURL through, so
// dropping that argument would leave a configured host accepted while every
// request still went to the default endpoint.
//
// Here the default is pointed at a server that fails every request and the
// override at one that answers, so the test can only pass if the override is
// the host being queried.
func TestObserverSendsRequestsToTheConfiguredBaseURL(t *testing.T) {
	var defaultHits, overrideHits atomic.Int32

	defaultSrv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			defaultHits.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		}),
	)
	defer defaultSrv.Close()

	overrideSrv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			overrideHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"epoch_no":42}]`))
		}),
	)
	defer overrideSrv.Close()

	// The network default must not be the server that answers.
	withTestKoiosBaseURL(t, defaultSrv.URL)

	source, err := NewDatabaseSource(newTestDatabaseSourceDB(t))
	require.NoError(t, err)

	o, err := NewObserver(ObserverConfig{
		Network:   "preview",
		CachePath: filepath.Join(t.TempDir(), "cache.db"),
		Source:    source,
		// httptest serves plain HTTP, so the override needs the same escape
		// hatch an operator would use for a local deployment. That keeps the
		// transport guard in the path rather than bypassing it.
		BaseURL:           overrideSrv.URL,
		AllowInsecureHTTP: true,
		Logger:            slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = o.Stop(context.Background()) })

	require.Equal(
		t, overrideSrv.URL, o.koios.ResolvedBaseURL(),
		"the observer's client must resolve to the configured BaseURL",
	)

	epoch, err := o.koios.GetTipEpoch(context.Background())
	require.NoError(t, err, "the request must reach the override, not the default")
	require.Equal(t, uint64(42), epoch)

	require.Positive(
		t, overrideHits.Load(),
		"the configured BaseURL received no request",
	)
	require.Zero(
		t, defaultHits.Load(),
		"a request reached the network default despite a configured BaseURL",
	)
}

// TestFetchSendsRequestsToTheConfiguredBaseURL is the companion for the other
// member of the same class: Fetch builds its own KoiosClient, and that
// pass-through was equally unpinned — blanking cfg.BaseURL there passed the
// whole suite.
//
// Fetch is expected to fail here, because the override serves only a tip
// epoch. What is being asserted is the routing: the configured host is the one
// contacted, and the network default is not.
func TestFetchSendsRequestsToTheConfiguredBaseURL(t *testing.T) {
	var defaultHits, overrideHits atomic.Int32

	defaultSrv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			defaultHits.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		}),
	)
	defer defaultSrv.Close()

	overrideSrv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			overrideHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"epoch_no":42}]`))
		}),
	)
	defer overrideSrv.Close()

	withTestKoiosBaseURL(t, defaultSrv.URL)

	_, _ = Fetch(
		context.Background(),
		FetchConfig{
			Network:           "preview",
			CachePath:         filepath.Join(t.TempDir(), "cache.db"),
			BaseURL:           overrideSrv.URL,
			AllowInsecureHTTP: true,
			FromEpoch:         1,
			ThroughEpoch:      1,
		},
		slog.New(slog.DiscardHandler),
	)

	require.Positive(
		t, overrideHits.Load(),
		"the configured BaseURL received no request",
	)
	require.Zero(
		t, defaultHits.Load(),
		"a request reached the network default despite a configured BaseURL",
	)
}
