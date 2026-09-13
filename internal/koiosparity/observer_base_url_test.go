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

// The two tests here pin the hop from a config's BaseURL to the client the
// caller actually queries. The other base-URL tests construct a KoiosClient
// directly, so nothing else covers NewObserver and Fetch passing cfg.BaseURL
// through: dropping that argument would leave a configured host accepted while
// every request still went to the network default.
//
// The negative — that the network default is not the host contacted — is
// asserted against koiosBaseURLs rather than by pointing the default at a
// local server. Redirecting the default means writing to that process-wide
// map, which every concurrently constructed client reads, and that is what
// kept this package's tests from running in parallel. Reading it is safe, so
// these tests are parallel like the rest of the package.

// TestObserverSendsRequestsToTheConfiguredBaseURL covers NewObserver.
func TestObserverSendsRequestsToTheConfiguredBaseURL(t *testing.T) {
	t.Parallel()

	var overrideHits atomic.Int32
	overrideSrv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			overrideHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"epoch_no":42}]`))
		}),
	)
	defer overrideSrv.Close()

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

	// Both assertions abort the test, so a dropped pass-through is caught
	// here and no request is made to the real public host below.
	require.Equal(
		t, overrideSrv.URL, o.koios.ResolvedBaseURL(),
		"the observer's client must resolve to the configured BaseURL",
	)
	require.NotEqual(
		t,
		koiosBaseURLs["preview"],
		o.koios.ResolvedBaseURL(),
		"the observer's client resolved to the network default despite a configured BaseURL",
	)

	epoch, err := o.koios.GetTipEpoch(context.Background())
	require.NoError(
		t,
		err,
		"the request must reach the override, not the default",
	)
	require.Equal(t, uint64(42), epoch)

	require.Positive(
		t, overrideHits.Load(),
		"the configured BaseURL received no request",
	)
}

// TestFetchSendsRequestsToTheConfiguredBaseURL is the companion for the other
// member of the same class: Fetch builds its own KoiosClient, and that
// pass-through was equally unpinned — blanking cfg.BaseURL there passed the
// whole suite.
//
// Fetch is expected to fail here, because the override serves only a tip
// epoch. What is being asserted is the routing. Fetch returns no client, so
// the resolved root is read back from the koios_source row recordKoiosSource
// stamps on the cache before any epoch is fetched.
func TestFetchSendsRequestsToTheConfiguredBaseURL(t *testing.T) {
	t.Parallel()

	var overrideHits atomic.Int32
	overrideSrv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			overrideHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"epoch_no":42}]`))
		}),
	)
	defer overrideSrv.Close()

	cachePath := filepath.Join(t.TempDir(), "cache.db")

	_, _ = Fetch(
		context.Background(),
		FetchConfig{
			Network:           "preview",
			CachePath:         cachePath,
			BaseURL:           overrideSrv.URL,
			AllowInsecureHTTP: true,
			FromEpoch:         1,
			ThroughEpoch:      1,
		},
		slog.New(slog.DiscardHandler),
	)

	cache, err := OpenCache(cachePath, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })

	recorded, ok, err := cache.GetKoiosSource("preview")
	require.NoError(t, err)
	require.True(t, ok, "Fetch recorded no koios source")
	require.Equal(
		t, overrideSrv.URL, recorded,
		"Fetch's client must resolve to the configured BaseURL",
	)
	require.NotEqual(
		t, koiosBaseURLs["preview"], recorded,
		"Fetch resolved to the network default despite a configured BaseURL",
	)

	require.Positive(
		t, overrideHits.Load(),
		"the configured BaseURL received no request",
	)
}
