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

package offchainmetadata

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// headerRecorder is a registry server that records the Authorization header
// of every request it serves.
func headerRecorder(
	t *testing.T,
	handler func(w http.ResponseWriter, r *http.Request),
) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			seen = append(seen, r.Header.Get("Authorization"))
			mu.Unlock()
			handler(w, r)
		}),
	)
	t.Cleanup(server.Close)
	return server, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

func serveRegistry(t *testing.T) func(http.ResponseWriter, *http.Request) {
	body := tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut, "nutcoin", "NUT", "",
		),
	})
	return func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}
}

func TestTokenRegistrySyncSendsConfiguredHeaders(t *testing.T) {
	t.Parallel()

	server, seen := headerRecorder(t, serveRegistry(t))
	sync := newTestSync(t, newFakeTokenRegistryStore(), server.URL, nil)

	_, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(t, []string{"Bearer " + redactHeader}, seen())
}

func TestTokenRegistrySyncSendsNoHeadersByDefault(t *testing.T) {
	t.Parallel()

	server, seen := headerRecorder(t, serveRegistry(t))
	sync := newTestSync(
		t, newFakeTokenRegistryStore(), server.URL,
		func(cfg *TokenRegistryConfig) { cfg.Headers = nil },
	)

	_, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(t, []string{""}, seen())
}

func TestTokenRegistrySyncKeepsHeadersOnSameOriginRedirect(t *testing.T) {
	t.Parallel()

	server, seen := headerRecorder(
		t,
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/moved" {
				serveRegistry(t)(w, r)
				return
			}
			http.Redirect(w, r, "/moved", http.StatusFound)
		},
	)
	sync := newTestSync(t, newFakeTokenRegistryStore(), server.URL, nil)

	_, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(
		t,
		[]string{"Bearer " + redactHeader, "Bearer " + redactHeader},
		seen(),
	)
}

func TestTokenRegistrySyncDropsHeadersOnCrossOriginRedirect(t *testing.T) {
	t.Parallel()

	target, targetSeen := headerRecorder(t, serveRegistry(t))
	origin, originSeen := headerRecorder(
		t,
		func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target.URL, http.StatusFound)
		},
	)
	sync := newTestSync(t, newFakeTokenRegistryStore(), origin.URL, nil)

	_, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(t, []string{"Bearer " + redactHeader}, originSeen())
	require.Equal(
		t, []string{""}, targetSeen(),
		"a redirect to another origin must not carry the credential",
	)
}

func TestNewTokenRegistrySyncRejectsInvalidHeaders(t *testing.T) {
	t.Parallel()

	for name, headers := range map[string]map[string]string{
		"name with space": {"Bad Name": redactHeader},
		"empty name":      {"": redactHeader},
		"value with line break": {
			"Authorization": "Bearer " + redactHeader + "\r\nX-Injected: 1",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := NewTokenRegistrySync(TokenRegistryConfig{
				Store:   newFakeTokenRegistryStore(),
				Headers: headers,
			})

			require.Error(t, err)
			require.NotContains(t, err.Error(), redactHeader)
		})
	}
}
