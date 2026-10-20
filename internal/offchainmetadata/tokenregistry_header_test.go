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

func headerRecorder(
	t *testing.T,
	body []byte,
	handler func(w http.ResponseWriter, r *http.Request),
) (*httptest.Server, func() []string) {
	return headerRecorderFor(t, "Authorization", body, handler)
}

func headerRecorderFor(
	t *testing.T,
	header string,
	body []byte,
	handler func(http.ResponseWriter, *http.Request),
) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	var manifest []byte
	server := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			seen = append(seen, r.Header.Get(header))
			mu.Unlock()
			if r.URL.Path == "/manifest" {
				_, _ = w.Write(manifest)
				return
			}
			handler(w, r)
		}),
	)
	manifest = signedTokenRegistryManifest(t, server.URL, body, 1)
	testRegistryServers.Store(server.URL, &registryServer{
		Server:     server,
		archiveURL: server.URL,
	})
	t.Cleanup(func() {
		testRegistryServers.Delete(server.URL)
		server.Close()
	})
	return server, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

func registryArchive(t *testing.T) []byte {
	return tarballOf(t, map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut, "nutcoin", "NUT", "",
		),
	})
}

func serveRegistry(body []byte) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}
}

func TestTokenRegistrySyncSendsConfiguredHeaders(t *testing.T) {
	t.Parallel()

	body := registryArchive(t)
	server, seen := headerRecorder(t, body, serveRegistry(body))
	sync := newTestSync(t, newFakeTokenRegistryStore(), server.URL, nil)

	_, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(t, []string{"", "Bearer " + redactHeader}, seen())
}

func TestTokenRegistrySyncSendsNoHeadersByDefault(t *testing.T) {
	t.Parallel()

	body := registryArchive(t)
	server, seen := headerRecorder(t, body, serveRegistry(body))
	sync := newTestSync(
		t, newFakeTokenRegistryStore(), server.URL,
		func(cfg *TokenRegistryConfig) { cfg.Headers = nil },
	)

	_, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(t, []string{"", ""}, seen())
}

func TestTokenRegistrySyncKeepsHeadersOnSameOriginRedirect(t *testing.T) {
	t.Parallel()

	body := registryArchive(t)
	server, seen := headerRecorderFor(
		t, "X-API-Key", body,
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/moved" {
				serveRegistry(body)(w, r)
				return
			}
			http.Redirect(w, r, "/moved", http.StatusFound)
		},
	)
	sync := newTestSync(
		t,
		newFakeTokenRegistryStore(),
		server.URL,
		func(cfg *TokenRegistryConfig) { cfg.Headers = map[string]string{"X-API-Key": "Bearer " + redactHeader} },
	)

	_, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(
		t,
		[]string{"", "Bearer " + redactHeader, "Bearer " + redactHeader},
		seen(),
	)
}

func TestTokenRegistrySyncDropsHeadersOnCrossOriginRedirect(t *testing.T) {
	t.Parallel()

	body := registryArchive(t)
	target, targetSeen := headerRecorderFor(t, "X-API-Key", body, serveRegistry(body))
	origin, originSeen := headerRecorderFor(
		t, "X-API-Key", body,
		func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target.URL, http.StatusFound)
		},
	)
	sync := newTestSync(
		t,
		newFakeTokenRegistryStore(),
		origin.URL,
		func(cfg *TokenRegistryConfig) { cfg.Headers = map[string]string{"X-API-Key": "Bearer " + redactHeader} },
	)

	_, err := sync.SyncOnce(t.Context())

	require.NoError(t, err)
	require.Equal(t, []string{"", "Bearer " + redactHeader}, originSeen())
	require.Equal(
		t, []string{""}, targetSeen(),
		"a redirect to another origin must not carry the credential",
	)
}

func TestNewTokenRegistrySyncRejectsInvalidHeaders(t *testing.T) {
	t.Parallel()

	for name, headers := range map[string]map[string]string{
		"name with space":         {"Bad Name": redactHeader},
		"empty name":              {"": redactHeader},
		"reserved user agent":     {"uSeR-aGeNt": redactHeader},
		"reserved accept":         {"Accept": redactHeader},
		"reserved etag":           {"If-None-Match": redactHeader},
		"reserved host":           {"Host": redactHeader},
		"reserved content length": {"Content-Length": redactHeader},
		"value with line break": {
			"Authorization": "Bearer " + redactHeader + "\r\nX-Injected: 1",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := NewTokenRegistrySync(TokenRegistryConfig{
				Store:              newFakeTokenRegistryStore(),
				ManifestURL:        "https://registry.example/manifest",
				TrustedManifestKey: testTokenRegistryManifestKey(),
				Headers:            headers,
			})

			require.Error(t, err)
			require.NotContains(t, err.Error(), redactHeader)
		})
	}
}

func TestNewTokenRegistrySyncRejectsHeadersOverRemoteHTTP(t *testing.T) {
	t.Parallel()

	_, err := NewTokenRegistrySync(TokenRegistryConfig{
		Store:              newFakeTokenRegistryStore(),
		SourceURL:          "http://registry.example/snapshot.tar.gz",
		ManifestURL:        "https://registry.example/manifest",
		TrustedManifestKey: testTokenRegistryManifestKey(),
		Headers:            map[string]string{"Authorization": "Bearer " + redactHeader},
	})

	require.ErrorContains(t, err, "headers require an HTTPS or loopback")
	require.NotContains(t, err.Error(), redactHeader)
}

func TestTokenRegistryRedirectGuardRunsAfterCustomCallback(t *testing.T) {
	t.Parallel()
	body := registryArchive(t)
	target, seen := headerRecorderFor(t, "X-API-Key", body, serveRegistry(body))
	origin, _ := headerRecorderFor(
		t,
		"X-API-Key",
		body,
		func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) },
	)
	sync := newTestSync(
		t,
		newFakeTokenRegistryStore(),
		origin.URL,
		func(cfg *TokenRegistryConfig) {
			cfg.Headers = map[string]string{"X-API-Key": redactHeader}
			cfg.HTTPClient = origin.Client()
			cfg.HTTPClient.CheckRedirect = func(req *http.Request, _ []*http.Request) error {
				req.Header.Set("X-API-Key", redactHeader)
				req.Header["x-api-key"] = []string{redactHeader}
				return nil
			}
		},
	)
	_, err := sync.SyncOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, []string{""}, seen())
}

func TestTokenRegistryRedirectDropsReferer(t *testing.T) {
	t.Parallel()
	client := &http.Client{
		CheckRedirect: func(req *http.Request, _ []*http.Request) error {
			req.Header.Set(
				"Referer", "https://registry.example/archive?token="+redactHeader,
			)
			return nil
		},
	}
	sync, err := NewTokenRegistrySync(TokenRegistryConfig{
		Store:              newFakeTokenRegistryStore(),
		SourceURL:          "https://registry.example/archive",
		ManifestURL:        "https://registry.example/manifest",
		TrustedManifestKey: testTokenRegistryManifestKey(),
		HTTPClient:         client,
	})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodGet, "https://mirror.example/archive", nil)
	require.NoError(t, err)
	via, err := http.NewRequest(
		http.MethodGet,
		"https://registry.example/archive?token="+redactHeader,
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, sync.client.CheckRedirect(req, []*http.Request{via}))
	require.Empty(t, req.Header.Get("Referer"))
}
