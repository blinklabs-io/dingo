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

package dingo

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// TestNewTokenRegistrySyncSendsConfiguredHeaders drives the syncer node.go
// builds from the runtime config, so a header that stops at the config layer
// is caught.
func TestNewTokenRegistrySyncSendsConfiguredHeaders(t *testing.T) {
	t.Parallel()

	auth := make(chan string, 1)
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	var manifest []byte
	server := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/manifest" {
				_, _ = w.Write(manifest)
				return
			}
			select {
			case auth <- r.Header.Get("Authorization"):
			default:
			}
			w.WriteHeader(http.StatusNotFound)
		}),
	)
	t.Cleanup(server.Close)
	payload, err := json.Marshal(map[string]any{
		"version":       1,
		"sequence":      1,
		"archiveUrl":    server.URL + "/archive",
		"archiveBytes":  0,
		"archiveDigest": strings.Repeat("0", 64),
	})
	require.NoError(t, err)
	message := append([]byte("dingo-token-registry-manifest-v1\x00"), payload...)
	manifest, err = json.Marshal(map[string]string{
		"payload": base64.RawURLEncoding.EncodeToString(payload),
		"signature": base64.RawURLEncoding.EncodeToString(
			ed25519.Sign(privateKey, message),
		),
	})
	require.NoError(t, err)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })
	n := &Node{
		db: db,
		config: Config{
			tokenRegistry: TokenRegistryConfig{
				HTTPClient:            server.Client(),
				SourceURL:             server.URL + "/archive",
				ManifestURL:           server.URL + "/manifest",
				TrustedManifestKey:    hex.EncodeToString(publicKey),
				AllowPrivateAddresses: true,
				Headers:               map[string]string{"Authorization": "Bearer node"},
			},
		},
	}

	sync, err := n.newTokenRegistrySync()
	require.NoError(t, err)
	_, err = sync.SyncOnce(t.Context())

	require.Error(t, err)
	// SyncOnce has returned, so the request was served or never made.
	select {
	case got := <-auth:
		require.Equal(t, "Bearer node", got)
	default:
		t.Fatal("the registry server received no request")
	}
}
