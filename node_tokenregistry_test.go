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
	"net/http"
	"net/http/httptest"
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
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case auth <- r.Header.Get("Authorization"):
			default:
			}
			w.WriteHeader(http.StatusNotFound)
		}),
	)
	t.Cleanup(server.Close)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })
	n := &Node{
		db: db,
		config: Config{
			tokenRegistry: TokenRegistryConfig{
				SourceURL:             server.URL,
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
