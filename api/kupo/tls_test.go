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

package kupo

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// startOnFreePort binds cfg to a free loopback port, retrying up to
// testutil.BindAttempts times. testutil.FreePort can only report a port that
// was free when it looked, so another process claiming it before Start binds
// is an ordinary race rather than a failure of the code under test. This
// mirrors api/blockfrost's helper of the same name.
func startOnFreePort(
	t *testing.T,
	ctx context.Context,
	cfg Config,
) (*Server, string) {
	t.Helper()
	var lastErr error
	for range testutil.BindAttempts {
		addr := testutil.FreePort(t)
		cfg.ListenAddress = addr
		srv := New(cfg, &mockNode{}, nil)
		attemptCtx, cancel := context.WithCancel(ctx)
		lastErr = srv.Start(attemptCtx)
		if lastErr == nil {
			t.Cleanup(cancel)
			return srv, addr
		}
		cancel()
	}
	t.Fatalf("could not start on a free loopback port: %v", lastErr)
	return nil, ""
}

func TestAnonymousPlaintextTLSAndCORS(t *testing.T) {
	t.Parallel()

	const origin = "https://wallet.example"
	cert, key := testutil.GenerateTestTLSCertKey(t)
	for _, tc := range []struct {
		name string
		tls  apiconfig.EffectiveTLS
		url  string
		cli  *http.Client
	}{
		{"plaintext", apiconfig.EffectiveTLS{}, "http://", http.DefaultClient},
		{"tls", apiconfig.EffectiveTLS{Enabled: true, CertFilePath: cert, KeyFilePath: key}, "https://", testutil.InsecureHTTPClient()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := *tc.cli
			client.Timeout = 5 * time.Second
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			srv, addr := startOnFreePort(t, ctx, Config{
				TLS:                tc.tls,
				CORSAllowedOrigins: []string{origin},
			})
			t.Cleanup(func() {
				stopCtx, stopCancel := context.WithTimeout(
					context.Background(),
					5*time.Second,
				)
				defer stopCancel()
				require.NoError(t, srv.Stop(stopCtx))
			})
			getCtx, getCancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer getCancel()
			getReq, err := http.NewRequestWithContext(
				getCtx, http.MethodGet, tc.url+addr+"/matches/*/*", nil,
			)
			require.NoError(t, err)
			getReq.Header.Set("Origin", origin)
			resp, err := client.Do(getReq)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(
				t,
				origin,
				resp.Header.Get("Access-Control-Allow-Origin"),
			)
			preflight, err := http.NewRequestWithContext(
				t.Context(),
				http.MethodOptions,
				tc.url+addr+"/matches/*/*",
				nil,
			)
			require.NoError(t, err)
			preflight.Header.Set("Origin", origin)
			preflight.Header.Set(
				"Access-Control-Request-Method",
				http.MethodGet,
			)
			corsResp, err := client.Do(preflight)
			require.NoError(t, err)
			defer corsResp.Body.Close()
			require.Equal(t, http.StatusNoContent, corsResp.StatusCode)
			require.Equal(
				t,
				origin,
				corsResp.Header.Get("Access-Control-Allow-Origin"),
			)
		})
	}
}
