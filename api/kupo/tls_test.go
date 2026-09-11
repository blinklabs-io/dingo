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

func TestAnonymousPlaintextTLSAndCORS(t *testing.T) {
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
			addr := testutil.FreePort(t)
			srv := New(
				Config{
					ListenAddress:      addr,
					TLS:                tc.tls,
					CORSAllowedOrigins: []string{origin},
				},
				&mockNode{},
				nil,
			)
			require.NoError(t, srv.Start(ctx))
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
