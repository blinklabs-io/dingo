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

package utxorpc

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestUtxorpcAnonymousPlaintextTLSAndCORS(t *testing.T) {
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
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			t.Cleanup(cancel)
			client := *tc.cli
			client.Timeout = 5 * time.Second
			u, addr := startOnFreePort(
				t,
				ctx,
				tc.tls,
				func(cfg *UtxorpcConfig) {
					cfg.CORSAllowedOrigins = []string{origin}
				},
			)
			t.Cleanup(func() { stopUtxorpc(t, u) })
			resp := healthCheckAnonymous(t, ctx, &client, tc.url+addr, origin)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(
				t,
				origin,
				resp.Header.Get("Access-Control-Allow-Origin"),
			)
			preflight, err := http.NewRequestWithContext(
				ctx,
				http.MethodOptions,
				tc.url+addr+"/grpc.health.v1.Health/Check",
				nil,
			)
			require.NoError(t, err)
			preflight.Header.Set("Origin", origin)
			preflight.Header.Set(
				"Access-Control-Request-Method",
				http.MethodPost,
			)
			preflight.Header.Set(
				"Access-Control-Request-Headers",
				"Content-Type, Connect-Protocol-Version",
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

func healthCheckAnonymous(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	origin string,
) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		baseURL+"/grpc.health.v1.Health/Check",
		strings.NewReader("{}"),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connect-Protocol-Version", "1")
	req.Header.Set("Origin", origin)
	resp, err := client.Do(req)
	require.NoError(t, err)
	return resp
}
