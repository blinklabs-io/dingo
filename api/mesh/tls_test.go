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

package mesh

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

func TestMeshAnonymousPlaintextTLSAndCORS(t *testing.T) {
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
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			srv, addr := startOnFreePort(
				t,
				ctx,
				newTestDeps(),
				func(c *ServerConfig) { c.TLS = tc.tls; c.CORSAllowedOrigins = []string{origin} },
			)
			t.Cleanup(func() {
				stopCtx, stopCancel := context.WithTimeout(
					context.Background(),
					5*time.Second,
				)
				defer stopCancel()
				require.NoError(t, srv.Stop(stopCtx))
			})
			body := `{"network_identifier":{"blockchain":"cardano","network":"testnet"}}`
			req, err := http.NewRequestWithContext(
				t.Context(),
				http.MethodPost,
				tc.url+addr+"/network/list",
				strings.NewReader(body),
			)
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			resp, err := tc.cli.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			preflight, err := http.NewRequestWithContext(
				t.Context(),
				http.MethodOptions,
				tc.url+addr+"/network/list",
				nil,
			)
			require.NoError(t, err)
			preflight.Header.Set("Origin", origin)
			preflight.Header.Set(
				"Access-Control-Request-Method",
				http.MethodPost,
			)
			corsResp, err := tc.cli.Do(preflight)
			require.NoError(t, err)
			defer corsResp.Body.Close()
			require.Equal(t, http.StatusNoContent, corsResp.StatusCode)
		})
	}
}
