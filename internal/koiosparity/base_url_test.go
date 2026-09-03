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

package koiosparity

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewKoiosClientBaseURLOverride covers pointing the client at a self-hosted
// Koios instance. The requests have to actually reach that host, not the public
// one, so the assertion is a served request rather than a field comparison.
func TestNewKoiosClientBaseURLOverride(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/v1/tip" {
				http.NotFound(w, r)
				return
			}
			hits.Add(1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"epoch_no":42}]`))
		}),
	)
	defer srv.Close()

	// httptest serves plain HTTP, so this is the one case that needs the
	// insecure escape hatch.
	client, err := NewKoiosClient("preview", "", srv.URL+"/api/v1", true)
	require.NoError(t, err)

	epoch, err := client.GetTipEpoch(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(42), epoch)
	assert.Equal(t, int32(1), hits.Load(),
		"the request must reach the configured host")
}

// TestNewKoiosClientBaseURLTrimsTrailingSlash pins the ergonomics: an operator
// pasting a root with a trailing slash must not produce doubled separators.
func TestNewKoiosClientBaseURLTrimsTrailingSlash(t *testing.T) {
	client, err := NewKoiosClient("preview", "", "https://host.example/api/v1/", false)
	require.NoError(t, err)
	assert.Equal(t, "https://host.example/api/v1", client.baseURL)

	spaced, err := NewKoiosClient("preview", "", "  https://host.example/api/v1  ", false)
	require.NoError(t, err)
	assert.Equal(t, "https://host.example/api/v1", spaced.baseURL)
}

// TestNewKoiosClientDefaultsToPublicHost pins that an empty override changes
// nothing, including the burst cap that koios.rest's tiers require.
func TestNewKoiosClientDefaultsToPublicHost(t *testing.T) {
	client, err := NewKoiosClient("preview", "", "", false)
	require.NoError(t, err)
	assert.Equal(t, koiosBaseURLs["preview"], client.baseURL)
	require.NotNil(t, client.limiter)
	assert.Equal(t, koiosBurstLimitSafe, client.limiter.limit,
		"the public host keeps the published tier cap")
}

// TestNewKoiosClientCustomHostDropsBurstCap covers the reason the override
// exists. koiosBurstLimitSafe describes koios.rest's own Public/Free window and
// says nothing about another deployment, so throttling a self-hosted instance
// against it would enforce a limit that does not exist.
func TestNewKoiosClientCustomHostDropsBurstCap(t *testing.T) {
	client, err := NewKoiosClient("preview", "", "https://host.example/api/v1", false)
	require.NoError(t, err)
	require.NotNil(t, client.limiter)
	assert.LessOrEqual(t, client.limiter.limit, 0,
		"a self-hosted host is not subject to the public tier cap")

	// And an unlimited limiter really does not block.
	for range koiosBurstLimitSafe + 5 {
		require.NoError(t, client.limiter.wait(context.Background()))
	}
}

// TestNewKoiosClientRejectsUnsupportedNetworkWithOverride pins that supplying a
// host does not bypass network validation: StakeAddressFromCredential hardcodes
// the testnet address network ID, so an unvalidated "mainnet" would silently
// generate wrong-network stake addresses.
func TestNewKoiosClientRejectsUnsupportedNetworkWithOverride(t *testing.T) {
	_, err := NewKoiosClient("mainnet", "", "https://host.example/api/v1", false)
	require.Error(t, err)
}

// TestNewKoiosClientRejectsPlainHTTPByDefault covers the transport guard. get
// and post attach the API key as a Bearer token to every request, so a
// plain-HTTP host would put it on the wire in cleartext — and forged reference
// data can make a parity comparison report a false PASS, the one outcome this
// tool must never produce.
func TestNewKoiosClientRejectsPlainHTTPByDefault(t *testing.T) {
	_, err := NewKoiosClient(
		"preview", "secret-token", "http://host.example/api/v1", false,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plain HTTP")
	assert.NotContains(t, err.Error(), "secret-token",
		"the error must not echo the API key")
}

// TestNewKoiosClientAllowsPlainHTTPWithEscapeHatch pins the local dev/test
// opt-out, mirroring Mithril.AllowInsecureHTTP.
func TestNewKoiosClientAllowsPlainHTTPWithEscapeHatch(t *testing.T) {
	client, err := NewKoiosClient(
		"preview", "", "http://host.example/api/v1", true,
	)
	require.NoError(t, err)
	assert.Equal(t, "http://host.example/api/v1", client.baseURL)
}

// TestNewKoiosClientRejectsMalformedBaseURL covers the shapes an operator can
// plausibly paste: a bare host with no scheme, and a scheme this client cannot
// speak. Neither may fall through to the public host silently.
func TestNewKoiosClientRejectsMalformedBaseURL(t *testing.T) {
	for _, raw := range []string{
		"preview-koios.example.com/api/v1",
		"ftp://host.example/api/v1",
		"://broken",
	} {
		_, err := NewKoiosClient("preview", "", raw, false)
		require.Error(t, err, "base URL %q must be rejected", raw)
	}
}

// TestNewKoiosClientPlainHTTPGuardDoesNotAffectPublicHost pins that the guard
// only looks at a custom URL: the built-in hosts are https already, and an
// empty override must not be able to trip it.
func TestNewKoiosClientPlainHTTPGuardDoesNotAffectPublicHost(t *testing.T) {
	client, err := NewKoiosClient("preview", "", "", false)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(client.baseURL, "https://"))
}
