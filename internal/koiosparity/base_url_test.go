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

// TestNewKoiosClientKeepsBurstCapForPublicHostOverride covers an override that
// names koios.rest explicitly. The cap is dropped for a custom deployment, not
// for a custom spelling of the public one — that host's published window
// applies however the URL was written, and ignoring it earns 429 cooldowns.
func TestNewKoiosClientKeepsBurstCapForPublicHostOverride(t *testing.T) {
	for _, raw := range []string{
		"https://preview.koios.rest/api/v1",
		"https://PREPROD.KOIOS.REST/api/v1",
		"https://koios.rest/api/v1",
	} {
		client, err := NewKoiosClient("preview", "", raw, false)
		require.NoError(t, err)
		require.NotNil(t, client.limiter)
		assert.Equal(t, koiosBurstLimitSafe, client.limiter.limit,
			"override %q names the public host and keeps its cap", raw)
	}

	// A host that merely mentions the string is not the public host.
	client, err := NewKoiosClient(
		"preview", "", "https://koios.rest.example.com/api/v1", false,
	)
	require.NoError(t, err)
	assert.LessOrEqual(t, client.limiter.limit, 0)
}

// TestNewKoiosClientRejectsQueryOrFragment covers a root that already carries a
// delimiter. get and post append an endpoint path and their own query to this
// value, so a root ending in "?x=1" or "#frag" would put the appended path
// after that delimiter and silently reach a different endpoint.
func TestNewKoiosClientRejectsQueryOrFragment(t *testing.T) {
	for _, raw := range []string{
		"https://host.example/api/v1?token=abc",
		"https://host.example/api/v1#frag",
		"https://host.example/api/v1?",
	} {
		_, err := NewKoiosClient("preview", "", raw, false)
		require.Error(t, err, "base URL %q must be rejected", raw)
	}
}

// TestValidateKoiosBaseURLErrorsOmitTheURL covers the errors themselves. An
// operator can put credentials in the URL as userinfo or as a
// credential-shaped query parameter, and a validation error is written to the
// same log that logURIConfigFields exists to protect — so the raw value must
// never appear in it.
func TestValidateKoiosBaseURLErrorsOmitTheURL(t *testing.T) {
	const secret = "SENTINEL-URL-PASSWORD"
	for _, raw := range []string{
		"http://dingo:" + secret + "@host.example/api/v1",
		"ftp://dingo:" + secret + "@host.example/api/v1",
		"https://host.example/api/v1?api_key=" + secret,
		"://dingo:" + secret + "@broken",
	} {
		err := validateKoiosBaseURL(raw, false)
		require.Error(t, err, "base URL %q must be rejected", raw)
		assert.NotContains(t, err.Error(), secret,
			"validation error must not echo the URL's credentials")
	}
}

// TestNewKoiosClientPublicHostSpellings covers DNS spellings of the public host
// that must keep its published burst cap. A single terminal dot is a valid,
// fully-qualified spelling of the same name.
func TestNewKoiosClientPublicHostSpellings(t *testing.T) {
	for _, raw := range []string{
		"https://preview.koios.rest./api/v1",
		"https://PREVIEW.KOIOS.REST./api/v1",
	} {
		client, err := NewKoiosClient("preview", "", raw, false)
		require.NoError(t, err)
		assert.Equal(t, koiosBurstLimitSafe, client.limiter.limit,
			"%q is the public host and keeps its cap", raw)
	}
}

// TestNewKoiosClientRejectsBareFragment covers a root ending in "#". url.URL
// has no ForceFragment counterpart to ForceQuery, so it parses to an empty
// Fragment — but get and post would still append the endpoint path after the
// delimiter and reach the base path instead.
func TestNewKoiosClientRejectsBareFragment(t *testing.T) {
	_, err := NewKoiosClient("preview", "", "https://host.example/api/v1#", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fragment")
}

// TestResolvedBaseURLDropsUserinfo covers the value that gets logged and
// persisted. validateKoiosBaseURL already rejects a query and a fragment, so
// userinfo is the only place a credential survives into a validated root, and
// both the log line and the cache row must be safe to read.
func TestResolvedBaseURLDropsUserinfo(t *testing.T) {
	c, err := NewKoiosClient(
		"preview",
		"key",
		"https://dingo:hunter2@koios.example/api/v1",
		false,
	)
	require.NoError(t, err)
	resolved := c.ResolvedBaseURL()
	assert.Equal(t, "https://koios.example/api/v1", resolved)
	assert.NotContains(t, resolved, "hunter2")
	assert.NotContains(t, resolved, "dingo:")
}

// TestResolvedBaseURLReportsTheDefaultHost pins that the accessor names the
// host actually queried when no override is given, rather than reporting the
// empty override back.
func TestResolvedBaseURLReportsTheDefaultHost(t *testing.T) {
	c, err := NewKoiosClient("preview", "", "", false)
	require.NoError(t, err)
	assert.Equal(t, koiosBaseURLs["preview"], c.ResolvedBaseURL())
}
