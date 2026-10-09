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
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A registry source is operator-supplied and may carry credentials in its
// userinfo or its query string. These sentinels stand in for them: none may
// reach a log field or a returned error string.
const (
	redactUser     = "SENTINEL-USERINFO-USER"
	redactPassword = "SENTINEL-USERINFO-PASSWORD"
	redactQuery    = "SENTINEL-QUERY-TOKEN"
	redactFragment = "SENTINEL-FRAGMENT-TOKEN"
	redactRedirect = "SENTINEL-REDIRECT-TOKEN"
)

// redactSecrets lists every sentinel that must never be rendered. The
// username is included: userinfo is a credential component whole, and a
// redaction that dropped only the password would still identify the account.
var redactSecrets = []string{
	redactUser,
	redactPassword,
	redactQuery,
	redactFragment,
	redactRedirect,
}

// withCredentials appends a credential-bearing query and fragment to a base
// URL. Userinfo is added separately, because the SSRF guard rejects it before
// a request is ever built.
func withCredentials(base string) string {
	return base + "/?apikey=" + redactQuery + "#" + redactFragment
}

// captureSync builds a sync whose logger writes to the returned buffer at
// debug level, so every log site in the package is observable.
func captureSync(
	t *testing.T,
	store TokenRegistryStore,
	source string,
) (*TokenRegistrySync, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	sync := newTestSync(t, store, source, func(cfg *TokenRegistryConfig) {
		cfg.Logger = slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
	})
	return sync, buf
}

// requireRedacted asserts that rendered text is non-empty, carries the
// expected marker, and contains none of the sentinels.
func requireRedacted(t *testing.T, rendered, marker string) {
	t.Helper()
	require.NotEmpty(t, rendered)
	for _, secret := range redactSecrets {
		require.NotContains(t, rendered, secret)
	}
	// Asserted after the sentinels so a regression reports the leak rather
	// than the wording, and asserted at all so an empty render cannot pass.
	require.Contains(t, rendered, marker)
}

// deadServerURL returns the URL of a server that has already been shut down,
// so a request to it fails at the transport rather than being answered.
func deadServerURL(t *testing.T) string {
	t.Helper()
	dead := httptest.NewTLSServer(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
	)
	addr := dead.URL
	dead.Close()
	return addr
}

// TestRegistryLogURLRedactsCredentialComponents pins the log rendering of a
// source URL directly: userinfo, query, and fragment are dropped, and an
// input with no safe parsed form never falls back to echoing itself.
func TestRegistryLogURLRedactsCredentialComponents(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "plain",
			raw:  "https://registry.example/reg.tar.gz",
			want: "https://registry.example/reg.tar.gz",
		},
		{
			name: "userinfo",
			raw: "https://" + redactUser + ":" + redactPassword +
				"@registry.example/reg.tar.gz",
			want: "https://registry.example/reg.tar.gz",
		},
		{
			name: "query",
			raw:  "https://registry.example/reg.tar.gz?apikey=" + redactQuery,
			want: "https://registry.example/reg.tar.gz",
		},
		{
			name: "empty forced query",
			raw:  "https://registry.example/reg.tar.gz?",
			want: "https://registry.example/reg.tar.gz",
		},
		{
			name: "fragment",
			raw:  "https://registry.example/reg.tar.gz#" + redactFragment,
			want: "https://registry.example/reg.tar.gz",
		},
		{
			name: "every component",
			raw: "https://" + redactUser + ":" + redactPassword +
				"@registry.example:8443/reg.tar.gz?apikey=" + redactQuery +
				"#" + redactFragment,
			want: "https://registry.example:8443/reg.tar.gz",
		},
		{
			name: "malformed escape",
			raw: "https://registry.example/%zz?apikey=" + redactQuery +
				"#" + redactFragment,
			want: "[invalid URL]",
		},
		{
			name: "control character",
			raw:  "https://registry.example/\x7f?apikey=" + redactQuery,
			want: "[invalid URL]",
		},
		{
			// An opaque URL has one undifferentiated section, so there is
			// no host or path to keep once the credentials are removed.
			name: "opaque carries userinfo",
			raw: "https:" + redactUser + ":" + redactPassword +
				"@registry.example/reg.tar.gz",
			want: "[invalid URL]",
		},
		{
			name: "opaque carries query",
			raw:  "https:registry.example/reg.tar.gz?apikey=" + redactQuery,
			want: "[invalid URL]",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := registryLogURL(tc.raw)
			require.Equal(t, tc.want, got)
			for _, secret := range redactSecrets {
				require.NotContains(t, got, secret)
			}
		})
	}
}

// TestRegistryRequestErrorKeepsCauseIdentity proves the wrapper drops the
// cause from the message without dropping it from the error chain, so
// cancellation and transport diagnosis still work.
func TestRegistryRequestErrorKeepsCauseIdentity(t *testing.T) {
	cause := &url.Error{
		Op:  "Get",
		URL: "https://registry.example/?apikey=" + redactQuery,
		Err: context.Canceled,
	}
	err := error(&registryRequestError{
		operation: "fetch token registry",
		cause:     cause,
	})

	require.Equal(t, "fetch token registry failed", err.Error())
	requireRedacted(t, err.Error(), "fetch token registry failed")

	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, cause)

	var urlErr *url.Error
	require.ErrorAs(t, err, &urlErr)
	require.Equal(t, cause, urlErr)

	require.Equal(t, cause, errors.Unwrap(err))
}

// TestTokenRegistrySyncRedactsCredentialsFromLogs drives every log site in
// the sync that renders the source URL, plus the failure site that renders a
// URL-bearing error, and asserts no sentinel reaches the log.
func TestTokenRegistrySyncRedactsCredentialsFromLogs(t *testing.T) {
	good := map[string]string{
		"mappings/" + syncSubjectNut + ".json": mappingJSON(
			syncSubjectNut, "nutcoin", "NUT", "",
		),
	}

	t.Run("sync complete", func(t *testing.T) {
		server := newRegistryServer(t, tarballOf(t, good))
		sync, buf := captureSync(
			t,
			newFakeTokenRegistryStore(),
			withCredentials(server.URL),
		)

		sync.runOnce(t.Context())

		requireRedacted(t, buf.String(), "token registry sync complete")
		require.Contains(t, buf.String(), "url="+server.URL+"/")
	})

	t.Run("unchanged", func(t *testing.T) {
		server := newRegistryServer(t, tarballOf(t, good))
		server.notModifiedOnTag = true
		source := withCredentials(server.URL)
		sync, buf := captureSync(t, newFakeTokenRegistryStore(), source)
		_, err := sync.SyncOnce(t.Context())
		require.NoError(t, err)
		buf.Reset()

		_, err = sync.SyncOnce(t.Context())

		require.NoError(t, err)
		requireRedacted(t, buf.String(), "token registry unchanged")
	})

	t.Run("no usable mappings", func(t *testing.T) {
		server := newRegistryServer(t, tarballOf(t, map[string]string{
			"README.md": "the layout changed",
		}))
		sync, buf := captureSync(
			t,
			newFakeTokenRegistryStore(),
			withCredentials(server.URL),
		)

		_, err := sync.SyncOnce(t.Context())

		require.NoError(t, err)
		requireRedacted(t, buf.String(), "no usable mappings")
	})

	t.Run("unusable mappings", func(t *testing.T) {
		body := tarballOf(t, map[string]string{
			"mappings/" + syncSubjectNut + ".json": mappingJSON(
				syncSubjectNut, "nutcoin", "NUT", "",
			),
			"mappings/broken.json": `{"subject": `,
		})
		server := newRegistryServer(t, body)
		sync, buf := captureSync(
			t,
			newFakeTokenRegistryStore(),
			withCredentials(server.URL),
		)

		_, err := sync.SyncOnce(t.Context())

		require.ErrorContains(t, err, "unusable mappings: 1")
		requireRedacted(t, buf.String(), "unusable mappings")
	})

	t.Run("userinfo rejected", func(t *testing.T) {
		server := newRegistryServer(t, tarballOf(t, good))
		source := strings.Replace(
			withCredentials(server.URL),
			"https://",
			"https://"+redactUser+":"+redactPassword+"@",
			1,
		)
		sync, buf := captureSync(t, newFakeTokenRegistryStore(), source)

		sync.runOnce(t.Context())

		requireRedacted(t, buf.String(), "token registry sync failed")
	})

	t.Run("malformed source", func(t *testing.T) {
		source := "https://registry.example/%zz?apikey=" + redactQuery +
			"#" + redactFragment
		sync, buf := captureSync(t, newFakeTokenRegistryStore(), source)

		sync.runOnce(t.Context())

		requireRedacted(t, buf.String(), "token registry sync failed")
		// The raw input is not a safe fallback for an unparsable URL.
		require.Contains(t, buf.String(), "[invalid URL]")
		require.NotContains(t, buf.String(), "%zz")
	})

	t.Run("transport error through redirect", func(t *testing.T) {
		target := deadServerURL(t) + "/?token=" + redactRedirect
		server := httptest.NewTLSServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, target, http.StatusFound)
			}),
		)
		t.Cleanup(server.Close)
		sync, buf := captureSync(
			t,
			newFakeTokenRegistryStore(),
			withCredentials(server.URL),
		)
		sync.client.Transport = server.Client().Transport

		sync.runOnce(t.Context())

		requireRedacted(t, buf.String(), "token registry sync failed")
	})
}

// TestTokenRegistrySyncRedactsCredentialsFromReturnedError covers the same
// credential exposure through SyncOnce's return value, which a caller may
// render somewhere the package does not control.
func TestTokenRegistrySyncRedactsCredentialsFromReturnedError(t *testing.T) {
	t.Run("userinfo rejected", func(t *testing.T) {
		server := newRegistryServer(t, nil)
		source := strings.Replace(
			withCredentials(server.URL),
			"https://",
			"https://"+redactUser+":"+redactPassword+"@",
			1,
		)
		sync := newTestSync(t, newFakeTokenRegistryStore(), source, nil)

		_, err := sync.SyncOnce(t.Context())

		require.Error(t, err)
		requireRedacted(t, err.Error(), "validate token registry source URL")
	})

	t.Run("malformed source", func(t *testing.T) {
		source := "https://registry.example/%zz?apikey=" + redactQuery +
			"#" + redactFragment
		sync := newTestSync(t, newFakeTokenRegistryStore(), source, nil)

		_, err := sync.SyncOnce(t.Context())

		require.Error(t, err)
		requireRedacted(t, err.Error(), "validate token registry source URL")
		require.NotContains(t, err.Error(), "%zz")
	})

	t.Run("transport error through redirect", func(t *testing.T) {
		target := deadServerURL(t) + "/?token=" + redactRedirect
		server := httptest.NewTLSServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, target, http.StatusFound)
			}),
		)
		t.Cleanup(server.Close)
		sync := newTestSync(
			t,
			newFakeTokenRegistryStore(),
			withCredentials(server.URL),
			nil,
		)
		sync.client.Transport = server.Client().Transport

		_, err := sync.SyncOnce(t.Context())

		require.Error(t, err)
		requireRedacted(t, err.Error(), "fetch token registry manifest failed")
	})
}

// TestTokenRegistrySyncFetchErrorStaysIdentifiable proves the redaction does
// not cost the caller the ability to tell a cancellation from a real failure,
// which is what an unwrappable opaque error would have done.
func TestTokenRegistrySyncFetchErrorStaysIdentifiable(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cancel()
			<-r.Context().Done()
		}),
	)
	t.Cleanup(server.Close)
	sync := newTestSync(
		t,
		newFakeTokenRegistryStore(),
		withCredentials(server.URL),
		nil,
	)
	sync.client.Transport = server.Client().Transport

	_, err := sync.SyncOnce(ctx)

	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	var urlErr *url.Error
	require.ErrorAs(t, err, &urlErr)
	requireRedacted(t, err.Error(), "fetch token registry manifest failed")
}

// nutcoinSubject is the real registry subject for the nutcoin test asset:
// a 56-hex-character policy ID followed by the hex-encoded asset name.
const nutcoinSubject = "00000002df633853f6a47465c9496721d2d5b1291b8398016c0e87ae" +
	"6e7574636f696e"

func TestParseTokenRegistryEntryExtractsAllProperties(t *testing.T) {
	// Shape taken verbatim from the live registry mapping for nutcoin:
	// every property is an envelope of {sequenceNumber, value, signatures}
	// except "policy", which is a bare string.
	raw := []byte(`{
		"subject": "` + nutcoinSubject + `",
		"policy": "82008200581c` + strings.Repeat("ab", 28) + `",
		"url": {"sequenceNumber": 0, "value": "https://fivebinaries.com/nutcoin",
			"signatures": [{"signature": "aa", "publicKey": "bb"}]},
		"name": {"sequenceNumber": 0, "value": "nutcoin", "signatures": []},
		"ticker": {"sequenceNumber": 0, "value": "NUT", "signatures": []},
		"description": {"sequenceNumber": 0, "value": "Nutcoin on Cardano",
			"signatures": []},
		"logo": {"sequenceNumber": 0, "value": "iVBORw0KGgo=", "signatures": []}
	}`)

	entry, err := ParseTokenRegistryEntry(raw)

	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, nutcoinSubject, entry.Subject)
	require.Equal(t, "nutcoin", entry.Name)
	require.Equal(t, "NUT", entry.Ticker)
	require.Equal(t, "Nutcoin on Cardano", entry.Description)
	require.Equal(t, "https://fivebinaries.com/nutcoin", entry.URL)
	require.Equal(t, "iVBORw0KGgo=", entry.Logo)
	require.Nil(t, entry.Decimals)
}

func TestParseTokenRegistryEntryExtractsDecimals(t *testing.T) {
	// decimals carries a JSON number in the same envelope the string
	// properties use; the live DjedMicroUSD mapping is the reference.
	raw := []byte(`{
		"subject": "` + nutcoinSubject + `",
		"name": {"sequenceNumber": 0, "value": "Djed USD", "signatures": []},
		"decimals": {"sequenceNumber": 0, "value": 6, "signatures": []}
	}`)

	entry, err := ParseTokenRegistryEntry(raw)

	require.NoError(t, err)
	require.NotNil(t, entry.Decimals)
	require.Equal(t, 6, *entry.Decimals)
}

func TestParseTokenRegistryEntryAcceptsSubjectWithoutAssetName(t *testing.T) {
	// A policy-only subject (56 hex characters, empty asset name) is legal:
	// it names the asset whose name is the empty string.
	policyOnly := strings.Repeat("ab", 28)
	raw := []byte(
		`{"subject":"` + policyOnly + `",` +
			`"name":{"sequenceNumber":0,"value":"Policy Token","signatures":[]}}`,
	)

	entry, err := ParseTokenRegistryEntry(raw)

	require.NoError(t, err)
	require.Equal(t, policyOnly, entry.Subject)
}

func TestParseTokenRegistryEntryRejectsMissingSubject(t *testing.T) {
	raw := []byte(`{"name":{"sequenceNumber":0,"value":"x","signatures":[]}}`)

	_, err := ParseTokenRegistryEntry(raw)

	require.Error(t, err)
	require.Contains(t, err.Error(), "subject")
}

func TestParseTokenRegistryEntryRejectsNonHexSubject(t *testing.T) {
	// The subject is policy ID + asset name, both hex; anything else cannot
	// be matched back to an on-chain asset and is dropped rather than stored
	// under a key no lookup will ever produce.
	//
	// The subject has to be a legal *length* for this to reach the hex check
	// at all -- a short non-hex string fails the length check first and would
	// pass this test without ever exercising hex validation.
	raw := []byte(`{"subject":"` + strings.Repeat("zz", 28) + `"}`)

	_, err := ParseTokenRegistryEntry(raw)

	require.Error(t, err)
	require.Contains(t, err.Error(), "not hex")
}

func TestParseTokenRegistryEntryRejectsShortSubject(t *testing.T) {
	// Shorter than a 56-hex-character policy ID.
	raw := []byte(`{"subject":"` + strings.Repeat("ab", 20) + `"}`)

	_, err := ParseTokenRegistryEntry(raw)

	require.Error(t, err)
	require.Contains(t, err.Error(), "subject")
}

func TestParseTokenRegistryEntryRejectsOddLengthSubject(t *testing.T) {
	raw := []byte(`{"subject":"` + strings.Repeat("ab", 28) + `a"}`)

	_, err := ParseTokenRegistryEntry(raw)

	require.Error(t, err)
	require.Contains(t, err.Error(), "subject")
}

func TestParseTokenRegistryEntryNormalizesSubjectCase(t *testing.T) {
	// Lookups build the subject from raw on-chain bytes hex-encoded in lower
	// case, so an upper-case registry subject has to normalize or it will
	// never match.
	upper := strings.ToUpper(nutcoinSubject)
	raw := []byte(`{"subject":"` + upper + `"}`)

	entry, err := ParseTokenRegistryEntry(raw)

	require.NoError(t, err)
	require.Equal(t, nutcoinSubject, entry.Subject)
}

func TestParseTokenRegistryEntrySkipsMalformedProperties(t *testing.T) {
	// One property that is a bare string rather than a CIP-26 envelope must
	// not cost us the rest of the entry: a bulk sync that discards a whole
	// mapping over one bad field loses metadata for no good reason.
	raw := []byte(`{
		"subject": "` + nutcoinSubject + `",
		"name": "bare string, not an envelope",
		"ticker": {"sequenceNumber": 0, "value": "NUT", "signatures": []}
	}`)

	entry, err := ParseTokenRegistryEntry(raw)

	require.NoError(t, err)
	require.Empty(t, entry.Name)
	require.Equal(t, "NUT", entry.Ticker)
}

func TestParseTokenRegistryEntrySkipsBlankValues(t *testing.T) {
	raw := []byte(`{
		"subject": "` + nutcoinSubject + `",
		"name": {"sequenceNumber": 0, "value": "   ", "signatures": []},
		"ticker": {"sequenceNumber": 0, "value": "", "signatures": []}
	}`)

	entry, err := ParseTokenRegistryEntry(raw)

	require.NoError(t, err)
	require.Empty(t, entry.Name)
	require.Empty(t, entry.Ticker)
}

func TestParseTokenRegistryEntrySkipsOutOfRangeDecimals(t *testing.T) {
	for name, value := range map[string]string{
		"negative":         "-1",
		"absurd":           "1000",
		"above CIP-26 max": "20",
		"fractional":       "2.5",
		"wrong type":       `"6"`,
	} {
		t.Run(name, func(t *testing.T) {
			raw := []byte(`{
				"subject": "` + nutcoinSubject + `",
				"decimals": {"sequenceNumber": 0, "value": ` + value +
				`, "signatures": []}
			}`)

			entry, err := ParseTokenRegistryEntry(raw)

			require.NoError(t, err)
			require.Nil(t, entry.Decimals)
		})
	}
}

func TestParseTokenRegistryEntryRejectsInvalidJSON(t *testing.T) {
	_, err := ParseTokenRegistryEntry([]byte(`{"subject":`))

	require.Error(t, err)
}

func TestParseTokenRegistryEntryReportsEmptyWhenNoProperties(t *testing.T) {
	// A mapping carrying only a subject has nothing worth persisting; the
	// syncer uses this to skip the row entirely.
	raw := []byte(`{"subject":"` + nutcoinSubject + `"}`)

	entry, err := ParseTokenRegistryEntry(raw)

	require.NoError(t, err)
	require.True(t, entry.IsEmpty())
}

func TestParseTokenRegistryEntryIsNotEmptyWithDecimalsOnly(t *testing.T) {
	raw := []byte(`{
		"subject": "` + nutcoinSubject + `",
		"decimals": {"sequenceNumber": 0, "value": 0, "signatures": []}
	}`)

	entry, err := ParseTokenRegistryEntry(raw)

	require.NoError(t, err)
	require.False(t, entry.IsEmpty())
}

// TestParseTokenRegistryEntryRejectsNullDecimals covers an explicit JSON null.
// encoding/json accepts null into an int as a no-op, leaving the zero value
// and returning no error, so a null decimals would otherwise be published as
// a declared 0 -- which a wallet would use to render balances unscaled.
func TestParseTokenRegistryEntryRejectsNullDecimals(t *testing.T) {
	raw := []byte(`{
		"subject": "` + nutcoinSubject + `",
		"name": {"sequenceNumber": 0, "value": "nutcoin", "signatures": []},
		"decimals": {"sequenceNumber": 0, "value": null, "signatures": []}
	}`)

	entry, err := ParseTokenRegistryEntry(raw)

	require.NoError(t, err)
	require.Equal(t, "nutcoin", entry.Name)
	require.Nil(t, entry.Decimals, "a null decimals is absent, not zero")
}

// TestParseTokenRegistryEntryRejectsNullStringProperties is the same hazard
// for the string properties: null must read as absent.
func TestParseTokenRegistryEntryRejectsNullStringProperties(t *testing.T) {
	raw := []byte(`{
		"subject": "` + nutcoinSubject + `",
		"name": {"sequenceNumber": 0, "value": null, "signatures": []},
		"ticker": {"sequenceNumber": 0, "value": "NUT", "signatures": []}
	}`)

	entry, err := ParseTokenRegistryEntry(raw)

	require.NoError(t, err)
	require.Empty(t, entry.Name)
	require.Equal(t, "NUT", entry.Ticker)
}

// TestParseTokenRegistryEntryRejectsNullPropertyEnvelope covers a null in
// place of the whole envelope.
func TestParseTokenRegistryEntryRejectsNullPropertyEnvelope(t *testing.T) {
	raw := []byte(`{
		"subject": "` + nutcoinSubject + `",
		"name": null,
		"decimals": null
	}`)

	entry, err := ParseTokenRegistryEntry(raw)

	require.NoError(t, err)
	require.Empty(t, entry.Name)
	require.Nil(t, entry.Decimals)
	require.True(t, entry.IsEmpty())
}

// TestParseTokenRegistryEntryAcceptsDecimalsBounds pins both ends of CIP-26's
// declared range ({"minimum": 0, "maximum": 19}) so the cap cannot drift.
func TestParseTokenRegistryEntryAcceptsDecimalsBounds(t *testing.T) {
	for _, value := range []int{0, 19} {
		raw := []byte(`{
			"subject": "` + nutcoinSubject + `",
			"decimals": {"sequenceNumber": 0, "value": ` +
			strconv.Itoa(value) + `, "signatures": []}
		}`)

		entry, err := ParseTokenRegistryEntry(raw)

		require.NoError(t, err)
		require.NotNil(t, entry.Decimals, "decimals %d is in range", value)
		require.Equal(t, value, *entry.Decimals)
	}
}
