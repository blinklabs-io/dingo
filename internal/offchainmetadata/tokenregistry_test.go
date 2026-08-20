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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package offchainmetadata

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

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
