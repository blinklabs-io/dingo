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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func TestValidatePoolMetadataAcceptsValidDocument(t *testing.T) {
	raw := []byte(
		`{"name":"Test Pool","description":"A pool used for testing.",` +
			`"ticker":"TEST","homepage":"https://example.com"}`,
	)

	fields, err := ValidatePoolMetadata(raw)

	require.NoError(t, err)
	require.NotNil(t, fields)
	require.Equal(t, "Test Pool", fields.Name)
	require.Equal(t, "A pool used for testing.", fields.Description)
	require.Equal(t, "TEST", fields.Ticker)
	require.Equal(t, "https://example.com", fields.Homepage)
}

func TestValidatePoolMetadataAcceptsBoundaryLengths(t *testing.T) {
	// name at exactly 50 characters, description at exactly 255, ticker at
	// exactly 5: all at the reference validator's inclusive boundary.
	name := strings.Repeat("n", 50)
	description := strings.Repeat("d", 255)
	raw := []byte(
		`{"name":"` + name + `","description":"` + description + `",` +
			`"ticker":"ABCDE","homepage":"https://example.com"}`,
	)

	fields, err := ValidatePoolMetadata(raw)

	require.NoError(t, err)
	require.Equal(t, name, fields.Name)
	require.Equal(t, description, fields.Description)
	require.Equal(t, "ABCDE", fields.Ticker)
}

func TestValidatePoolMetadataCountsCodePointsNotBytes(t *testing.T) {
	// 5 multi-byte runes: valid ticker length by Unicode code point count
	// (matching Data.Text.length) even though it is more than 5 bytes.
	raw := []byte(
		`{"name":"n","description":"d","ticker":"ÀÉÎÕÜ",` +
			`"homepage":"https://example.com"}`,
	)

	fields, err := ValidatePoolMetadata(raw)

	require.NoError(t, err)
	require.Equal(t, "ÀÉÎÕÜ", fields.Ticker)
}

func TestValidatePoolMetadataRejectsEmptyContent(t *testing.T) {
	fields, err := ValidatePoolMetadata(nil)

	require.Error(t, err)
	require.Nil(t, fields)
	require.ErrorContains(t, err, models.OffchainFetchErrDecodeErrorPrefix)
}

func TestValidatePoolMetadataRejectsEmptyObject(t *testing.T) {
	fields, err := ValidatePoolMetadata([]byte(`{}`))

	require.Error(t, err)
	require.Nil(t, fields)
	require.ErrorContains(t, err, models.OffchainFetchErrDecodeErrorPrefix)
	require.ErrorContains(t, err, `"name"`)
}

func TestValidatePoolMetadataRejectsMissingRequiredField(t *testing.T) {
	testCases := []struct {
		name    string
		raw     string
		missing string
	}{
		{
			name: "missing name",
			raw: `{"description":"d","ticker":"TEST",` +
				`"homepage":"https://example.com"}`,
			missing: "name",
		},
		{
			name: "missing description",
			raw: `{"name":"n","ticker":"TEST",` +
				`"homepage":"https://example.com"}`,
			missing: "description",
		},
		{
			name:    "missing ticker",
			raw:     `{"name":"n","description":"d","homepage":"https://example.com"}`,
			missing: "ticker",
		},
		{
			name:    "missing homepage",
			raw:     `{"name":"n","description":"d","ticker":"TEST"}`,
			missing: "homepage",
		},
		{
			name: "null name",
			raw: `{"name":null,"description":"d","ticker":"TEST",` +
				`"homepage":"https://example.com"}`,
			missing: "name",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fields, err := ValidatePoolMetadata([]byte(tc.raw))

			require.Error(t, err)
			require.Nil(t, fields)
			require.ErrorContains(
				t, err, models.OffchainFetchErrDecodeErrorPrefix,
			)
			require.ErrorContains(t, err, tc.missing)
		})
	}
}

func TestValidatePoolMetadataRejectsFieldConstraintViolations(t *testing.T) {
	testCases := []struct {
		name string
		raw  string
	}{
		{
			name: "name too long",
			raw: `{"name":"` + strings.Repeat("n", 51) + `",` +
				`"description":"d","ticker":"TEST",` +
				`"homepage":"https://example.com"}`,
		},
		{
			name: "description too long",
			raw: `{"name":"n","description":"` +
				strings.Repeat("d", 256) + `",` +
				`"ticker":"TEST","homepage":"https://example.com"}`,
		},
		{
			name: "ticker too short",
			raw: `{"name":"n","description":"d","ticker":"AB",` +
				`"homepage":"https://example.com"}`,
		},
		{
			name: "ticker too long",
			raw: `{"name":"n","description":"d","ticker":"ABCDEF",` +
				`"homepage":"https://example.com"}`,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fields, err := ValidatePoolMetadata([]byte(tc.raw))

			require.Error(t, err)
			require.Nil(t, fields)
			require.ErrorContains(
				t, err, models.OffchainFetchErrDecodeErrorPrefix,
			)
		})
	}
}

func TestValidatePoolMetadataRejectsMalformedJSON(t *testing.T) {
	fields, err := ValidatePoolMetadata([]byte(`not json`))

	require.Error(t, err)
	require.Nil(t, fields)
	require.ErrorContains(t, err, models.OffchainFetchErrDecodeErrorPrefix)
}

func TestValidatePoolMetadataRejectsOversizedDocument(t *testing.T) {
	// A 513-byte, otherwise-valid document: over the reference validator's
	// 512-byte cap, checked before any JSON decoding is attempted.
	padding := strings.Repeat("a", 500)
	raw := []byte(
		`{"name":"` + padding + `","description":"d","ticker":"TEST",` +
			`"homepage":"https://example.com"}`,
	)
	require.Greater(t, len(raw), poolMetadataMaxBytes)

	fields, err := ValidatePoolMetadata(raw)

	require.Error(t, err)
	require.Nil(t, fields)
	require.ErrorContains(t, err, models.OffchainFetchErrBodyTooLargePrefix)
	require.NotContains(
		t, err.Error(), models.OffchainFetchErrDecodeErrorPrefix,
	)
}

func TestValidatePoolMetadataRejectsNonObjectTopLevelValue(t *testing.T) {
	testCases := []string{`[]`, `"a string"`, `123`, `true`}
	for _, raw := range testCases {
		t.Run(raw, func(t *testing.T) {
			fields, err := ValidatePoolMetadata([]byte(raw))

			require.Error(t, err)
			require.Nil(t, fields)
		})
	}
}
