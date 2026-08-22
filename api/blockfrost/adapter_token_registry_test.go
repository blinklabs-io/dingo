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

package blockfrost

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

//go:fix inline
func registryIntPtr(v int) *int { return new(v) }

// TestTokenRegistryMetadataValueShape pins the object served in the
// `metadata` field of GET /assets/{asset} to the Blockfrost CIP-26 shape:
// name, description, ticker, url, logo, decimals.
func TestTokenRegistryMetadataValueShape(t *testing.T) {
	entry := &models.TokenRegistryEntry{
		Subject:     "abc",
		Name:        "nutcoin",
		Ticker:      "NUT",
		Description: "The legendary Nutcoin.",
		URL:         "https://fivebinaries.com/nutcoin",
		Logo:        "iVBORw0KGgo=",
		Decimals:    new(6),
	}

	value := tokenRegistryMetadataValue(entry)

	require.NotNil(t, value)
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"name": "nutcoin",
		"description": "The legendary Nutcoin.",
		"ticker": "NUT",
		"url": "https://fivebinaries.com/nutcoin",
		"logo": "iVBORw0KGgo=",
		"decimals": 6
	}`, string(encoded))
}

// TestTokenRegistryMetadataValueOmitsAbsentProperties keeps a registry entry
// that declares only some properties from advertising empty strings for the
// rest: a consumer must be able to tell "no ticker" from "empty ticker".
func TestTokenRegistryMetadataValueOmitsAbsentProperties(t *testing.T) {
	entry := &models.TokenRegistryEntry{
		Subject: "abc",
		Name:    "nutcoin",
	}

	value := tokenRegistryMetadataValue(entry)

	require.NotNil(t, value)
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"nutcoin"}`, string(encoded))
}

// TestTokenRegistryMetadataValueKeepsZeroDecimals guards the one property
// where the zero value is meaningful: decimals 0 is a real declaration and
// must survive, unlike an absent decimals.
func TestTokenRegistryMetadataValueKeepsZeroDecimals(t *testing.T) {
	entry := &models.TokenRegistryEntry{
		Subject:  "abc",
		Decimals: new(0),
	}

	value := tokenRegistryMetadataValue(entry)

	require.NotNil(t, value)
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	require.JSONEq(t, `{"decimals":0}`, string(encoded))
}

func TestTokenRegistryMetadataValueNilForNilEntry(t *testing.T) {
	require.Nil(t, tokenRegistryMetadataValue(nil))
}

func TestTokenRegistryMetadataValueNilForEmptyEntry(t *testing.T) {
	// An all-blank entry must serialize the field as null rather than as an
	// empty object, which would read as "the registry knows this asset".
	require.Nil(t, tokenRegistryMetadataValue(&models.TokenRegistryEntry{
		Subject: "abc",
	}))
}

func TestTokenRegistrySubjectFor(t *testing.T) {
	// The subject is the lower-case hex policy ID concatenated with the
	// hex-encoded asset name, matching how registry mappings are keyed.
	subject := tokenRegistrySubjectFor(
		"00000002DF633853F6A47465C9496721D2D5B1291B8398016C0E87AE",
		[]byte("nutcoin"),
	)

	require.Equal(
		t,
		"00000002df633853f6a47465c9496721d2d5b1291b8398016c0e87ae6e7574636f696e",
		subject,
	)
}

func TestTokenRegistrySubjectForEmptyAssetName(t *testing.T) {
	subject := tokenRegistrySubjectFor(
		"00000002df633853f6a47465c9496721d2d5b1291b8398016c0e87ae",
		nil,
	)

	require.Equal(
		t,
		"00000002df633853f6a47465c9496721d2d5b1291b8398016c0e87ae",
		subject,
	)
}

// TestAssetPopulatesRegistryMetadata is the end-to-end check against a real
// metadata store: an asset whose subject is in the registry serves the
// off-chain `metadata` object, which was null for every asset before the
// registry sync existed.
func TestAssetPopulatesRegistryMetadata(t *testing.T) {
	adapter, _, db := newDBBackedAdapter(t)
	const policyID = "00000002df633853f6a47465c9496721d2d5b1291b8398016c0e87ae"
	assetName := []byte("nutcoin")
	subject := tokenRegistrySubjectFor(policyID, assetName)

	_, err := db.Metadata().UpsertTokenRegistryEntries(
		t.Context(),
		[]models.TokenRegistryEntry{{
			Subject:  subject,
			Name:     "nutcoin",
			Ticker:   "NUT",
			Decimals: new(6),
		}},
		time.Now(),
		nil,
	)
	require.NoError(t, err)

	var info AssetInfo
	require.NoError(t, adapter.populateAssetRegistryMetadata(
		&info,
		policyID,
		assetName,
	))

	require.NotNil(t, info.Metadata)
	encoded, err := json.Marshal(*info.Metadata)
	require.NoError(t, err)
	require.JSONEq(
		t,
		`{"name":"nutcoin","ticker":"NUT","decimals":6}`,
		string(encoded),
	)
}

func TestAssetLeavesRegistryMetadataNilWhenUnknown(t *testing.T) {
	adapter, _, _ := newDBBackedAdapter(t)

	var info AssetInfo
	require.NoError(t, adapter.populateAssetRegistryMetadata(
		&info,
		"00000002df633853f6a47465c9496721d2d5b1291b8398016c0e87ae",
		[]byte("unknown"),
	))

	require.Nil(t, info.Metadata)
}
