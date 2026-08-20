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
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
)

// tokenRegistrySubjectFor builds the CIP-26 registry subject for an asset:
// the lower-case hex policy ID concatenated with the hex-encoded asset name,
// which is exactly how registry mappings are keyed. An asset with an empty
// name yields the bare policy ID, which is a legal subject.
func tokenRegistrySubjectFor(policyID string, assetName []byte) string {
	return strings.ToLower(strings.TrimSpace(policyID)) +
		hex.EncodeToString(assetName)
}

// tokenRegistryMetadataValue converts a stored registry entry into the object
// Blockfrost serves in the `metadata` field of GET /assets/{asset}.
//
// Absent properties are omitted rather than emitted as empty strings, so a
// consumer can tell "the registry declares no ticker" from "the registry
// declares an empty ticker". An entry with nothing to report yields nil, which
// serializes as a null field rather than as an empty object that would read as
// "the registry knows this asset but says nothing about it".
func tokenRegistryMetadataValue(entry *models.TokenRegistryEntry) *any {
	if entry == nil || entry.IsEmpty() {
		return nil
	}
	metadata := make(map[string]any, 6)
	if entry.Name != "" {
		metadata["name"] = entry.Name
	}
	if entry.Description != "" {
		metadata["description"] = entry.Description
	}
	if entry.Ticker != "" {
		metadata["ticker"] = entry.Ticker
	}
	if entry.URL != "" {
		metadata["url"] = entry.URL
	}
	if entry.Logo != "" {
		metadata["logo"] = entry.Logo
	}
	// Decimals 0 is a real declaration, so this keys off the pointer rather
	// than off a zero check the way the string properties do.
	if entry.Decimals != nil {
		metadata["decimals"] = *entry.Decimals
	}
	if len(metadata) == 0 {
		return nil
	}
	var value any = metadata
	return &value
}

// populateAssetRegistryMetadata fills info.Metadata from the CIP-26 off-chain
// token registry, leaving it nil when the registry has nothing for the asset.
//
// An asset missing from the registry is the common case, not an error: most
// native assets are never registered, and the registry sync is off by default.
// Only a store failure is reported.
func (a *NodeAdapter) populateAssetRegistryMetadata(
	info *AssetInfo,
	policyID string,
	assetName []byte,
) error {
	subject := tokenRegistrySubjectFor(policyID, assetName)
	entry, err := a.ledgerState.Database().
		Metadata().
		GetTokenRegistryEntry(subject, nil)
	if err != nil {
		return fmt.Errorf(
			"get token registry metadata for asset %s: %w",
			subject,
			err,
		)
	}
	info.Metadata = tokenRegistryMetadataValue(entry)
	return nil
}
