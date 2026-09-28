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

package models

import "time"

// TokenRegistryEntry holds the CIP-26 off-chain token registry properties for
// a single asset, keyed by the registry subject (hex policy ID followed by the
// hex-encoded asset name). It backs the `metadata` field of the Blockfrost
// GET /assets/{asset} response, which is distinct from `onchain_metadata`
// (CIP-25/CIP-68 mint metadata read from the chain itself).
//
// Rows are a best-effort cache of a periodically synced upstream registry, not
// consensus data: the node serves whatever the last successful sync produced
// and an absent subject simply yields a null `metadata` field. Only populated
// in API storage mode, and only when the token registry sync is enabled.
type TokenRegistryEntry struct {
	// Decimals is nil when the registry declares no decimals for the
	// subject. Zero is a meaningful declared value distinct from absent, so
	// this cannot collapse to a bare int.
	Decimals    *int
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Subject     string
	Name        string
	Ticker      string
	Description string
	URL         string
	// Logo is the base64 payload as published by the registry. Logos are
	// roughly 90% of registry bytes, so the syncer drops them unless the
	// operator opts in; an empty Logo means "not stored", never "the
	// registry published an empty logo".
	Logo string
	ID   uint
}

// IsEmpty reports whether the entry carries no property worth persisting. A
// subject with no properties tells a consumer nothing the asset ID did not
// already, so the syncer skips it rather than writing an all-null row.
func (e *TokenRegistryEntry) IsEmpty() bool {
	return e.Name == "" && e.Ticker == "" && e.Description == "" &&
		e.URL == "" && e.Logo == "" && e.Decimals == nil
}
