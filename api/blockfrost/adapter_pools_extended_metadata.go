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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/offchainmetadata"
)

// poolExtendedMetadataKey builds the lookup key used to match a batched
// GetOffchainMetadataBatch result row back to a specific pool's on-chain
// (url, hash) pointer. Two documents can share a URL under different
// hashes (metadata republished at the same URL with new content), so the
// match must be on the (url, hash) pair, not url alone.
func poolExtendedMetadataKey(url string, hash []byte) string {
	return url + "\x00" + string(hash)
}

// buildPoolExtendedMetadata classifies one pool's persisted off-chain
// metadata state into pool_list_extended's nullable metadata object. It is
// a pure function over already-fetched state (doc comes from one batched
// GetOffchainMetadataBatch call across the whole pool set, not a per-pool
// query) and intentionally mirrors NodeAdapter.PoolMetadata's
// classification logic (adapter.go): a nil metadataURL means the pool has
// no registered anchor at all (metadata: null); a pending or missing doc
// returns the anchor with every off-chain field left nil; a failed doc or
// one that fails re-validation (internal/offchainmetadata.ValidatePoolMetadata)
// returns the anchor plus the schema's error object; otherwise every
// off-chain field is populated. This logic is duplicated rather than
// factored out of PoolMetadata because that function is owned by the pool
// detail/metadata feature (#2936/#2995) and is not modified here.
func buildPoolExtendedMetadata(
	metadataURL string,
	metadataHash []byte,
	doc *models.OffchainMetadata,
) *PoolExtendedMetadataInfo {
	if metadataURL == "" {
		return nil
	}
	url := metadataURL
	hash := hex.EncodeToString(metadataHash)
	ret := &PoolExtendedMetadataInfo{URL: &url, Hash: &hash}

	if doc == nil || doc.Status == models.OffchainMetadataStatusPending {
		return ret
	}
	if doc.Status == models.OffchainMetadataStatusFailed {
		ret.Error = offchainFetchError("Pool", metadataURL, metadataHash, doc)
		return ret
	}
	// Validation happens in the fetcher at fetch time
	// (internal/offchainmetadata.ValidatePoolMetadata, invoked from
	// fetchOne). This defensive re-validation only matters for rows
	// persisted as "fetched" before that validation existed; see
	// PoolMetadata's identical comment in adapter.go for the full
	// rationale.
	fields, err := offchainmetadata.ValidatePoolMetadata(doc.Content)
	if err != nil {
		legacyFailure := &models.OffchainMetadata{LastError: err.Error()}
		ret.Error = offchainFetchError(
			"Pool", metadataURL, metadataHash, legacyFailure,
		)
		return ret
	}
	ret.Name = &fields.Name
	ret.Description = &fields.Description
	ret.Ticker = &fields.Ticker
	ret.Homepage = &fields.Homepage
	return ret
}
