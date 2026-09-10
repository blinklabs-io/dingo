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
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// koiosAccountChunkMaxBytesDefault is the default encoded-JSON byte bound
// applied to a chunk's _stake_addresses array when the caller doesn't
// configure one (chunkMaxBytes <= 0 — see fetchAccountRewardsForEpoch).
// Koios's public endpoint rejects request bodies at 5120 bytes, so 4 KiB
// leaves headroom while still packing dozens of normal stake addresses into
// each request. The fixed JSON envelope is reserved separately below.
const koiosAccountChunkMaxBytesDefault = 4 * 1024

// koiosAccountRequestEnvelopeOverhead reserves space, out of a configured
// --account-chunk-max-bytes budget, for the fixed JSON wrapper around
// /account_reward_history's address array:
// {"_stake_addresses":[...],"_epoch_no":18446744073709551615} — the
// "_stake_addresses":/"_epoch_no": key names, braces, and the widest
// possible uint64 epoch number add up to roughly 60 bytes regardless of how
// many addresses are in the array; chunkAddressesByCountAndSize itself only
// bounds the array's own encoded size, so fetchAccountRewardsForEpoch
// subtracts this constant from the configured budget before chunking —
// otherwise the true request body could exceed the configured bound by this
// fixed amount every time.
const koiosAccountRequestEnvelopeOverhead = 64

// hashAddressChunk returns a content-addressed identifier for one chunk's
// address set: sha256 of the addresses joined in the given order. Called
// with an already-sorted chunk (chunkAddressesByCountAndSize's chunks are
// slices of a sorted input, so the address order within a chunk is stable
// across calls for the same universe/bounds), so the same underlying set of
// addresses always hashes to the same value regardless of when or how many
// times it's computed — the property FetchAccountRewardsForEpoch's resumable
// checkpointing (koios_account_checked/koios_account_fetch_staged_rows) and
// selective chunk invalidation depend on.
func hashAddressChunk(addrs []string) string {
	sum := sha256.Sum256([]byte(strings.Join(addrs, "\x00")))
	return hex.EncodeToString(sum[:])
}

// chunkAddressesByCountAndSize splits a sorted address list into groups
// bounded by both address count (maxCount) and encoded-JSON body size
// (maxBytes) — dingo #3099's "shape requests by both account count and
// encoded request/response size" requirement, on top of #3097's
// count-only chunkAddresses/koiosAccountChunkSize.
//
// addrs must already be sorted (FetchAccountRewardsForEpoch sorts the
// address universe before calling this) so that, for a fixed input universe
// and fixed maxCount/maxBytes, this always produces the same chunk
// boundaries — required for content-addressed chunk hashing (sha256 of a
// chunk's own sorted address list) to work as a stable resume key: an
// operator changing --account-chunk-size/--account-chunk-max-bytes between
// runs must yield different-but-valid boundaries, never corrupt resume
// state by silently reordering which addresses share a chunk.
//
// A single address whose own encoded size already exceeds maxBytes still
// gets its own one-address chunk rather than being dropped or causing an
// error — the byte bound shapes chunk boundaries, it never discards
// addresses.
func chunkAddressesByCountAndSize(
	addrs []string,
	maxCount, maxBytes int,
) [][]string {
	if maxCount <= 0 {
		maxCount = koiosAccountChunkSize
	}
	if len(addrs) == 0 {
		return nil
	}

	var chunks [][]string
	var current []string
	currentBytes := 0

	flush := func() {
		if len(current) > 0 {
			chunks = append(chunks, current)
			current = nil
			currentBytes = 0
		}
	}

	for _, addr := range addrs {
		// Encoded size of one address in a JSON string array: the quoted
		// string plus a comma/bracket separator byte, mirroring the actual
		// _stake_addresses request-body encoding closely enough to bound it
		// without re-marshaling on every candidate.
		addrBytes := len(addr) + 3

		switch {
		case maxBytes <= 0:
			// No byte bound configured — count alone governs.
		case len(current) == 0:
			// Always place at least one address in a fresh chunk, even if it
			// alone exceeds maxBytes: a byte-bound this address can never
			// satisfy must not cause it to be dropped.
		case currentBytes+addrBytes > maxBytes:
			flush()
		}

		if len(current) >= maxCount {
			flush()
		}

		current = append(current, addr)
		currentBytes += addrBytes
	}
	flush()

	return chunks
}
