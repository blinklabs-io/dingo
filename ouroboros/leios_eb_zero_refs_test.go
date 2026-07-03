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

package ouroboros

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// Investigation for dingo #2729.
//
// A from-genesis musashi Leios sync stalls in the epoch-15 endorser-block
// region: a ranking block references an endorser block whose manifest is
// non-empty (eb_size 35,499 / 65,343 bytes) yet decodes to ZERO transaction
// references and is rejected with:
//
//	store manifest: decode leios endorser block:
//	leios endorser block must contain at least one transaction reference
//
// These tests establish, without a node, WHERE the zero-refs failure can come
// from. They prove:
//
//  1. The gouroboros manifest decoder correctly handles large reference maps
//     (multi-byte CBOR map headers), in both the array-wrapped (CIP-0164/dingo)
//     and bare-map (IOG prototype) wire shapes. A large, non-empty manifest
//     therefore NEVER decodes to zero refs. This disproves the "hand-rolled
//     header length mis-parse" hypothesis.
//
//  2. The exact "must contain at least one transaction reference" error is
//     produced ONLY by a manifest that is genuinely an empty references map
//     (0xa0 or [{}]). A real 35-65 KB manifest cannot produce it. So the bytes
//     handed to the decoder in the field are an empty/truncated/wrong manifest,
//     i.e. a FETCH/serving problem, not a decode bug on authentic bytes. This
//     matches the in-tree note (ledger/leios_apply.go leiosBackfiller) that the
//     prototype relay "returns empty manifests when hammered".
//
//  3. With the fix in storeLeiosEndorserBlock (verify the manifest hash before
//     decoding), a peer that serves an empty manifest for a valid endorser-block
//     point is diagnosed as a "point hash mismatch" (a fetch/serving error the
//     backfill can retry against other peers) rather than the misleading decode
//     invariant failure that made #2729 look like a consensus/decode defect.

// bareRefMapFromArrayWrapped strips the single-element array wrapper produced by
// LeiosEndorserBlock.MarshalCBOR (0x81 || refMap) to yield the bare {hash=>size}
// references map, i.e. the IOG Leios prototype wire shape (CBOR major type 5).
func bareRefMapFromArrayWrapped(t *testing.T, arrayWrapped []byte) []byte {
	t.Helper()
	require.GreaterOrEqual(t, len(arrayWrapped), 2)
	// Definite one-element array header for a single map element.
	require.Equalf(
		t,
		byte(0x81),
		arrayWrapped[0],
		"expected single-element array wrapper, got 0x%x",
		arrayWrapped[0],
	)
	return arrayWrapped[1:]
}

// TestLeiosEndorserBlockLargeMapDecodesAllRefs proves the manifest decoder reads
// every reference of a large map. 1200 refs requires a 2-byte CBOR map-header
// length (0xb9 || uint16); 300 also needs the multi-byte branch. If the header
// parsing dropped or mis-counted refs for large maps (the #2729 "decode to zero"
// hypothesis), these would fail.
func TestLeiosEndorserBlockLargeMapDecodesAllRefs(t *testing.T) {
	for _, refCount := range []int{24, 256, 300, 715, 1200, 1604} {
		_, arrayWrapped := testLeiosEndorserBlockRawWithRefs(t, 42, refCount)

		// Array-wrapped shape (CIP-0164 / dingo).
		decoded, err := lcommon.NewLeiosEndorserBlockFromCbor(arrayWrapped)
		require.NoErrorf(t, err, "array-wrapped decode failed for %d refs", refCount)
		require.Lenf(
			t,
			decoded.TransactionReferences,
			refCount,
			"array-wrapped: expected %d refs", refCount,
		)

		// Bare-map shape (IOG Leios prototype).
		bareMap := bareRefMapFromArrayWrapped(t, arrayWrapped)
		decodedBare, err := lcommon.NewLeiosEndorserBlockFromCbor(bareMap)
		require.NoErrorf(t, err, "bare-map decode failed for %d refs", refCount)
		require.Lenf(
			t,
			decodedBare.TransactionReferences,
			refCount,
			"bare-map: expected %d refs", refCount,
		)
	}
}

// TestLeiosEndorserBlockZeroRefsErrorOnlyFromEmptyManifest proves the exact
// "must contain at least one transaction reference" error is emitted only for a
// genuinely empty references map, and never for a large non-empty manifest.
func TestLeiosEndorserBlockZeroRefsErrorOnlyFromEmptyManifest(t *testing.T) {
	const zeroRefsMsg = "must contain at least one transaction reference"

	// Empty bare map: 0xa0.
	_, err := lcommon.NewLeiosEndorserBlockFromCbor([]byte{0xa0})
	require.ErrorContains(t, err, zeroRefsMsg)

	// Array-wrapped empty map: [ {} ] = 0x81 0xa0.
	_, err = lcommon.NewLeiosEndorserBlockFromCbor([]byte{0x81, 0xa0})
	require.ErrorContains(t, err, zeroRefsMsg)

	// A large, non-empty manifest of the size reported in #2729 (~1000 refs is
	// ~35 KB) does NOT produce the zero-refs error in either wire shape.
	_, arrayWrapped := testLeiosEndorserBlockRawWithRefs(t, 15, 1000)
	require.Greater(t, len(arrayWrapped), 30000, "manifest should be ~35 KB")

	_, err = lcommon.NewLeiosEndorserBlockFromCbor(arrayWrapped)
	require.NoError(t, err)

	_, err = lcommon.NewLeiosEndorserBlockFromCbor(
		bareRefMapFromArrayWrapped(t, arrayWrapped),
	)
	require.NoError(t, err)
}

// TestStoreLeiosEndorserBlockEmptyManifestIsHashMismatch reproduces the #2729
// field scenario at the dingo store boundary: a peer returns an empty manifest
// (0xa0) in response to a by-point fetch for a valid, non-empty endorser block.
//
// With the hash-before-decode fix, storeLeiosEndorserBlock reports a "point hash
// mismatch" (correctly identifying a wrong/empty peer response, which the
// backfill treats as a retryable fetch failure) instead of the misleading
// "decode leios endorser block: ... must contain at least one transaction
// reference" that made the fetch problem look like a decode/consensus defect and
// wedged the ledger.
func TestStoreLeiosEndorserBlockEmptyManifestIsHashMismatch(t *testing.T) {
	// The point identifies a real, non-empty endorser block (1000 refs).
	point, _ := testLeiosEndorserBlockRawWithRefs(t, 15, 1000)

	// The peer serves an empty manifest instead of the real bytes.
	emptyManifest := []byte{0xa0}

	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	err := o.storeLeiosEndorserBlock(point, emptyManifest, nil)
	require.Error(t, err)
	require.ErrorContains(
		t,
		err,
		"leios endorser block cache: point hash mismatch",
	)
	// The wrong-bytes response is no longer misreported as a decode invariant
	// failure.
	require.NotContains(
		t,
		err.Error(),
		"must contain at least one transaction reference",
	)

	// Nothing was cached for the point.
	_, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.False(t, ok)
}

// TestStoreLeiosEndorserBlockGenuinelyEmptyEbStillRejected confirms the fix does
// not weaken the invariant: a manifest that genuinely IS an empty references map
// AND whose hash matches the requested point (a producer-side protocol
// violation, not a wrong peer response) is still rejected by the decode
// invariant.
func TestStoreLeiosEndorserBlockGenuinelyEmptyEbStillRejected(t *testing.T) {
	emptyManifest := []byte{0xa0}
	hash := lcommon.Blake2b256Hash(emptyManifest)
	point := ocommon.NewPoint(15, hash.Bytes())

	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	err := o.storeLeiosEndorserBlock(point, emptyManifest, nil)
	require.Error(t, err)
	require.ErrorContains(
		t,
		err,
		"must contain at least one transaction reference",
	)
}

// TestStoreLeiosEndorserBlockValidManifestStillStores confirms the reordered
// checks do not regress the happy path: a valid, hash-matching manifest is
// decoded and cached.
func TestStoreLeiosEndorserBlockValidManifestStillStores(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 15, 300)

	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))

	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.Equal(t, 300, data.txCount)
	require.Equal(t, []byte(cbor.RawMessage(blockRaw)), data.blockRaw)
}
