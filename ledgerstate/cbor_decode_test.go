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

package ledgerstate

import (
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

// This file exercises the "Local CBOR decode limits policy" documented
// at the top of cbor_decode.go: for each hand-rolled decode path in
// this package, it proves the limit is accepted exactly at the
// boundary and rejected one past it, using synthetic fixtures rather
// than mainnet-scale data (which would be impractical to check in or
// generate at test time for a 10,000,000-entry cap).

// mustEncodeCbor encodes v and fails the test on error.
func mustEncodeCbor(t *testing.T, v any) []byte {
	t.Helper()
	b, err := cbor.Encode(v)
	require.NoError(t, err)
	return b
}

// cborTypeHeader builds a CBOR major-type header (major<<5 |
// additional-info) for the given argument/count, choosing the
// smallest encoding that fits, mirroring the on-wire encodings that
// cborArgument (cbor_decode.go) and gouroboros's StreamDecoder header
// parsers accept.
func cborTypeHeader(major byte, count uint64) []byte {
	switch {
	case count < 24:
		return []byte{major<<5 | byte(count)}
	case count <= 0xff:
		return []byte{major<<5 | 24, byte(count)}
	case count <= 0xffff:
		b := make([]byte, 3)
		b[0] = major<<5 | 25
		binary.BigEndian.PutUint16(b[1:], uint16(count))
		return b
	case count <= 0xffffffff:
		b := make([]byte, 5)
		b[0] = major<<5 | 26
		binary.BigEndian.PutUint32(b[1:], uint32(count))
		return b
	default:
		b := make([]byte, 9)
		b[0] = major<<5 | 27
		binary.BigEndian.PutUint64(b[1:], count)
		return b
	}
}

const cborMajorMap = 5

// buildDefiniteMapCbor builds a definite-length CBOR map with n
// trivial uint64->uint64 entries. Structural decoders in this
// package (decodeMapEntriesLimit) only need each key/value to be a
// well-formed CBOR item, not any particular value.
func buildDefiniteMapCbor(t *testing.T, n int) []byte {
	t.Helper()
	data := cborTypeHeader(cborMajorMap, uint64(n))
	for i := range n {
		data = append(data, mustEncodeCbor(t, uint64(i))...)
		data = append(data, mustEncodeCbor(t, uint64(i))...)
	}
	return data
}

// buildIndefiniteMapCbor builds an indefinite-length (0xbf ... 0xff)
// CBOR map with n trivial uint64->uint64 entries.
func buildIndefiniteMapCbor(t *testing.T, n int) []byte {
	t.Helper()
	data := []byte{0xbf}
	for i := range n {
		data = append(data, mustEncodeCbor(t, uint64(i))...)
		data = append(data, mustEncodeCbor(t, uint64(i))...)
	}
	data = append(data, 0xff)
	return data
}

func TestDecodeMapEntriesLimit_DefiniteLength_AtLimitAccepted(t *testing.T) {
	t.Parallel()
	const limit = 5
	data := buildDefiniteMapCbor(t, limit)

	entries, err := decodeMapEntriesLimit(data, limit)

	require.NoError(t, err)
	require.Len(t, entries, limit)
}

func TestDecodeMapEntriesLimit_DefiniteLength_OverLimitRejected(t *testing.T) {
	t.Parallel()
	const limit = 5
	// The header alone claims limit+1 entries. The bounds check runs
	// against the header-declared count before any entry bytes are
	// read, so no entry payload is required to prove rejection.
	header := cborTypeHeader(cborMajorMap, uint64(limit+1))

	_, err := decodeMapEntriesLimit(header, limit)

	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds max")
}

func TestDecodeMapEntriesLimit_IndefiniteLength_AtLimitAccepted(t *testing.T) {
	t.Parallel()
	const limit = 5
	data := buildIndefiniteMapCbor(t, limit)

	entries, err := decodeMapEntriesLimit(data, limit)

	require.NoError(t, err)
	require.Len(t, entries, limit)
}

func TestDecodeMapEntriesLimit_IndefiniteLength_OverLimitRejected(
	t *testing.T,
) {
	t.Parallel()
	const limit = 5
	data := buildIndefiniteMapCbor(t, limit+1)

	_, err := decodeMapEntriesLimit(data, limit)

	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeded max entries")
}

// TestDecodeMapEntries_ProductionLimitIsExplicit locks the documented
// production cap. If this constant is ever changed, this test forces
// an update to both this test and the "Local CBOR decode limits
// policy" doc comment (and DATABASE.md, per CLAUDE.md doc parity).
func TestDecodeMapEntries_ProductionLimitIsExplicit(t *testing.T) {
	t.Parallel()
	require.Equal(t, 10_000_000, maxMapEntries)
}

func TestDecodeMapEntries_ProductionCapRejectsOversizedHeader(t *testing.T) {
	t.Parallel()
	// Header-only fixture: claims one more than the real production
	// cap. No payload is needed since the check runs before entries
	// are parsed, so this proves the deployed 10,000,000 cap without
	// allocating a mainnet-scale fixture.
	header := cborTypeHeader(cborMajorMap, uint64(maxMapEntries+1))

	_, err := decodeMapEntries(header)

	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds max (10000000)")
}

func TestCheckUTxOMapEntryCount(t *testing.T) {
	t.Parallel()

	require.NoError(t, checkUTxOMapEntryCount(0))
	require.NoError(t, checkUTxOMapEntryCount(maxMapEntries))

	err := checkUTxOMapEntryCount(maxMapEntries + 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds max")
}

func TestParseUTxOsStreaming_RejectsOversizedMapHeader(t *testing.T) {
	t.Parallel()
	// Header-only fixture claiming one more than maxMapEntries; the
	// new checkUTxOMapEntryCount guard must reject it before the
	// streaming loop (and therefore the callback) ever runs.
	header := cborTypeHeader(cborMajorMap, uint64(maxMapEntries+1))
	callbackInvoked := false

	_, err := ParseUTxOsStreaming(
		cbor.RawMessage(header),
		func(batch []ParsedUTxO) error {
			callbackInvoked = true
			return nil
		},
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds max")
	require.False(
		t,
		callbackInvoked,
		"callback must not run for a rejected oversized header",
	)
}

// buildNestedTelescope builds a right-nested HardFork telescope CBOR
// structure with pastCount "past era" (tag 1) layers wrapping a
// single "current era" (tag 0) leaf, i.e. pastCount+1 total era
// entries. Each past-era summary and the current-era bound reuse the
// same minimal valid Bound encoding: [relativeTime, slot, epoch].
func buildNestedTelescope(t *testing.T, pastCount int) []byte {
	t.Helper()
	bound := []any{uint64(0), uint64(1000), uint64(1)}
	// Current era: [tag(0), [bound]]
	node := []any{uint64(0), []any{bound}}
	for range pastCount {
		// Past era: [tag(1), summary, rest], summary = [bound, ...]
		summary := []any{bound}
		node = []any{uint64(1), summary, node}
	}
	return mustEncodeCbor(t, node)
}

func TestExtractAllEraBounds_NestedTelescope_AtMaxDepthAccepted(t *testing.T) {
	t.Parallel()
	// MaxTelescopeDepth-1 past layers + 1 current era = exactly
	// MaxTelescopeDepth total era entries, the deepest nesting that
	// must still succeed.
	data := buildNestedTelescope(t, MaxTelescopeDepth-1)

	bounds, err := extractAllEraBounds(data)

	require.NoError(t, err)
	require.Len(t, bounds, MaxTelescopeDepth)
}

func TestExtractAllEraBounds_NestedTelescope_OverMaxDepthRejected(
	t *testing.T,
) {
	t.Parallel()
	// One more past layer than the accepted case: recursion must
	// exceed MaxTelescopeDepth and be rejected.
	data := buildNestedTelescope(t, MaxTelescopeDepth)

	_, err := extractAllEraBounds(data)

	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds maximum")
}

func TestNavigateTelescope_NestedTelescope_AtMaxDepthAccepted(t *testing.T) {
	t.Parallel()
	data := buildNestedTelescope(t, MaxTelescopeDepth-1)

	eraIndex, _, err := navigateTelescope(data)

	require.NoError(t, err)
	require.Equal(t, MaxTelescopeDepth-1, eraIndex)
}

func TestNavigateTelescope_NestedTelescope_OverMaxDepthRejected(t *testing.T) {
	t.Parallel()
	data := buildNestedTelescope(t, MaxTelescopeDepth)

	_, _, err := navigateTelescope(data)

	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds maximum")
}

// nestedTagCbor wraps a scalar CBOR value (unsigned int 0) in depth
// single-byte tag(0) headers (0xC0), used to probe cborItemSize's
// maxCborDepth recursion cap.
func nestedTagCbor(depth int) []byte {
	data := []byte{0x00} // uint64(0), single byte
	for range depth {
		data = append([]byte{0xc0}, data...)
	}
	return data
}

func TestCborItemSize_AtMaxDepthAccepted(t *testing.T) {
	t.Parallel()
	data := nestedTagCbor(maxCborDepth)

	size, err := cborItemSize(data)

	require.NoError(t, err)
	require.Equal(t, len(data), size)
}

func TestCborItemSize_OverMaxDepthRejected(t *testing.T) {
	t.Parallel()
	data := nestedTagCbor(maxCborDepth + 1)

	_, err := cborItemSize(data)

	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds max depth")
}

func TestDecodeRawElements_IntKeyedMap_AtMaxKeyAccepted(t *testing.T) {
	t.Parallel()
	data := mustEncodeCbor(t, map[uint64]uint64{256: 42})

	elements, err := decodeRawElements(data)

	require.NoError(t, err)
	require.Len(t, elements, 257) // maxAllowedKey(256) + 1
}

func TestDecodeRawElements_IntKeyedMap_OverMaxKeyRejected(t *testing.T) {
	t.Parallel()
	data := mustEncodeCbor(t, map[uint64]uint64{257: 42})

	_, err := decodeRawElements(data)

	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeding maximum")
}
