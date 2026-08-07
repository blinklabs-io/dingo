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
	"encoding/hex"
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

// utxoTvarTxOutHex is a real MemPack TxOut (tag 2, AddrHash28
// ADA-only), reused from TestDecodeMempackTxOutTag2, used below as a
// minimal but genuinely parseable TxOut value so indefinite-length
// UTxO map fixtures exercise the real parseUTxOEntry path rather
// than a trivial placeholder.
const utxoTvarTxOutHex = "02015691d68ad87582fc89b9ac43fd0227cfa4108efb79" +
	"1b9987b290a9ba85b06c5a4edd9c1b857a1b55106ee4" +
	"0191bb091025984a9d01000000f68597d600c99f00"

// buildIndefiniteUTxOMapCbor builds an indefinite-length (0xbf ...
// 0xff) CBOR UTxO map with n entries, each a valid 34-byte binary
// TxIn (32-byte hash + big-endian output index) mapped to the same
// valid MemPack TxOut payload, both CBOR byte-string wrapped as
// parseUTxOEntry expects.
func buildIndefiniteUTxOMapCbor(t *testing.T, n int) []byte {
	t.Helper()
	txOutBytes, err := hex.DecodeString(utxoTvarTxOutHex)
	require.NoError(t, err)

	data := []byte{0xbf}
	for i := range n {
		txIn := make([]byte, 34)
		binary.BigEndian.PutUint16(txIn[32:], uint16(i))
		data = append(data, mustEncodeCbor(t, txIn)...)
		data = append(data, mustEncodeCbor(t, txOutBytes)...)
	}
	data = append(data, 0xff)
	return data
}

func TestCheckUTxOMapRunningEntryCount(t *testing.T) {
	t.Parallel()

	const limit = 5
	require.NoError(t, checkUTxOMapRunningEntryCount(0, limit))
	require.NoError(t, checkUTxOMapRunningEntryCount(limit-1, limit))

	err := checkUTxOMapRunningEntryCount(limit, limit)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeded max entries")
}

func TestParseIndefiniteUTxOMap_AtLimitAccepted(t *testing.T) {
	t.Parallel()
	const limit = 3
	data := buildIndefiniteUTxOMapCbor(t, limit)

	var total int
	count, err := parseIndefiniteUTxOMapWithProgressLimit(
		data,
		func(batch []ParsedUTxO) error {
			total += len(batch)
			return nil
		},
		nil,
		limit,
	)

	require.NoError(t, err)
	require.Equal(t, limit, count)
	require.Equal(t, limit, total)
}

func TestParseIndefiniteUTxOMap_OverLimitRejected(t *testing.T) {
	t.Parallel()
	const limit = 3
	// limit valid entries, then a single arbitrary non-0xff byte in
	// place of a (limit+1)th entry. The running-count check must
	// fire before the decoder ever reads that byte as an entry, so
	// it doesn't need to be well-formed CBOR to prove rejection.
	data := buildIndefiniteUTxOMapCbor(t, limit)
	data = append(data[:len(data)-1], 0x00, 0xff)
	callbackInvoked := false

	_, err := parseIndefiniteUTxOMapWithProgressLimit(
		data,
		func(batch []ParsedUTxO) error {
			callbackInvoked = true
			return nil
		},
		nil,
		limit,
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeded max entries")
	require.False(
		t,
		callbackInvoked,
		"callback must not run once the running cap rejects an entry",
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
