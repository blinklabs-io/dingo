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
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// buildFlatMultiAsset builds a MemPack "flattened multi-asset" buffer in the
// layout decodeFlatMultiAsset documents, mimicking the Haskell toCompact
// encoder: names are deduplicated, so several entries that carry the SAME
// asset name under different policies share one name offset.
func buildFlatMultiAsset(
	qty []uint64,
	pids [][]byte,
	pidIdx []int,
	nameOffIdx []int,
) []byte {
	n := len(qty)
	quantitiesEnd := n * 8
	pidOffsetsEnd := quantitiesEnd + n*2
	nameOffsetsEnd := pidOffsetsEnd + n*2

	// region D: unique policy IDs, in order of first appearance
	var uniq [][]byte
	seen := map[int]bool{}
	for _, p := range pidIdx {
		if !seen[p] {
			seen[p] = true
			uniq = append(uniq, pids[p])
		}
	}
	pids = uniq
	pidBase := nameOffsetsEnd
	pidOff := make([]int, len(pids))
	for i := range pids {
		pidOff[i] = pidBase + i*28
	}
	// region E: one 8-byte "sharednm" slot per distinct name index, in
	// order of first appearance; entries with the same index share it.
	nameOffByIdx := map[int]int{}
	nameOff := make([]int, n)
	cursor := pidBase + len(pids)*28
	for i, ni := range nameOffIdx {
		off, ok := nameOffByIdx[ni]
		if !ok {
			off = cursor
			nameOffByIdx[ni] = off
			cursor += 8
		}
		nameOff[i] = off
	}

	flat := make([]byte, 0, cursor)
	flat = append(flat, make([]byte, cursor)...)
	for i, q := range qty {
		binary.LittleEndian.PutUint64(flat[i*8:], q)
	}
	for i := range qty {
		binary.LittleEndian.PutUint16(
			flat[quantitiesEnd+i*2:],
			uint16(pidOff[pidIdx[i]]),
		)
		binary.LittleEndian.PutUint16(
			flat[pidOffsetsEnd+i*2:],
			uint16(nameOff[i]),
		)
	}
	for i, p := range pids {
		copy(flat[pidOff[i]:], p)
	}
	for _, off := range nameOffByIdx {
		copy(flat[off:], []byte("sharednm"))
	}
	return flat
}

var (
	polA = bytes.Repeat([]byte{0x47, 0xc4}, 14)
	polB = bytes.Repeat([]byte{0xda, 0xa5}, 14)
)

// TestDecodeFlatMultiAssetDuplicateNameIsNotDropped pins the regression: two
// policies that share one asset name (deduplicated to a single name offset by
// the MemPack encoder) must both decode with that name. The first of the pair
// has nameLen 0 and used to be left nil, which encodeMempackTxOut then wrote
// as CBOR null, wiping the asset from the stored UTxO.
func TestDecodeFlatMultiAssetDuplicateNameIsNotDropped(t *testing.T) {
	t.Parallel()

	// Two assets, different policies, ONE shared asset name.
	flat := buildFlatMultiAsset(
		[]uint64{4639030549, 4639030549},
		[][]byte{polA, polB},
		[]int{0, 1},
		[]int{0, 0},
	)
	assets, err := decodeFlatMultiAsset(2, flat)
	if err != nil {
		t.Fatalf("decodeFlatMultiAsset: %v", err)
	}
	if len(assets) != 2 {
		t.Fatalf("got %d assets, want 2", len(assets))
	}
	for i, a := range assets {
		if a.Name == nil {
			t.Errorf("asset %d (%x): name is nil, want %x",
				i, a.PolicyId, []byte("sharednm"))
		} else if len(a.Name) != 8 {
			t.Errorf("asset %d (%x): name len = %d, want 8 (%x)",
				i, a.PolicyId, len(a.Name), a.Name)
		}
		if a.Amount != 4639030549 {
			t.Errorf("asset %d: amount = %d, want 4639030549", i, a.Amount)
		}
	}
}

// TestMempackTxOutSharedAssetNameSurvivesEncoding is the end-to-end shape of
// the production defect: the re-encoded TxOut CBOR must carry the asset name
// bytes, never CBOR null (0xf6) in the asset-name key position.
func TestMempackTxOutSharedAssetNameSurvivesEncoding(t *testing.T) {
	t.Parallel()

	flat := buildFlatMultiAsset(
		[]uint64{4639030549, 4639030549},
		[][]byte{polA, polB},
		[]int{0, 1},
		[]int{0, 0},
	)
	assets, err := decodeFlatMultiAsset(2, flat)
	if err != nil {
		t.Fatalf("decodeFlatMultiAsset: %v", err)
	}

	addr := bytes.Repeat([]byte{0x01}, 57)
	enc, err := encodeMempackTxOut(&decodedMempackTxOut{
		Address:  addr,
		Lovelace: 37318479,
		Assets:   assets,
	})
	if err != nil {
		t.Fatalf("encodeMempackTxOut: %v", err)
	}

	// The name offset inside the encoded buffer for the shared name.
	i := bytes.Index(enc, polA)
	if i < 0 {
		t.Fatalf("policy A not found in encoded output")
	}
	after := enc[i+len(polA):]
	if bytes.HasPrefix(after, []byte{0xa1, 0xf6}) {
		t.Fatalf("encoded asset map for policy A starts with a1 f6 "+
			"(CBOR null asset name): %x", after[:12])
	}
	if !bytes.HasPrefix(after, []byte{0xa1, 0x48}) {
		t.Errorf("encoded asset map for policy A does not carry a "+
			"bytes(8) asset name: %x", after[:12])
	}
}

// buildFlatMultiAssetVariable is a more general flat multi-asset buffer
// builder than buildFlatMultiAsset: it supports asset names of arbitrary
// (including zero) length, assigned directly by value rather than by index
// into a fixed alphabet. Entries with byte-identical names -- including two
// entries that both carry a genuinely empty name -- share one occurrence in
// the built Region E, exactly as the production encoder deduplicates. A
// genuinely empty name is placed past the end of Region E, mirroring the
// Haskell encoder's "asset names sorted descending, so the empty string
// sorts last and its offset points past the end of the concatenated names"
// rule (Value.hs `to`). Callers must supply names in non-decreasing offset
// order (i.e. group identical names together, with any empty name(s) last)
// to produce a structurally valid buffer.
func buildFlatMultiAssetVariable(
	qty []uint64,
	pids [][]byte,
	pidIdx []int,
	names [][]byte,
) []byte {
	n := len(qty)
	quantitiesEnd := n * 8
	pidOffsetsEnd := quantitiesEnd + n*2
	nameOffsetsEnd := pidOffsetsEnd + n*2

	var uniqPids [][]byte
	seenPid := map[int]bool{}
	for _, p := range pidIdx {
		if !seenPid[p] {
			seenPid[p] = true
			uniqPids = append(uniqPids, pids[p])
		}
	}
	pidBase := nameOffsetsEnd
	pidOff := make([]int, len(uniqPids))
	for i := range uniqPids {
		pidOff[i] = pidBase + i*28
	}

	regionEStart := pidBase + len(uniqPids)*28

	// Unique non-empty names, in order of first appearance.
	var order [][]byte
	seenName := map[string]bool{}
	for _, nm := range names {
		if len(nm) == 0 {
			continue
		}
		if !seenName[string(nm)] {
			seenName[string(nm)] = true
			order = append(order, nm)
		}
	}
	offsetOf := map[string]int{}
	cursor := regionEStart
	for _, nm := range order {
		offsetOf[string(nm)] = cursor
		cursor += len(nm)
	}
	emptyOffset := cursor // end of Region E, whether or not any name is empty

	nameOff := make([]int, n)
	for i, nm := range names {
		if len(nm) == 0 {
			nameOff[i] = emptyOffset
		} else {
			nameOff[i] = offsetOf[string(nm)]
		}
	}

	flat := make([]byte, cursor)
	for i, q := range qty {
		binary.LittleEndian.PutUint64(flat[i*8:], q)
	}
	for i := range qty {
		binary.LittleEndian.PutUint16(
			flat[quantitiesEnd+i*2:], uint16(pidOff[pidIdx[i]]))
		binary.LittleEndian.PutUint16(
			flat[pidOffsetsEnd+i*2:], uint16(nameOff[i]))
	}
	for i, p := range uniqPids {
		copy(flat[pidOff[i]:], p)
	}
	for _, nm := range order {
		copy(flat[offsetOf[string(nm)]:], nm)
	}
	return flat
}

var polC = bytes.Repeat([]byte{0x5b, 0x11}, 14)

// TestDecodeFlatMultiAssetMixedSharedAndUniqueNames covers 3 assets where
// the first two (different policies) share one deduplicated name and the
// third (a different policy) has its own unique name occupying the tail of
// Region E.
func TestDecodeFlatMultiAssetMixedSharedAndUniqueNames(t *testing.T) {
	t.Parallel()

	nameX := []byte("nameXXXX")
	nameY := []byte("uniqueY")
	flat := buildFlatMultiAssetVariable(
		[]uint64{10, 20, 30},
		[][]byte{polA, polB, polC},
		[]int{0, 1, 2},
		[][]byte{nameX, nameX, nameY},
	)

	assets, err := decodeFlatMultiAsset(3, flat)
	require.NoError(t, err)
	require.Len(t, assets, 3)

	require.NotNil(t, assets[0].Name)
	require.Equal(t, nameX, assets[0].Name)
	require.NotNil(t, assets[1].Name)
	require.Equal(t, nameX, assets[1].Name,
		"second entry sharing the deduplicated offset must decode "+
			"the same name, not a truncated/nil one")
	require.NotNil(t, assets[2].Name)
	require.Equal(t, nameY, assets[2].Name,
		"unique trailing name must extend to the end of the buffer")
}

// TestDecodeFlatMultiAssetGenuineEmptyNameAfterSharedName covers a
// genuinely empty asset name (valid on Cardano) following a deduplicated
// shared name. The empty name must decode as a proper non-nil, zero-length
// slice -- distinguishable from the duplicate-name bug, which also
// produces a zero length but for a NON-empty name.
func TestDecodeFlatMultiAssetGenuineEmptyNameAfterSharedName(t *testing.T) {
	t.Parallel()

	shared := []byte("shared")
	flat := buildFlatMultiAssetVariable(
		[]uint64{1, 2, 3},
		[][]byte{polA, polB, polC},
		[]int{0, 1, 2},
		[][]byte{shared, shared, {}},
	)

	assets, err := decodeFlatMultiAsset(3, flat)
	require.NoError(t, err)
	require.Len(t, assets, 3)

	require.Equal(t, shared, assets[0].Name)
	require.Equal(t, shared, assets[1].Name,
		"duplicate-name entry must not decode as nil/empty")

	require.NotNil(t, assets[2].Name,
		"a genuine zero-length asset name must decode as a non-nil "+
			"empty slice, not Go nil")
	require.Empty(t, assets[2].Name)
}

// TestDecodeFlatMultiAssetGenuineEmptyNameIsNotNil isolates the
// always-allocate fix from the offset-keyed dedup fix: no two entries here
// share a nameOff, so this covers only the pre-existing defect where the
// "name extends to end of buffer" special case left a genuinely empty last
// name as Go nil instead of a proper empty slice.
func TestDecodeFlatMultiAssetGenuineEmptyNameIsNotNil(t *testing.T) {
	t.Parallel()

	onlyName := []byte("onlyname")
	flat := buildFlatMultiAssetVariable(
		[]uint64{7, 9},
		[][]byte{polA, polB},
		[]int{0, 1},
		[][]byte{onlyName, {}},
	)

	assets, err := decodeFlatMultiAsset(2, flat)
	require.NoError(t, err)
	require.Len(t, assets, 2)

	require.Equal(t, onlyName, assets[0].Name)
	require.NotNil(t, assets[1].Name,
		"a genuine zero-length asset name must decode as a non-nil "+
			"empty slice, not Go nil")
	require.Empty(t, assets[1].Name)
}

// TestMempackTxOutGenuineEmptyNameEncodesAsEmptyBytestring is the
// end-to-end shape of the always-allocate fix: a genuinely empty asset name
// must round-trip through encodeMempackTxOut as CBOR bytestring(0) (0x40),
// never CBOR null (0xf6).
func TestMempackTxOutGenuineEmptyNameEncodesAsEmptyBytestring(t *testing.T) {
	t.Parallel()

	onlyName := []byte("onlyname")
	flat := buildFlatMultiAssetVariable(
		[]uint64{7, 9},
		[][]byte{polA, polB},
		[]int{0, 1},
		[][]byte{onlyName, {}},
	)
	assets, err := decodeFlatMultiAsset(2, flat)
	require.NoError(t, err)

	addr := bytes.Repeat([]byte{0x01}, 57)
	enc, err := encodeMempackTxOut(&decodedMempackTxOut{
		Address:  addr,
		Lovelace: 12345,
		Assets:   assets,
	})
	require.NoError(t, err)

	i := bytes.Index(enc, polB)
	require.GreaterOrEqual(t, i, 0, "policy B not found in encoded output")
	after := enc[i+len(polB):]
	require.False(t, bytes.HasPrefix(after, []byte{0xa1, 0xf6}),
		"encoded asset map for policy B (empty name) starts with "+
			"a1 f6 (CBOR null asset name): %x", after[:min(12, len(after))])
	require.True(t, bytes.HasPrefix(after, []byte{0xa1, 0x40}),
		"encoded asset map for policy B does not carry an empty "+
			"bytestring(0) asset name: %x", after[:min(12, len(after))])
}

// TestDecodeFlatMultiAssetSharedNameAtBufferEnd covers the combination of
// the "last asset's name extends to the end of the buffer" rule with a
// shared (deduplicated) offset: the last TWO entries share the final
// distinct name offset, and both must decode the full name extending to
// the end of the buffer, not just the literal last array entry.
func TestDecodeFlatMultiAssetSharedNameAtBufferEnd(t *testing.T) {
	t.Parallel()

	unique := []byte("uniqueone")
	shared := []byte("shared2x")
	flat := buildFlatMultiAssetVariable(
		[]uint64{100, 200, 300},
		[][]byte{polA, polB, polC},
		[]int{0, 1, 2},
		[][]byte{unique, shared, shared},
	)

	assets, err := decodeFlatMultiAsset(3, flat)
	require.NoError(t, err)
	require.Len(t, assets, 3)

	require.Equal(t, unique, assets[0].Name)
	require.Equal(t, shared, assets[1].Name,
		"first of the two entries sharing the final offset must "+
			"decode the full name, not a zero-length one")
	require.Equal(t, shared, assets[2].Name)
}

// TestDecodeFlatMultiAssetRejectsBackwardOffset ensures a genuinely
// corrupted (backward-jumping, non-repeating) name offset is still
// rejected as an error. This is distinct from a repeated offset that
// exactly matches an earlier entry's, which is valid deduplication.
func TestDecodeFlatMultiAssetRejectsBackwardOffset(t *testing.T) {
	t.Parallel()

	flat := buildFlatMultiAsset(
		[]uint64{1, 2, 3},
		[][]byte{polA, polB, polC},
		[]int{0, 1, 2},
		[]int{0, 1, 2},
	)

	// entries are laid out as: quantities | pid offsets | name offsets |
	// pids | names. Corrupt the THIRD entry's name offset (last Word16
	// in the name-offset region) to point one byte before the SECOND
	// entry's name offset -- a backward jump that does not exactly
	// match any earlier entry's offset, so it must be rejected rather
	// than silently treated as a shared/duplicate offset.
	n := 3
	pidOffsetsEnd := n*8 + n*2
	nameOffsetsEnd := pidOffsetsEnd + n*2
	secondNameOffIdx := pidOffsetsEnd + 1*2
	secondNameOff := binary.LittleEndian.Uint16(
		flat[secondNameOffIdx : secondNameOffIdx+2])
	require.Greater(t, int(secondNameOff), nameOffsetsEnd)

	thirdNameOffIdx := pidOffsetsEnd + 2*2
	binary.LittleEndian.PutUint16(
		flat[thirdNameOffIdx:thirdNameOffIdx+2], secondNameOff-1)

	_, err := decodeFlatMultiAsset(3, flat)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not ascending")
}
