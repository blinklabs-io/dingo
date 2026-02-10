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

// mempack implements decoders for Haskell's MemPack binary format as
// used in Cardano UTxO-HD tvar files. The format is defined by the
// lehins/mempack Haskell package and used by cardano-ledger for
// on-disk UTxO serialization.
//
// Key encoding details verified from Haskell source:
//
//   - VarLen: Big-endian 7-bit continuation encoding. MSB=1 means
//     more bytes follow. First byte holds the most significant bits.
//     This is NOT LEB128 (which is little-endian). The Haskell
//     unpack7BitVarLen reads bytes and shifts accumulator left.
//
//   - CompactForm Coin: tag(0) + VarLen(Word64). The tag 0 prefix
//     is for binary compatibility with CompactValue.
//
//   - CompactValue (CompactForm MaryValue):
//     Tag 0 (AdaOnly): VarLen(coin)
//     Tag 1 (MultiAsset): VarLen(coin) + VarLen(numAssets:Word32)
//       + Length-prefixed ShortByteString (flat multi-asset rep)
//     Note: coin inside CompactValue skips the tag 0 prefix that
//     the standalone CompactForm Coin uses.
//
//   - Credential: tag(0=ScriptHash, 1=KeyHash) + 28 raw bytes
//     (Hash via PackedBytes, no length prefix).
//     Note: MemPack tags are OPPOSITE of CBOR (KeyHash=0 in CBOR).
//
//   - Addr28Extra: 4 * Word64 LE = 32 bytes. Stores payment
//     credential hash (28 bytes packed across the first 3.5 Word64s)
//     plus network bit and credential type bit in the low bits of
//     the 4th Word64.
//
//   - DataHash (SafeHash): PackedBytes 32 = 32 raw bytes, big-endian
//     Word64 packing inside PackedBytes.
//
//   - DataHash32: 4 * Word64 LE = 32 bytes (same physical layout).
//
//   - Datum (era): tag 0=NoDatum, tag 1=DatumHash(+32 bytes),
//     tag 2=Datum(+BinaryData as ShortByteString)
//
//   - BinaryData: deriving newtype from ShortByteString =
//     Length-prefixed bytes
//
//   - CompactAddr: deriving newtype from ShortByteString =
//     Length-prefixed bytes
//
// Reference:
//   - lehins/mempack: https://github.com/lehins/mempack
//   - BabbageTxOut MemPack: cardano-ledger/eras/babbage/impl/src/
//     Cardano/Ledger/Babbage/TxOut.hs
//   - AlonzoTxOut (Addr28Extra): cardano-ledger/eras/alonzo/impl/src/
//     Cardano/Ledger/Alonzo/TxOut.hs
//   - CompactValue MemPack: cardano-ledger/eras/mary/impl/src/
//     Cardano/Ledger/Mary/Value.hs
//   - CompactForm Coin: cardano-ledger/libs/cardano-ledger-core/src/
//     Cardano/Ledger/Coin.hs
//   - Credential: cardano-ledger/libs/cardano-ledger-core/src/
//     Cardano/Ledger/Credential.hs
//   - PackedBytes: cardano-base/cardano-crypto-class/src/
//     Cardano/Crypto/PackedBytes/Internal.hs

import (
	"encoding/binary"
	"fmt"
	"math"
)

// mempackReader is a cursor over a byte slice for reading MemPack
// encoded data.
type mempackReader struct {
	data []byte
	pos  int
}

func newMempackReader(data []byte) *mempackReader {
	return &mempackReader{data: data}
}

// readByte reads a single byte.
func (r *mempackReader) readByte() (byte, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf(
			"mempack: unexpected EOF at pos %d", r.pos,
		)
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

// readBytes reads exactly n bytes.
func (r *mempackReader) readBytes(n int) ([]byte, error) {
	if n < 0 || n > len(r.data)-r.pos {
		return nil, fmt.Errorf(
			"mempack: need %d bytes at pos %d, have %d",
			n, r.pos, len(r.data)-r.pos,
		)
	}
	b := make([]byte, n)
	copy(b, r.data[r.pos:r.pos+n])
	r.pos += n
	return b, nil
}

// readWord64LE reads a little-endian uint64 (native MemPack Prim).
func (r *mempackReader) readWord64LE() (uint64, error) {
	b, err := r.readBytes(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

// readTag reads a MemPack tag (1 byte = Word8).
func (r *mempackReader) readTag() (byte, error) {
	return r.readByte()
}

// readVarLen reads a MemPack VarLen-encoded unsigned integer.
//
// MemPack VarLen uses big-endian 7-bit continuation encoding:
// MSB=1 means more bytes follow, MSB=0 means last byte. The first
// byte holds the most significant data bits. Accumulator is built
// by shifting left.
//
// Haskell implementation (unpack7BitVarLen):
//
//	acc = (acc << 7) | (b8 & 0x7f)  // for each byte
//	if MSB clear: return result
//
// This differs from LEB128 which puts LSBs first.
func (r *mempackReader) readVarLen() (int, error) {
	result, err := r.readVarLenUint64()
	if err != nil {
		return 0, err
	}
	if result > uint64(math.MaxInt) {
		return 0, fmt.Errorf(
			"mempack: VarLen value %d overflows int",
			result,
		)
	}
	return int(result), nil
}

// readVarLenUint64 reads a VarLen-encoded uint64.
func (r *mempackReader) readVarLenUint64() (uint64, error) {
	var acc uint64
	for range 10 { // max 10 bytes for 64-bit
		b, err := r.readByte()
		if err != nil {
			return 0, fmt.Errorf("reading VarLen: %w", err)
		}
		if acc > (math.MaxUint64 >> 7) {
			return 0, fmt.Errorf(
				"mempack: VarLen overflow at pos %d",
				r.pos-1,
			)
		}
		acc = (acc << 7) | uint64(b&0x7f)
		if b&0x80 == 0 {
			return acc, nil
		}
	}
	return 0, fmt.Errorf(
		"mempack: VarLen overflow at pos %d", r.pos,
	)
}

// readLengthPrefixedBytes reads a VarLen length prefix followed by
// that many raw bytes. This is the MemPack encoding for
// ShortByteString, ByteString, and ByteArray (via Length newtype).
func (r *mempackReader) readLengthPrefixedBytes() ([]byte, error) {
	length, err := r.readVarLen()
	if err != nil {
		return nil, fmt.Errorf(
			"reading byte string length: %w", err,
		)
	}
	return r.readBytes(length)
}

// BabbageTxOut MemPack tags.
const (
	babbageTxOutCompact          = 0 // CompactAddr + Value
	babbageTxOutCompactDH        = 1 // CompactAddr + Value + DataHash
	babbageTxOutAddrHash28       = 2 // Credential + Addr28Extra + Coin
	babbageTxOutAddrHash28DH     = 3 // Credential + Addr28Extra + Coin + DataHash32
	babbageTxOutCompactDatum     = 4 // CompactAddr + Value + BinaryData
	babbageTxOutCompactRefScript = 5 // CompactAddr + Value + Datum + Script
)

// decodedMempackTxOut holds the decoded fields from a MemPack-encoded
// Babbage/Conway TxOut.
type decodedMempackTxOut struct {
	Address   []byte // raw address bytes
	Lovelace  uint64 // ADA amount in lovelace
	Assets    []ParsedAsset
	DatumHash []byte // 32 bytes, optional
	Datum     []byte // inline datum bytes, optional
	ScriptRef []byte // reference script bytes, optional
}

// decodeMempackTxOut decodes a MemPack-encoded BabbageTxOut (also
// used for Conway). Returns the parsed fields needed for UTxO import.
func decodeMempackTxOut(data []byte) (*decodedMempackTxOut, error) {
	r := newMempackReader(data)
	tag, err := r.readTag()
	if err != nil {
		return nil, fmt.Errorf("reading TxOut tag: %w", err)
	}

	var result *decodedMempackTxOut
	switch tag {
	case babbageTxOutCompact:
		result, err = decodeTxOutCompact(r)
	case babbageTxOutCompactDH:
		result, err = decodeTxOutCompactDH(r)
	case babbageTxOutAddrHash28:
		result, err = decodeTxOutAddrHash28(r, false)
	case babbageTxOutAddrHash28DH:
		result, err = decodeTxOutAddrHash28(r, true)
	case babbageTxOutCompactDatum:
		result, err = decodeTxOutCompactDatum(r)
	case babbageTxOutCompactRefScript:
		result, err = decodeTxOutCompactRefScript(r)
	default:
		return nil, fmt.Errorf(
			"unknown MemPack TxOut tag: %d", tag,
		)
	}
	if err != nil {
		return nil, err
	}

	// Verify all bytes were consumed. Trailing bytes indicate
	// corrupted, truncated, or mis-tagged data.
	if r.pos != len(r.data) {
		return nil, fmt.Errorf(
			"mempack: %d trailing bytes after TxOut "+
				"tag %d (consumed %d of %d)",
			len(r.data)-r.pos, tag, r.pos, len(r.data),
		)
	}
	return result, nil
}

// decodeTxOutCompact: tag 0 = CompactAddr + CompactForm Value
func decodeTxOutCompact(
	r *mempackReader,
) (*decodedMempackTxOut, error) {
	addr, err := r.readLengthPrefixedBytes()
	if err != nil {
		return nil, fmt.Errorf("reading CompactAddr: %w", err)
	}
	lovelace, assets, err := decodeCompactValue(r)
	if err != nil {
		return nil, fmt.Errorf("reading Value: %w", err)
	}
	return &decodedMempackTxOut{
		Address:  addr,
		Lovelace: lovelace,
		Assets:   assets,
	}, nil
}

// decodeTxOutCompactDH: tag 1 = CompactAddr + CompactForm Value +
// DataHash (SafeHash = PackedBytes 32 = 32 raw bytes)
func decodeTxOutCompactDH(
	r *mempackReader,
) (*decodedMempackTxOut, error) {
	addr, err := r.readLengthPrefixedBytes()
	if err != nil {
		return nil, fmt.Errorf("reading CompactAddr: %w", err)
	}
	lovelace, assets, err := decodeCompactValue(r)
	if err != nil {
		return nil, fmt.Errorf("reading Value: %w", err)
	}
	datumHash, err := r.readBytes(32)
	if err != nil {
		return nil, fmt.Errorf("reading DataHash: %w", err)
	}
	return &decodedMempackTxOut{
		Address:   addr,
		Lovelace:  lovelace,
		Assets:    assets,
		DatumHash: datumHash,
	}, nil
}

// decodeTxOutAddrHash28: tags 2,3 = Credential (staking) +
// Addr28Extra (32 bytes) + CompactForm Coin [+ DataHash32]
//
// The Addr28Extra stores the payment credential hash (28 bytes)
// plus network and credential type bits packed into 4 * Word64 LE
// = 32 bytes. The staking credential is stored as a separate
// Credential field (tag + 28-byte hash).
//
// The CompactForm Coin standalone instance uses: tag(0) + VarLen.
//
// Haskell field order (from BabbageTxOut MemPack instance):
//
//	packTagM 2 >> packM cred >> packM addr28 >> packM cCoin
//
// Where:
//   - cred = Credential Staking (tag + 28 bytes)
//   - addr28 = Addr28Extra (4 * Word64 = 32 bytes)
//   - cCoin = CompactForm Coin (tag 0 + VarLen Word64)
func decodeTxOutAddrHash28(
	r *mempackReader, hasDataHash bool,
) (*decodedMempackTxOut, error) {
	// Read staking Credential: tag + 28-byte hash
	// MemPack tags: 0=ScriptHashObj, 1=KeyHashObj
	stakingCredTag, err := r.readTag()
	if err != nil {
		return nil, fmt.Errorf(
			"reading staking Credential tag: %w", err,
		)
	}
	if stakingCredTag > 1 {
		return nil, fmt.Errorf(
			"invalid staking Credential tag: %d",
			stakingCredTag,
		)
	}
	stakingHash, err := r.readBytes(28)
	if err != nil {
		return nil, fmt.Errorf(
			"reading staking Credential hash: %w", err,
		)
	}

	// Read Addr28Extra: 4 * Word64 LE = 32 bytes
	// Encodes the payment credential hash (28 bytes) +
	// network bit + credential type bit in the 4th Word64
	w0, err := r.readWord64LE()
	if err != nil {
		return nil, fmt.Errorf(
			"reading Addr28Extra w0: %w", err,
		)
	}
	w1, err := r.readWord64LE()
	if err != nil {
		return nil, fmt.Errorf(
			"reading Addr28Extra w1: %w", err,
		)
	}
	w2, err := r.readWord64LE()
	if err != nil {
		return nil, fmt.Errorf(
			"reading Addr28Extra w2: %w", err,
		)
	}
	w3, err := r.readWord64LE()
	if err != nil {
		return nil, fmt.Errorf(
			"reading Addr28Extra w3: %w", err,
		)
	}

	// Read CompactForm Coin: tag(0) + VarLen(Word64)
	coinTag, err := r.readTag()
	if err != nil {
		return nil, fmt.Errorf(
			"reading CompactForm Coin tag: %w", err,
		)
	}
	if coinTag != 0 {
		return nil, fmt.Errorf(
			"expected CompactForm Coin tag 0, got %d",
			coinTag,
		)
	}
	coin, err := r.readVarLenUint64()
	if err != nil {
		return nil, fmt.Errorf("reading Coin VarLen: %w", err)
	}

	var datumHash []byte
	if hasDataHash {
		// DataHash32: 4 * Word64 LE = 32 bytes
		datumHash, err = r.readBytes(32)
		if err != nil {
			return nil, fmt.Errorf(
				"reading DataHash32: %w", err,
			)
		}
	}

	// Reconstruct the full Shelley address from Addr28Extra
	// + staking credential.
	addr := reconstructAddr28(
		stakingCredTag, stakingHash, w0, w1, w2, w3,
	)

	return &decodedMempackTxOut{
		Address:   addr,
		Lovelace:  coin,
		DatumHash: datumHash,
	}, nil
}

// decodeTxOutCompactDatum: tag 4 = CompactAddr + CompactForm Value +
// BinaryData (inline datum as ShortByteString)
func decodeTxOutCompactDatum(
	r *mempackReader,
) (*decodedMempackTxOut, error) {
	addr, err := r.readLengthPrefixedBytes()
	if err != nil {
		return nil, fmt.Errorf("reading CompactAddr: %w", err)
	}
	lovelace, assets, err := decodeCompactValue(r)
	if err != nil {
		return nil, fmt.Errorf("reading Value: %w", err)
	}
	// BinaryData is a ShortByteString (Length-prefixed bytes)
	datum, err := r.readLengthPrefixedBytes()
	if err != nil {
		return nil, fmt.Errorf("reading BinaryData: %w", err)
	}
	return &decodedMempackTxOut{
		Address:  addr,
		Lovelace: lovelace,
		Assets:   assets,
		Datum:    datum,
	}, nil
}

// decodeTxOutCompactRefScript: tag 5 = CompactAddr +
// CompactForm Value + Datum (era) + Script (era)
func decodeTxOutCompactRefScript(
	r *mempackReader,
) (*decodedMempackTxOut, error) {
	addr, err := r.readLengthPrefixedBytes()
	if err != nil {
		return nil, fmt.Errorf("reading CompactAddr: %w", err)
	}
	lovelace, assets, err := decodeCompactValue(r)
	if err != nil {
		return nil, fmt.Errorf("reading Value: %w", err)
	}
	// Datum era: tag 0=NoDatum, 1=DatumHash, 2=Datum(BinaryData)
	datumHash, datum, err := decodeMempackDatum(r)
	if err != nil {
		return nil, fmt.Errorf("reading Datum: %w", err)
	}
	// Script (era) = AlonzoScript: tag + payload
	script, err := decodeMempackScript(r)
	if err != nil {
		return nil, fmt.Errorf("reading Script: %w", err)
	}
	return &decodedMempackTxOut{
		Address:   addr,
		Lovelace:  lovelace,
		Assets:    assets,
		DatumHash: datumHash,
		Datum:     datum,
		ScriptRef: script,
	}, nil
}

// decodeCompactValue decodes a MemPack CompactValue (CompactForm
// MaryValue).
//
// Tag 0 (CompactValueAdaOnly): VarLen(coin)
// Tag 1 (CompactValueMultiAsset): VarLen(coin) +
//
//	VarLen(numAssets:Word32) + Length-prefixed ShortByteString
//
// Note: Inside CompactValue, the coin is packed directly as
// VarLen(Word64) WITHOUT the tag 0 prefix that the standalone
// CompactForm Coin instance uses.
func decodeCompactValue(
	r *mempackReader,
) (uint64, []ParsedAsset, error) {
	tag, err := r.readTag()
	if err != nil {
		return 0, nil, fmt.Errorf("reading Value tag: %w", err)
	}

	switch tag {
	case 0: // CompactValueAdaOnly
		coin, err := r.readVarLenUint64()
		if err != nil {
			return 0, nil, fmt.Errorf(
				"reading AdaOnly Coin: %w", err,
			)
		}
		return coin, nil, nil

	case 1: // CompactValueMultiAsset
		coin, err := r.readVarLenUint64()
		if err != nil {
			return 0, nil, fmt.Errorf(
				"reading MultiAsset Coin: %w", err,
			)
		}
		// Number of assets (VarLen Word32)
		numAssets, err := r.readVarLen()
		if err != nil {
			return 0, nil, fmt.Errorf(
				"reading asset count: %w", err,
			)
		}
		// Flattened multi-asset representation (ShortByteString)
		flatBytes, err := r.readLengthPrefixedBytes()
		if err != nil {
			return 0, nil, fmt.Errorf(
				"reading multi-asset bytes: %w", err,
			)
		}
		assets, err := decodeFlatMultiAsset(numAssets, flatBytes)
		if err != nil {
			return coin, nil, fmt.Errorf(
				"decoding multi-asset: %w", err,
			)
		}
		return coin, assets, nil

	default:
		return 0, nil, fmt.Errorf(
			"unknown CompactValue tag: %d", tag,
		)
	}
}

// decodeFlatMultiAsset decodes the compact multi-asset
// representation. Layout (sorted descending by asset name):
//
//	Region A: Word64 quantities (8 bytes LE x n)
//	Region B: Word16 policy ID offsets (2 bytes LE x n)
//	Region C: Word16 asset name offsets (2 bytes LE x n)
//	Region D: Concatenated unique policy IDs (28 bytes each)
//	Region E: Concatenated asset names (no trailing padding)
//
// All offsets (in B and C) are ABSOLUTE byte offsets within the
// flat buffer, NOT relative to Region D. For example, if there
// are 3 assets, the first policy ID starts at offset
// 3*8 + 3*2 + 3*2 = 36.
func decodeFlatMultiAsset(
	numAssets int,
	flat []byte,
) ([]ParsedAsset, error) {
	if numAssets == 0 {
		return nil, nil
	}

	// Each asset requires at least 12 bytes in the flat buffer
	// (8 quantity + 2 pid offset + 2 name offset). Reject early
	// to prevent excessive allocation or integer overflow in the
	// offset arithmetic below.
	if numAssets > len(flat)/12 {
		return nil, fmt.Errorf(
			"multi-asset count %d exceeds maximum for "+
				"buffer size %d",
			numAssets, len(flat),
		)
	}

	// Calculate region boundaries
	quantitiesEnd := numAssets * 8
	pidOffsetsEnd := quantitiesEnd + numAssets*2
	nameOffsetsEnd := pidOffsetsEnd + numAssets*2

	if len(flat) < nameOffsetsEnd {
		return nil, fmt.Errorf(
			"multi-asset data too short: need %d, have %d",
			nameOffsetsEnd, len(flat),
		)
	}

	// Read all offsets and quantities in a first pass.
	type assetEntry struct {
		quantity uint64
		pidOff   int
		nameOff  int
	}
	entries := make([]assetEntry, numAssets)
	uniquePidOffs := make(map[int]struct{})
	for i := range numAssets {
		qOff := i * 8
		entries[i].quantity = binary.LittleEndian.Uint64(
			flat[qOff : qOff+8],
		)
		pidOffIdx := quantitiesEnd + i*2
		entries[i].pidOff = int(binary.LittleEndian.Uint16(
			flat[pidOffIdx : pidOffIdx+2],
		))
		nameOffIdx := pidOffsetsEnd + i*2
		entries[i].nameOff = int(binary.LittleEndian.Uint16(
			flat[nameOffIdx : nameOffIdx+2],
		))
		// Validate lower bound: policy IDs and asset names
		// reside after the offset regions (Region D starts
		// at nameOffsetsEnd).
		if entries[i].pidOff < nameOffsetsEnd {
			return nil, fmt.Errorf(
				"policy ID offset %d for asset %d "+
					"is before content region "+
					"(starts at %d)",
				entries[i].pidOff, i, nameOffsetsEnd,
			)
		}
		if entries[i].nameOff < nameOffsetsEnd {
			return nil, fmt.Errorf(
				"asset name offset %d for asset %d "+
					"is before content region "+
					"(starts at %d)",
				entries[i].nameOff, i, nameOffsetsEnd,
			)
		}
		uniquePidOffs[entries[i].pidOff] = struct{}{}
	}

	// Compute the sum of known name lengths from non-last
	// assets (bounded by adjacent name offsets). Offsets are
	// ascending since names are stored contiguously in entry
	// order.
	knownNameBytes := 0
	for i := range numAssets - 1 {
		nameLen := entries[i+1].nameOff - entries[i].nameOff
		if nameLen < 0 {
			return nil, fmt.Errorf(
				"multi-asset name offsets not ascending "+
					"at index %d: %d > %d",
				i, entries[i].nameOff,
				entries[i+1].nameOff,
			)
		}
		knownNameBytes += nameLen
	}

	// Compute the last asset's name length. The flat buffer
	// is a length-prefixed ShortByteString from MemPack with
	// NO trailing padding. The Haskell encoder (toCompact in
	// Value.hs) calculates repSize as the exact sum of all
	// regions, and the Haskell decoder uses SBS.length rep
	// as the end boundary for the last name. We do the same:
	// the last name extends from its offset to the end of
	// the buffer.
	numUniquePolicies := len(uniquePidOffs)
	base := nameOffsetsEnd + numUniquePolicies*28 + knownNameBytes
	if base > len(flat) {
		return nil, fmt.Errorf(
			"multi-asset buffer too short for content: "+
				"need at least %d bytes, have %d",
			base, len(flat),
		)
	}

	// Second validation pass: now that numUniquePolicies is
	// known, verify name offsets don't fall within the policy
	// ID region (Region D). Valid name offsets must point into
	// Region E, which starts after all policy IDs.
	regionEStart := nameOffsetsEnd + numUniquePolicies*28
	for i := range entries {
		if entries[i].nameOff < regionEStart {
			return nil, fmt.Errorf(
				"asset name offset %d for asset %d "+
					"falls within policy ID region "+
					"(names start at %d)",
				entries[i].nameOff, i, regionEStart,
			)
		}
	}

	namesContentEnd := len(flat)

	assets := make([]ParsedAsset, numAssets)

	for i := range numAssets {
		e := entries[i]

		// Extract policy ID (28 bytes at absolute offset)
		if e.pidOff+28 > len(flat) {
			return nil, fmt.Errorf(
				"policy ID offset %d out of bounds "+
					"(flat len %d)",
				e.pidOff, len(flat),
			)
		}
		policyId := make([]byte, 28)
		copy(policyId, flat[e.pidOff:e.pidOff+28])

		// Extract asset name from absolute offset. Names are
		// stored contiguously in entry order with ascending
		// offsets. Validate offset bounds for all assets
		// (not just those with non-zero length) to catch
		// corrupted offsets consistently.
		var assetName []byte
		if e.nameOff > len(flat) {
			return nil, fmt.Errorf(
				"asset name offset %d for asset %d "+
					"out of bounds (flat len %d)",
				e.nameOff, i, len(flat),
			)
		}
		if i+1 < numAssets {
			nameLen := entries[i+1].nameOff - e.nameOff
			nameEnd := e.nameOff + nameLen
			if nameEnd > len(flat) {
				return nil, fmt.Errorf(
					"asset name offset %d "+
						"(len %d) out of "+
						"bounds (flat len %d)",
					e.nameOff, nameLen,
					len(flat),
				)
			}
			if nameLen > 0 {
				assetName = make([]byte, nameLen)
				copy(
					assetName,
					flat[e.nameOff:nameEnd],
				)
			}
		} else {
			// Last asset: name extends to the end of
			// the buffer (no padding in the rep).
			nameLen := namesContentEnd - e.nameOff
			if nameLen > 0 {
				assetName = make([]byte, nameLen)
				copy(
					assetName,
					flat[e.nameOff:namesContentEnd],
				)
			}
		}

		assets[i] = ParsedAsset{
			PolicyId: policyId,
			Name:     assetName,
			Amount:   e.quantity,
		}
	}

	return assets, nil
}

// decodeMempackDatum decodes a MemPack Datum (era).
//
// Haskell Datum era MemPack instance:
//
//	Tag 0 = NoDatum
//	Tag 1 = DatumHash (+ SafeHash = 32 raw bytes)
//	Tag 2 = Datum (+ BinaryData = ShortByteString)
//
// Returns (datumHash, datumBytes, error).
func decodeMempackDatum(
	r *mempackReader,
) ([]byte, []byte, error) {
	tag, err := r.readTag()
	if err != nil {
		return nil, nil, fmt.Errorf(
			"reading Datum tag: %w", err,
		)
	}
	switch tag {
	case 0: // NoDatum
		return nil, nil, nil
	case 1: // DatumHash
		dh, err := r.readBytes(32)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"reading DatumHash: %w", err,
			)
		}
		return dh, nil, nil
	case 2: // Datum (BinaryData = ShortByteString)
		datum, err := r.readLengthPrefixedBytes()
		if err != nil {
			return nil, nil, fmt.Errorf(
				"reading inline Datum: %w", err,
			)
		}
		return nil, datum, nil
	default:
		return nil, nil, fmt.Errorf(
			"unknown Datum tag: %d", tag,
		)
	}
}

// AlonzoScript MemPack tags.
const (
	alonzoScriptNative = 0
	alonzoScriptPlutus = 1
)

// decodeMempackScript decodes a MemPack AlonzoScript (Script era).
//
// AlonzoScript MemPack instance (from Alonzo/Scripts.hs):
//
//	Tag 0 = NativeScript (Timelock): MemoBytes as ShortByteString
//	Tag 1 = PlutusScript: version tag + Plutus (ShortByteString)
//
// PlutusScript version tags (ConwayEra):
//
//	Tag 0 = PlutusV1
//	Tag 1 = PlutusV2
//	Tag 2 = PlutusV3
//
// NativeScript (Timelock) stores its CBOR via MemoBytes, which
// packs as a length-prefixed ShortByteString.
//
// PlutusScript stores the raw PlutusBinary via Plutus (newtype
// over PlutusBinary over ShortByteString), also length-prefixed.
func decodeMempackScript(
	r *mempackReader,
) ([]byte, error) {
	scriptTag, err := r.readTag()
	if err != nil {
		return nil, fmt.Errorf(
			"reading AlonzoScript tag: %w", err,
		)
	}
	switch scriptTag {
	case alonzoScriptNative:
		// NativeScript: MemoBytes packs as ShortByteString
		// (length-prefixed CBOR bytes)
		script, err := r.readLengthPrefixedBytes()
		if err != nil {
			return nil, fmt.Errorf(
				"reading NativeScript: %w", err,
			)
		}
		return script, nil
	case alonzoScriptPlutus:
		// PlutusScript: version tag + length-prefixed bytes
		versionTag, err := r.readTag()
		if err != nil {
			return nil, fmt.Errorf(
				"reading PlutusScript version tag: %w",
				err,
			)
		}
		if versionTag > 2 {
			return nil, fmt.Errorf(
				"unknown PlutusScript version tag: %d",
				versionTag,
			)
		}
		script, err := r.readLengthPrefixedBytes()
		if err != nil {
			return nil, fmt.Errorf(
				"reading PlutusScript V%d: %w",
				versionTag+1, err,
			)
		}
		return append(
			[]byte{byte(versionTag)}, script...,
		), nil
	default:
		return nil, fmt.Errorf(
			"unknown AlonzoScript tag: %d", scriptTag,
		)
	}
}

// reconstructAddr28 rebuilds a Shelley-format address from the
// Addr28Extra fields and staking credential.
//
// Addr28Extra in Haskell (4 * Word64 LE = 32 bytes):
//
//	Addr28Extra w0 w1 w2 w3
//
// The payment credential hash (28 bytes) is packed as big-endian
// Word64s into the PackedBytes28 structure:
//
//	PackedBytes28 w0 w1 w2 (fromIntegral (w3 >> 32))
//
// The low 32 bits of w3 contain metadata:
//   - bit 0: 1 = KeyHashObj, 0 = ScriptHashObj (payment cred type)
//   - bit 1: 1 = Mainnet, 0 = Testnet
//
// The staking credential (tag + 28-byte hash) is the first field
// in the BabbageTxOut.
//
// Address header byte format:
//
//	high nibble: address type (0-7)
//	low nibble: network id (0=testnet, 1=mainnet)
//
// Base address types (always with staking ref):
//
//	0x00: KeyHash payment + KeyHash staking
//	0x01: ScriptHash payment + KeyHash staking
//	0x02: KeyHash payment + ScriptHash staking
//	0x03: ScriptHash payment + ScriptHash staking
func reconstructAddr28(
	stakingCredTag byte, stakingHash []byte,
	w0, w1, w2, w3 uint64,
) []byte {
	// Extract payment credential hash from PackedBytes28.
	// In Haskell, encodeAddress28 puts the hash via
	// hashToPackedBytes -> PackedBytes28 a b c d where a,b,c
	// are Word64 and d is Word32. These are stored as:
	//   w0 = a (big-endian in PackedBytes, but stored as
	//          native Word64 in Addr28Extra)
	//   w1 = b
	//   w2 = c
	//   w3 high 32 bits = d (Word32)
	//   w3 low 32 bits = metadata
	paymentHash := make([]byte, 28)
	// PackedBytes28 writes Word64s in big-endian
	binary.BigEndian.PutUint64(paymentHash[0:8], w0)
	binary.BigEndian.PutUint64(paymentHash[8:16], w1)
	binary.BigEndian.PutUint64(paymentHash[16:24], w2)
	// #nosec G115
	binary.BigEndian.PutUint32(
		paymentHash[24:28], uint32(w3>>32),
	)

	// Extract metadata from low 32 bits of w3
	isPayKeyHash := (w3 & 1) != 0
	isMainnet := (w3 & 2) != 0

	// Determine network nibble
	var networkNibble byte
	if isMainnet {
		networkNibble = 1
	}

	// Determine address type from payment and staking cred types.
	// MemPack Credential tags: 0=ScriptHashObj, 1=KeyHashObj
	isStakingKeyHash := (stakingCredTag == 1)

	var addrType byte
	switch {
	case isPayKeyHash && isStakingKeyHash:
		addrType = 0x00
	case !isPayKeyHash && isStakingKeyHash:
		addrType = 0x01
	case isPayKeyHash && !isStakingKeyHash:
		addrType = 0x02
	case !isPayKeyHash && !isStakingKeyHash:
		addrType = 0x03
	}

	headerByte := (addrType << 4) | networkNibble

	// Build base address: header + 28 payment + 28 staking
	addr := make([]byte, 1+28+28)
	addr[0] = headerByte
	copy(addr[1:29], paymentHash)
	copy(addr[29:57], stakingHash)
	return addr
}

// isMempackFormat returns true if the byte slice starts with a
// MemPack TxOut tag (0-5) rather than a CBOR type marker.
// CBOR arrays start with 0x80-0x9f (major type 4) and CBOR maps
// with 0xa0-0xbf (major type 5). MemPack tags 0-5 fall in CBOR
// major type 0 (unsigned integer) range.
func isMempackFormat(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	return data[0] <= 5
}
