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
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/blinklabs-io/gouroboros/cbor"
)

// decodeRawArray decodes a CBOR array, returning elements as raw
// byte slices. Each element's raw CBOR is preserved for deferred
// decoding.
func decodeRawArray(data []byte) ([][]byte, error) {
	var items []cbor.RawMessage
	if _, err := cbor.Decode(data, &items); err != nil {
		return nil, fmt.Errorf("decoding CBOR array: %w", err)
	}
	result := make([][]byte, len(items))
	for i, item := range items {
		result[i] = []byte(item)
	}
	return result, nil
}

// maxMapEntries limits the number of entries in a single CBOR map
// to prevent unbounded memory allocation from malformed data.
// Cardano mainnet stake maps can exceed 1M entries.
const maxMapEntries = 10_000_000

// MapEntry holds a raw key-value pair from a CBOR map.
type MapEntry struct {
	KeyRaw   []byte
	ValueRaw []byte
}

// decodeMapEntries manually parses a CBOR map (definite or
// indefinite length) by calculating the byte size of each CBOR
// item directly. This handles indefinite-length maps (0xbf prefix)
// and non-comparable keys (arrays, e.g. Cardano credentials).
func decodeMapEntries(data []byte) ([]MapEntry, error) {
	if len(data) == 0 {
		return nil, errors.New("empty data")
	}

	first := data[0]

	if first == 0xbf {
		// Indefinite-length map: skip 0xbf, read key-value
		// pairs until 0xff break code.
		pos := 1
		var entries []MapEntry
		for pos < len(data) {
			if data[pos] == 0xff {
				return entries, nil
			}
			if len(entries) >= maxMapEntries {
				return entries, fmt.Errorf(
					"indefinite-length map exceeded "+
						"max entries (%d)",
					maxMapEntries,
				)
			}

			// Key
			keySize, err := cborItemSize(data[pos:])
			if err != nil {
				return entries, fmt.Errorf(
					"sizing map key at %d: %w",
					len(entries), err,
				)
			}
			keyRaw := data[pos : pos+keySize]
			pos += keySize

			if pos >= len(data) {
				return entries, errors.New(
					"indefinite-length map: truncated after key",
				)
			}

			// Value
			valSize, err := cborItemSize(data[pos:])
			if err != nil {
				return entries, fmt.Errorf(
					"sizing map value at %d: %w",
					len(entries), err,
				)
			}
			valRaw := data[pos : pos+valSize]
			pos += valSize

			entries = append(entries, MapEntry{
				KeyRaw:   keyRaw,
				ValueRaw: valRaw,
			})
		}
		return entries, errors.New(
			"indefinite-length map: break code not found (truncated data)",
		)
	}

	// Definite-length map
	major := first >> 5
	if major != 5 {
		return nil, fmt.Errorf(
			"expected map, got major type %d", major,
		)
	}

	// Read the map header to get the count
	count, headerLen, err := cborArgument(data)
	if err != nil {
		return nil, fmt.Errorf("reading map header: %w", err)
	}

	if count > maxMapEntries {
		return nil, fmt.Errorf(
			"definite-length map claims %d entries, "+
				"exceeds max (%d)",
			count, maxMapEntries,
		)
	}
	entries := make([]MapEntry, 0, count)
	pos := headerLen
	// #nosec G115
	for i := 0; i < int(count); i++ {
		// Key
		keySize, err := cborItemSize(data[pos:])
		if err != nil {
			return entries, fmt.Errorf(
				"sizing map key %d: %w", i, err,
			)
		}
		keyRaw := data[pos : pos+keySize]
		pos += keySize

		// Value
		valSize, err := cborItemSize(data[pos:])
		if err != nil {
			return entries, fmt.Errorf(
				"sizing map value %d: %w", i, err,
			)
		}
		valRaw := data[pos : pos+valSize]
		pos += valSize

		entries = append(entries, MapEntry{
			KeyRaw:   keyRaw,
			ValueRaw: valRaw,
		})
	}

	return entries, nil
}

// cborArgument reads the argument (count/length) from a CBOR
// header byte. Returns (argument, headerLength, error).
func cborArgument(data []byte) (uint64, int, error) {
	if len(data) == 0 {
		return 0, 0, errors.New("empty data")
	}
	info := data[0] & 0x1f
	switch {
	case info < 24:
		return uint64(info), 1, nil
	case info == 24:
		if len(data) < 2 {
			return 0, 0, errors.New("truncated header")
		}
		return uint64(data[1]), 2, nil
	case info == 25:
		if len(data) < 3 {
			return 0, 0, errors.New("truncated header")
		}
		return uint64(data[1])<<8 | uint64(data[2]), 3, nil
	case info == 26:
		if len(data) < 5 {
			return 0, 0, errors.New("truncated header")
		}
		return uint64(data[1])<<24 | uint64(data[2])<<16 |
			uint64(data[3])<<8 | uint64(data[4]), 5, nil
	case info == 27:
		if len(data) < 9 {
			return 0, 0, errors.New("truncated header")
		}
		return uint64(data[1])<<56 | uint64(data[2])<<48 |
			uint64(data[3])<<40 | uint64(data[4])<<32 |
			uint64(data[5])<<24 | uint64(data[6])<<16 |
			uint64(data[7])<<8 | uint64(data[8]), 9, nil
	default:
		return 0, 0, fmt.Errorf(
			"unsupported additional info: %d", info,
		)
	}
}

// cborItemSize returns the total byte size of one CBOR data item
// starting at data[0]. This parses the CBOR structure without
// decoding values, just calculating sizes.
func cborItemSize(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, errors.New("empty data")
	}

	first := data[0]
	major := first >> 5
	info := first & 0x1f

	switch major {
	case 0, 1: // unsigned int, negative int
		_, hLen, err := cborArgument(data)
		if err != nil {
			return 0, err
		}
		return hLen, nil

	case 2, 3: // byte string, text string
		if info == 31 {
			// Indefinite-length string: chunks until break
			pos := 1
			for pos < len(data) {
				if data[pos] == 0xff {
					return pos + 1, nil
				}
				chunkSize, err := cborItemSize(data[pos:])
				if err != nil {
					return 0, err
				}
				pos += chunkSize
			}
			return 0, errors.New("unterminated indef string")
		}
		arg, hLen, err := cborArgument(data)
		if err != nil {
			return 0, err
		}
		// Guard against integer overflow: arg is untrusted input
		// and could exceed MaxInt when cast to int below.
		// hLen is always small (1-9) so subtracting it is safe.
		if arg > uint64(math.MaxInt) { //nolint:gosec // MaxInt is always positive
			return 0, fmt.Errorf(
				"byte/text string length %d overflows int",
				arg,
			)
		}
		// #nosec G115
		size := hLen + int(arg)
		if size > len(data) {
			return 0, fmt.Errorf(
				"byte/text string claims %d bytes but only %d available",
				size, len(data),
			)
		}
		return size, nil

	case 4: // array
		if info == 31 {
			// Indefinite-length array
			pos := 1
			for pos < len(data) {
				if data[pos] == 0xff {
					return pos + 1, nil
				}
				itemSize, err := cborItemSize(data[pos:])
				if err != nil {
					return 0, err
				}
				pos += itemSize
			}
			return 0, errors.New("unterminated indef array")
		}
		arg, hLen, err := cborArgument(data)
		if err != nil {
			return 0, err
		}
		pos := hLen
		// #nosec G115
		for i := 0; i < int(arg); i++ {
			itemSize, err := cborItemSize(data[pos:])
			if err != nil {
				return 0, err
			}
			pos += itemSize
		}
		return pos, nil

	case 5: // map
		if info == 31 {
			// Indefinite-length map
			pos := 1
			for pos < len(data) {
				if data[pos] == 0xff {
					return pos + 1, nil
				}
				kSize, err := cborItemSize(data[pos:])
				if err != nil {
					return 0, err
				}
				pos += kSize
				vSize, err := cborItemSize(data[pos:])
				if err != nil {
					return 0, err
				}
				pos += vSize
			}
			return 0, errors.New("unterminated indef map")
		}
		arg, hLen, err := cborArgument(data)
		if err != nil {
			return 0, err
		}
		pos := hLen
		// #nosec G115
		for i := 0; i < int(arg); i++ {
			kSize, err := cborItemSize(data[pos:])
			if err != nil {
				return 0, err
			}
			pos += kSize
			vSize, err := cborItemSize(data[pos:])
			if err != nil {
				return 0, err
			}
			pos += vSize
		}
		return pos, nil

	case 6: // tag
		_, hLen, err := cborArgument(data)
		if err != nil {
			return 0, err
		}
		contentSize, err := cborItemSize(data[hLen:])
		if err != nil {
			return 0, err
		}
		return hLen + contentSize, nil

	case 7: // simple values, floats, break
		switch info {
		case 24:
			if len(data) < 2 {
				return 0, errors.New(
					"truncated simple value",
				)
			}
			return 2, nil // 1-byte simple value
		case 25:
			if len(data) < 3 {
				return 0, errors.New(
					"truncated float16",
				)
			}
			return 3, nil // float16
		case 26:
			if len(data) < 5 {
				return 0, errors.New(
					"truncated float32",
				)
			}
			return 5, nil // float32
		case 27:
			if len(data) < 9 {
				return 0, errors.New(
					"truncated float64",
				)
			}
			return 9, nil // float64
		case 31:
			return 1, nil // break code
		default:
			return 1, nil // simple value 0-23
		}

	default:
		return 0, fmt.Errorf(
			"unknown CBOR major type %d", major,
		)
	}
}

// decodeRawElements decodes CBOR data that may be either an array
// or a map with integer keys (Conway-era record encoding). Returns
// elements as raw byte slices in positional order. For maps, values
// are returned sorted by their integer key (0, 1, 2, ...).
func decodeRawElements(data []byte) ([][]byte, error) {
	// Try array first
	items, err := decodeRawArray(data)
	if err == nil {
		return items, nil
	}

	// Try map with integer keys (Conway record encoding)
	var m map[uint64]cbor.RawMessage
	if _, mapErr := cbor.Decode(data, &m); mapErr != nil {
		return nil, fmt.Errorf(
			"not an array (%w) or int-keyed map (%w)",
			err,
			mapErr,
		)
	}

	// Sort keys and return values in order
	keys := make([]uint64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	result := make([][]byte, len(keys))
	for i, k := range keys {
		result[i] = []byte(m[k])
	}
	return result, nil
}
