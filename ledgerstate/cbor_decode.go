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
				return nil, fmt.Errorf(
					"indefinite-length map exceeded "+
						"max entries (%d)",
					maxMapEntries,
				)
			}

			// Key
			keySize, err := cborItemSize(data[pos:])
			if err != nil {
				return nil, fmt.Errorf(
					"sizing map key at %d: %w",
					len(entries), err,
				)
			}
			keyRaw := data[pos : pos+keySize]
			pos += keySize

			if pos >= len(data) {
				return nil, errors.New(
					"indefinite-length map: truncated after key",
				)
			}

			// Value
			valSize, err := cborItemSize(data[pos:])
			if err != nil {
				return nil, fmt.Errorf(
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
		return nil, errors.New(
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
			return nil, fmt.Errorf(
				"sizing map key %d: %w", i, err,
			)
		}
		keyRaw := data[pos : pos+keySize]
		pos += keySize

		// Value
		valSize, err := cborItemSize(data[pos:])
		if err != nil {
			return nil, fmt.Errorf(
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

// maxCborDepth caps recursion in cborItemSize to prevent stack
// overflow from deeply nested CBOR (e.g., tag(tag(tag(...)))).
const maxCborDepth = 128

// cborItemSize returns the total byte size of one CBOR data item
// starting at data[0]. This parses the CBOR structure without
// decoding values, just calculating sizes.
func cborItemSize(data []byte) (int, error) {
	return cborItemSizeDepth(data, 0)
}

func cborItemSizeDepth(data []byte, depth int) (int, error) {
	if depth > maxCborDepth {
		return 0, errors.New("CBOR nesting exceeds max depth")
	}
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
				chunkSize, err := cborItemSizeDepth(
					data[pos:], depth+1,
				)
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
		// Subtract hLen (1-9) so that hLen + int(arg) cannot
		// wrap negative.
		if arg > uint64( //nolint:gosec // hLen is 1-9, subtraction is safe
			math.MaxInt-hLen,
		) {
			return 0, fmt.Errorf(
				"byte/text string length %d overflows int",
				arg,
			)
		}
		// #nosec G115
		size := hLen + int(arg)
		if size > len(data) {
			return 0, fmt.Errorf(
				"byte/text string claims %d bytes "+
					"but only %d available",
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
				itemSize, err := cborItemSizeDepth(
					data[pos:], depth+1,
				)
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
		if arg > uint64(math.MaxInt) { //nolint:gosec
			return 0, fmt.Errorf(
				"array length %d overflows int", arg,
			)
		}
		pos := hLen
		// #nosec G115
		for i := 0; i < int(arg); i++ {
			itemSize, err := cborItemSizeDepth(
				data[pos:], depth+1,
			)
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
				kSize, err := cborItemSizeDepth(
					data[pos:], depth+1,
				)
				if err != nil {
					return 0, err
				}
				pos += kSize
				vSize, err := cborItemSizeDepth(
					data[pos:], depth+1,
				)
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
		if arg > uint64(math.MaxInt) { //nolint:gosec
			return 0, fmt.Errorf(
				"map length %d overflows int", arg,
			)
		}
		pos := hLen
		// #nosec G115
		for i := 0; i < int(arg); i++ {
			kSize, err := cborItemSizeDepth(
				data[pos:], depth+1,
			)
			if err != nil {
				return 0, err
			}
			pos += kSize
			vSize, err := cborItemSizeDepth(
				data[pos:], depth+1,
			)
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
		contentSize, err := cborItemSizeDepth(
			data[hLen:], depth+1,
		)
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
		case 28, 29, 30:
			// RFC 8949 §3.3: additional info values
			// 28-30 are reserved and must not appear
			// in well-formed CBOR.
			return 0, fmt.Errorf(
				"reserved CBOR additional info "+
					"%d in major type 7",
				info,
			)
		case 31:
			// Break code (0xff) is only valid as a
			// terminator for indefinite-length
			// containers. Callers that handle
			// indefinite-length items check for 0xff
			// before calling cborItemSize.
			return 0, errors.New(
				"unexpected break code " +
					"(0xff) in CBOR item",
			)
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
// elements as raw byte slices indexed by key value. For maps,
// result[k] holds the value for key k, with nil entries for any
// gaps in sparse key sets (e.g., keys {0, 2} yield a length-3
// slice with result[1] == nil).
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

	if len(m) == 0 {
		return nil, nil
	}

	// Find the maximum key to size the result slice so that
	// result[k] corresponds to map key k.
	var maxKey uint64
	for k := range m {
		if k > maxKey {
			maxKey = k
		}
	}

	// Guard against unreasonably large keys that would cause
	// excessive memory allocation. Conway-era records have
	// fewer than 64 fields.
	const maxAllowedKey = 256
	if maxKey > maxAllowedKey {
		return nil, fmt.Errorf(
			"int-keyed map has key %d exceeding maximum %d",
			maxKey,
			maxAllowedKey,
		)
	}

	result := make([][]byte, maxKey+1)
	for k, v := range m {
		result[k] = []byte(v)
	}
	return result, nil
}

// MaxTelescopeDepth caps recursion in the nested telescope
// traversal. Cardano has 7 eras (Byron through Conway) so 16
// is generous while still preventing stack exhaustion from
// attacker-controlled CBOR.
const MaxTelescopeDepth = 16

// extractAllEraBounds extracts the start boundary (Slot, Epoch) of
// every era from the HardFork telescope CBOR. This is needed to
// reconstruct the full epoch history from genesis.
//
// Flat format: items[0..N-1] are era entries. Each is an array
// whose first element is the start Bound [RelativeTime, Slot, Epoch].
//
// Nested format: [tag, past_data, rest] recursively. We extract
// bounds from past era summaries and the current era.
func extractAllEraBounds(
	data cbor.RawMessage,
) ([]EraBound, error) {
	items, err := decodeRawArray(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding telescope for era bounds: %w",
			err,
		)
	}
	if len(items) < 2 {
		return nil, fmt.Errorf(
			"telescope has %d items, expected at least 2",
			len(items),
		)
	}

	// Detect format: try to decode first item as uint64 (tag).
	var firstTag uint64
	if _, err := cbor.Decode(items[0], &firstTag); err != nil {
		// Flat format
		return extractEraBoundsFlat(items)
	}

	// Nested format
	return extractEraBoundsNested(items, 0)
}

// extractEraBoundsFlat handles the flat telescope encoding. Each
// item is an array [start_bound, ...]. We parse the start bound
// of every entry.
func extractEraBoundsFlat(
	items [][]byte,
) ([]EraBound, error) {
	bounds := make([]EraBound, 0, len(items))
	for i, item := range items {
		entry, err := decodeRawArray(item)
		if err != nil {
			return nil, fmt.Errorf(
				"decoding era %d entry: %w",
				i, err,
			)
		}
		if len(entry) < 1 {
			return nil, fmt.Errorf(
				"era %d entry is empty", i,
			)
		}
		slot, epoch, err := parseBound(entry[0])
		if err != nil {
			return nil, fmt.Errorf(
				"parsing era %d bound: %w",
				i, err,
			)
		}
		bounds = append(bounds, EraBound{
			Slot:  slot,
			Epoch: epoch,
		})
	}
	return bounds, nil
}

// extractEraBoundsNested traverses the right-nested telescope
// encoding and collects era start bounds.
func extractEraBoundsNested(
	items [][]byte,
	eraIndex int,
) ([]EraBound, error) {
	return extractEraBoundsNestedDepth(
		items, eraIndex, 0,
	)
}

func extractEraBoundsNestedDepth(
	items [][]byte,
	eraIndex int,
	depth int,
) ([]EraBound, error) {
	if depth >= MaxTelescopeDepth {
		return nil, fmt.Errorf(
			"telescope recursion depth %d exceeds maximum %d",
			depth, MaxTelescopeDepth,
		)
	}
	if len(items) < 2 {
		return nil, fmt.Errorf(
			"telescope at era %s has %d items, "+
				"expected at least 2",
			EraName(eraIndex), len(items),
		)
	}

	var tag uint64
	if _, err := cbor.Decode(items[0], &tag); err != nil {
		return nil, fmt.Errorf(
			"decoding telescope tag at era %s: %w",
			EraName(eraIndex), err,
		)
	}

	switch tag {
	case 0:
		// Current era: items[1] is [Bound, state]
		current, err := decodeRawArray(items[1])
		if err != nil {
			return nil, fmt.Errorf(
				"decoding current era %s: %w",
				EraName(eraIndex), err,
			)
		}
		if len(current) < 1 {
			return nil, fmt.Errorf(
				"current era %s entry is empty",
				EraName(eraIndex),
			)
		}
		slot, epoch, err := parseBound(current[0])
		if err != nil {
			return nil, fmt.Errorf(
				"parsing current era %s bound: %w",
				EraName(eraIndex), err,
			)
		}
		return []EraBound{{Slot: slot, Epoch: epoch}}, nil

	case 1:
		// Past era: items[1] is summary with bounds
		if len(items) < 3 {
			return nil, fmt.Errorf(
				"past era %s has %d items, expected 3",
				EraName(eraIndex), len(items),
			)
		}
		// Past era summary = [startBound, endBound, eraParams]
		summary, err := decodeRawArray(items[1])
		if err != nil {
			return nil, fmt.Errorf(
				"decoding past era %s summary: %w",
				EraName(eraIndex), err,
			)
		}
		if len(summary) == 0 {
			return nil, fmt.Errorf(
				"past era %s summary is empty",
				EraName(eraIndex),
			)
		}
		slot, epoch, bErr := parseBound(summary[0])
		if bErr != nil {
			return nil, fmt.Errorf(
				"parsing past era %s bound: %w",
				EraName(eraIndex), bErr,
			)
		}
		bound := EraBound{Slot: slot, Epoch: epoch}

		// Continue to rest
		rest, err := decodeRawArray(items[2])
		if err != nil {
			return nil, fmt.Errorf(
				"decoding telescope rest at era %s: %w",
				EraName(eraIndex), err,
			)
		}
		restBounds, err := extractEraBoundsNestedDepth(
			rest, eraIndex+1, depth+1,
		)
		if err != nil {
			return nil, err
		}
		return append(
			[]EraBound{bound}, restBounds...,
		), nil

	default:
		return nil, fmt.Errorf(
			"unexpected telescope tag %d at era %s",
			tag, EraName(eraIndex),
		)
	}
}

// Era name constants for logging and error messages.
const (
	EraByron = iota
	EraShelley
	EraAllegra
	EraMary
	EraAlonzo
	EraBabbage
	EraConway
)

// EraName returns the human-readable name for an era index.
func EraName(eraIndex int) string {
	switch eraIndex {
	case EraByron:
		return "Byron"
	case EraShelley:
		return "Shelley"
	case EraAllegra:
		return "Allegra"
	case EraMary:
		return "Mary"
	case EraAlonzo:
		return "Alonzo"
	case EraBabbage:
		return "Babbage"
	case EraConway:
		return "Conway"
	default:
		return fmt.Sprintf("Unknown(%d)", eraIndex)
	}
}

// navigateTelescope traverses the HardFork telescope CBOR structure
// to find the current era's ledger state.
//
// Two telescope encodings are supported:
//
// Flat (UTxO-HD / node 10.x+):
//
//	array(N) = [[bound, bound], ..., [bound, <current-state>]]
//
// Each past era entry is [start_bound, end_bound]. The last entry
// is the current era: [start_bound, era_state].
//
// Nested (legacy .lstate):
//
//	[1, <past>, [1, <past>, [..., [0, <current>]]]]
//
// Each level uses tag 0 for the current era and tag 1 for past.
func navigateTelescope(
	data cbor.RawMessage,
) (int, []byte, error) {
	items, err := decodeRawArray(data)
	if err != nil {
		return 0, nil, fmt.Errorf(
			"decoding telescope: %w",
			err,
		)
	}

	if len(items) < 2 {
		return 0, nil, fmt.Errorf(
			"telescope has %d items, expected at least 2",
			len(items),
		)
	}

	// Detect format: try to decode first item as uint64 (tag).
	// In nested format, items[0] is a tag integer (0 or 1).
	// In flat format, items[0] is an array [bound, payload].
	var firstTag uint64
	if _, err := cbor.Decode(items[0], &firstTag); err != nil {
		// Flat format: each item is [bound, payload]
		return navigateTelescopeFlat(items)
	}

	// Nested format: [tag, payload, rest] or [tag, payload]
	return navigateTelescopeNested(items, 0)
}

// navigateTelescopeFlat handles the flat telescope encoding where
// the outer array contains one entry per era. Past eras are
// [start_bound, end_bound] and the current era (always last) is
// [start_bound, era_state].
//
// Returns the raw bytes of the current era entry [Bound, state],
// matching the nested format's return value for parseCurrentEra.
func navigateTelescopeFlat(
	items [][]byte,
) (int, []byte, error) {
	// The current era is always the last entry in the flat
	// telescope. Return the raw entry bytes directly — the
	// caller (parseCurrentEra) will decode [Bound, state].
	eraIndex := len(items) - 1
	return eraIndex, items[eraIndex], nil
}

// navigateTelescopeNested recursively traverses the right-nested
// telescope encoding.
func navigateTelescopeNested(
	items [][]byte,
	eraIndex int,
) (int, []byte, error) {
	return navigateTelescopeNestedDepth(
		items, eraIndex, 0,
	)
}

func navigateTelescopeNestedDepth(
	items [][]byte,
	eraIndex int,
	depth int,
) (int, []byte, error) {
	if depth >= MaxTelescopeDepth {
		return 0, nil, fmt.Errorf(
			"telescope recursion depth %d exceeds maximum %d",
			depth, MaxTelescopeDepth,
		)
	}
	if len(items) < 2 {
		return 0, nil, fmt.Errorf(
			"telescope at era %s has %d items, "+
				"expected at least 2",
			EraName(eraIndex), len(items),
		)
	}

	// Decode the tag (0 = Current, 1 = Past)
	var tag uint64
	if _, err := cbor.Decode(items[0], &tag); err != nil {
		return 0, nil, fmt.Errorf(
			"decoding telescope tag at era %s: %w",
			EraName(eraIndex),
			err,
		)
	}

	switch tag {
	case 0:
		// Current era found — items[1] is safe due to
		// the length check at the top of this function.
		return eraIndex, items[1], nil
	case 1:
		// Past era, continue to the rest
		if len(items) < 3 {
			return 0, nil, fmt.Errorf(
				"past era %s has %d items, expected 3",
				EraName(eraIndex),
				len(items),
			)
		}
		// Decode the nested rest element
		rest, err := decodeRawArray(items[2])
		if err != nil {
			return 0, nil, fmt.Errorf(
				"decoding telescope rest at era %s: %w",
				EraName(eraIndex),
				err,
			)
		}
		return navigateTelescopeNestedDepth(
			rest, eraIndex+1, depth+1,
		)
	default:
		return 0, nil, fmt.Errorf(
			"unexpected telescope tag %d at era %s",
			tag,
			EraName(eraIndex),
		)
	}
}
