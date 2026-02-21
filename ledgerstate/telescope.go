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
	"fmt"

	"github.com/blinklabs-io/gouroboros/cbor"
)

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
