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

// Package txsize implements the transaction-size measure shared by ledger
// validation and raw mempool admission.
package txsize

import (
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
)

// FromCBOR returns the Cardano ledger size of a transaction from its exact
// CBOR bytes. Shelley through Dijkstra are measured from their components, so
// the outer array framing does not contribute. Malformed or unrecognized
// envelopes retain their full size.
func FromCBOR(txType uint, data []byte) uint64 {
	fullSize := uint64(len(data))
	switch {
	case txType >= uint(ledger.TxTypeShelley) &&
		txType <= uint(ledger.TxTypeMary):
		if size, ok := shelleyFamily(data); ok {
			return size
		}
	case txType >= uint(ledger.TxTypeAlonzo) &&
		txType <= uint(ledger.TxTypeConway):
		if size, ok := alonzoFamily(data); ok {
			return size
		}
	case txType == uint(ledger.TxTypeDijkstra):
		if size, ok := dijkstra(data); ok {
			return size
		}
	}
	return fullSize
}

// shelleyFamily measures a Shelley, Allegra or Mary transaction as
// toCBORForSizeComputation encodes it (cardano-ledger Shelley/Tx.hs,
// sizeShelleyTxF; Allegra and Mary set sizeTxF to it): a one-byte array
// header, the body, the witness set and the auxiliary data, which is one null
// byte when absent. The received outer framing does not contribute. It reports
// false unless data is exactly one three-component array.
func shelleyFamily(data []byte) (uint64, bool) {
	components, ok := decodeComponents(data)
	if !ok || len(components) != 3 {
		return 0, false
	}
	return canonicalSize(components...), true
}

func alonzoFamily(data []byte) (uint64, bool) {
	components, ok := decodeComponents(data)
	if !ok {
		return 0, false
	}
	if len(components) != 3 && len(components) != 4 {
		return 0, false
	}
	auxiliaryDataIndex := 2
	if len(components) == 3 {
		if isBool(components[2]) {
			return 0, false
		}
	} else {
		isValid := components[2]
		if len(isValid) != 1 ||
			(isValid[0] != 0xf4 && isValid[0] != 0xf5) {
			return 0, false
		}
		auxiliaryDataIndex = 3
	}
	return canonicalSize(
		components[0], components[1], components[auxiliaryDataIndex],
	), true
}

func dijkstra(data []byte) (uint64, bool) {
	components, ok := decodeComponents(data)
	if !ok {
		return 0, false
	}
	switch len(components) {
	case 3:
		if isBool(components[2]) {
			return 0, false
		}
		return canonicalSize(components...), true
	case 4:
		thirdIsBool := isBool(components[2])
		fourthIsBool := isBool(components[3])
		switch {
		case isTrue(components[2]) && !fourthIsBool:
			// Legacy mempool order: [body, witnesses, true, auxiliary].
			return canonicalSize(
				components[0], components[1], components[3],
			), true
		case !thirdIsBool && fourthIsBool:
			// Block order: [body, witnesses, auxiliary, isValid].
			return canonicalSize(
				components[0], components[1], components[2],
			), true
		}
	}
	return 0, false
}

// maxComponents is the most components any measured envelope has.
const maxComponents = 4

// decodeComponents splits data, which must be exactly one CBOR array of at
// most maxComponents items, into each item's span of the received bytes.
// Decoding the array into RawMessage values instead would drop a tag in front
// of an item (the self-describe tag 55799), and the reference counts a
// component as the bytes it was decoded from. A tag around the whole array is
// not an array, so it is reported as unrecognized.
func decodeComponents(data []byte) ([]cbor.RawMessage, bool) {
	if len(data) == 0 || data[0]>>5 != 4 {
		return nil, false
	}
	count, offset, indefinite, ok := arrayHeader(data)
	if !ok || (!indefinite && count > maxComponents) {
		return nil, false
	}
	var components []cbor.RawMessage
	for {
		if indefinite {
			if offset >= len(data) {
				return nil, false
			}
			if data[offset] == 0xff {
				offset++
				break
			}
			if len(components) == maxComponents {
				return nil, false
			}
		} else if uint64(len(components)) == count {
			break
		}
		var item cbor.RawMessage
		bytesRead, err := cbor.Decode(data[offset:], &item)
		if err != nil || bytesRead <= 0 {
			return nil, false
		}
		components = append(components, data[offset:offset+bytesRead])
		offset += bytesRead
	}
	if offset != len(data) {
		return nil, false
	}
	return components, true
}

// arrayHeader reads the header of the array that data starts with: its
// element count, the offset of its first element, and whether it is
// indefinite-length.
func arrayHeader(data []byte) (uint64, int, bool, bool) {
	info := data[0] & 0x1f
	switch {
	case info < 24:
		return uint64(info), 1, false, true
	case info == 31:
		return 0, 1, true, true
	case info > 27:
		return 0, 0, false, false
	}
	width := 1 << (info - 24)
	if len(data) < 1+width {
		return 0, 0, false, false
	}
	var count uint64
	for _, b := range data[1 : 1+width] {
		count = count<<8 | uint64(b)
	}
	return count, 1 + width, false, true
}

func canonicalSize(components ...cbor.RawMessage) uint64 {
	size := 1
	for _, component := range components {
		size += len(component)
	}
	return uint64(size) // #nosec G115 -- size is a sum of byte lengths
}

func isBool(data cbor.RawMessage) bool {
	return len(data) == 1 && (data[0] == 0xf4 || data[0] == 0xf5)
}

func isTrue(data cbor.RawMessage) bool {
	return len(data) == 1 && data[0] == 0xf5
}
