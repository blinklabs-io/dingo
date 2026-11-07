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
// CBOR bytes. Malformed or unrecognized envelopes retain their full size.
func FromCBOR(txType uint, data []byte) uint64 {
	fullSize := uint64(len(data))
	switch {
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

func decodeComponents(data []byte) ([]cbor.RawMessage, bool) {
	var components []cbor.RawMessage
	bytesRead, err := cbor.Decode(data, &components)
	if err != nil || bytesRead != len(data) {
		return nil, false
	}
	return components, true
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
