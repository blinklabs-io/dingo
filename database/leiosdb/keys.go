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

package leiosdb

import (
	"encoding/binary"
	"errors"
)

// Key layout. Every record key is a partition byte, the slot as eight
// big-endian bytes, the hash, and a record-kind byte, so a partition iterates
// in slot order and a slot range is a contiguous key range.
const (
	partVolatile  = 'v'
	partImmutable = 'i'
	partPending   = 'q'

	kindManifest = 'm'
	kindTxs      = 't'
	kindClosure  = 'c'
	kindPin      = 'p'
)

var (
	keyPinnedThrough = []byte("wp")
	keyGCBefore      = []byte("wg")
	keyMigration     = []byte("M")
)

func recordKey(part byte, eb Ref, kind byte) []byte {
	key := make([]byte, 0, 1+8+len(eb.Hash)+1)
	key = append(key, part)
	key = binary.BigEndian.AppendUint64(key, eb.Slot)
	key = append(key, eb.Hash...)
	key = append(key, kind)
	return key
}

func pendingKey(eb Ref) []byte {
	key := make([]byte, 0, 1+8+len(eb.Hash))
	key = append(key, partPending)
	key = binary.BigEndian.AppendUint64(key, eb.Slot)
	key = append(key, eb.Hash...)
	return key
}

// slotPrefix is the partition byte and slot, the start of every key for that
// slot.
func slotPrefix(part byte, slot uint64) []byte {
	key := make([]byte, 0, 9)
	key = append(key, part)
	return binary.BigEndian.AppendUint64(key, slot)
}

var errMalformedKey = errors.New("leiosdb: malformed key")

// parseRecordKey splits a partition record key into its EB reference and kind.
func parseRecordKey(key []byte) (eb Ref, kind byte, err error) {
	if len(key) < 1+8+1+1 {
		return Ref{}, 0, errMalformedKey
	}
	eb.Slot = binary.BigEndian.Uint64(key[1:9])
	eb.Hash = append([]byte(nil), key[9:len(key)-1]...)
	return eb, key[len(key)-1], nil
}

func encodeRef(ref Ref) []byte {
	out := make([]byte, 0, 8+len(ref.Hash))
	out = binary.BigEndian.AppendUint64(out, ref.Slot)
	return append(out, ref.Hash...)
}

func decodeRef(data []byte) (Ref, error) {
	if len(data) < 8 {
		return Ref{}, errMalformedKey
	}
	return Ref{
		Slot: binary.BigEndian.Uint64(data[:8]),
		Hash: append([]byte(nil), data[8:]...),
	}, nil
}

func isZeroRef(ref Ref) bool {
	return ref.Slot == 0 && len(ref.Hash) == 0
}
