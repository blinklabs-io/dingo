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

package ledger

import (
	"bytes"
	"errors"
	"unicode/utf8"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// memUtxoSet is an in-memory live UTxO set with the ledger's stake
// classification.
type memUtxoSet struct{ outputs map[string][]byte }

func (m *memUtxoSet) key(txID []byte, index uint32) string {
	return string(txID) + string(rune(index))
}

func (m *memUtxoSet) ApplyUtxoChanges(
	_ *ocommon.Point,
	changes []UtxoEvent,
) error {
	for _, c := range changes {
		for _, r := range c.Removed {
			delete(m.outputs, m.key(r.Hash, r.Idx))
		}
		for _, o := range c.Added {
			m.outputs[m.key(o.Ref.Hash, o.Ref.Idx)] = o.Cbor
		}
	}
	return nil
}

func (m *memUtxoSet) Get(txID []byte, index uint32) ([]byte, bool, error) {
	c, ok := m.outputs[m.key(txID, index)]
	return c, ok, nil
}

func (m *memUtxoSet) EachUtxo(
	fn func(txID []byte, index uint32, cbor []byte) error,
) error {
	for key, output := range m.outputs {
		index, size := utf8.DecodeLastRuneInString(key)
		if size == 0 || size == 1 && index == utf8.RuneError {
			return errors.New("invalid in-memory UTxO key")
		}
		if err := fn([]byte(key[:len(key)-size]), uint32(index), output); err != nil {
			return err
		}
	}
	return nil
}

func (m *memUtxoSet) Sum(key []byte) (uint64, error) {
	var n uint64
	for _, c := range m.outputs {
		if k, l, ok := UtxoStakeOf(c); ok && bytes.Equal(k, key) {
			n += l
		}
	}
	return n, nil
}

func (m *memUtxoSet) EachSum(fn func([]byte, uint64) error) error {
	sums := map[string]uint64{}
	for _, c := range m.outputs {
		if k, l, ok := UtxoStakeOf(c); ok {
			sums[string(k)] += l
		}
	}
	for k, n := range sums {
		if err := fn([]byte(k), n); err != nil {
			return err
		}
	}
	return nil
}

func (m *memUtxoSet) RollbackUtxoSet(ocommon.Point) (UtxoEvent, error) {
	return UtxoEvent{}, errors.New("unused")
}

func (m *memUtxoSet) PreviewUtxoSetRollback(ocommon.Point) (UtxoEvent, error) {
	return UtxoEvent{}, errors.New("unused")
}
