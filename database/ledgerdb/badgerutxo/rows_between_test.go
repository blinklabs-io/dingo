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

package badgerutxo_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

func collectRowsAt(
	t *testing.T, s *badgerutxo.Store, prefix, from, to []byte, slot uint64,
) map[string]byte {
	t.Helper()
	got := map[string]byte{}
	var last []byte
	require.NoError(t, s.RowsAtBetween(prefix, from, to, slot,
		func(k, v []byte) error {
			require.Less(t, bytes.Compare(last, k), 0, "key order")
			last = bytes.Clone(k)
			got[string(k)] = v[0]
			return nil
		}))
	return got
}

// A bounded read as of a slot is the unbounded read restricted to the
// bounds, for rows changed, added and removed after the slot and for rows
// under a tombstone.
func TestRowsAtBetweenIsTheRowsAtRestrictedToTheRange(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(0, 0))
	key := func(prefix byte, i uint32) []byte {
		return binary.BigEndian.AppendUint32([]byte{'Z', prefix}, i)
	}
	mid, tip := point(10, 0x0a), point(20, 0x14)
	var base []badgerutxo.RowPut
	for i := range uint32(40) {
		base = append(base, badgerutxo.RowPut{Slot: 10, Key: key(1, i), Value: []byte{1}})
	}
	_, err := s.ApplyLedgerBatch(&mid, badgerutxo.LedgerBatch{Rows: base})
	require.NoError(t, err)
	var later []badgerutxo.RowPut
	for i := uint32(0); i < 40; i += 3 {
		later = append(later, badgerutxo.RowPut{Slot: 20, Key: key(1, i), Value: []byte{2}})
	}
	for i := uint32(40); i < 50; i++ {
		later = append(later, badgerutxo.RowPut{Slot: 20, Key: key(1, i), Value: []byte{3}})
	}
	for i := uint32(1); i < 40; i += 5 {
		later = append(later, badgerutxo.RowPut{Slot: 20, Key: key(1, i), Delete: true})
	}
	later = append(later, bulkRows(20, 2, 30, 4)...)
	_, err = s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{Rows: later})
	require.NoError(t, err)

	for _, prefix := range [][]byte{{'Z', 1}, {'Z', 2}} {
		for _, b := range [][2][]byte{
			{nil, nil},
			{key(prefix[1], 10), key(prefix[1], 20)},
			{key(prefix[1], 35), nil},
			{nil, key(prefix[1], 5)},
			{key(prefix[1], 45), key(prefix[1], 60)},
		} {
			for _, slot := range []uint64{10, 20} {
				want := map[string]byte{}
				for k, v := range collectRowsAt(t, s, prefix, nil, nil, slot) {
					if (b[0] == nil || bytes.Compare([]byte(k), b[0]) >= 0) &&
						(b[1] == nil || bytes.Compare([]byte(k), b[1]) < 0) {
						want[k] = v
					}
				}
				require.Equal(
					t, want, collectRowsAt(t, s, prefix, b[0], b[1], slot),
					"prefix %x bounds %x slot %d", prefix, b, slot,
				)
			}
		}
	}
	require.Equal(t, []byte{0, 2}, badgerutxo.KeyAfter([]byte{0, 1}))
	require.Equal(t, []byte{1}, badgerutxo.KeyAfter([]byte{0, 0xff}))
	require.Nil(t, badgerutxo.KeyAfter([]byte{0xff, 0xff}))
}
