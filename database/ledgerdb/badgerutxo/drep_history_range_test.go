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

// A ranged read of the DRep history seeks to its first slot and stops after
// its last: the rows outside the range are not stepped over.
func TestRowsInRangeSeeksIntoTheDRepHistory(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	const slots = 2000
	require.NoError(t, s.LoadDRepHistory(func(
		put func(k, v []byte) error,
	) error {
		for slot := uint64(1); slot <= slots; slot++ {
			if err := put(badgerutxo.DRepExpiryEventKey(slot), nil); err != nil {
				return err
			}
		}
		return nil
	}))

	prefix := []byte{badgerutxo.DRepHistoryTag}
	slotKey := func(slot uint64) []byte {
		return binary.BigEndian.AppendUint64(bytes.Clone(prefix), slot)
	}
	before := s.RangeRowsVisited()
	var got []uint64
	require.NoError(t, s.RowsInRange(
		prefix, slotKey(1000), slotKey(1011),
		func(k, _ []byte) error {
			got = append(got, binary.BigEndian.Uint64(k[1:9]))
			return nil
		},
	))
	require.Len(t, got, 11)
	require.Equal(t, uint64(1000), got[0])
	require.Equal(t, uint64(1010), got[10])
	require.LessOrEqual(t, s.RangeRowsVisited()-before, uint64(12),
		"the range and the row that ends it, not the history before it")
}
