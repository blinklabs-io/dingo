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
	"cmp"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

// A page of accounts costs the rows it returns: resuming after a key deep in a
// large set steps over about one page, and the pages together are every
// account once, in the order the export compares keys by (tag, key).
func TestAccountsAfterSeeksAndKeepsTheExportKeyOrder(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))

	// Keys of different lengths under both tags, so the order that matters
	// is the one Badger sorts and the export compares: tag, then key bytes.
	var want []badgerutxo.AccountRow
	for tag := range uint8(2) {
		for i := range 600 {
			key := []byte{byte(i >> 8), byte(i)}
			if i%7 == 0 {
				key = append(key, 0x00)
			}
			want = append(want, badgerutxo.AccountRow{Tag: tag, Key: key, Active: true})
		}
	}
	require.NoError(t, s.LoadAccounts(func(put func(badgerutxo.AccountRow) error) error {
		for _, r := range want {
			if err := put(r); err != nil {
				return err
			}
		}
		return nil
	}))
	slices.SortFunc(want, func(a, b badgerutxo.AccountRow) int {
		return cmp.Or(cmp.Compare(a.Tag, b.Tag), bytes.Compare(a.Key, b.Key))
	})

	const limit = 100
	var got []badgerutxo.AccountRow
	var after []byte
	var maxVisited uint64
	for {
		before := s.RangeRowsVisited()
		n := 0
		var last []byte
		require.NoError(t, s.AccountsAfter(after, func(r badgerutxo.AccountRow, key []byte) error {
			got = append(got, r)
			last = key
			n++
			if n >= limit {
				return badgerutxo.ErrStopRows
			}
			return nil
		}))
		maxVisited = max(maxVisited, s.RangeRowsVisited()-before)
		if n < limit {
			break
		}
		after = last
	}
	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, want[i].Tag, got[i].Tag, "row %d", i)
		require.Equal(t, want[i].Key, got[i].Key, "row %d", i)
	}
	require.LessOrEqual(t, maxVisited, uint64(limit+1),
		"a page steps over its own rows, not the accounts before its cursor")
}
