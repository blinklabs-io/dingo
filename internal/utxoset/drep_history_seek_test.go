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

package utxoset

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// A page of DRep history for a slot range costs the rows it steps over in that
// range, not the history before or after it: pages from a key cover the range
// once, and each steps over about its own rows.
func TestDRepHistoryPagesSeekWithinTheirSlotRange(t *testing.T) {
	t.Parallel()
	set, err := Open(Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	require.NoError(t, set.store.Bootstrap(
		ocommon.NewPoint(1, bytes.Repeat([]byte{1}, 32)),
		func(func(badgerutxo.Output) error) error { return nil },
	))
	const slots = 2000
	require.NoError(t, set.store.LoadDRepHistory(func(
		put func(k, v []byte) error,
	) error {
		for slot := uint64(1); slot <= slots; slot++ {
			cred := []byte{byte(slot >> 8), byte(slot)}
			rows := [][2][]byte{
				{badgerutxo.DRepExpiryHistoryKey(slot, 0, cred), badgerutxo.EncodeDRepExpiryHistoryValue(slot, slot+1)},
				{badgerutxo.DRepDormancyHistoryKey(slot, slot), nil},
				{badgerutxo.DRepExpiryEventKey(slot), nil},
			}
			for _, r := range rows {
				if err := put(r[0], r[1]); err != nil {
					return err
				}
			}
		}
		return nil
	}))

	const from, through, limit = 500, 1500, 100
	var got []uint64
	var after []byte
	var maxVisited uint64
	for {
		before := set.store.RangeRowsVisited()
		n := 0
		var last []byte
		require.NoError(t, set.DRepExpiryHistoryAfter(from, through, after,
			func(r ledgerdb.DRepExpiryHistoryRow, key []byte) error {
				got = append(got, r.Slot)
				last = key
				n++
				if n >= limit {
					return ledger.ErrStopRows
				}
				return nil
			}))
		maxVisited = max(maxVisited, set.store.RangeRowsVisited()-before)
		if n < limit {
			break
		}
		after = last
	}
	require.Len(t, got, through-from+1)
	require.Equal(t, uint64(from), got[0])
	require.Equal(t, uint64(through), got[len(got)-1])
	require.LessOrEqual(t, maxVisited, uint64(3*limit+3),
		"a page steps over its own slots' rows (three kinds a slot), not the history outside it")
}
