// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package badgerutxo_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

// DistinctUint64Keys lists the same values a scan of every row under the
// prefix finds, with gaps between them and many rows per value, while stepping
// over one row per value rather than one per row.
func TestDistinctUint64KeysMatchesAFullScan(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(0, 0))

	prefix := []byte{'Z', 7}
	values := []uint64{0, 1, 5, 6, 200, 70000, math.MaxUint64 - 1, math.MaxUint64}
	const perValue = 25
	var puts []badgerutxo.RowPut
	for _, v := range values {
		for i := range uint32(perValue) {
			k := binary.BigEndian.AppendUint64(append([]byte{}, prefix...), v)
			k = binary.BigEndian.AppendUint32(k, i)
			puts = append(puts, badgerutxo.RowPut{Slot: 10, Key: k, Value: []byte{1}})
		}
	}
	// Rows under neighbouring prefixes are not part of the answer.
	for _, other := range [][]byte{{'Z', 6}, {'Z', 8}} {
		k := binary.BigEndian.AppendUint64(append([]byte{}, other...), 3)
		puts = append(puts, badgerutxo.RowPut{Slot: 10, Key: k, Value: []byte{1}})
	}
	tip := point(10, 0x0a)
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{Rows: puts})
	require.NoError(t, err)

	var scanned []uint64
	require.NoError(t, s.Rows(prefix, func(k, _ []byte) error {
		v := binary.BigEndian.Uint64(k[len(prefix):])
		if len(scanned) == 0 || scanned[len(scanned)-1] != v {
			scanned = append(scanned, v)
		}
		return nil
	}))
	require.Equal(t, values, scanned)

	before := s.RangeRowsVisited()
	got, err := s.DistinctUint64Keys(prefix)
	require.NoError(t, err)
	require.Equal(t, scanned, got)
	require.LessOrEqual(t, s.RangeRowsVisited()-before, uint64(len(values)),
		"stepped over rows beyond one per distinct value")

	none, err := s.DistinctUint64Keys([]byte{'Z', 9})
	require.NoError(t, err)
	require.Empty(t, none)
}

// A key too short to hold the value is reported, not skipped.
func TestDistinctUint64KeysRejectsAShortKey(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(0, 0))
	tip := point(10, 0x0a)
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{Rows: []badgerutxo.RowPut{
		{Slot: 10, Key: []byte{'Z', 7, 1, 2}, Value: []byte{1}},
	}})
	require.NoError(t, err)
	_, err = s.DistinctUint64Keys([]byte{'Z', 7})
	require.Error(t, err)
}
