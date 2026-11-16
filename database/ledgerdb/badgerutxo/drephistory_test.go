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
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

func rowKeys(t *testing.T, s *badgerutxo.Store, tag byte) [][]byte {
	t.Helper()
	var out [][]byte
	require.NoError(t, s.Rows([]byte{tag}, func(k, _ []byte) error {
		out = append(out, append([]byte(nil), k...))
		return nil
	}))
	return out
}

func TestDRepHistoryRowsDecodeToWhatWasKeyed(t *testing.T) {
	t.Parallel()
	cred := []byte{0xaa, 0xbb}
	for _, tc := range []struct {
		name string
		k, v []byte
		want badgerutxo.DRepHistoryRow
	}{
		{
			"expiry",
			badgerutxo.DRepExpiryHistoryKey(7, 1, cred),
			badgerutxo.EncodeDRepExpiryHistoryValue(3, 9),
			badgerutxo.DRepHistoryRow{
				Kind: badgerutxo.DRepHistoryExpiry, Slot: 7, Tag: 1, Key: cred,
				Activity: 3, Expiry: 9,
			},
		},
		{
			"dormancy", badgerutxo.DRepDormancyHistoryKey(7, 4), nil,
			badgerutxo.DRepHistoryRow{
				Kind: badgerutxo.DRepHistoryDormancy, Slot: 7, Previous: 4,
			},
		},
		{
			"event", badgerutxo.DRepExpiryEventKey(7), nil,
			badgerutxo.DRepHistoryRow{Kind: badgerutxo.DRepHistoryEvent, Slot: 7},
		},
		{
			"clear", badgerutxo.DRepClearKey(7, 0, cred), nil,
			badgerutxo.DRepHistoryRow{
				Kind: badgerutxo.DRepHistoryClear, Slot: 7, Tag: 0, Key: cred,
			},
		},
	} {
		got, err := badgerutxo.DecodeDRepHistoryRow(tc.k, tc.v)
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.want, got, tc.name)
	}
	_, err := badgerutxo.DecodeDRepHistoryRow([]byte{badgerutxo.DRepHistoryTag}, nil)
	require.Error(t, err)
}

// A catch-up merge restates the imported block counts of the epochs its
// snapshot carries, replacing those epochs as the metadata store does, and
// keeps the counts of the other epochs; the DRep history is the whole history,
// so the merge adds the snapshot's rows to it.
func TestCatchUpMergeRestatesImportedBlocksPerEpochAndKeepsDRepHistory(t *testing.T) {
	t.Parallel()

	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(10, 0x01), out(0x11, 0))
	pool := bytes.Repeat([]byte{0x61}, 28)
	load := func(slot, blocks uint64, rows ...[]byte) {
		require.NoError(t, s.LoadImportedBlocks(false, func(
			put func(k, v []byte) error,
			_ func(slot uint64, prefix []byte) error,
		) error {
			for _, k := range rows {
				if err := put(
					k, badgerutxo.EncodeImportedBlockValue(blocks, slot),
				); err != nil {
					return err
				}
			}
			return nil
		}))
		require.NoError(t, s.LoadDRepHistory(false, func(
			put func(k, v []byte) error,
		) error {
			return put(badgerutxo.DRepExpiryEventKey(slot), nil)
		}))
	}
	// The first snapshot carries epochs 5 and 6, with a pool row in epoch 6.
	load(
		5, 3,
		badgerutxo.ImportedBlockTotalKey(5),
		badgerutxo.ImportedBlockTotalKey(6),
		badgerutxo.ImportedBlockPoolKey(6, pool),
	)
	require.NoError(t, s.BootstrapCatchUp(point(20, 0x02),
		func(put func(badgerutxo.Output) error) error {
			return put(out(0x21, 0))
		},
	))
	require.NoError(t, s.BeginMerge())
	// The catch-up snapshot carries epochs 6 and 7, with more blocks.
	load(
		20, 9,
		badgerutxo.ImportedBlockTotalKey(6),
		badgerutxo.ImportedBlockPoolKey(6, pool),
		badgerutxo.ImportedBlockTotalKey(7),
	)
	require.NoError(t, s.EndMerge(true))

	require.Equal(
		t,
		[][]byte{
			badgerutxo.ImportedBlockTotalKey(5),
			badgerutxo.ImportedBlockPoolKey(6, pool),
			badgerutxo.ImportedBlockTotalKey(6),
			badgerutxo.ImportedBlockTotalKey(7),
		},
		rowKeys(t, s, badgerutxo.ImportedBlocksTag),
		"the merge keeps epoch 5 and adds epoch 7",
	)
	restated := map[string]uint64{}
	require.NoError(t, s.Rows([]byte{badgerutxo.ImportedBlocksTag}, func(k, v []byte) error {
		blocks, slot, err := badgerutxo.DecodeImportedBlockValue(v)
		require.NoError(t, err)
		restated[string(k)] = blocks*100 + slot
		return nil
	}))
	require.Equal(t, uint64(3*100+5), restated[string(badgerutxo.ImportedBlockTotalKey(5))])
	require.Equal(t, uint64(9*100+20), restated[string(badgerutxo.ImportedBlockTotalKey(6))],
		"epoch 6 is the catch-up snapshot's")
	require.Equal(t, uint64(9*100+20), restated[string(badgerutxo.ImportedBlockPoolKey(6, pool))])
	require.Equal(
		t,
		[][]byte{badgerutxo.DRepExpiryEventKey(5), badgerutxo.DRepExpiryEventKey(20)},
		rowKeys(t, s, badgerutxo.DRepHistoryTag),
		"the merge keeps the DRep history it held",
	)
}
