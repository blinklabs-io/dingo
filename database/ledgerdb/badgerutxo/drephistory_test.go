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

// A catch-up import restates the imported block counts of the epochs its
// snapshot carries, replacing those epochs whole, and keeps the counts of the
// other epochs.
func TestCatchUpImportRestatesImportedBlocksPerEpoch(t *testing.T) {
	t.Parallel()

	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(10, 0x01), out(0x11, 0))
	pool := bytes.Repeat([]byte{0x61}, 28)
	other := bytes.Repeat([]byte{0x62}, 28)
	require.NoError(t, s.LoadImportedBlocks(false, func(
		func(k, v []byte) error, func(uint64, []byte) error,
	) error {
		return nil
	}))
	// The first snapshot carries epochs 5 and 6, with two pools in epoch 6.
	require.NoError(t, s.ReplaceImportedBlocks(5, nil, 3, 5))
	require.NoError(t, s.ReplaceImportedBlocks(
		6, map[string]uint64{string(pool): 2, string(other): 1}, 3, 5,
	))
	require.NoError(t, s.BootstrapImportCatchUp(point(20, 0x02),
		func(put func(badgerutxo.Output) error) error {
			return put(out(0x21, 0))
		},
	))
	// The catch-up snapshot carries epochs 6 and 7, with more blocks.
	require.NoError(t, s.ReplaceImportedBlocks(
		6, map[string]uint64{string(pool): 9}, 9, 20,
	))
	require.NoError(t, s.ReplaceImportedBlocks(7, nil, 9, 20))

	require.Equal(
		t,
		[][]byte{
			badgerutxo.ImportedBlockTotalKey(5),
			badgerutxo.ImportedBlockPoolKey(6, pool),
			badgerutxo.ImportedBlockTotalKey(6),
			badgerutxo.ImportedBlockTotalKey(7),
		},
		rowKeys(t, s, badgerutxo.ImportedBlocksTag),
		"the catch-up keeps epoch 5, replaces epoch 6 and adds epoch 7",
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
}
