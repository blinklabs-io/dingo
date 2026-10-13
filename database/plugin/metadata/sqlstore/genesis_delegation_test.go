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

package sqlstore

import (
	"bytes"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func seedGenesisDelegationRow(
	t *testing.T,
	store *Store,
	genesis []byte,
	delegate byte,
	slot uint64,
) {
	t.Helper()
	_, err := store.writeDB.Exec(`
INSERT INTO genesis_delegation (
    genesis_hash, genesis_delegate_hash, vrf_key_hash, added_slot,
    block_index, cert_index, certificate_id
) VALUES (?, ?, ?, ?, 0, 0, ?)`,
		genesis,
		credentialHash(delegate),
		bytes.Repeat([]byte{delegate}, lcommon.Blake2b256Size),
		slot,
		slot,
	)
	require.NoError(t, err)
}

// A genesis delegation is selected only once its certificate slot plus the
// stability window has been reached, and the window is a lower bound on the
// block slot rather than something to subtract from a smaller one.
func TestGetGenesisDelegationForSlotAppliesStabilityWindow(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	genesis := credentialHash(0x11)
	const window = uint64(50)
	seedGenesisDelegationRow(t, store, genesis, 0xa1, 100)
	seedGenesisDelegationRow(t, store, genesis, 0xa2, 120)

	for _, tc := range []struct {
		name string
		slot uint64
		want []byte
	}{
		{"below the window", 10, nil},
		{"one slot short of the first", 100 + window - 1, nil},
		{"first at its activation slot", 100 + window, credentialHash(0xa1)},
		{"second still pending", 120 + window - 1, credentialHash(0xa1)},
		{"second at its activation slot", 120 + window, credentialHash(0xa2)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row, err := store.GetGenesisDelegationForSlot(
				genesis, tc.slot, window, nil,
			)
			require.NoError(t, err)
			if tc.want == nil {
				require.Nil(t, row)
				return
			}
			require.NotNil(t, row)
			require.Equal(t, tc.want, row.GenesisDelegateHash)
		})
	}
}

func TestGetGenesisDelegationsInSlotRangeIsInclusive(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	genesis := credentialHash(0x11)
	for i, slot := range []uint64{100, 120, 140} {
		seedGenesisDelegationRow(t, store, genesis, byte(0xb0+i), slot)
	}

	rows, err := store.GetGenesisDelegationsInSlotRange(120, 140, nil)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, uint64(120), rows[0].AddedSlot)
	require.Equal(t, uint64(140), rows[1].AddedSlot)

	rows, err = store.GetGenesisDelegationsInSlotRange(141, 140, nil)
	require.NoError(t, err)
	require.Empty(t, rows)
}
