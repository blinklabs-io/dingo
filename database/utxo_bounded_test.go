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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package database

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestUtxosByAddressBoundedRefusesBeforeCborLoad seeds UTxOs whose output
// CBOR is absent from the blob store, so any CBOR load fails. A refusal that
// returns models.ErrTooManyUtxoResults therefore happened in the metadata
// read, before CBOR resolution; the generous-budget control shows the same
// rows do reach the CBOR load.
func TestUtxosByAddressBoundedRefusesBeforeCborLoad(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x57}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	const total = 3
	for i := range total {
		require.NoError(t, db.CreateUtxo(t.Context(), nil, &models.Utxo{
			TxId:       bytes.Repeat([]byte{byte(i + 1)}, 32),
			OutputIdx:  0,
			PaymentKey: addr.PaymentKeyHash().Bytes(),
			Address:    []byte(addr.String()),
			AddedSlot:  uint64(i + 1),
			Amount:     types.Uint64(1_000_000),
			Assets: []models.Asset{{
				PolicyId: bytes.Repeat([]byte{0x01}, 28),
				Name:     []byte{byte(i)},
				Amount:   1,
			}},
		}))
	}
	addrs := []lcommon.Address{addr}
	// One entry per UTxO plus one per asset.
	const entries = 2 * total

	_, err = db.UtxosByAddressBounded(t.Context(), addrs, total, entries, nil)
	require.Error(t, err)
	require.NotErrorIs(t, err, models.ErrTooManyUtxoResults)

	// The assets alone fit this budget; the rows plus assets do not.
	_, err = db.UtxosByAddressBounded(t.Context(), addrs, total, total, nil)
	require.ErrorIs(t, err, models.ErrTooManyUtxoResults)

	_, err = db.UtxosByAddressBounded(t.Context(), addrs, total, entries-1, nil)
	require.ErrorIs(t, err, models.ErrTooManyUtxoResults)
}
