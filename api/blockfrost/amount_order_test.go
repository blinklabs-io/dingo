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

package blockfrost

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// The amount array lists lovelace first, then assets by policy ID then asset
// name bytes, whatever order the UTxO's rows were stored in.
func TestAddressAmountsFromUtxoCanonicalOrder(t *testing.T) {
	t.Parallel()
	p1 := make([]byte, 28)
	p2 := make([]byte, 28)
	p1[0], p2[0] = 0x01, 0x02
	utxo := models.Utxo{
		Amount: types.Uint64(7),
		Assets: []models.Asset{
			{PolicyId: p2, Name: []byte("b"), Amount: 4},
			{PolicyId: p1, Name: []byte("b"), Amount: 2},
			{PolicyId: p2, Name: []byte("a"), Amount: 3},
			{PolicyId: p1, Name: []byte("a"), Amount: 1},
		},
	}
	got := addressAmountsFromUtxo(utxo)
	units := make([]string, 0, len(got))
	for _, amount := range got {
		units = append(units, amount.Unit)
	}
	require.Equal(t, []string{
		"lovelace",
		"01000000000000000000000000000000000000000000000000000000" + "61",
		"01000000000000000000000000000000000000000000000000000000" + "62",
		"02000000000000000000000000000000000000000000000000000000" + "61",
		"02000000000000000000000000000000000000000000000000000000" + "62",
	}, units)
	// The caller's slice is not reordered.
	require.Equal(t, p2, utxo.Assets[0].PolicyId)
}
