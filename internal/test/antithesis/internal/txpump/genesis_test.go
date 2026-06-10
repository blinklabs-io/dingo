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

package txpump

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestLoadGenesisUTxOsExplicitList(t *testing.T) {
	path := filepath.Join(t.TempDir(), "utxos.json")
	require.NoError(
		t,
		os.WriteFile(
			path,
			[]byte(`[
				{"txHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","index":0,"amount":1000000},
				{"txHash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","index":2,"amount":2000000}
			]`),
			0o600,
		),
	)

	got, err := LoadGenesisUTxOs(path)
	require.NoError(t, err)
	require.Equal(t, []UTxO{
		{
			TxHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Index:  0,
			Amount: 1_000_000,
		},
		{
			TxHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Index:  2,
			Amount: 2_000_000,
		},
	}, got)
}

func TestLoadGenesisUTxOsShelleyInitialFunds(t *testing.T) {
	const (
		baseAddress      = "600f8eff407d885a400f931375864d5c69d2cc2d00fe6dd0b4b8073072"
		delegatedAddress = "" +
			"0017a9d0f206c5ef2c17547fdf9f630880d42ff00e937271e9" +
			"d5b1c4472273fbe1a34890afba7547610a8f0768c7440e5d30a4818fc6b27438"
	)
	path := filepath.Join(t.TempDir(), "shelley-genesis.json")
	require.NoError(
		t,
		os.WriteFile(
			path,
			[]byte(`{
				"initialFunds": {
					"`+baseAddress+`": 120000000000000,
					"`+delegatedAddress+`": 340000000000000
				}
			}`),
			0o600,
		),
	)

	got, err := LoadGenesisUTxOs(path)
	require.NoError(t, err)
	require.Equal(t, []UTxO{
		{
			TxHash: genesisAddressTxHash(t, delegatedAddress),
			Index:  0,
			Amount: 340_000_000_000_000,
		},
		{
			TxHash: genesisAddressTxHash(t, baseAddress),
			Index:  0,
			Amount: 120_000_000_000_000,
		},
	}, got)
}

func genesisAddressTxHash(t *testing.T, address string) string {
	t.Helper()
	addrBytes, err := hex.DecodeString(address)
	require.NoError(t, err)
	return common.Blake2b256Hash(addrBytes).String()
}
