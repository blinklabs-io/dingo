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

package ledger

import (
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestCreateGenesisBlockInitializesMusashiNetworkState(t *testing.T) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	require.NoError(t, ls.createGenesisBlock())

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(0), state.Slot)
	require.Equal(t, uint64(0), uint64(state.Treasury))
	require.Equal(
		t,
		uint64(14_999_999_100_000_000),
		uint64(state.Reserves),
	)
}

func TestCreateGenesisBlockBackfillsMissingNetworkState(t *testing.T) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	genesisHash, err := GenesisBlockHash(nodeCfg)
	require.NoError(t, err)
	require.NoError(t, db.SetGenesisCbor(
		0,
		genesisHash[:],
		[]byte{0x80},
		nil,
	))

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	ls.currentTip.Point.Slot = 42
	require.NoError(t, ls.createGenesisBlock())

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(0), state.Slot)
	require.Equal(t, uint64(0), uint64(state.Treasury))
	require.Equal(
		t,
		uint64(14_999_999_100_000_000),
		uint64(state.Reserves),
	)
}

func TestGenesisReserveBalanceRejectsInvalidInputs(t *testing.T) {
	_, err := genesisReserveBalance(1, []lcommon.Utxo{{}})
	require.ErrorContains(t, err, "has no output")

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	utxos, err := nodeCfg.ShelleyGenesis().GenesisUtxos()
	require.NoError(t, err)
	require.NotEmpty(t, utxos)
	_, err = genesisReserveBalance(0, utxos)
	require.ErrorContains(t, err, "exceeds max lovelace supply")
}
