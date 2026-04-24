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
	"bytes"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	sqliteplugin "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// allegraAvvmFixtureKeys holds the TxIds seeded by seedAllegraAvvmFixtures
// so tests can look them up after the rule runs.
type allegraAvvmFixtureKeys struct {
	Redeem [][]byte
	Pubkey []byte
}

// seedAllegraAvvmFixtures plants two live Byron redeem UTxOs (AVVM) and
// one Byron pubkey UTxO that must survive the pv3 rule.
func seedAllegraAvvmFixtures(
	t *testing.T,
	db *database.Database,
) allegraAvvmFixtureKeys {
	t.Helper()
	store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	require.True(t, ok, "test DB must be backed by sqlite")

	keys := allegraAvvmFixtureKeys{
		Redeem: [][]byte{
			bytes.Repeat([]byte{0x01}, 32),
			bytes.Repeat([]byte{0x02}, 32),
		},
		Pubkey: bytes.Repeat([]byte{0x03}, 32),
	}

	for i, id := range keys.Redeem {
		require.NoError(t, store.DB().Create(&models.Utxo{
			TxId:             id,
			OutputIdx:        0,
			PaymentKey:       bytes.Repeat([]byte{0xA0 + byte(i)}, 28),
			Amount:           types.Uint64(uint64(1_000_000 * (i + 1))),
			AddedSlot:        100,
			ByronAddressType: uint8(lcommon.ByronAddressTypeRedeem),
		}).Error)
	}
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:             keys.Pubkey,
		OutputIdx:        0,
		PaymentKey:       bytes.Repeat([]byte{0xB1}, 28),
		Amount:           types.Uint64(5_555_555),
		AddedSlot:        150,
		ByronAddressType: uint8(lcommon.ByronAddressTypePubkey),
	}).Error)
	return keys
}

// newTestLSForHardForkRule wires just enough of a LedgerState to call
// applyIntraEraHardForkRule against the given test DB.
func newTestLSForHardForkRule(
	t *testing.T,
	db *database.Database,
) *LedgerState {
	t.Helper()
	return &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
}

// pv3 (Shelley→Allegra): every live AVVM UTxO is marked deleted at the
// boundary slot; non-redeem UTxOs are preserved.
func TestApplyIntraEraHardForkRule_Pv3_RemovesAvvm(t *testing.T) {
	db := newTestDB(t)
	keys := seedAllegraAvvmFixtures(t, db)

	ls := newTestLSForHardForkRule(t, db)
	const boundarySlot uint64 = 4_492_800 // approx mainnet Allegra start
	require.NoError(t, ls.applyIntraEraHardForkRule(
		nil,          // nil txn → owned metadata txn inside the Database wrapper
		3,            // newMajor (Allegra)
		boundarySlot, // boundarySlot
		208,          // newEpoch (log-only)
	))

	store := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	for _, id := range keys.Redeem {
		var u models.Utxo
		require.NoError(t, store.DB().
			Where("tx_id = ?", id).First(&u).Error)
		assert.Equal(t, boundarySlot, u.DeletedSlot,
			"AVVM UTxO must be deleted at the Allegra boundary slot")
	}

	var pubkey models.Utxo
	require.NoError(t, store.DB().
		Where("tx_id = ?", keys.Pubkey).First(&pubkey).Error)
	assert.Equal(t, uint64(0), pubkey.DeletedSlot,
		"Byron pubkey UTxO must survive the pv3 rule")
}

// Unknown major versions are a no-op — matches the Haskell rule's
// `otherwise = id` branch. Explicitly verifies that pv2 (pre-Allegra),
// pv4 (Allegra→Mary), and pv10 (Plomin, handled elsewhere) do not touch
// AVVM UTxOs through this dispatch.
func TestApplyIntraEraHardForkRule_OtherMajors_DoNotTouchAvvm(t *testing.T) {
	db := newTestDB(t)
	keys := seedAllegraAvvmFixtures(t, db)

	ls := newTestLSForHardForkRule(t, db)
	for _, major := range []uint{2, 4, 10, 99} {
		require.NoError(t, ls.applyIntraEraHardForkRule(
			nil, major, 1_234_567, 42,
		))
	}

	store := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	for _, id := range keys.Redeem {
		var u models.Utxo
		require.NoError(t, store.DB().
			Where("tx_id = ?", id).First(&u).Error)
		assert.Equal(t, uint64(0), u.DeletedSlot,
			"non-pv3 dispatch must not touch AVVM UTxOs")
	}
}
