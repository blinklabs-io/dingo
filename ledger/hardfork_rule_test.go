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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// plominFixtureKeys holds the staking keys seeded by
// seedPlominFixtures, used by the assertions below.
type plominFixtureKeys struct {
	Live, Dead []byte
}

// seedPlominFixtures writes:
//   - an active DRep (liveCred)
//   - an Account (stakeKeyLive) delegating to liveCred  → should survive
//     the pv10 rule.
//   - an Account (stakeKeyDead) delegating to a credential with NO DRep row
//     → should be cleared by the pv10 rule.
//
// Returns the two staking keys for later lookups.
func seedPlominFixtures(t *testing.T, db *database.Database) plominFixtureKeys {
	t.Helper()
	liveCred := bytes.Repeat([]byte{0x11}, 28)
	deadCred := bytes.Repeat([]byte{0x22}, 28)
	keys := plominFixtureKeys{
		Live: bytes.Repeat([]byte{0xA1}, 28),
		Dead: bytes.Repeat([]byte{0xA2}, 28),
	}

	require.NoError(t, db.CreateDrep(nil, &models.Drep{
		Credential: liveCred,
		Active:     true,
		AddedSlot:  10,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: keys.Live,
		Drep:       liveCred,
		DrepType:   models.DrepTypeAddrKeyHash,
		Active:     true,
		AddedSlot:  100,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: keys.Dead,
		Drep:       deadCred,
		DrepType:   models.DrepTypeAddrKeyHash,
		Active:     true,
		AddedSlot:  100,
	}))
	return keys
}

// pv10 is the major-version bump that currently has a non-no-op handler:
// accounts with credential-backed delegations to unregistered DReps are
// cleared; accounts delegating to registered DReps are preserved.
func TestApplyIntraEraHardForkRule_Pv10_ClearsDangling(t *testing.T) {
	db := newTestDB(t)
	keys := seedPlominFixtures(t, db)

	ls := newTestLSForHardForkRule(t, db)
	require.NoError(t, ls.applyIntraEraHardForkRule(
		nil,  // nil txn → owned metadata txn inside the Database wrapper
		10,   // newMajor
		7777, // boundarySlot
		500,  // newEpoch (log-only)
	))

	live, err := db.GetAccount(keys.Live, true, nil)
	require.NoError(t, err)
	assert.NotNil(t, live.Drep,
		"delegation to registered DRep must survive the rule")

	dead, err := db.GetAccount(keys.Dead, true, nil)
	require.NoError(t, err)
	assert.Nil(t, dead.Drep,
		"dangling delegation must be cleared at pv10")
	assert.Equal(t, uint64(7777), dead.AddedSlot,
		"AddedSlot must be bumped to the boundary slot so a later "+
			"rollback past boundarySlot re-derives from cert history")
}

// Every major-version bump other than the ones with an explicit case
// falls through to a no-op (matching the Haskell rule's `otherwise = id`).
// Verified against a representative sample of values below and above the
// handled pv10 case.
func TestApplyIntraEraHardForkRule_UnknownMajor_NoOp(t *testing.T) {
	db := newTestDB(t)
	keys := seedPlominFixtures(t, db)

	ls := newTestLSForHardForkRule(t, db)

	for _, major := range []uint{9, 11, 12, 99} {
		require.NoError(t, ls.applyIntraEraHardForkRule(
			nil, major, 1234, 100,
		))
	}

	live, err := db.GetAccount(keys.Live, true, nil)
	require.NoError(t, err)
	assert.NotNil(t, live.Drep)
	assert.Equal(t, uint64(100), live.AddedSlot,
		"unknown-major dispatch must not touch unrelated accounts")

	dead, err := db.GetAccount(keys.Dead, true, nil)
	require.NoError(t, err)
	assert.NotNil(t, dead.Drep,
		"unknown-major dispatch must not touch even the dangling account")
	assert.Equal(t, uint64(100), dead.AddedSlot)
}

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
	keys := allegraAvvmFixtureKeys{
		Redeem: [][]byte{
			bytes.Repeat([]byte{0x01}, 32),
			bytes.Repeat([]byte{0x02}, 32),
		},
		Pubkey: bytes.Repeat([]byte{0x03}, 32),
	}

	for i, id := range keys.Redeem {
		require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
			TxId:             id,
			OutputIdx:        0,
			PaymentKey:       bytes.Repeat([]byte{0xA0 + byte(i)}, 28),
			Amount:           types.Uint64(uint64(1_000_000 * (i + 1))),
			AddedSlot:        100,
			ByronAddressType: uint8(lcommon.ByronAddressTypeRedeem),
		}))
	}
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:             keys.Pubkey,
		OutputIdx:        0,
		PaymentKey:       bytes.Repeat([]byte{0xB1}, 28),
		Amount:           types.Uint64(5_555_555),
		AddedSlot:        150,
		ByronAddressType: uint8(lcommon.ByronAddressTypePubkey),
	}))
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

// pv3 (Shelley→Allegra): every live AVVM UTxO is removed from the live
// UTxO set. The dispatcher's contract with the database layer (exact
// DeletedSlot bookkeeping, idempotency) is covered in the sqlite
// plugin's allegra_avvm_test.go. Here we only verify that the dispatch
// reached the rule, distinguishing "metadata row gone" (ErrUtxoNotFound)
// from "metadata row present but CBOR unavailable"
// (ErrUtxoCborUnavailable, which is what fixture rows return before the
// rule runs because no blob backs them).
func TestApplyIntraEraHardForkRule_Pv3_RemovesAvvm(t *testing.T) {
	db := newTestDB(t)
	keys := seedAllegraAvvmFixtures(t, db)

	// Sanity-check the seed: pre-rule, fixture rows are present in
	// metadata but have no CBOR backing.
	for _, id := range keys.Redeem {
		_, err := db.UtxoByRef(id, 0, nil)
		require.ErrorIs(t, err, database.ErrUtxoCborUnavailable,
			"pre-rule fixture row should be in live set with no CBOR")
	}

	ls := newTestLSForHardForkRule(t, db)
	const boundarySlot uint64 = 4_492_800 // approx mainnet Allegra start
	require.NoError(t, ls.applyIntraEraHardForkRule(
		nil,          // nil txn → owned metadata txn inside the Database wrapper
		3,            // newMajor (Allegra)
		boundarySlot, // boundarySlot
		208,          // newEpoch (log-only)
	))

	// Post-rule, the metadata rows are gone from the live set.
	for _, id := range keys.Redeem {
		_, err := db.UtxoByRef(id, 0, nil)
		require.ErrorIs(t, err, database.ErrUtxoNotFound,
			"AVVM UTxO must be removed from the live UTxO set")
	}

	// Non-redeem Byron (pubkey) UTxO must survive the pv3 rule: the
	// only thing standing between the dispatcher and over-deletion of
	// every Byron UTxO is the ByronAddressType == Redeem filter in the
	// metadata layer, so check it directly here.
	_, err := db.UtxoByRef(keys.Pubkey, 0, nil)
	require.ErrorIs(t, err, database.ErrUtxoCborUnavailable,
		"non-redeem Byron UTxO must survive the pv3 AVVM return rule")
	require.NotErrorIs(t, err, database.ErrUtxoNotFound)
}

// Only pv3 may touch AVVM UTxOs. Verifies that other majors — both the
// no-op branches matching the Haskell rule's `otherwise = id` (pv2, pv99)
// and majors with their own non-AVVM handlers (pv10/Plomin) — leave
// redeem UTxOs in the live set. The rows still exist in metadata so
// UtxoByRef returns ErrUtxoCborUnavailable (not ErrUtxoNotFound)
// because the fixture has no blob backing.
func TestApplyIntraEraHardForkRule_OtherMajors_DoNotTouchAvvm(t *testing.T) {
	db := newTestDB(t)
	keys := seedAllegraAvvmFixtures(t, db)

	ls := newTestLSForHardForkRule(t, db)
	for _, major := range []uint{2, 4, 10, 99} {
		require.NoError(t, ls.applyIntraEraHardForkRule(
			nil, major, 1_234_567, 42,
		))
	}

	for _, id := range keys.Redeem {
		_, err := db.UtxoByRef(id, 0, nil)
		require.ErrorIs(t, err, database.ErrUtxoCborUnavailable,
			"non-pv3 dispatch must leave AVVM rows in the live set")
		require.NotErrorIs(t, err, database.ErrUtxoNotFound,
			"non-pv3 dispatch must not remove AVVM UTxOs")
	}

	// Pubkey (non-AVVM) Byron UTxO must also remain untouched: a
	// regression that incorrectly broadens the metadata filter beyond
	// ByronAddressType == Redeem on a non-pv3 major would otherwise
	// slip through.
	_, err := db.UtxoByRef(keys.Pubkey, 0, nil)
	require.ErrorIs(t, err, database.ErrUtxoCborUnavailable,
		"non-pv3 dispatch must not touch AVVM pubkey UTxOs")
	require.NotErrorIs(t, err, database.ErrUtxoNotFound)
}
