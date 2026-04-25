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
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
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

// seedByronAvvmFixtures inserts two live Byron UTxOs — one at a redeem
// (AVVM) address, one at a pubkey address — through the metadata + blob
// stores so the iterator's loadCbor path resolves a real address on
// each row. Returns the two synthetic 32-byte tx ids.
func seedByronAvvmFixtures(
	t *testing.T,
	db *database.Database,
) (avvmTxId, pubkeyTxId []byte) {
	t.Helper()
	redeemAddr, err := lcommon.NewByronAddressRedeem(
		bytes.Repeat([]byte{0x42}, 32),
		lcommon.ByronAddressAttributes{},
	)
	require.NoError(t, err)
	pubkeyAddr, err := lcommon.NewByronAddressFromParts(
		lcommon.ByronAddressTypePubkey,
		bytes.Repeat([]byte{0x55}, lcommon.AddressHashSize),
		lcommon.ByronAddressAttributes{},
	)
	require.NoError(t, err)

	avvmTxId = seedByronUtxo(t, db, 0xAA, redeemAddr)
	pubkeyTxId = seedByronUtxo(t, db, 0xBB, pubkeyAddr)
	return avvmTxId, pubkeyTxId
}

// seedByronUtxo inserts a Utxo metadata row plus a raw-CBOR blob entry
// for a Byron output at addr. Returns the synthetic tx id (txIdSeed
// repeated 32×) so callers can look it up afterwards.
func seedByronUtxo(
	t *testing.T,
	db *database.Database,
	txIdSeed byte,
	addr lcommon.Address,
) []byte {
	t.Helper()
	out := byron.ByronTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  1000,
	}
	cborBytes, err := cbor.Encode(out)
	require.NoError(t, err)

	txId := bytes.Repeat([]byte{txIdSeed}, 32)
	txn := db.Transaction(true)
	require.NoError(t, db.CreateUtxo(txn, &models.Utxo{
		TxId:      txId,
		OutputIdx: 0,
		AddedSlot: 100,
	}))
	blob := db.Blob()
	require.NotNil(t, blob)
	require.NoError(t, blob.SetUtxo(txn.Blob(), txId, 0, cborBytes))
	require.NoError(t, txn.Commit())
	return txId
}

// pv3 (Shelley→Allegra): every live AVVM UTxO is removed from the live
// UTxO set; non-AVVM Byron UTxOs survive. Exercises the full
// classification path — IterateLiveUtxos, loadCbor, address decode,
// AddressTypeByron + ByronAddressTypeRedeem check, MarkUtxosDeletedAtSlot.
func TestApplyIntraEraHardForkRule_Pv3_RemovesAvvm(t *testing.T) {
	db := newTestDB(t)
	avvmTxId, pubkeyTxId := seedByronAvvmFixtures(t, db)

	ls := newTestLSForHardForkRule(t, db)
	const boundarySlot uint64 = 4_492_800
	require.NoError(t, ls.applyIntraEraHardForkRule(
		nil, 3, boundarySlot, 208,
	))

	_, err := db.UtxoByRef(avvmTxId, 0, nil)
	assert.ErrorIs(t, err, database.ErrUtxoNotFound,
		"AVVM redeem UTxO must be marked deleted by pv3")

	stillLive, err := db.UtxoByRefIncludingSpent(avvmTxId, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, stillLive)
	assert.Equal(t, boundarySlot, stillLive.DeletedSlot,
		"DeletedSlot must equal the boundary slot so a rollback "+
			"across the boundary correctly un-deletes via "+
			"SetUtxosNotDeletedAfterSlot")

	pubkey, err := db.UtxoByRef(pubkeyTxId, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, pubkey)
	assert.Equal(t, uint64(0), pubkey.DeletedSlot,
		"non-AVVM Byron UTxO must survive the pv3 rule")
}

// A non-pv3 major-version dispatch must not touch AVVM UTxOs — the
// rule is only fired at Shelley→Allegra. Combined with
// TestApplyIntraEraHardForkRule_UnknownMajor_NoOp (pv10-shaped
// fixtures), this pins both sides of the dispatcher's switch.
func TestApplyIntraEraHardForkRule_NonPv3_PreservesAvvm(t *testing.T) {
	db := newTestDB(t)
	avvmTxId, _ := seedByronAvvmFixtures(t, db)

	ls := newTestLSForHardForkRule(t, db)
	require.NoError(t, ls.applyIntraEraHardForkRule(nil, 4, 1234, 100))

	avvm, err := db.UtxoByRef(avvmTxId, 0, nil)
	require.NoError(t, err)
	assert.NotNil(t, avvm,
		"non-pv3 dispatch must not touch AVVM UTxOs")
}
