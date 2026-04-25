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

// Pv3's classification path requires the iterator to load CBOR for
// each row and decode the address — that needs real UTxO+blob
// fixtures. Coverage of the AVVM identification and the database
// primitives (IterateLiveUtxos, MarkUtxosDeletedAtSlot) lives in the
// integration suite and the sqlite plugin tests respectively. Here we
// only check the dispatcher's no-op-on-empty-DB invariant for pv3 so
// the wiring stays exercised.
func TestApplyIntraEraHardForkRule_Pv3_EmptyDB(t *testing.T) {
	db := newTestDB(t)
	ls := newTestLSForHardForkRule(t, db)
	require.NoError(t, ls.applyIntraEraHardForkRule(
		nil, 3, 4_492_800, 208,
	))
}
