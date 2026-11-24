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
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// expiryRollbackTestEpochCount covers every slot any test in this file
// seeds a rollback/witness/activation point at (the highest currently is
// 50550, epoch 505), with headroom for new cases.
const expiryRollbackTestEpochCount = 600

// expiryRollbackSmallEpochCount covers the tests that seed slots below
// epoch 4.
const expiryRollbackSmallEpochCount = 8

// newExpiryRollbackTestLedger builds a DB-backed LedgerState with the CIP-0163
// delegator-inactivity gate configured and a fixed epoch schedule so
// SlotToEpoch resolves the seeded witness slots. Epochs are 100 slots
// long: E0 = [0,100), E1 = [100,200), ... up to
// expiryRollbackTestEpochCount, all persisted (not just cached in
// memory): database.RecomputeAccountExpirationsAfterTruncate resolves
// rollback and witness slots via database.EpochBySlot, which reads the
// persisted epoch table directly with no live-LedgerState projection
// capability at all (unlike ls.SlotToEpoch) -- required for the
// offline/live truncate path, which has no LedgerState to project from.
// In real usage this is never a gap: by the time the chain has actually
// reached a given slot, normal forward processing has already persisted
// that slot's epoch row, the same one this recompute reads.
func newExpiryRollbackTestLedger(
	t *testing.T,
	enabled bool,
	inactivity uint64,
) (*LedgerState, *database.Database) {
	t.Helper()
	return newExpiryRollbackTestLedgerWithEpochs(
		t, enabled, inactivity, expiryRollbackTestEpochCount,
	)
}

// newExpiryRollbackTestLedgerWithEpochs persists only as many epochs as
// the caller's slots reach; each epoch is a separate database write.
func newExpiryRollbackTestLedgerWithEpochs(
	t *testing.T,
	enabled bool,
	inactivity uint64,
	epochs int,
) (*LedgerState, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)

	epochCache := make([]models.Epoch, epochs)
	for i := range epochCache {
		epochCache[i] = models.Epoch{
			EpochId:       uint64(i),
			StartSlot:     uint64(i) * 100,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         1,
		}
	}
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
			DelegatorInactivityEnabled: enabled,
			DelegatorInactivity:        inactivity,
		},
		currentEra: eras.ShelleyEraDesc,
		epochCache: epochCache,
	}
	attachTestLedgerDB(t, ls)
	ls.publishSnapshotsLocked()
	for _, e := range epochCache {
		require.NoError(t, seedDB(t, db).SetEpoch(
			e.StartSlot,
			e.EpochId,
			nil, nil, nil, nil,
			e.EraId,
			e.SlotLength,
			e.LengthInSlots,
			nil,
		))
	}
	return ls, db
}

// seedRollbackCertificate records a certificate through the public database
// ingestion boundary, and in db's LedgerDB set once that is open, as the
// certificate's own transaction does.
func seedRollbackCertificate(
	t *testing.T,
	db *database.Database,
	slot uint64,
	cert lcommon.Certificate,
) {
	t.Helper()
	txID := make([]byte, 32)
	binary.BigEndian.PutUint64(txID[len(txID)-8:], slot)
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(txID)
	tx.WithCertificates(cert)
	require.NoError(t, seedLedgerOf(t, db).stageAccountTransaction(
		nil, tx, slot, 0, txID,
		database.BatchedTxIngestOpts{}, map[int]uint64{0: 0},
	))
}

func rollbackStakeRegistrationCertificate(
	cred []byte,
) lcommon.Certificate {
	return &lcommon.StakeRegistrationCertificate{
		StakeCredential: lcommon.Credential{
			CredType:   0,
			Credential: lcommon.NewBlake2b224(cred),
		},
	}
}

// expiryRollbackBlock commits what stage writes as the block at slot: the
// set's journal records it under that point, so a rollback below slot undoes
// it.
func expiryRollbackBlock(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	slot uint64,
	stage func(txn *database.Txn) error,
) {
	t.Helper()
	hash := make([]byte, 32)
	binary.BigEndian.PutUint64(hash[len(hash)-8:], slot)
	point := ocommon.NewPoint(slot, hash)
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := stage(txn); err != nil {
			return err
		}
		ls.stageUtxoSet(txn, &point)
		return db.SetTip(ochainsync.Tip{Point: point}, txn)
	}))
	require.False(t, ls.utxoSetFailed.Load())
}

// expiryRollbackEmptyBlock applies a block at slot that changes no account: a
// rollback targets a block on the chain, so the scenarios put one at the slot
// they roll back to.
func expiryRollbackEmptyBlock(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	slot uint64,
) {
	t.Helper()
	expiryRollbackBlock(t, ls, db, slot, func(*database.Txn) error {
		return nil
	})
}

// expiryRollbackAccount applies a block at slot that leaves account in the
// given state.
func expiryRollbackAccount(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	slot uint64,
	account models.Account,
) {
	t.Helper()
	account.AddedSlot = slot
	expiryRollbackBlock(t, ls, db, slot, func(txn *database.Txn) error {
		ls.stageLedgerChanges(txn, LedgerChanges{Accounts: []AccountChange{{
			Slot: slot, Account: account,
		}}})
		return nil
	})
}

// expiryRollbackRenew applies a block at slot that witnesses cred, renewing
// its expiration to expiration.
func expiryRollbackRenew(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	slot uint64,
	cred []byte,
	expiration uint64,
) {
	t.Helper()
	expiryRollbackBlock(t, ls, db, slot, func(txn *database.Txn) error {
		ls.stageAccountExpirations(
			txn,
			[]models.StakeCredentialRef{models.NewStakeCredentialRef(0, cred)},
			expiration,
			slot,
		)
		return nil
	})
}

// expiryRollbackActivate runs the one-time CIP-0163 activation at the epoch
// boundary block at slot.
func expiryRollbackActivate(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	slot uint64,
	epoch uint64,
) {
	t.Helper()
	expiryRollbackBlock(t, ls, db, slot, func(txn *database.Txn) error {
		return ls.activateDelegatorInactivityIfNeeded(
			txn, epoch, slot,
		)
	})
}

// expiryRollbackAccountAt reads cred's account, inactive ones included.
func expiryRollbackAccountAt(
	t *testing.T,
	ls *LedgerState,
	cred []byte,
) *models.Account {
	t.Helper()
	acct, err := ls.ledgerDB().GetAccountByCredential(
		context.Background(), 0, cred, true, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, acct)
	return acct
}

// runRollbackRecompute rolls the LedgerDB set back to rollbackSlot from its
// journal inside one write transaction, as the rollback path does; the
// journal restores each account's expiration and the activation record.
func runRollbackRecompute(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	rollbackSlot uint64,
) {
	t.Helper()
	hash := make([]byte, 32)
	binary.BigEndian.PutUint64(hash[len(hash)-8:], rollbackSlot)
	point := ocommon.NewPoint(rollbackSlot, hash)
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		ls.stageUtxoSetRollback(txn, point)
		return nil
	}))
	require.False(t, ls.utxoSetFailed.Load())
}

// TestRecomputeAccountExpirationsAfterRollbackDropsOrphanedRenewal is the brief
// scenario: a credential witnessed in E1 (slot 150) then E2 (slot 250) has its
// expiration stamped to E2+W. Rolling back into E1 (slot 199) must drop the
// orphaned E2 renewal and restore the surviving E1 renewal: ExpirationEpoch ==
// E1 + DelegatorInactivity.
func TestRecomputeAccountExpirationsAfterRollbackDropsOrphanedRenewal(
	t *testing.T,
) {
	t.Parallel()

	const inactivity = uint64(90)
	ls, db := newExpiryRollbackTestLedgerWithEpochs(
		t, true, inactivity, expiryRollbackSmallEpochCount,
	)

	cred := renewTestCred(0x01)
	expiryRollbackAccount(t, ls, db, 150, models.Account{
		StakingKey:      cred,
		Active:          true,
		CreatedSlot:     150,
		ExpirationEpoch: 1 + inactivity,
	})
	expiryRollbackEmptyBlock(t, ls, db, 199)
	expiryRollbackRenew(t, ls, db, 250, cred, 2+inactivity)
	require.Equal(
		t, 2+inactivity, expiryRollbackAccountAt(t, ls, cred).ExpirationEpoch,
	)

	runRollbackRecompute(t, ls, db, 199)

	require.Equal(
		t, 1+inactivity, expiryRollbackAccountAt(t, ls, cred).ExpirationEpoch,
	)
}

// TestRecomputeAccountExpirationsAfterRollbackGateOff verifies that with the
// gate off neither a witness nor the rollback of its block gives an account an
// expiration.
func TestRecomputeAccountExpirationsAfterRollbackGateOff(t *testing.T) {
	t.Parallel()

	const inactivity = uint64(90)
	ls, db := newExpiryRollbackTestLedgerWithEpochs(
		t, false, inactivity, expiryRollbackSmallEpochCount,
	)

	cred := renewTestCred(0x02)
	expiryRollbackAccount(t, ls, db, 150, models.Account{
		StakingKey:  cred,
		Active:      true,
		CreatedSlot: 150,
	})
	expiryRollbackEmptyBlock(t, ls, db, 199)
	expiryRollbackBlock(t, ls, db, 250, func(txn *database.Txn) error {
		ls.renewWitnessedAccountExpirations(
			txn, 2, 250, []lcommon.Transaction{stakeDelegationTx(cred)},
		)
		return nil
	})
	require.Zero(t, expiryRollbackAccountAt(t, ls, cred).ExpirationEpoch)

	runRollbackRecompute(t, ls, db, 199)

	acct := expiryRollbackAccountAt(t, ls, cred)
	require.True(t, acct.Active)
	require.Zero(t, acct.ExpirationEpoch)
}

// TestRecomputeAccountExpirationsAfterRollbackClampsToActivationFloor: the
// one-time CIP-0163 activation stamp gives every pre-existing account
// expiration = A + W without leaving a witness. An account stamped at
// activation and then renewed by a later witness must fall back to the
// activation floor A + W when the witness is rolled away, not to 0 and not to
// its registration epoch + W.
func TestRecomputeAccountExpirationsAfterRollbackClampsToActivationFloor(
	t *testing.T,
) {
	t.Parallel()

	const (
		inactivity      = uint64(90)
		activationEpoch = uint64(500)
	)
	ls, db := newExpiryRollbackTestLedger(t, true, inactivity)

	cred := renewTestCred(0x04)
	// Registered in epoch 5 (slot 550), before activation.
	expiryRollbackAccount(t, ls, db, 550, models.Account{
		StakingKey:  cred,
		Active:      true,
		CreatedSlot: 550,
	})
	expiryRollbackActivate(t, ls, db, 50000, activationEpoch)
	expiryRollbackEmptyBlock(t, ls, db, 50150)
	require.Equal(
		t,
		activationEpoch+inactivity,
		expiryRollbackAccountAt(t, ls, cred).ExpirationEpoch,
		"activation must stamp A+W",
	)
	// A witness in epoch 505 (slot 50550) renews past the floor.
	expiryRollbackRenew(t, ls, db, 50550, cred, 505+inactivity)

	runRollbackRecompute(t, ls, db, 50150)

	require.Equal(
		t,
		activationEpoch+inactivity,
		expiryRollbackAccountAt(t, ls, cred).ExpirationEpoch,
		"must fall back to the activation floor A+W (590), not cert+W (95) or 0",
	)
	marker, err := ls.ledgerDB().GetSyncState(
		database.DelegatorInactivityActivatedSyncKey, nil,
	)
	require.NoError(t, err)
	require.Equal(t, "500", marker, "a surviving activation keeps its marker")
}

// TestRecomputeAccountExpirationsAfterRollbackResetsOrphanOnly verifies that a
// credential whose only witness is rolled away returns to no expiration.
func TestRecomputeAccountExpirationsAfterRollbackResetsOrphanOnly(
	t *testing.T,
) {
	t.Parallel()

	const inactivity = uint64(90)
	ls, db := newExpiryRollbackTestLedgerWithEpochs(
		t, true, inactivity, expiryRollbackSmallEpochCount,
	)

	cred := renewTestCred(0x03)
	expiryRollbackAccount(t, ls, db, 150, models.Account{
		StakingKey:  cred,
		Active:      true,
		CreatedSlot: 150,
	})
	expiryRollbackEmptyBlock(t, ls, db, 199)
	expiryRollbackRenew(t, ls, db, 250, cred, 2+inactivity)

	runRollbackRecompute(t, ls, db, 199)

	require.Zero(t, expiryRollbackAccountAt(t, ls, cred).ExpirationEpoch)
}

// TestRecomputeAccountExpirationsAfterRollbackActivationMembership verifies
// that rolling back below the activation boundary undoes the stamp for every
// account it touched: one with no earlier witness returns to 0, one with an
// earlier witness returns to that witness's expiration, and the marker clears
// so a re-sync activates again.
func TestRecomputeAccountExpirationsAfterRollbackActivationMembership(
	t *testing.T,
) {
	t.Parallel()

	const (
		inactivity      = uint64(90)
		activationEpoch = uint64(2)
	)
	ls, db := newExpiryRollbackTestLedgerWithEpochs(
		t, true, inactivity, expiryRollbackSmallEpochCount,
	)

	stamped := renewTestCred(0x51)
	expiryRollbackAccount(t, ls, db, 50, models.Account{
		StakingKey:  stamped,
		Active:      true,
		CreatedSlot: 50,
	})
	witnessed := renewTestCred(0x52)
	expiryRollbackAccount(t, ls, db, 150, models.Account{
		StakingKey:      witnessed,
		Active:          true,
		CreatedSlot:     150,
		ExpirationEpoch: 1 + inactivity,
	})
	expiryRollbackEmptyBlock(t, ls, db, 199)
	expiryRollbackActivate(t, ls, db, 200, activationEpoch)
	expiryRollbackRenew(t, ls, db, 250, witnessed, activationEpoch+inactivity)
	for _, cred := range [][]byte{stamped, witnessed} {
		require.Equal(
			t,
			activationEpoch+inactivity,
			expiryRollbackAccountAt(t, ls, cred).ExpirationEpoch,
		)
	}

	runRollbackRecompute(t, ls, db, 199)

	require.Zero(t, expiryRollbackAccountAt(t, ls, stamped).ExpirationEpoch,
		"activation-stamped account resets to 0 (its pre-activation value)")
	require.Equal(
		t,
		1+inactivity,
		expiryRollbackAccountAt(t, ls, witnessed).ExpirationEpoch,
		"epoch-2-witnessed account returns to its surviving epoch-1 witness, not 0",
	)
	marker, err := ls.ledgerDB().GetSyncState(
		database.DelegatorInactivityActivatedSyncKey, nil,
	)
	require.NoError(t, err)
	require.Empty(t, marker)
}

// TestRecomputeAccountExpirationsAfterRollbackRestoresPreActivationWitness
// verifies that activation overwriting a shorter witness expiration is undone
// by a rollback below the activation boundary.
func TestRecomputeAccountExpirationsAfterRollbackRestoresPreActivationWitness(
	t *testing.T,
) {
	t.Parallel()

	const (
		inactivity      = uint64(90)
		activationEpoch = uint64(2)
	)
	ls, db := newExpiryRollbackTestLedgerWithEpochs(
		t, true, inactivity, expiryRollbackSmallEpochCount,
	)
	cred := renewTestCred(0x53)
	expiryRollbackAccount(t, ls, db, 150, models.Account{
		StakingKey:      cred,
		Active:          true,
		CreatedSlot:     150,
		ExpirationEpoch: 1 + inactivity,
	})
	expiryRollbackEmptyBlock(t, ls, db, 199)
	expiryRollbackActivate(t, ls, db, 200, activationEpoch)
	require.Equal(
		t,
		activationEpoch+inactivity,
		expiryRollbackAccountAt(t, ls, cred).ExpirationEpoch,
		"activation must replace the shorter pre-activation witness expiration",
	)

	runRollbackRecompute(t, ls, db, 199)

	require.Equal(
		t,
		1+inactivity,
		expiryRollbackAccountAt(t, ls, cred).ExpirationEpoch,
		"rollback before activation must restore the epoch-(A-1) witness expiration",
	)
}

// TestRecomputeAccountExpirationsAfterRollbackDoesNotFloorAccountInactiveAtActivation
// verifies that an account deregistered before activation is not stamped, so
// rolling away its later re-registration leaves the expiration its last
// surviving witness gave it rather than the activation floor.
func TestRecomputeAccountExpirationsAfterRollbackDoesNotFloorAccountInactiveAtActivation(
	t *testing.T,
) {
	t.Parallel()

	const (
		inactivity      = uint64(90)
		activationEpoch = uint64(2)
	)
	ls, db := newExpiryRollbackTestLedgerWithEpochs(
		t, true, inactivity, expiryRollbackSmallEpochCount,
	)
	cred := renewTestCred(0x54)
	expiryRollbackAccount(t, ls, db, 50, models.Account{
		StakingKey:      cred,
		Active:          true,
		CreatedSlot:     50,
		ExpirationEpoch: inactivity,
	})
	// Deregistered in epoch 1: the witness renews, the account goes inactive.
	expiryRollbackAccount(t, ls, db, 150, models.Account{
		StakingKey:      cred,
		Active:          false,
		CreatedSlot:     50,
		ExpirationEpoch: 1 + inactivity,
	})
	expiryRollbackActivate(t, ls, db, 200, activationEpoch)
	expiryRollbackEmptyBlock(t, ls, db, 225)
	require.Equal(
		t, 1+inactivity, expiryRollbackAccountAt(t, ls, cred).ExpirationEpoch,
		"activation stamps active accounts only",
	)
	// Re-registered in epoch 2.
	expiryRollbackAccount(t, ls, db, 250, models.Account{
		StakingKey:      cred,
		Active:          true,
		CreatedSlot:     250,
		ExpirationEpoch: 2 + inactivity,
	})

	runRollbackRecompute(t, ls, db, 225)

	acct := expiryRollbackAccountAt(t, ls, cred)
	require.False(t, acct.Active)
	require.Equal(t, 1+inactivity, acct.ExpirationEpoch)
}

// TestRecomputeAccountExpirationsAfterRollbackBeforeActivation verifies that
// rolling back below the activation boundary clears both the stamp and the
// durable marker.
func TestRecomputeAccountExpirationsAfterRollbackBeforeActivation(
	t *testing.T,
) {
	t.Parallel()

	const inactivity = uint64(90)
	ls, db := newExpiryRollbackTestLedgerWithEpochs(
		t, true, inactivity, expiryRollbackSmallEpochCount,
	)
	cred := renewTestCred(0x44)
	expiryRollbackAccount(t, ls, db, 150, models.Account{
		StakingKey:  cred,
		Active:      true,
		CreatedSlot: 150,
	})
	expiryRollbackEmptyBlock(t, ls, db, 199)
	expiryRollbackActivate(t, ls, db, 200, 2)
	require.Equal(
		t, 2+inactivity, expiryRollbackAccountAt(t, ls, cred).ExpirationEpoch,
	)

	runRollbackRecompute(t, ls, db, 199) // epoch 1, before activation epoch 2

	require.Zero(t, expiryRollbackAccountAt(t, ls, cred).ExpirationEpoch)
	marker, err := ls.ledgerDB().GetSyncState(
		database.DelegatorInactivityActivatedSyncKey,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, marker)
}
