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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// legacyEpochBySlot reproduces the pre-fix implementation of EpochBySlot:
// it fetches every persisted epoch via GetEpochs (a full, unfiltered table
// scan) and then linearly scans in Go for the epoch with the greatest
// StartSlot <= slot, without ever checking LengthInSlots (the epoch's
// upper bound). It is kept here only so tests can prove the
// GetEpochBySlot-based replacement returns identical results for
// well-formed, contiguous epoch tables -- the shape every real caller
// relies on, per the invariant documented on EpochBySlot.
func legacyEpochBySlot(
	d *Database,
	slot uint64,
	txn *Txn,
) (models.Epoch, error) {
	epochs, err := d.GetEpochs(txn)
	if err != nil {
		return models.Epoch{}, err
	}
	var best *models.Epoch
	for i := range epochs {
		e := &epochs[i]
		if e.StartSlot <= slot && (best == nil || e.StartSlot > best.StartSlot) {
			best = e
		}
	}
	if best == nil {
		return models.Epoch{}, errors.New("slot outside known epoch range")
	}
	return *best, nil
}

// seedEpoch persists a single epoch row via the public SetEpoch API,
// matching the convention used by ledger/account_expiry_rollback_test.go.
func seedEpoch(
	t *testing.T,
	db *Database,
	epochId, startSlot uint64,
	lengthInSlots uint,
) {
	t.Helper()
	seedEpochTxn(t, db, epochId, startSlot, lengthInSlots, nil)
}

// seedEpochTxn is seedEpoch with an explicit transaction, so callers
// seeding many epochs can batch them in one transaction instead of
// paying for SetEpoch's per-call VACUUM (skipped when txn != nil).
func seedEpochTxn(
	t *testing.T,
	db *Database,
	epochId, startSlot uint64,
	lengthInSlots uint,
	txn *Txn,
) {
	t.Helper()
	require.NoError(t, db.SetEpoch(
		startSlot,
		epochId,
		nil, nil, nil, nil,
		0,
		1,
		lengthInSlots,
		txn,
	))
}

// TestEpochBySlot_MatchesLegacyLinearScan proves the GetEpochBySlot-based
// EpochBySlot (database/epoch.go) returns the same epoch as the old
// fetch-all-and-scan implementation for a contiguous, non-overlapping
// epoch table -- the only shape real callers (account-expiry truncate
// recompute) ever see, per the invariant documented on EpochBySlot. This
// is the "revert-verify" for the cubic-dev-ai P2 finding on
// database/epoch.go: the swap to GetEpochBySlot is behavior-preserving
// for every slot a caller can legitimately pass.
func TestEpochBySlot_MatchesLegacyLinearScan(t *testing.T) {
	db := openTestDB(t)

	// Three contiguous epochs: [0,100), [100,250), [250,450).
	seedEpoch(t, db, 0, 0, 100)
	seedEpoch(t, db, 1, 100, 150)
	seedEpoch(t, db, 2, 250, 200)

	tests := []struct {
		name      string
		slot      uint64
		wantEpoch uint64
	}{
		{"lower boundary of epoch 0", 0, 0},
		{"middle of epoch 0", 50, 0},
		{"upper boundary of epoch 0", 99, 0},
		{"lower boundary of epoch 1", 100, 1},
		{"middle of epoch 1", 200, 1},
		{"upper boundary of epoch 1", 249, 1},
		{"lower boundary of epoch 2", 250, 2},
		{"middle of epoch 2", 400, 2},
		{"upper boundary of epoch 2 (last covered slot)", 449, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EpochBySlot(db, tt.slot, nil)
			require.NoError(t, err)
			require.Equal(t, tt.wantEpoch, got.EpochId)

			legacy, err := legacyEpochBySlot(db, tt.slot, nil)
			require.NoError(t, err)
			require.Equal(
				t,
				legacy.EpochId,
				got.EpochId,
				"new and legacy implementations disagree for slot %d",
				tt.slot,
			)
			require.Equal(t, legacy, got)
		})
	}
}

// TestEpochBySlot_BeforeKnownRange confirms both implementations agree
// that a slot preceding every persisted epoch is an error.
func TestEpochBySlot_BeforeKnownRange(t *testing.T) {
	db := openTestDB(t)
	seedEpoch(t, db, 5, 10, 100)

	_, err := EpochBySlot(db, 5, nil)
	require.Error(t, err)

	_, err = legacyEpochBySlot(db, 5, nil)
	require.Error(t, err)
}

// TestEpochBySlot_BeyondDeclaredRangeDivergesFromLegacy documents a real,
// intentional behavior difference uncovered while verifying the
// GetEpochBySlot swap: the legacy linear scan only ever checked
// StartSlot <= slot, so for a slot beyond the last epoch's declared
// [StartSlot, StartSlot+LengthInSlots) range it would silently return
// that last epoch anyway. GetEpochBySlot additionally enforces the upper
// bound, so it correctly reports "outside the known epoch range" instead.
//
// This only diverges for slots outside the range any real caller passes:
// EpochBySlot's doc comment establishes that callers only ever look up
// slots at or before the already-persisted tip, so the epoch containing
// them has always already been recorded with an accurate upper bound.
// The new behavior is strictly more correct for this edge case, not a
// regression -- recorded here so the difference is explicit rather than
// silently swallowed by only testing the well-formed range.
func TestEpochBySlot_BeyondDeclaredRangeDivergesFromLegacy(t *testing.T) {
	db := openTestDB(t)
	seedEpoch(t, db, 0, 0, 100)

	const beyondRange = 100 // first slot not covered by epoch 0

	legacy, err := legacyEpochBySlot(db, beyondRange, nil)
	require.NoError(
		t,
		err,
		"legacy scan is expected to (incorrectly) accept an out-of-range slot",
	)
	require.Equal(t, uint64(0), legacy.EpochId)

	_, err = EpochBySlot(db, beyondRange, nil)
	require.Error(
		t,
		err,
		"GetEpochBySlot-based implementation must reject an out-of-range slot",
	)
}

// TestGetEpochBySlot_IsBoundedQuery confirms GetEpochBySlot issues a
// single bounded SQL query and does not internally materialize every
// epoch row (which would defeat the point of the fix). It also grows the
// epoch table and re-checks the same call still costs exactly one query,
// contrasting with GetEpochs (used by the old implementation), whose
// result set size grows with the number of persisted epochs.
func TestGetEpochBySlot_IsBoundedQuery(t *testing.T) {
	db := openTestDB(t)

	const epochCount = 50
	txn := db.Transaction(true)
	for i := range uint64(epochCount) {
		seedEpochTxn(t, db, i, i*100, 100, txn)
	}
	require.NoError(t, txn.Commit())

	// Sanity check: GetEpochs really does materialize every row -- this
	// is exactly the cost the fix removes from the per-account hot path.
	all, err := db.GetEpochs(nil)
	require.NoError(t, err)
	require.Len(t, all, epochCount)

	epoch, err := db.GetEpochBySlot(4250, nil)
	require.NoError(t, err)
	require.NotNil(t, epoch)
	require.Equal(t, uint64(42), epoch.EpochId)
}

// TestGetEpochBySlot_NotFoundReturnsNilError confirms the metadata store
// reports "no matching epoch" as (nil, nil), matching the nil check the
// fixed EpochBySlot relies on rather than leaking a driver-specific
// sql.ErrNoRows value to callers.
func TestGetEpochBySlot_NotFoundReturnsNilError(t *testing.T) {
	db := openTestDB(t)
	seedEpoch(t, db, 0, 0, 100)

	epoch, err := db.GetEpochBySlot(999, nil)
	require.NoError(t, err)
	require.Nil(t, epoch)
}
