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

package governance

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func markMithrilBootstrapped(t *testing.T, db *database.Database) {
	t.Helper()
	require.NoError(t, db.SetSyncState("mithril_ledger_slot", "150", nil))
}

func purposeRootsProcessEpoch(
	t *testing.T,
	db *database.Database,
) (*EpochOutput, error) {
	t.Helper()
	txn := db.MetadataTxn(true)
	defer txn.Release()
	return ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    stabilityTestEpoch - 1,
		NewEpoch:     stabilityTestEpoch,
		BoundarySlot: stabilityTestEpoch * 100,
		PParams:      chainTestPParams(),
		UpdateFn:     eras.PParamsUpdateConway,
	})
}

// unseededChild is a chained proposal whose parent exists nowhere in the
// database, the shape a missing snapshot root leaves behind.
func unseededChild(t *testing.T) (*models.GovernanceProposal, *models.GovernanceProposal) {
	t.Helper()
	phantom := &models.GovernanceProposal{TxHash: testBytes(32, 0x61)}
	child := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0x62), phantom,
		400, 0, testBytes(29, 0), chainTestParameterChange(t, 62),
	)
	return phantom, child
}

func TestProcessEpochMithrilMissingPurposeRootFails(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	require.NoError(t, store.SetNetworkState(10, 20, 1, nil))
	markMithrilBootstrapped(t, db)
	_, child := unseededChild(t)
	chainTestStore(t, db, child)

	_, err := purposeRootsProcessEpoch(t, db)
	require.ErrorIs(t, err, ErrMissingEnactedRoot)
	var typed *MissingEnactedRootError
	require.ErrorAs(t, err, &typed)
	assert.Equal(t, child.TxHash, typed.TxHash)
	assert.Equal(t, child.ParentTxHash, typed.ParentTxHash)

	require.ErrorIs(t, VerifyPurposeRoots(
		db, nil, stabilityTestEpoch-1,
	), ErrMissingEnactedRoot)
}

func TestProcessEpochGenesisSyncedRootlessPurposeSkips(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	require.NoError(t, store.SetNetworkState(10, 20, 1, nil))
	_, child := unseededChild(t)
	stored := chainTestStore(t, db, child)

	out, err := purposeRootsProcessEpoch(t, db)
	require.NoError(t, err)
	assert.Equal(t, 0, out.RatifiedCount)
	assert.Nil(t, chainTestReload(t, db, stored[0]).RatifiedEpoch)
	require.NoError(t, VerifyPurposeRoots(db, nil, stabilityTestEpoch-1))
}

func TestProcessEpochMithrilPendingSiblingParentIsNotAnError(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	require.NoError(t, store.SetNetworkState(10, 20, 1, nil))
	markMithrilBootstrapped(t, db)
	parent := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0x72), nil,
		400, 0, testBytes(29, 0), chainTestParameterChange(t, 61),
	)
	child := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0x71), parent,
		401, 0, testBytes(29, 0), chainTestParameterChange(t, 62),
	)
	stored := chainTestStore(t, db, parent, child)
	seedHardForkCommitteeAndSPOVotes(t, db, store, stored...)

	out, err := purposeRootsProcessEpoch(t, db)
	require.NoError(t, err)
	assert.Equal(t, 2, out.RatifiedCount)
	require.NoError(t, VerifyPurposeRoots(db, nil, stabilityTestEpoch-1))
}

func TestProcessEpochMithrilSupersededParentKeepsSkip(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	require.NoError(t, store.SetNetworkState(10, 20, 1, nil))
	markMithrilBootstrapped(t, db)
	expiredEpoch := uint64(stabilityTestEpoch - 3)
	parent := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0x72), nil,
		400, 0, testBytes(29, 0), chainTestParameterChange(t, 61),
	)
	parent.ExpiresEpoch = expiredEpoch
	parent.ExpiredEpoch = &expiredEpoch
	child := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0x71), parent,
		401, 0, testBytes(29, 0), chainTestParameterChange(t, 62),
	)
	stored := chainTestStore(t, db, parent, child)

	out, err := purposeRootsProcessEpoch(t, db)
	require.NoError(t, err)
	assert.Equal(t, 0, out.RatifiedCount)
	assert.Nil(t, chainTestReload(t, db, stored[1]).RatifiedEpoch)
	require.NoError(t, VerifyPurposeRoots(db, nil, stabilityTestEpoch-1))
}
