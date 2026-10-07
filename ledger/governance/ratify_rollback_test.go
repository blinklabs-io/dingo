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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errRollbackTestUpdate = errors.New("rejected parameter update")

// rollbackTestUpdateFn applies Conway updates, rejecting the update whose
// MotionNoConfidence is rejectNum/100 on every call after the first
// allowedCalls of it.
func rollbackTestUpdateFn(
	rejectNum int64,
	allowedCalls int,
) func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error) {
	calls := 0
	return func(
		pparams lcommon.ProtocolParameters,
		update any,
	) (lcommon.ProtocolParameters, error) {
		if u, ok := update.(conway.ConwayProtocolParameterUpdate); ok &&
			u.DRepVotingThresholds != nil &&
			u.DRepVotingThresholds.MotionNoConfidence.Cmp(
				newRat(rejectNum, 100).Rat,
			) == 0 {
			calls++
			if calls > allowedCalls {
				return nil, errRollbackTestUpdate
			}
		}
		return eras.PParamsUpdateConway(pparams, update)
	}
}

func rollbackTestRunEpoch(
	t *testing.T,
	db *database.Database,
	newEpoch uint64,
	updateFn func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error),
) (*EpochOutput, error) {
	t.Helper()
	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    newEpoch - 1,
		NewEpoch:     newEpoch,
		BoundarySlot: newEpoch * 100,
		PParams:      chainTestPParams(),
		UpdateFn:     updateFn,
	})
	if err != nil {
		return nil, err
	}
	require.NoError(t, txn.Commit())
	return out, nil
}

// TestProcessEpochFailedChildLeavesStagedStateUnchanged pins that a later
// ParameterChange failing its enactment precondition contributes nothing to
// RATIFY's staged state. The failed child is not accepted and does not
// advance the purpose root, so its sibling, submitted later against the same
// accepted parent, is still accepted; ENACT then publishes the parent and that
// sibling only.
func TestProcessEpochFailedChildLeavesStagedStateUnchanged(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	require.NoError(t, store.SetNetworkState(10, 20, 1, nil))
	parent := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0xF1), nil,
		400, 0, testBytes(29, 0), chainTestParameterChange(t, 61),
	)
	parent = chainTestStore(t, db, parent)[0]
	failing := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0xF2), parent,
		500, 0, testBytes(29, 0), chainTestParameterChange(t, 99),
	)
	sibling := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0xF3), parent,
		600, 0, testBytes(29, 0), chainTestParameterChange(t, 63),
	)
	all := chainTestStore(t, db, failing, sibling)
	seedHardForkCommitteeAndSPOVotes(t, db, store, parent, all[0], all[1])

	updateFn := rollbackTestUpdateFn(99, 0)
	out, err := rollbackTestRunEpoch(t, db, stabilityTestEpoch, updateFn)
	require.NoError(t, err)
	assert.Equal(t, 2, out.RatifiedCount)
	assert.NotNil(t, chainTestReload(t, db, parent).RatifiedEpoch)
	assert.Nil(t, chainTestReload(t, db, failing).RatifiedEpoch)
	assert.NotNil(
		t,
		chainTestReload(t, db, sibling).RatifiedEpoch,
		"the failed child must not take the purpose root",
	)

	enactOut, err := rollbackTestRunEpoch(t, db, stabilityTestEpoch+1, updateFn)
	require.NoError(t, err)
	assert.Equal(t, 2, enactOut.EnactedCount)
	assert.Equal(
		t,
		newRat(63, 100),
		chainTestMotionNoConfidence(t, enactOut.UpdatedPParams),
	)
}

// TestProcessEpochRatifyErrorWritesNoVerdicts pins RATIFY's atomicity: when a
// later action fails with an error after an earlier one was accepted, the
// boundary fails and no verdict reaches the database, not even through the
// boundary's own transaction. A deferred Decide runs on a read snapshot, so
// the decision must be complete before anything is written.
func TestProcessEpochRatifyErrorWritesNoVerdicts(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	require.NoError(t, store.SetNetworkState(10, 20, 1, nil))
	parent := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0xF4), nil,
		400, 0, testBytes(29, 0), chainTestParameterChange(t, 61),
	)
	parent = chainTestStore(t, db, parent)[0]
	child := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0xF5), parent,
		500, 0, testBytes(29, 0), chainTestParameterChange(t, 98),
	)
	child = chainTestStore(t, db, child)[0]
	seedHardForkCommitteeAndSPOVotes(t, db, store, parent, child)

	txn := db.MetadataTxn(true)
	defer txn.Release()
	// The child's update passes its enactment precondition and then fails
	// while RATIFY stages it, after the parent was accepted.
	_, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    stabilityTestEpoch - 1,
		NewEpoch:     stabilityTestEpoch,
		BoundarySlot: stabilityTestEpoch * 100,
		PParams:      chainTestPParams(),
		UpdateFn:     rollbackTestUpdateFn(98, 1),
	})
	require.ErrorIs(t, err, errRollbackTestUpdate)
	for _, proposal := range []*struct {
		name string
		id   []byte
	}{{"parent", parent.TxHash}, {"child", child.TxHash}} {
		inTxn, err := db.GetGovernanceProposal(proposal.id, 0, txn)
		require.NoError(t, err)
		assert.Nil(t, inTxn.RatifiedEpoch, proposal.name)
	}
}
