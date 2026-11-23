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

package utxoset_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// The verification of a migrated store compares the incomplete stake input
// record, the delegator inactivity activation with every credential it
// stamped, and the pending ratification record: a copy of any of them that
// differs from the store's is reported.
func TestVerifyFamilyComparesTheStateMarks(t *testing.T) {
	t.Parallel()
	const (
		activationEpoch = uint64(5)
		pendingInputs   = `{"entries":[{"epoch":7,"boundary_slot":700}]}`
		pendingRatify   = `{"epoch":7,"boundary_slot":700,"id":3}`
	)
	ctx := t.Context()
	db := ledgerAt(t, slotPoint(800))
	meta := db.Metadata()
	require.NoError(t, db.SetEpoch(
		500, activationEpoch, nil, nil, nil, nil, 6, 1000, 100, nil,
	))
	stamped := bytes.Repeat([]byte{0x41}, 28)
	unstamped := bytes.Repeat([]byte{0x42}, 28)
	require.NoError(t, db.CreateAccount(ctx, nil, &models.Account{
		StakingKey: stamped, Active: true,
	}))
	_, err := meta.StampAllActiveAccountExpirations(activationEpoch+4, nil)
	require.NoError(t, err)
	require.NoError(t, db.CreateAccount(ctx, nil, &models.Account{
		StakingKey: unstamped, Active: true,
	}))
	for key, value := range map[string]string{
		database.DelegatorInactivityActivatedSyncKey: "5",
		statedrain.RewardStakeInputsPendingSyncKey:   pendingInputs,
		statedrain.PendingRatificationSyncKey:        pendingRatify,
	} {
		require.NoError(t, meta.SetSyncState(key, value, nil))
	}

	set := openRecoverySet(t, utxoset.Config{})
	ldb := dbadapter.New(db)
	require.NoError(t, set.LoadLedgerState(ctx, ldb))
	families := []nodesettings.Family{
		nodesettings.FamilyRewardInputs,
		nodesettings.FamilyAccounts,
		nodesettings.FamilyGovernance,
	}
	verify := func(f nodesettings.Family) error {
		return ledger.VerifyFamily(ctx, f, ldb, set)
	}
	for _, f := range families {
		require.NoError(t, verify(f), "family %s", f)
	}

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := raw.Exec(query, args...)
		require.NoError(t, err)
	}
	setState := func(key, value string) {
		t.Helper()
		require.NoError(t, meta.SetSyncState(key, value, nil))
	}
	for _, tc := range []struct {
		name    string
		family  nodesettings.Family
		differ  func()
		restore func()
		reports string
	}{
		{
			name:   "an incomplete snapshot the set does not mark",
			family: nodesettings.FamilyRewardInputs,
			differ: func() {
				setState(
					statedrain.RewardStakeInputsPendingSyncKey,
					`{"entries":[{"epoch":7,"boundary_slot":700},`+
						`{"epoch":8,"boundary_slot":800}]}`,
				)
			},
			restore: func() {
				setState(
					statedrain.RewardStakeInputsPendingSyncKey, pendingInputs,
				)
			},
			reports: "pending stake inputs differ",
		},
		{
			name:   "a snapshot the set marks and the store does not",
			family: nodesettings.FamilyRewardInputs,
			differ: func() {
				setState(
					statedrain.RewardStakeInputsPendingSyncKey,
					`{"entries":[]}`,
				)
			},
			restore: func() {
				setState(
					statedrain.RewardStakeInputsPendingSyncKey, pendingInputs,
				)
			},
			reports: "pending stake inputs differ",
		},
		{
			name:   "another activation epoch",
			family: nodesettings.FamilyAccounts,
			differ: func() {
				setState(database.DelegatorInactivityActivatedSyncKey, "6")
			},
			restore: func() {
				setState(database.DelegatorInactivityActivatedSyncKey, "5")
			},
			reports: "delegator inactivity activation differs",
		},
		{
			name:   "a stamped credential the set lacks",
			family: nodesettings.FamilyAccounts,
			differ: func() {
				exec(
					`INSERT INTO account_inactivity_activation`+
						`(credential_tag, staking_key) VALUES (0, ?)`,
					unstamped,
				)
			},
			restore: func() {
				exec(
					`DELETE FROM account_inactivity_activation`+
						` WHERE staking_key = ?`, unstamped,
				)
			},
			reports: "stamped differ",
		},
		{
			name:   "another pending ratification record",
			family: nodesettings.FamilyGovernance,
			differ: func() {
				setState(
					statedrain.PendingRatificationSyncKey,
					`{"epoch":7,"boundary_slot":700,"id":4}`,
				)
			},
			restore: func() {
				setState(statedrain.PendingRatificationSyncKey, pendingRatify)
			},
			reports: "pending ratification record differs",
		},
		{
			name:   "a pending ratification record only the set holds",
			family: nodesettings.FamilyGovernance,
			differ: func() {
				require.NoError(t, meta.DeleteSyncState(
					statedrain.PendingRatificationSyncKey, nil,
				))
			},
			restore: func() {
				setState(statedrain.PendingRatificationSyncKey, pendingRatify)
			},
			reports: "pending ratification record differs",
		},
	} {
		tc.differ()
		require.ErrorContains(t, verify(tc.family), tc.reports, tc.name)
		tc.restore()
		require.NoError(t, verify(tc.family), tc.name)
	}

	// A store that never recorded the stamp lists no credential, while the
	// set marks the accounts its load found active before the activation.
	exec(
		`DELETE FROM account_inactivity_activation WHERE staking_key = ?`,
		stamped,
	)
	require.NoError(t, verify(nodesettings.FamilyAccounts))
}
