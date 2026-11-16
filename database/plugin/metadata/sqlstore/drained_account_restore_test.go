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

package sqlstore

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

// An account rollback lists the credentials it restored under the reward
// eligibility recheck key only while the reward-outputs family still lives in
// the store: once it is drained that key is the set's to keep, and a write
// would leave a key no drain deletes again.
func TestAccountRestoreWritesNoRecheckKeyOnceRewardOutputsAreDrained(
	t *testing.T,
) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		drained bool
	}{
		{"undrained records the credential", false},
		{"drained writes nothing", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newManagementTestStore(t)
			key := snapshotStakingKey(0x6c)
			importSnapshotAccount(t, store, key, nil, nil, 0, 100)
			execAccountSQL(t, store, `
INSERT INTO deregistration (staking_key, credential_tag, added_slot)
VALUES (?, 0, 200)`, key)
			execAccountSQL(t, store, `
UPDATE account SET active = 0, added_slot = 200
WHERE credential_tag = 0 AND staking_key = ?`, key)
			if tc.drained {
				store.SetDrainedFamilies(
					nodesettings.FamilyMask(0).
						With(nodesettings.FamilyRewardOutputs),
				)
			}

			require.NoError(t, store.RestoreAccountStateAtSlot(150, nil))

			raw, err := store.GetSyncState(
				models.RewardEligibilityRecheckKey, nil,
			)
			require.NoError(t, err)
			if tc.drained {
				require.Empty(t, raw)
				return
			}
			require.NotEmpty(t, raw)
		})
	}
}
