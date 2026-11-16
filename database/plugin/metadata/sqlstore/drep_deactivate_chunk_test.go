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
	"github.com/stretchr/testify/require"
)

// TestDeactivateDrepsFitsAnEvenParameterLimit deactivates more DReps than one
// chunk holds under an even parameter limit, where the delegator statement
// binds the removal slot beside two parameters per DRep.
func TestDeactivateDrepsFitsAnEvenParameterLimit(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	d := store.dialect.(dialect)
	d.parameterLimit = 4
	store.dialect = d

	var credentials []models.StakeCredentialRef
	for i := range 5 {
		key := []byte{byte(i + 1)}
		_, err := store.writeDB.Exec(`INSERT INTO drep
 (credential_tag, credential, added_slot, active) VALUES (0, ?, 5, TRUE)`, key)
		require.NoError(t, err)
		_, err = store.writeDB.Exec(`INSERT INTO drep_delegator
 (drep_credential_tag, drep_credential, stake_credential_tag, stake_credential, added_slot)
 VALUES (0, ?, 0, ?, 5)`, key, []byte{0x80 + byte(i)})
		require.NoError(t, err)
		credentials = append(credentials, models.NewStakeCredentialRef(0, key))
	}
	limitSQLiteVariableNumber(t, store.writeDB, d.ParameterLimit())

	require.NoError(t, store.DeactivateDreps(nil, credentials, 9))

	active, err := store.GetActiveDreps(nil)
	require.NoError(t, err)
	require.Empty(t, active)
	links := 0
	require.NoError(t, store.IterateLiveDrepDelegators(nil,
		func(_, _ models.StakeCredentialRef) error {
			links++
			return nil
		}))
	require.Zero(t, links, "every delegator link of a deactivated DRep is closed")
}
