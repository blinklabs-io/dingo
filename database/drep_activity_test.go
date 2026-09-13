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

package database_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

func TestUpdateDRepActivityExpiryBounds(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	for i, tc := range []struct {
		name                 string
		activity, inactivity uint64
		valid                bool
	}{
		{"zero", 0, 0, true},
		{"below_storage_maximum", math.MaxInt64 - 2, 1, true},
		{"exact_storage_maximum", math.MaxInt64 - 1, 1, true},
		{"maximum_activity", math.MaxInt64, 0, true},
		{"one_past_storage_maximum", math.MaxInt64, 1, false},
		{"inactivity_outside_storage", 0, math.MaxInt64 + 1, false},
		{"exact_unsigned_maximum", 0, math.MaxUint64, false},
		{"unsigned_wrap_to_zero", 1, math.MaxUint64, false},
		{"unsigned_wrap_to_valid_epoch", math.MaxInt64, math.MaxUint64, false},
		{"activity_outside_storage", math.MaxUint64, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			credential := bytes.Repeat([]byte{byte(i + 1)}, 28)
			require.NoError(t, db.CreateDrep(nil, &models.Drep{
				CredentialTag: 1, Credential: credential, Active: true,
				LastActivityEpoch: 10, ExpiryEpoch: 20,
			}))
			err := db.UpdateDRepActivity(
				1,
				credential,
				tc.activity,
				tc.inactivity,
				nil,
			)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err, "out-of-range DRep expiry was persisted")
			}
			got, err := db.GetDrepByCredential(1, credential, true, nil)
			require.NoError(t, err)
			if tc.valid {
				require.Equal(t, tc.activity, got.LastActivityEpoch)
				require.Equal(t, tc.activity+tc.inactivity, got.ExpiryEpoch)
			} else {
				require.Equal(t, uint64(10), got.LastActivityEpoch, "failed activity update mutated existing state")
				require.Equal(t, uint64(20), got.ExpiryEpoch, "failed activity update mutated existing expiry")
			}
		})
	}
}
