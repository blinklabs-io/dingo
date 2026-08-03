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

package database

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGetPoolStakeSnapshotsByEpoch_RejectsInvalidSnapshotType verifies a
// typo'd or stale snapshot type fails fast with an error instead of
// silently querying and returning zero rows.
func TestGetPoolStakeSnapshotsByEpoch_RejectsInvalidSnapshotType(t *testing.T) {
	db := newTestDB(t)
	_, err := db.GetPoolStakeSnapshotsByEpoch(1, "marc", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid snapshot type")
}

// TestGetPoolStakeSnapshotsByEpoch_AcceptsKnownTypes verifies every known
// snapshot type passes validation and reaches the query layer (returning an
// empty, not an error, result for an epoch with no rows).
func TestGetPoolStakeSnapshotsByEpoch_AcceptsKnownTypes(t *testing.T) {
	db := newTestDB(t)
	for _, snapshotType := range []string{"mark", "set", "go", "actv"} {
		rows, err := db.GetPoolStakeSnapshotsByEpoch(1, snapshotType, nil)
		require.NoError(t, err, "snapshot type %q must be accepted", snapshotType)
		require.Empty(t, rows)
	}
}
