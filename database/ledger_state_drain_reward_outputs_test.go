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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

// The account reward history reads reward_account_output, which a drained
// reward-outputs family no longer holds, so the reads refuse rather than
// report an empty history, and no statement reaches the table.
func TestRewardHistoryReadsRefuseDrainedRewardOutputs(t *testing.T) {
	t.Parallel()
	db, guard, _ := newDrainedTruncateDB(
		t, nodesettings.FamilyRewardOutputs,
	)
	key := bytes.Repeat([]byte{0x31}, 28)

	_, err := db.GetRewardAccountOutputsByCredential(
		t.Context(), 0, key, 10, 0, "asc", nil,
	)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
	_, err = db.CountRewardAccountOutputsByCredential(
		t.Context(), 0, key, nil,
	)
	require.ErrorIs(t, err, database.ErrFamilyDrained)

	require.Empty(t, guard.Trips(), "the refusal reached the table")
}
