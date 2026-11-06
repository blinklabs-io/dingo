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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/drepquery"
	"github.com/stretchr/testify/require"
)

func TestUtxoStakingPartialIndexSupportsVotingQueries(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	for _, epoch := range []uint64{0, 100} {
		single, _ := drepquery.VotingPowerSQL("sqlite", 0, []byte{1}, epoch)
		for _, query := range []string{single, drepquery.VotingPowerBatchSQL("sqlite", epoch), drepquery.VotingPowerByTypeSQL("sqlite", epoch)} {
			// Collection placeholders normally expand through the caller's binder.
			stmt, err := store.writeDB.Prepare(
				strings.ReplaceAll(query, " IN ?", " IN (?)"),
			)
			require.NoError(t, err)
			require.NoError(t, stmt.Close())
		}
	}
}
