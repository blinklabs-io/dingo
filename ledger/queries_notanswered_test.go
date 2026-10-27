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

package ledger

import (
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// TestQueryNotAnswered fails each query Dingo cannot answer faithfully with
// ErrQueryNotAnswered, naming the query and what is missing, whether it is
// sent directly or inside GetCBOR. The reply to a failed query is the end of
// the client's session, so the error is what the connection log carries.
func TestQueryNotAnswered(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.publishSnapshotsLocked()
	tests := map[string]struct {
		query   any
		missing string
	}{
		"GetNonMyopicMemberRewards": {
			query:   &olocalstatequery.ShelleyNonMyopicMemberRewardsQuery{},
			missing: "performance",
		},
		"GetRewardInfoPools": {
			query:   &olocalstatequery.ShelleyRewardInfoPoolsQuery{},
			missing: "performance",
		},
		"DebugEpochState": {
			query:   &olocalstatequery.ShelleyDebugEpochStateQuery{},
			missing: "epoch state",
		},
		"DebugNewEpochState": {
			query:   &olocalstatequery.ShelleyDebugNewEpochStateQuery{},
			missing: "epoch state",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			for wrapName, wrap := range map[string]func(any) any{
				"direct": func(q any) any { return q },
				"GetCBOR": func(q any) any {
					return &olocalstatequery.ShelleyCborQuery{Query: q}
				},
			} {
				t.Run(wrapName, func(t *testing.T) {
					_, err := ls.Query(
						t.Context(),
						shelleyBlockQuery(wrap(tt.query)),
						QueryPoint{},
					)
					require.ErrorIs(t, err, ErrQueryNotAnswered)
					require.ErrorContains(t, err, name)
					require.ErrorContains(t, err, tt.missing)
				})
			}
		})
	}
}
