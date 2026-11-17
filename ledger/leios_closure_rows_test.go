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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/leiosclosurefixture"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// The recorded rows in leiosclosurefixture/testdata are this apply's output;
// the IndexerDB writer's test holds its replay to the same files.
func TestLiveClosureApplyWritesRecordedRows(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"mid_epoch", "boundary"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			scenario, err := leiosclosurefixture.New()
			require.NoError(t, err)
			ls, db, raw, _ := newLeiosAPIApplyTestLedger(t)
			scenario.Seed(t, db)
			var contextSlot *uint64
			if name == "boundary" {
				slot := leiosclosurefixture.BoundaryContextSlot
				contextSlot = &slot
			}
			point := ocommon.Point{
				Slot: leiosclosurefixture.CertifyingSlot,
				Hash: scenario.Certifying.Hash().Bytes(),
			}
			var applied int
			require.NoError(t, db.Transaction(t.Context(), true).Do(
				func(txn *database.Txn) error {
					var err error
					applied, _, err = ls.applyEndorserBlockInContext(
						t.Context(), txn, point,
						scenario.Certifying.BlockNumber(),
						leiosclosurefixture.AnnouncingSlot,
						scenario.EBHash.Bytes(), scenario.Raw, contextSlot,
					)
					return err
				}))
			require.Equal(t, 2, applied)
			got := leiosclosurefixture.Project(t, raw)
			t.Logf("rows:\n%s", got)
			require.Equal(t, leiosclosurefixture.Golden(t, name), got)
		})
	}
}
