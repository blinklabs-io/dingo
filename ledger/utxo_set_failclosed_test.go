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

package ledger_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// failingSet fails every tip-moving apply from the failAt-th on and records
// the tips it was asked to apply.
type failingSet struct {
	*utxoset.Set
	failAt int
	mu     sync.Mutex
	tips   []ocommon.Point
}

func (f *failingSet) ApplyUtxoChanges(
	tip *ocommon.Point,
	changes []ledger.UtxoEvent,
) error {
	return f.ApplyLedgerChanges(tip, ledger.LedgerChanges{Utxo: changes})
}

func (f *failingSet) ApplyLedgerChanges(
	tip *ocommon.Point,
	lc ledger.LedgerChanges,
) error {
	if tip != nil {
		f.mu.Lock()
		f.tips = append(f.tips, *tip)
		n := len(f.tips)
		f.mu.Unlock()
		if n >= f.failAt {
			return errors.New("injected apply failure")
		}
	}
	return f.Set.ApplyLedgerChanges(tip, lc)
}

// A failed update of an authoritative live UTxO set halts the ledger through
// FatalErrorFunc, records the failed slot in the set, and stops validation
// reading around it. Verify mode keeps the fallback: it only stops following
// the ledger, with no halt and no mark.
func TestFailedUtxoSetUpdateHaltsOnlyWhenTheSetIsAuthoritative(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		reads   string
		only    bool
		authoty bool
	}{
		{"set reads", ledger.UtxoSetReadsSet, false, true},
		{"set only", ledger.UtxoSetReadsOff, true, true},
		{"verify reads", ledger.UtxoSetReadsVerify, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inner, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = inner.Close() })
			set := &failingSet{Set: inner, failAt: 2}
			var (
				mu    sync.Mutex
				fatal []error
				db    *database.Database
			)
			scenario := ledger.RunUtxoSetFailureScenarioForTest(
				t, set, tc.reads, tc.only,
				func(err error) {
					mu.Lock()
					fatal = append(fatal, err)
					mu.Unlock()
				},
				func(d *database.Database) { db = d },
			)
			require.True(t, scenario.UtxoSetFailed())
			require.GreaterOrEqual(t, len(set.tips), 2,
				"the scenario made no commit after the injected failure")
			failedSlot := set.tips[1].Slot

			mu.Lock()
			halts := len(fatal)
			mu.Unlock()
			slot, marked, err := inner.FailedAt()
			require.NoError(t, err)
			if !tc.authoty {
				require.Zero(t, halts, "verify mode must keep its fallback")
				require.False(t, marked, "verify mode must not mark the set")
				return
			}
			require.Equal(t, 1, halts, "the failure must halt the ledger once")
			require.ErrorContains(t, fatal[0], "injected apply failure")
			require.True(t, marked, "the failed slot was not recorded")
			require.Equal(t, failedSlot, slot)
			tip, ok, err := inner.Tip()
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, set.tips[0].Slot, tip.Slot,
				"the set must stay at the last commit it took")

			if tc.only {
				return
			}
			// Validation no longer reads the metadata store around the set.
			var refs []models.UtxoId
			require.NoError(t, dbadapter.New(db).IterateLiveUtxos(
				t.Context(), nil,
				func(u *models.Utxo) error {
					refs = append(
						refs,
						models.UtxoId{Hash: u.TxId, Idx: u.OutputIdx},
					)
					return nil
				},
			))
			require.NotEmpty(t, refs)
			_, err = scenario.ReadsForValidation(refs)
			require.Error(t, err, "set reads fell back to the metadata store")
		})
	}
}
