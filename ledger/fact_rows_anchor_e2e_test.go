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
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// anchorStateLines renders what the ledger serves of the state a snapshot
// import established: the imported accounts with their delegations and
// balances, and the reward journal after the anchor.
func anchorStateLines(
	t *testing.T,
	src metadata.AnchorStateSource,
) []string {
	t.Helper()
	require.NoError(t, src.AnchorStateReady(t.Context()))
	var lines []string
	var cursor []byte
	for {
		accounts, next, err := src.AnchorImportedAccounts(
			t.Context(), 0, cursor, 2,
		)
		require.NoError(t, err)
		for _, a := range accounts {
			deposit := "none"
			if a.DepositAmount != nil {
				deposit = fmt.Sprint(*a.DepositAmount)
			}
			lines = append(lines, fmt.Sprintf(
				"account %d/%x pool=%x drep=%x/%d active=%t slot=%d deposit=%s reward=%d",
				a.CredentialTag, a.StakingKey, a.Pool, a.Drep, a.DrepType,
				a.Active, a.AddedSlot, deposit, a.Reward,
			))
		}
		if next == nil {
			break
		}
		cursor = next
	}
	slices.Sort(lines)
	var journal []string
	require.NoError(t, src.EachAnchorRewardJournal(
		t.Context(), 0, func(e metadata.RewardJournalEntry) error {
			journal = append(journal, fmt.Sprintf(
				"journal %d/%x amount=%d withdrawal=%t",
				e.CredentialTag, e.StakingKey, e.Amount, e.Withdrawal,
			))
			return nil
		},
	))
	slices.Sort(journal)
	return append(lines, journal...)
}

// With the accounts family drained, the ledger serves the imported accounts,
// delegations included, and the reward journal from the live UTxO set, as the
// metadata store served them.
func TestDrainedSetServesTheImportedAnchorStateTheStoreServes(t *testing.T) {
	t.Parallel()
	var lines []string
	out, _ := releasedFactsRun(t, func(h *p5Harness) func() []string {
		ref := h.refs[0]
		h.scenario.Credit(ref, 40)
		lines = anchorStateLines(t, h.scenario.AnchorState)
		return func() []string { return nil }
	})
	require.NotNil(t, out)
	// The scenario's credit is 40 from each of two sources; a repeat of the
	// first source is not credited again.
	require.Equal(t, 2, countPrefixed(lines, "journal "),
		"the journal has both credits: %v", lines)
	require.Equal(t, 1, countPrefixed(lines, fmt.Sprintf(
		"account 0/%x pool=", bytes.Repeat([]byte{0x71}, 28),
	)), "the credited account is served: %v", lines)
	require.Equal(t, 2, countPrefixed(lines, "account "),
		"the harness imported two accounts: %v", lines)
}

func countPrefixed(lines []string, prefix string) int {
	n := 0
	for _, l := range lines {
		if strings.HasPrefix(l, prefix) {
			n++
		}
	}
	return n
}

// lostDelegationsSet is a set whose import baselines were written before it
// kept their delegations, with the metadata store's rows already drained.
type lostDelegationsSet struct {
	*utxoset.Set
}

func (lostDelegationsSet) ImportBaselineDelegationsLost() (bool, error) {
	return true, nil
}

// An import baseline without the delegations the import recorded cannot give
// an anchor's accounts their delegations, so the ledger refuses to serve them
// and says why.
func TestDrainedSetRefusesImportedAccountsWhoseDelegationsItLost(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	s := ledger.NewGuardedBoundaryEpochStateScenarioForTest(
		t, set, nil,
	)
	s.InstallDrainedSources()
	s.WrapSet(func(ledger.UtxoSetWriter) ledger.UtxoSetWriter {
		return lostDelegationsSet{set}
	})

	err = s.AnchorState.AnchorStateReady(t.Context())
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	require.ErrorContains(t, err, "delegations they record")
	_, _, err = s.AnchorState.AnchorImportedAccounts(t.Context(), 0, nil, 10)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
}

// A pinned view of the anchor state reads the ledger at the point it was
// taken: a credit applied afterwards is in neither its balances nor its
// journal, so a balance less the journal still gives the balance at the
// anchor. The live source moves with the credit.
func TestAnchorStateViewReadsOnePointWhileTheLedgerMoves(t *testing.T) {
	t.Parallel()
	releasedFactsRun(t, func(h *p5Harness) func() []string {
		ref := h.refs[0]
		h.scenario.Credit(ref, 40)
		before := anchorStateLines(t, h.scenario.AnchorState)

		viewer, ok := h.scenario.AnchorState.(metadata.AnchorStateViewer)
		require.True(t, ok)
		view, release, err := viewer.AnchorStateView(t.Context())
		require.NoError(t, err)
		defer release()

		ldb := h.scenario.LedgerDB()
		txn := ldb.Begin(t.Context(), true)
		require.NoError(t, ldb.AddPostSnapshotAccountRewardByCredential(
			t.Context(), ref.Tag, ref.Key, 7,
			h.scenario.CurrentPoint().Slot, []byte{0x41}, txn,
		))
		require.NoError(t, txn.Commit())
		require.NotEqual(t, before, anchorStateLines(t, h.scenario.AnchorState),
			"the live source sees the later credit")
		require.Equal(t, before, anchorStateLines(t, view),
			"the view still reads the point it was taken at")
		return func() []string { return nil }
	})
}
