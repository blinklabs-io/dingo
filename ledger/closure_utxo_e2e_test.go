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
	"fmt"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func certifiedUtxoScenario(
	t *testing.T,
) (ledger.BoundaryEpochStateScenario, *utxoset.Set, ledgerdb.LedgerDB, ledger.CertifiedUtxoSlots) {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set, ledger.UtxoSetReadsSet,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	scenario.CertifyLeios()
	return scenario, set, store, scenario.CertifiedUtxos()
}

// storeStakeSums is the store's credential stake at slot: the outputs whose
// creating transaction's effective slot is at or before it and that are
// unspent or whose spending transaction's effective slot is after it, the
// predicate of sqlstore/historical_stake.go:1080-1081.
func storeStakeSums(
	t *testing.T,
	scenario ledger.BoundaryEpochStateScenario,
	slot uint64,
) map[string]uint64 {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, scenario.DB)
	require.NoError(t, err)
	rows, err := raw.Query(`
SELECT credential_tag, staking_key, amount FROM utxo
WHERE staking_key IS NOT NULL AND length(staking_key) > 0
  AND COALESCE((SELECT lc.slot FROM leios_transaction_context lc WHERE lc.transaction_id = utxo.tx_id), utxo.added_slot) <= ?
  AND (utxo.deleted_slot = 0 OR COALESCE((SELECT lc.slot FROM leios_transaction_context lc WHERE lc.transaction_id = utxo.spent_at_tx_id), utxo.deleted_slot) > ?)`,
		slot, slot)
	require.NoError(t, err)
	defer rows.Close()
	sums := map[string]uint64{}
	for rows.Next() {
		var tag uint8
		var key []byte
		var amount string
		require.NoError(t, rows.Scan(&tag, &key, &amount))
		n, err := strconv.ParseUint(amount, 10, 64)
		require.NoError(t, err)
		if n > 0 && key[0] >= 0xd1 {
			sums[string(ledger.CredentialStakeKey(tag, key))] += n
		}
	}
	require.NoError(t, rows.Err())
	return sums
}

func setStakeSums(
	t *testing.T,
	set *utxoset.Set,
	slot uint64,
) map[string]uint64 {
	t.Helper()
	view, err := set.UtxoSetAt(ocommon.Point{Slot: slot})
	require.NoError(t, err)
	defer view.Close()
	sums := map[string]uint64{}
	require.NoError(t, view.EachUtxo(func(_ []byte, _ uint32, cbor []byte) error {
		key, lovelace, ok := ledger.UtxoStakeOf(cbor)
		if ok && key[0] == 'c' && key[2] >= 0xd1 {
			sums[string(key)] += lovelace
		}
		return nil
	}))
	return sums
}

func closureEffects(
	t *testing.T,
	set *utxoset.Set,
	after uint64,
) []ledger.ClosureUtxoEffect {
	t.Helper()
	var out []ledger.ClosureUtxoEffect
	require.NoError(t, set.ClosureUtxoEffectsAfter(
		after, func(e ledger.ClosureUtxoEffect) error {
			out = append(out, e)
			return nil
		}))
	return out
}

// The stake sums read from the outputs live at an applied slot, corrected by
// the closure output effects, equal the store's sums at every slot around the
// closures, over outputs a closure creates and one spends with an effective
// slot before the applied one, one of them across a boundary, interleaved
// with an ordinary transaction. The sums at the applied slots alone differ.
func TestClosureUtxoEffectsCorrectTheAppliedStakeSums(t *testing.T) {
	t.Parallel()
	scenario, set, _, s := certifiedUtxoScenario(t)
	differs := 0
	for slot := s.Start + 30; slot <= s.End+1; slot++ {
		want := storeStakeSums(t, scenario, slot)
		sums := setStakeSums(t, set, slot)
		if fmt.Sprint(sums) != fmt.Sprint(want) {
			differs++
		}
		require.NoError(t, ledger.AdjustStakeSumsEffective(
			sums, closureEffects(t, set, slot), slot,
		))
		require.Equal(t, want, sums, "slot %d", slot)
	}
	require.Positive(t, differs, "the applied slots alone already agree")
}

// A set loaded from the store holds the effects the set that followed the
// closures does, and an ordinary output or spend has none: only the staked
// outputs the closures created and spent are recorded.
func TestClosureUtxoEffectsLoadFromTheStoreAndSkipOrdinaryOutputs(t *testing.T) {
	t.Parallel()
	_, followed, store, s := certifiedUtxoScenario(t)
	fresh, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fresh.Close() })
	require.NoError(t, fresh.LoadLedgerState(t.Context(), store))

	got := closureEffects(t, followed, 0)
	require.ElementsMatch(t, got, closureEffects(t, fresh, 0))
	type effect struct {
		ref                string
		applied, effective uint64
		spent              bool
	}
	var shapes []effect
	for _, e := range got {
		shapes = append(shapes, effect{
			fmt.Sprintf("%x#%d", e.Ref.Hash, e.Ref.Idx),
			e.Applied, e.Effective, e.Spent,
		})
	}
	ref := func(u interface{ String() string }) string { return u.String() }
	_ = ref
	id := func(h []byte, i uint32) string { return fmt.Sprintf("%x#%d", h, i) }
	require.ElementsMatch(t, []effect{
		{id(s.OA1.Hash, s.OA1.Idx), s.A1, s.E1, false},
		{id(s.OB1.Hash, s.OB1.Idx), s.A1, s.E1, false},
		{id(s.OA2.Hash, s.OA2.Idx), s.A2, s.E2, false},
		{id(s.OA1.Hash, s.OA1.Idx), s.A2, s.E2, true},
		{id(s.OA3.Hash, s.OA3.Idx), s.A4, s.E4, false},
		{id(s.OB1.Hash, s.OB1.Idx), s.A4, s.E4, true},
	}, shapes)
	for _, e := range shapes {
		require.NotEqual(t, id(s.OB2.Hash, s.OB2.Idx), e.ref,
			"the ordinary transaction's output has an effect row")
	}
}

// A rollback past the closures removes their effects with the rows they
// journal, so no correction outlives the outputs it adjusts.
func TestClosureUtxoEffectsRollBack(t *testing.T) {
	t.Parallel()
	scenario, set, _, _ := certifiedUtxoScenario(t)
	require.Len(t, closureEffects(t, set, 0), 6)

	scenario.RollbackBeforeBoundary()

	require.Empty(t, closureEffects(t, set, 0))
}
