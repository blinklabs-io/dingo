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
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"sync"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
)

// ErrFactTableUnserved reports a fact table whose rows the live UTxO set
// cannot export. It is returned instead of an empty answer, since no other
// store holds the table's rows.
var ErrFactTableUnserved = errors.New(
	"the live UTxO set cannot export this fact table",
)

// factTableFamilies names the ledger-state families each fact table's rows
// belong to. A table absent here is not ledger state.
var factTableFamilies = map[string][]nodesettings.Family{
	"epoch":                      {nodesettings.FamilyEpochs},
	"epoch_summary":              {nodesettings.FamilySummaries},
	"pool_stake_snapshot":        {nodesettings.FamilyEpochs, nodesettings.FamilyLiveStake},
	"reward_ada_pots":            {nodesettings.FamilyRewardInputs},
	"reward_snapshot":            {nodesettings.FamilyRewardInputs},
	"reward_pool_input":          {nodesettings.FamilyRewardInputs},
	"reward_stake_input":         {nodesettings.FamilyRewardInputs},
	"reward_seed_failure":        {nodesettings.FamilyRewardInputs},
	"reward_pool_output":         {nodesettings.FamilyRewardOutputs},
	"reward_account_output":      {nodesettings.FamilyRewardOutputs},
	"reward_credit_round":        {nodesettings.FamilyRewardOutputs},
	"imported_pool_block_count":  {nodesettings.FamilyRewardBlocksFees},
	"imported_epoch_block_total": {nodesettings.FamilyRewardBlocksFees},
	"network_state":              {nodesettings.FamilyNetworkDonations},
	"network_donation":           {nodesettings.FamilyNetworkDonations},
	"pparams":                    {nodesettings.FamilyPParamsUpdates},
	"drep_expiry_history":        {nodesettings.FamilyDReps},
	"drep_dormancy_history":      {nodesettings.FamilyDReps},
	"drep_expiry_epoch_event":    {nodesettings.FamilyDReps},
	"account_drep_clear":         {nodesettings.FamilyDReps},
	"governance_proposal":        {nodesettings.FamilyGovernance},
	"governance_proposal_drop":   {nodesettings.FamilyGovernance},
	"account":                    {nodesettings.FamilyAccounts},
	"drep":                       {nodesettings.FamilyDReps},
	"account_reward_delta":       {nodesettings.FamilyAccounts},
}

// factSetReaders are the fact tables the live UTxO set exports, each as the
// rows the metadata store holds them in.
var factSetReaders = map[string]func(
	*LedgerState, metadata.FactTable, uint64, uint64,
) (metadata.FactRows, error){
	"epoch":               (*LedgerState).epochFactRows,
	"pool_stake_snapshot": (*LedgerState).poolStakeSnapshotFactRows,
	"epoch_summary":       (*LedgerState).epochSummaryFactRows,
	"network_state":       (*LedgerState).networkStateFactRows,
	"network_donation":    (*LedgerState).networkDonationFactRows,
	"pparams":             (*LedgerState).pparamsFactRows,

	"reward_ada_pots":            (*LedgerState).rewardAdaPotsFactRows,
	"reward_snapshot":            (*LedgerState).rewardSnapshotFactRows,
	"reward_pool_input":          (*LedgerState).rewardPoolInputFactRows,
	"reward_seed_failure":        (*LedgerState).rewardSeedFailureFactRows,
	"reward_pool_output":         (*LedgerState).rewardPoolOutputFactRows,
	"reward_credit_round":        (*LedgerState).rewardCreditRoundFactRows,
	"imported_pool_block_count":  (*LedgerState).importedPoolBlockCountFactRows,
	"imported_epoch_block_total": (*LedgerState).importedEpochBlockTotalFactRows,
	"account":                    (*LedgerState).accountFactRows,
	"drep":                       (*LedgerState).drepFactRows,

	"drep_expiry_history":     (*LedgerState).drepExpiryHistoryFactRows,
	"drep_dormancy_history":   (*LedgerState).drepDormancyHistoryFactRows,
	"drep_expiry_epoch_event": (*LedgerState).drepExpiryEpochEventFactRows,
	"account_drep_clear":      (*LedgerState).accountDrepClearFactRows,

	"governance_proposal":      (*LedgerState).governanceProposalFactRows,
	"governance_proposal_drop": (*LedgerState).governanceProposalDropFactRows,
}

// factSetPagers are the fact tables the live UTxO set exports one page at a
// time, by a cursor over the table's own key order rather than a count of rows
// already served, so a page costs what it returns.
var factSetPagers = map[string]func(
	*LedgerState, metadata.FactTable, uint64, uint64, int64, int,
) (metadata.FactRows, int64, error){
	"reward_stake_input":    (*LedgerState).rewardStakeInputFactPage,
	"reward_account_output": (*LedgerState).rewardAccountOutputFactPage,
	"account_reward_delta":  (*LedgerState).accountRewardCreditPage,
}

func factSetServes(name string) bool {
	return factSetReaders[name] != nil || factSetPagers[name] != nil
}

// FactTableInSet reports whether table's rows are ledger state the live
// UTxO set holds.
func (ls *LedgerState) FactTableInSet(name string) bool {
	return len(factTableFamilies[name]) != 0
}

// UnservedFactTables lists the ledger-state fact tables the live UTxO set
// cannot export.
func (ls *LedgerState) UnservedFactTables() []string {
	var out []string
	for _, t := range metadata.FactTables {
		if ls.FactTableInSet(t.Name) && !factSetServes(t.Name) {
			out = append(out, t.Name)
		}
	}
	slices.Sort(out)
	return out
}

// ReadFactRows implements metadata.FactRowStore's read side over the ledger:
// every fact table is exported from the LedgerDB set, by natural key and as
// it stood at through. A table the set cannot export fails with
// ErrFactTableUnserved.
//
// The set is read as committed, so a caller must ask only for slots the ledger
// has applied.
func (ls *LedgerState) ReadFactRows(
	ctx context.Context,
	table metadata.FactTable,
	from, through uint64,
	cursor int64,
	limit int,
	txn types.Txn,
) (metadata.FactRows, int64, error) {
	if page := factSetPagers[table.Name]; page != nil {
		if limit <= 0 {
			return metadata.FactRows{}, 0, errors.New(
				"paged fact read needs a positive limit",
			)
		}
		return page(ls, table, from, through, cursor, limit)
	}
	read := factSetReaders[table.Name]
	if read == nil {
		return metadata.FactRows{}, 0, fmt.Errorf(
			"%w: %s", ErrFactTableUnserved, table.Name,
		)
	}
	var seek factSeeker
	if mk := factSetSeekers[table.Name]; mk != nil {
		seek = mk(ls, from, through)
	}
	return ls.pageSetFactRows(
		table, from, through, cursor, limit,
		func() (metadata.FactRows, error) {
			return read(ls, table, from, through)
		},
		seek,
	)
}

// factCursorState holds, for each table, the position of an export in
// progress. A cursor handed to the caller is a token for it: a natural key does
// not fit in the integer a cursor is.
type factCursorState struct {
	mu      sync.Mutex
	next    int64
	entries map[string]factCursorEntry
}

type factCursorEntry struct {
	token         int64
	from, through uint64
	// last is the key columns of the last row served.
	last any
}

// ErrStopRows is returned by the callback of a set's seeking reader to end its
// walk once a page is full.
var ErrStopRows = errors.New("ledger: stop rows")

// ErrFactCursorLost reports a cursor that names no export in progress, as
// after the node restarted between pages. The caller starts the table again.
var ErrFactCursorLost = metadata.ErrFactCursorLost

// pageSetFactRows serves one page of a table exported from the set, in
// natural-key order. The cursor stands for the key of the last row served, and
// a page holds the rows after that key that are in the range when the page is
// read. The set keeps changing between pages: a row whose slot moves out of
// the range is simply not served, and no row behind the cursor shifts the
// ones after it, as a count of rows served would. A table without its key
// columns among its rows falls back to that count, which is only right for
// rows whose slot never changes.
func (ls *LedgerState) pageSetFactRows(
	table metadata.FactTable,
	from, through uint64,
	cursor int64,
	limit int,
	read func() (metadata.FactRows, error),
	seek factSeeker,
) (metadata.FactRows, int64, error) {
	if seek != nil && table.Paged {
		return ls.seekFactPage(table, from, through, cursor, limit, seek)
	}
	rows, err := read()
	if err != nil {
		return metadata.FactRows{}, 0, err
	}
	if !table.Paged {
		return pageFactRows(table, rows, cursor, limit)
	}
	if limit <= 0 {
		return metadata.FactRows{}, 0, errors.New(
			"paged fact read needs a positive limit",
		)
	}
	keyCols := factKeyColumns(table, rows.Columns)
	if keyCols == nil {
		return pageFactRows(table, rows, cursor, limit)
	}
	slices.SortStableFunc(rows.Rows, func(a, b []any) int {
		return compareFactKeys(factKey(a, keyCols), factKey(b, keyCols))
	})
	start := 0
	st := &ls.factCursors
	st.mu.Lock()
	defer st.mu.Unlock()
	if cursor > 0 {
		e, ok := st.entries[table.Name]
		if !ok || e.token != cursor || e.from != from || e.through != through {
			return metadata.FactRows{}, 0, fmt.Errorf(
				"%w: %s cursor %d", ErrFactCursorLost, table.Name, cursor,
			)
		}
		last, _ := e.last.([]any)
		start = sort.Search(len(rows.Rows), func(i int) bool {
			return compareFactKeys(factKey(rows.Rows[i], keyCols), last) > 0
		})
	}
	rest := rows.Rows[start:]
	out := metadata.FactRows{Columns: rows.Columns}
	if len(rest) > limit {
		rest = rest[:limit]
	}
	out.Rows = rest
	if len(rest) < limit {
		delete(st.entries, table.Name)
		return out, 0, nil
	}
	st.next++
	if st.entries == nil {
		st.entries = make(map[string]factCursorEntry)
	}
	st.entries[table.Name] = factCursorEntry{
		token: st.next, from: from, through: through,
		last: factKey(rest[len(rest)-1], keyCols),
	}
	out.LastID = st.next
	return out, st.next, nil
}

// seekFactPage is pageSetFactRows for a table the set serves from a key: the
// cursor stands for the position of the last row served, and the set seeks to
// it, so the page costs what it returns whatever the size of the table. The
// rows come in the set's key order, which need not be FactTable.Key's: the
// copy writes them by key and does not depend on their order.
func (ls *LedgerState) seekFactPage(
	table metadata.FactTable,
	from, through uint64,
	cursor int64,
	limit int,
	seek factSeeker,
) (metadata.FactRows, int64, error) {
	if limit <= 0 {
		return metadata.FactRows{}, 0, errors.New(
			"paged fact read needs a positive limit",
		)
	}
	st := &ls.factCursors
	var after any
	if cursor > 0 {
		st.mu.Lock()
		e, ok := st.entries[table.Name]
		st.mu.Unlock()
		if !ok || e.token != cursor || e.from != from || e.through != through {
			return metadata.FactRows{}, 0, fmt.Errorf(
				"%w: %s cursor %d", ErrFactCursorLost, table.Name, cursor,
			)
		}
		after = e.last
	}
	rows, last, err := seek(after, limit)
	if err != nil {
		return metadata.FactRows{}, 0, err
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(rows.Rows) < limit {
		delete(st.entries, table.Name)
		return rows, 0, nil
	}
	st.next++
	if st.entries == nil {
		st.entries = make(map[string]factCursorEntry)
	}
	st.entries[table.Name] = factCursorEntry{
		token: st.next, from: from, through: through, last: last,
	}
	rows.LastID = st.next
	return rows, st.next, nil
}

// factKeyColumns returns the positions of table's key columns among columns,
// nil when one is missing.
func factKeyColumns(table metadata.FactTable, columns []string) []int {
	if len(table.Key) == 0 {
		return nil
	}
	idx := make([]int, 0, len(table.Key))
	for _, k := range table.Key {
		i := slices.Index(columns, k)
		if i < 0 {
			return nil
		}
		idx = append(idx, i)
	}
	return idx
}

func factKey(row []any, cols []int) []any {
	key := make([]any, len(cols))
	for i, c := range cols {
		key[i] = row[c]
	}
	return key
}

func compareFactKeys(a, b []any) int {
	for i := range a {
		if c := compareFactValue(a[i], b[i]); c != 0 {
			return c
		}
	}
	return 0
}

func compareFactValue(a, b any) int {
	switch x := a.(type) {
	case nil:
		if b == nil {
			return 0
		}
		return -1
	case int64:
		if y, ok := b.(int64); ok {
			return cmp.Compare(x, y)
		}
	case string:
		if y, ok := b.(string); ok {
			return cmp.Compare(x, y)
		}
	case []byte:
		if y, ok := b.([]byte); ok {
			return bytes.Compare(x, y)
		}
	}
	if b == nil {
		return 1
	}
	return cmp.Compare(fmt.Sprint(a), fmt.Sprint(b))
}

// pageFactRows serves the rows after cursor, at most limit of them. A set
// export has no row ids, so the cursor is the count of rows already served,
// over rows in a fixed natural-key order.
func pageFactRows(
	table metadata.FactTable,
	rows metadata.FactRows,
	cursor int64,
	limit int,
) (metadata.FactRows, int64, error) {
	if !table.Paged {
		return rows, 0, nil
	}
	if limit <= 0 {
		return metadata.FactRows{}, 0, errors.New(
			"paged fact read needs a positive limit",
		)
	}
	if cursor < 0 || cursor > int64(len(rows.Rows)) {
		return metadata.FactRows{}, 0, fmt.Errorf(
			"fact read cursor %d outside %d rows", cursor, len(rows.Rows),
		)
	}
	rest := rows.Rows[cursor:]
	out := metadata.FactRows{Columns: rows.Columns}
	if len(rest) > limit {
		rest = rest[:limit]
	}
	out.Rows = rest
	next := cursor + int64(len(rest))
	out.LastID = next
	if len(rest) < limit {
		next = 0
	}
	return out, next, nil
}

func inSlotRange(slot, from, through uint64) bool {
	return slot >= from && slot <= through
}

func uint64Text(v types.Uint64) any {
	return strconv.FormatUint(uint64(v), 10)
}

// sqlInt is v as the metadata store holds an integer column: SQL integers are
// signed 64-bit, so a value above that range cannot have been stored and
// clamps.
func sqlInt[T ~uint | ~uint64](v T) int64 {
	if uint64(v) > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v) //nolint:gosec // bounded above
}

func boolInt(b bool) any {
	if b {
		return int64(1)
	}
	return int64(0)
}

func (ls *LedgerState) epochFactRows(
	_ metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	r, _ := ls.epochStateReader(nil)
	if r == nil {
		return metadata.FactRows{}, ledgerStateUnserved(nodesettings.FamilyEpochs)
	}
	view, err := ls.epochStateView(r, nil)
	if err != nil {
		return metadata.FactRows{}, err
	}
	out := metadata.FactRows{Columns: []string{
		"nonce", "evolving_nonce", "candidate_nonce",
		"last_epoch_block_nonce", "epoch_id", "start_slot", "era_id",
		"slot_length", "length_in_slots",
	}}
	for _, e := range sortedEpochs(view, func(e models.Epoch) bool {
		return inSlotRange(e.StartSlot, from, through)
	}) {
		out.Rows = append(out.Rows, []any{
			nilBytes(e.Nonce), nilBytes(e.EvolvingNonce),
			nilBytes(e.CandidateNonce), nilBytes(e.LastEpochBlockNonce),
			sqlInt(e.EpochId), sqlInt(e.StartSlot), sqlInt(e.EraId),
			sqlInt(e.SlotLength), sqlInt(e.LengthInSlots),
		})
	}
	return out, nil
}

func nilBytes(b []byte) any {
	if b == nil {
		return nil
	}
	return slices.Clone(b)
}

func (ls *LedgerState) epochSummaryFactRows(
	_ metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	er, sr, _ := ls.epochSummaryReader(nil)
	lister, ok := sr.(EpochSummaryLister)
	if er == nil || sr == nil || !ok {
		return metadata.FactRows{}, ledgerStateUnserved(nodesettings.FamilySummaries)
	}
	ls.utxoSetApplyMu.RLock()
	view, err := ls.summaryListViewLocked(er, lister, nil)
	ls.utxoSetApplyMu.RUnlock()
	if err != nil {
		return metadata.FactRows{}, err
	}
	sums := make([]models.EpochSummary, 0, len(view))
	for _, s := range view {
		if inSlotRange(s.BoundarySlot, from, through) {
			sums = append(sums, s)
		}
	}
	slices.SortFunc(sums, func(a, b models.EpochSummary) int {
		return cmp.Compare(a.Epoch, b.Epoch)
	})
	out := metadata.FactRows{Columns: []string{
		"epoch", "total_active_stake", "total_pool_count",
		"total_delegators", "epoch_nonce", "boundary_slot", "snapshot_ready",
	}}
	for _, s := range sums {
		out.Rows = append(out.Rows, []any{
			sqlInt(s.Epoch), uint64Text(s.TotalActiveStake),
			sqlInt(s.TotalPoolCount), sqlInt(s.TotalDelegators),
			nilBytes(s.EpochNonce), sqlInt(s.BoundarySlot),
			boolInt(s.SnapshotReady),
		})
	}
	return out, nil
}

func (ls *LedgerState) networkStateFactRows(
	_ metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	r, _ := ls.networkStateReader(nil)
	lister, ok := r.(networkStateLister)
	if r == nil || !ok {
		return metadata.FactRows{}, ledgerStateUnserved(
			nodesettings.FamilyNetworkDonations,
		)
	}
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	rows, err := lister.NetworkStates()
	if err != nil {
		return metadata.FactRows{}, err
	}
	slices.SortFunc(rows, func(a, b models.NetworkState) int {
		return cmp.Compare(a.Slot, b.Slot)
	})
	out := metadata.FactRows{Columns: []string{
		"treasury", "reserves", "slot",
	}}
	for _, n := range rows {
		if !inSlotRange(n.Slot, from, through) {
			continue
		}
		out.Rows = append(out.Rows, []any{
			uint64Text(n.Treasury), uint64Text(n.Reserves), sqlInt(n.Slot),
		})
	}
	return out, nil
}

func (ls *LedgerState) networkDonationFactRows(
	_ metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	r, _ := ls.networkStateReader(nil)
	if r == nil {
		return metadata.FactRows{}, ledgerStateUnserved(
			nodesettings.FamilyNetworkDonations,
		)
	}
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	rows, err := r.NetworkDonations()
	if err != nil {
		return metadata.FactRows{}, err
	}
	slices.SortFunc(rows, func(a, b models.NetworkDonation) int {
		return cmp.Compare(a.Slot, b.Slot)
	})
	out := metadata.FactRows{Columns: []string{"slot", "epoch", "amount"}}
	for _, d := range rows {
		if !inSlotRange(d.Slot, from, through) {
			continue
		}
		out.Rows = append(out.Rows, []any{
			sqlInt(d.Slot), sqlInt(d.Epoch), sqlInt(d.Amount),
		})
	}
	return out, nil
}

func (ls *LedgerState) pparamsFactRows(
	_ metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	r, _ := ls.pparamsReader(nil)
	if r == nil {
		return metadata.FactRows{}, ledgerStateUnserved(
			nodesettings.FamilyPParamsUpdates,
		)
	}
	var rows []models.PParams
	for _, desc := range eras.ErasWithDijkstra {
		view, err := ls.pparamsView(r, nil, desc.Id)
		if err != nil {
			return metadata.FactRows{}, err
		}
		rows = append(rows, view...)
	}
	slices.SortStableFunc(rows, func(a, b models.PParams) int {
		return cmp.Or(
			cmp.Compare(a.AddedSlot, b.AddedSlot),
			cmp.Compare(a.Epoch, b.Epoch),
			cmp.Compare(a.EraId, b.EraId),
		)
	})
	out := metadata.FactRows{Columns: []string{
		"cbor", "added_slot", "epoch", "era_id",
	}}
	for _, p := range rows {
		if !inSlotRange(p.AddedSlot, from, through) {
			continue
		}
		out.Rows = append(out.Rows, []any{
			nilBytes(p.Cbor), sqlInt(p.AddedSlot), sqlInt(p.Epoch),
			sqlInt(p.EraId),
		})
	}
	return out, nil
}
