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
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
)

// factSeeker serves one page of a table straight from the set: the rows after
// the position after (the start of the table when nil), at most limit of them,
// that are in the slot range, in the set's own order, with the position of the
// last row it returned. Its cost is the page and the rows the range changed,
// not the table, except for a range the set cannot name from its journal.
type factSeeker func(
	after any,
	limit int,
) (rows metadata.FactRows, last any, err error)

// AccountSeeker is a set that walks its accounts from a key.
type AccountSeeker interface {
	AccountsAfter(after []byte, fn func(a *models.Account, key []byte) error) error
}

// DrepSeeker is a set that walks its DReps from a key.
type DrepSeeker interface {
	DrepsAfter(after []byte, fn func(d *models.Drep, key []byte) error) error
}

// DRepHistorySeeker is a set that walks one slot range of its DRep history
// from a key.
type DRepHistorySeeker interface {
	DRepExpiryHistoryAfter(
		from, through uint64,
		after []byte,
		fn func(r ledgerdb.DRepExpiryHistoryRow, key []byte) error,
	) error
	DRepDormancyHistoryAfter(
		from, through uint64,
		after []byte,
		fn func(r ledgerdb.DRepDormancyHistoryRow, key []byte) error,
	) error
}

// factSetSeekers are the fact tables whose rows grow with the chain, which
// the set serves page by page from a key. A table absent here, or whose set
// cannot seek, is read whole and cut into pages.
var factSetSeekers = map[string]func(
	*LedgerState, uint64, uint64,
) factSeeker{
	"account":               (*LedgerState).accountSeeker,
	"drep":                  (*LedgerState).drepSeeker,
	"drep_expiry_history":   (*LedgerState).drepExpirySeeker,
	"drep_dormancy_history": (*LedgerState).drepDormancySeeker,
}

func afterKey(after any) []byte {
	k, _ := after.([]byte)
	return k
}

// AccountChangeSeeker is a set that names the accounts changed in a slot range
// from its state journal.
type AccountChangeSeeker interface {
	AccountsChangedIn(
		from, through uint64,
		after []byte,
		maxVisit int,
		fn func(a *models.Account, slot uint64, pos []byte) error,
	) (next []byte, covered, done bool, err error)
}

// DrepChangeSeeker is AccountChangeSeeker for DReps.
type DrepChangeSeeker interface {
	DrepsChangedIn(
		from, through uint64,
		after []byte,
		maxVisit int,
		fn func(d *models.Drep, slot uint64, pos []byte) error,
	) (next []byte, covered, done bool, err error)
}

// factScanBound is how many rows or journal entries a seeker steps over
// before it lets the block apply take the lock, so a page that finds nothing
// in range does not hold the lock for the whole scan.
const factScanBound = 4096

// setPos is the position of an account or DRep export. A journal position
// names the last journal entry served, from which the rows changed in the range
// are read; a key position names the last row served by a read of the whole
// table, which a range below the journal's reach needs.
type setPos struct {
	journal bool
	pos     []byte
}

// rowSource reads one family from a set in the two ways seekChanged uses.
// Each read passes a row, the slot it was added at and its position to emit,
// and reports the position it stopped after, so a read cut at the scan bound
// resumes there.
type rowSource struct {
	columns []string
	// changed reads the rows the journal records changed in the range, or
	// reports covered false when the journal does not reach the range. It
	// is nil for a set that cannot.
	changed func(
		after []byte, maxVisit int,
		emit func(row []any, added, slot uint64, pos []byte) error,
	) (next []byte, covered, done bool, err error)
	// walk reads the whole table in key order after a key.
	walk func(
		after []byte, maxVisit int,
		emit func(row []any, added uint64, key []byte) error,
	) (next []byte, done bool, err error)
}

// seekChanged serves one page of an account or DRep export. The rows in the
// range are those the state journal names at the slot they were added, so the
// cost is the rows changed in the range; a range the journal does not reach,
// as a migration export from slot zero, reads the whole table. Both read in
// bounded steps, taking the apply lock for one step at a time.
func (ls *LedgerState) seekChanged(
	src rowSource,
	from, through uint64,
	after any,
	limit int,
) (metadata.FactRows, any, error) {
	out := metadata.FactRows{Columns: src.columns}
	cur, _ := after.(setPos)
	var last any
	journal := src.changed != nil && (after == nil || cur.journal)
	for {
		var (
			next []byte
			done bool
		)
		full := func() bool { return len(out.Rows) >= limit }
		ls.utxoSetApplyMu.RLock()
		if journal {
			var covered bool
			var err error
			next, covered, done, err = src.changed(cur.pos, factScanBound,
				func(row []any, added, slot uint64, pos []byte) error {
					if added != slot {
						return nil
					}
					out.Rows = append(out.Rows, row)
					last = setPos{journal: true, pos: pos}
					if full() {
						return ErrStopRows
					}
					return nil
				})
			ls.utxoSetApplyMu.RUnlock()
			if err != nil {
				return out, last, err
			}
			if !covered {
				if after != nil {
					return metadata.FactRows{}, nil, fmt.Errorf(
						"%w: the journal no longer reaches the export",
						ErrFactCursorLost,
					)
				}
				journal = false
				continue
			}
		} else {
			var err error
			next, done, err = src.walk(cur.pos, factScanBound,
				func(row []any, added uint64, key []byte) error {
					if !inSlotRange(added, from, through) {
						return nil
					}
					out.Rows = append(out.Rows, row)
					last = setPos{pos: key}
					if full() {
						return ErrStopRows
					}
					return nil
				})
			ls.utxoSetApplyMu.RUnlock()
			if err != nil {
				return out, last, err
			}
		}
		if full() || done {
			return out, last, nil
		}
		cur = setPos{journal: journal, pos: next}
		// Resume the next step as a continuation of this page, not as the
		// start of the export.
		after = cur
	}
}

func (ls *LedgerState) accountSeeker(from, through uint64) factSeeker {
	if ls.accountReader(nil) == nil {
		return nil
	}
	s, ok := ls.readSet(nil).(AccountSeeker)
	if !ok {
		return nil
	}
	src := rowSource{
		columns: accountFactColumns,
		walk: func(
			after []byte, maxVisit int,
			emit func([]any, uint64, []byte) error,
		) ([]byte, bool, error) {
			return walkBounded(maxVisit, func(
				visit func(key []byte, err error) error,
			) error {
				return s.AccountsAfter(after, func(a *models.Account, key []byte) error {
					return visit(key, emit(accountFactRow(a), a.AddedSlot, key))
				})
			})
		},
	}
	if c, ok := ls.readSet(nil).(AccountChangeSeeker); ok {
		src.changed = func(
			after []byte, maxVisit int,
			emit func([]any, uint64, uint64, []byte) error,
		) ([]byte, bool, bool, error) {
			return c.AccountsChangedIn(from, through, after, maxVisit,
				func(a *models.Account, slot uint64, pos []byte) error {
					return emit(accountFactRow(a), a.AddedSlot, slot, pos)
				})
		}
	}
	return func(after any, limit int) (metadata.FactRows, any, error) {
		return ls.seekChanged(src, from, through, after, limit)
	}
}

func (ls *LedgerState) drepSeeker(from, through uint64) factSeeker {
	if ls.drepReader(nil) == nil {
		return nil
	}
	s, ok := ls.readSet(nil).(DrepSeeker)
	if !ok {
		return nil
	}
	src := rowSource{
		columns: drepFactColumns,
		walk: func(
			after []byte, maxVisit int,
			emit func([]any, uint64, []byte) error,
		) ([]byte, bool, error) {
			return walkBounded(maxVisit, func(
				visit func(key []byte, err error) error,
			) error {
				return s.DrepsAfter(after, func(d *models.Drep, key []byte) error {
					return visit(key, emit(drepFactRow(d), d.AddedSlot, key))
				})
			})
		},
	}
	if c, ok := ls.readSet(nil).(DrepChangeSeeker); ok {
		src.changed = func(
			after []byte, maxVisit int,
			emit func([]any, uint64, uint64, []byte) error,
		) ([]byte, bool, bool, error) {
			return c.DrepsChangedIn(from, through, after, maxVisit,
				func(d *models.Drep, slot uint64, pos []byte) error {
					return emit(drepFactRow(d), d.AddedSlot, slot, pos)
				})
		}
	}
	return func(after any, limit int) (metadata.FactRows, any, error) {
		return ls.seekChanged(src, from, through, after, limit)
	}
}

// walkBounded runs a key-ordered walk that stops, with the key it stopped
// after, once it has stepped over maxVisit rows or its emit asks to stop, and
// reports whether the walk reached the end of the table.
func walkBounded(
	maxVisit int,
	walk func(visit func(key []byte, err error) error) error,
) (next []byte, done bool, err error) {
	visited := 0
	done = true
	err = walk(func(key []byte, err error) error {
		if err != nil {
			if errors.Is(err, ErrStopRows) {
				next, done = bytes.Clone(key), false
			}
			return err
		}
		visited++
		if visited >= maxVisit {
			next, done = bytes.Clone(key), false
			return ErrStopRows
		}
		return nil
	})
	return next, done, err
}

// historySeeker returns whether the set serves the DRep history from a key,
// and holds all of it.
func (ls *LedgerState) historySeeker() DRepHistorySeeker {
	if ls.drepReader(nil) == nil {
		return nil
	}
	h, ok := ls.readSet(nil).(DRepHistoryReader)
	if !ok {
		return nil
	}
	loaded, err := h.DRepHistoryLoaded()
	lost, lerr := h.DRepHistoryLost()
	if err != nil || lerr != nil || !loaded || lost {
		// The unseekable path reports why the history is unserved.
		return nil
	}
	s, _ := h.(DRepHistorySeeker)
	return s
}

func (ls *LedgerState) drepExpirySeeker(from, through uint64) factSeeker {
	s := ls.historySeeker()
	if s == nil {
		return nil
	}
	return func(after any, limit int) (metadata.FactRows, any, error) {
		out := metadata.FactRows{Columns: drepExpiryHistoryColumns}
		var last []byte
		ls.utxoSetApplyMu.RLock()
		defer ls.utxoSetApplyMu.RUnlock()
		err := s.DRepExpiryHistoryAfter(from, through, afterKey(after),
			func(r ledgerdb.DRepExpiryHistoryRow, key []byte) error {
				out.Rows = append(out.Rows, drepExpiryHistoryRow(r))
				last = key
				if len(out.Rows) >= limit {
					return ErrStopRows
				}
				return nil
			})
		return out, last, err
	}
}

func (ls *LedgerState) drepDormancySeeker(from, through uint64) factSeeker {
	s := ls.historySeeker()
	if s == nil {
		return nil
	}
	return func(after any, limit int) (metadata.FactRows, any, error) {
		out := metadata.FactRows{Columns: drepDormancyHistoryColumns}
		var last []byte
		ls.utxoSetApplyMu.RLock()
		defer ls.utxoSetApplyMu.RUnlock()
		err := s.DRepDormancyHistoryAfter(from, through, afterKey(after),
			func(r ledgerdb.DRepDormancyHistoryRow, key []byte) error {
				out.Rows = append(out.Rows, drepDormancyHistoryRow(r))
				last = key
				if len(out.Rows) >= limit {
					return ErrStopRows
				}
				return nil
			})
		return out, last, err
	}
}
