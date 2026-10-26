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
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
)

// barrierQueryer is the handle dbFromTxn returns for a transaction with an
// active batch UTxO overlay. Before any statement that names utxo,
// utxo_pointer or reward_live_stake it writes the overlay's pending rows and
// the ended blocks' stake deltas, so every reader -- known or not -- sees
// the tables as the per-row path would have them. The check sits on the
// handle rather than in the readers because a reader that skipped it would
// diverge silently.
type barrierQueryer struct {
	queryer
	store       *Store
	accumulator *transactionBatchAccumulator
}

func (q barrierQueryer) barrier(ctx context.Context, query string) error {
	if deletesAccountRows(query) {
		// The live-stake replay reads an account's existence at each
		// block's end from created_slot, which only holds while no account
		// row is deleted inside the batch.
		err := errOverlayAccountDelete
		if o := q.accumulator.overlay; o != nil && o.err == nil {
			o.err = err
		}
		return err
	}
	if !readsOverlayTables(query) {
		return nil
	}
	return q.store.overlayBarrier(ctx, q.queryer, q.accumulator)
}

var errOverlayAccountDelete = errors.New(
	"account row deleted inside a batch UTxO overlay",
)

// accountDeletePattern matches a DELETE whose target table is account,
// after any leading comment lines.
var accountDeletePattern = regexp.MustCompile(
	"(?im)^\\s*DELETE\\s+FROM\\s+[\"`]?account[\"`]?(\\s|;|$)",
)

func deletesAccountRows(query string) bool {
	return accountDeletePattern.MatchString(query)
}

func (q barrierQueryer) ExecContext(
	ctx context.Context,
	query string,
	args ...any,
) (sql.Result, error) {
	if err := q.barrier(ctx, query); err != nil {
		return nil, err
	}
	return q.queryer.ExecContext(ctx, query, args...)
}

func (q barrierQueryer) QueryContext(
	ctx context.Context,
	query string,
	args ...any,
) (*sql.Rows, error) {
	if err := q.barrier(ctx, query); err != nil {
		return nil, err
	}
	return q.queryer.QueryContext(ctx, query, args...)
}

// QueryRowContext cannot return the barrier's error; the overlay records it
// and FlushBatch returns it, so the batch fails before commit.
func (q barrierQueryer) QueryRowContext(
	ctx context.Context,
	query string,
	args ...any,
) *sql.Row {
	_ = q.barrier(ctx, query)
	return q.queryer.QueryRowContext(ctx, query, args...)
}

// overlayReadBarrier runs db's barrier for query when db is, or wraps, a
// barrierQueryer. The cached-statement helpers call it because they resolve
// db to a *sql.Stmt and never reach its methods.
func overlayReadBarrier(ctx context.Context, db queryer, query string) error {
	for {
		switch v := db.(type) {
		case barrierQueryer:
			return v.barrier(ctx, query)
		case countingQueryer:
			db = v.queryer
		case dialectQueryer:
			db = v.queryer
		default:
			return nil
		}
	}
}

// overlayReadBarrierAll runs db's barrier unconditionally. A caller that
// derives a value from the queued stake deltas before its first read calls
// it first, so the value and the read agree.
func overlayReadBarrierAll(ctx context.Context, db queryer) error {
	return overlayReadBarrier(ctx, db, "SELECT 1 FROM reward_live_stake")
}

// readsOverlayTables reports whether query names utxo, utxo_pointer or
// reward_live_stake as a whole identifier, so utxo_address, utxo_id and
// consumed_utxo_prune_floor do not match.
func readsOverlayTables(query string) bool {
	return containsIdentifier(query, "utxo") ||
		containsIdentifier(query, "utxo_pointer") ||
		containsIdentifier(query, "reward_live_stake")
}

func containsIdentifier(query, name string) bool {
	for offset := 0; ; {
		i := strings.Index(query[offset:], name)
		if i < 0 {
			return false
		}
		start := offset + i
		end := start + len(name)
		if (start == 0 || !isIdentifierByte(query[start-1])) &&
			(end == len(query) || !isIdentifierByte(query[end])) {
			return true
		}
		offset = start + 1
	}
}

func isIdentifierByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' ||
		c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}
