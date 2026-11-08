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
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/blinklabs-io/dingo/database/models"
)

// On SQLite an output produced by a transaction is identified by the
// producer's transaction row id and its output index, through the unique
// unique index idx_utxo_producer_output. The producer is the generated
// column producer_tx_id, COALESCE(transaction_id, collateral_return_for_tx_id): a collateral return
// shares its transaction's output-index space with the regular outputs, and a
// transaction that fails phase 2 has no regular outputs, so the pair is
// unique. An output with neither id (a ledger-state or snapshot import, or one
// recovered for a missing producer) is identified by its tx_id hash through
// the partial unique index idx_utxo_imported_ref. The hash index over every
// output is gone: that index received an insert at a random leaf per output.
//
// The shared statements still say "tx_id = ? AND output_idx = ?". On SQLite
// dialectQueryer rewrites that predicate into the two-way id lookup
// below and doubles its two arguments; other dialects keep the full unique
// index and run the statements as written.

// utxoRefIDSQL is the id of the output named by (hash, index): the produced
// output if there is one, else the imported one. Each arm matches at most one
// row, so the whole is a scalar, and "id = <scalar>" plans as a primary-key
// lookup whatever other predicates the statement carries. An "id IN
// (subquery)" form does not: with a liveness predicate beside it SQLite
// prefers a deleted_slot index and visits every live row.
const utxoRefIDSQL = `COALESCE(` +
	`(SELECT ref.id FROM utxo AS ref WHERE ref.producer_tx_id = ` +
	`(SELECT ref_tx.id FROM "transaction" AS ref_tx WHERE ref_tx.hash = ?) ` +
	`AND ref.output_idx = ?), ` +
	`(SELECT ref.id FROM utxo AS ref ` +
	`WHERE ref.transaction_id IS NULL AND ref.collateral_return_for_tx_id IS NULL ` +
	`AND ref.tx_id = ? AND ref.output_idx = ?))`

var (
	utxoRefPairPattern = regexp.MustCompile(
		`\b((?:[A-Za-z_]+\.)?)tx_id = \? AND (?:[A-Za-z_]+\.)?output_idx = \?`,
	)
	// utxoRefConflictPattern is the conflict target of the shared inserts.
	utxoRefConflictPattern = regexp.MustCompile(
		`ON CONFLICT \(tx_id, output_idx\) DO NOTHING`,
	)
)

const (
	// utxoConflictTargets names both unique indexes. A produced output can
	// only collide in the first and an imported one only in the second, so one
	// statement serves both kinds. A produced output colliding with an
	// imported row, or the reverse, is not a unique violation; see
	// existingUtxoRefs.
	utxoConflictTargets = `ON CONFLICT (producer_tx_id, output_idx) WHERE ` +
		`producer_tx_id IS NOT NULL ` +
		`DO NOTHING ON CONFLICT (tx_id, output_idx) WHERE ` +
		`transaction_id IS NULL AND collateral_return_for_tx_id IS NULL ` +
		`DO NOTHING`
)

// utxoRefIDsInSQL is the id lookup for a list of transaction hashes, with
// every output of those transactions. The two lists are the same hashes.
// utxoRefAsWritten marks a statement the rewrite must leave alone because it
// already names the producer and imported indexes itself.
const utxoRefAsWritten = "-- as written\n"

const utxoRefIDsInSQL = `SELECT ref.id FROM utxo AS ref ` +
	`WHERE ref.producer_tx_id IN ` +
	`(SELECT ref_tx.id FROM "transaction" AS ref_tx WHERE ref_tx.hash IN (%s)) ` +
	`UNION ALL SELECT ref.id FROM utxo AS ref ` +
	`WHERE ref.transaction_id IS NULL AND ref.collateral_return_for_tx_id IS NULL ` +
	`AND ref.tx_id IN (%s)`

var utxoRefInPattern = regexp.MustCompile(
	`\b((?:[A-Za-z_]+\.)?)tx_id IN \((\?(?:, ?\?)*)\)`,
)

// utxoRefSpan is one rewritten predicate: the index of its first argument in
// the original list, how many arguments it has, and how many times the
// rewrite repeats them.
type utxoRefSpan struct {
	at, n int
}

// utxoRefPlan is the rewrite of one statement: its expanded text and the
// argument spans the rewrite repeats.
type utxoRefPlan struct {
	query string
	spans []utxoRefSpan
}

var utxoRefPlans sync.Map

func planUtxoRefs(query string) *utxoRefPlan {
	if cached, ok := utxoRefPlans.Load(query); ok {
		return cached.(*utxoRefPlan)
	}
	if strings.Contains(query, "utxo AS ref") ||
		strings.Contains(query, utxoRefAsWritten) {
		// Already expanded, or written against the indexes directly.
		plan := &utxoRefPlan{query: query}
		utxoRefPlans.Store(query, plan)
		return plan
	}
	rewritten := utxoRefConflictPattern.ReplaceAllString(
		query, utxoConflictTargets,
	)
	type match struct {
		start, end int
		alias      string
		list       string
	}
	var found []match
	for _, m := range utxoRefPairPattern.FindAllStringSubmatchIndex(rewritten, -1) {
		found = append(found, match{
			start: m[0], end: m[1], alias: rewritten[m[2]:m[3]],
		})
	}
	for _, m := range utxoRefInPattern.FindAllStringSubmatchIndex(rewritten, -1) {
		found = append(found, match{
			start: m[0], end: m[1], alias: rewritten[m[2]:m[3]],
			list: rewritten[m[4]:m[5]],
		})
	}
	plan := &utxoRefPlan{query: rewritten}
	if len(found) > 0 {
		sort.Slice(found, func(i, j int) bool {
			return found[i].start < found[j].start
		})
		var out strings.Builder
		last, argIndex := 0, 0
		for _, m := range found {
			out.WriteString(rewritten[last:m.start])
			argIndex += strings.Count(rewritten[last:m.start], "?")
			if m.list == "" {
				out.WriteString(m.alias + "id = " + utxoRefIDSQL)
				plan.spans = append(plan.spans, utxoRefSpan{argIndex, 2})
				argIndex += 2
			} else {
				out.WriteString(m.alias + "id IN (" +
					strings.ReplaceAll(utxoRefIDsInSQL, "%s", m.list) + ")")
				n := strings.Count(m.list, "?")
				plan.spans = append(plan.spans, utxoRefSpan{argIndex, n})
				argIndex += n
			}
			last = m.end
		}
		out.WriteString(rewritten[last:])
		plan.query = out.String()
	}
	utxoRefPlans.Store(query, plan)
	return plan
}

// expandUtxoRefs rewrites every reference predicate of query and, when args
// is non-nil, repeats the matching arguments. It is a no-op for a statement
// without a predicate, and idempotent.
func expandUtxoRefs(query string, args []any) (string, []any) {
	plan := planUtxoRefs(query)
	return plan.query, plan.expandArgs(args)
}

// expandArgs repeats each span once: a pair becomes (hash, index, hash,
// index) and a list becomes the list twice.
func (p *utxoRefPlan) expandArgs(args []any) []any {
	if args == nil || len(p.spans) == 0 {
		return args
	}
	expanded := make([]any, 0, len(args)+len(args))
	consumed := 0
	for _, span := range p.spans {
		expanded = append(expanded, args[consumed:span.at]...)
		part := args[span.at : span.at+span.n]
		expanded = append(expanded, part...)
		expanded = append(expanded, part...)
		consumed = span.at + span.n
	}
	return append(expanded, args[consumed:]...)
}

// hasUtxoRef reports whether query carries a reference predicate.
func hasUtxoRef(query string) bool {
	return len(planUtxoRefs(query).spans) > 0
}

// utxoProducedKind reports whether the output names a producing transaction.
func utxoProducedKind(utxo *models.Utxo) bool {
	return utxo.TransactionID != nil || utxo.CollateralReturnForTxID != nil
}

// existingUtxoRefs returns the identity keys of the outputs already stored
// under another kind of identity than the one the insert would conflict on: a
// produced output whose hash an import already stored, or an imported output
// whose producing transaction already wrote it. Those pairs would insert as
// duplicates, since no unique index spans both kinds, so the caller treats
// them as conflicts. It is a no-op away from SQLite, whose full unique index
// sees both kinds.
func (s *Store) existingUtxoRefs(
	ctx context.Context,
	db queryer,
	utxos []*models.Utxo,
) (map[string]struct{}, error) {
	if s.dialect.Name() != "sqlite" || len(utxos) == 0 {
		return nil, nil
	}
	var produced, imported []*models.Utxo
	for _, utxo := range utxos {
		if utxoProducedKind(utxo) {
			produced = append(produced, utxo)
		} else {
			imported = append(imported, utxo)
		}
	}
	found := make(map[string]struct{})
	chunk := max(1, s.dialect.ParameterLimit()-1)
	for start := 0; start < len(produced); start += chunk {
		part := produced[start:min(start+chunk, len(produced))]
		hashes := distinctUtxoHashes(part)
		rows, err := db.QueryContext(ctx,
			utxoRefAsWritten+"SELECT tx_id, output_idx FROM utxo INDEXED BY idx_utxo_imported_ref "+
				"WHERE transaction_id IS NULL AND collateral_return_for_tx_id IS NULL "+
				"AND tx_id IN ("+bindPlaceholders(len(hashes))+")",
			hashes...,
		)
		if err != nil {
			return nil, err
		}
		if err := collectUtxoKeys(rows, found); err != nil {
			return nil, err
		}
	}
	for start := 0; start < len(imported); start += chunk {
		part := imported[start:min(start+chunk, len(imported))]
		hashes := distinctUtxoHashes(part)
		rows, err := db.QueryContext(ctx,
			`SELECT t.hash, u.output_idx FROM "transaction" AS t `+
				"JOIN utxo AS u ON u.producer_tx_id = t.id "+
				"WHERE t.hash IN ("+bindPlaceholders(len(hashes))+")",
			hashes...,
		)
		if err != nil {
			return nil, err
		}
		if err := collectUtxoKeys(rows, found); err != nil {
			return nil, err
		}
	}
	return found, nil
}

func distinctUtxoHashes(utxos []*models.Utxo) []any {
	seen := make(map[string]struct{}, len(utxos))
	hashes := make([]any, 0, len(utxos))
	for _, utxo := range utxos {
		if _, ok := seen[string(utxo.TxId)]; ok {
			continue
		}
		seen[string(utxo.TxId)] = struct{}{}
		hashes = append(hashes, utxo.TxId)
	}
	return hashes
}

func collectUtxoKeys(rows *sql.Rows, into map[string]struct{}) error {
	defer rows.Close()
	for rows.Next() {
		var (
			hash []byte
			idx  int64
		)
		if err := rows.Scan(&hash, &idx); err != nil {
			return err
		}
		into[utxoIdentityKey(hash, uint32(idx))] = struct{}{} //nolint:gosec
	}
	return rows.Err()
}
