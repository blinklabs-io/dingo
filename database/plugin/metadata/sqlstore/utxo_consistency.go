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
	"fmt"
)

// UtxoConsistencyReport counts the rows that break the identifier
// relationships of the utxo tables. Each count has the lowest offending utxo
// id beside it, zero when the count is zero.
type UtxoConsistencyReport struct {
	// ProducerHashMismatches counts outputs whose tx_id is not the hash of
	// their producing transaction.
	ProducerHashMismatches   int64
	ProducerHashMismatchSeen int64
	// OrphanProducers counts outputs naming a producing transaction row that
	// does not exist.
	OrphanProducers    int64
	OrphanProducerSeen int64
	// OrphanSpenders counts outputs whose spent_by_tx_id names no
	// transaction row.
	OrphanSpenders    int64
	OrphanSpenderSeen int64
	// OrphanReferenceEdges and OrphanCollateralEdges count edge rows whose
	// transaction row or output row does not exist.
	OrphanReferenceEdges    int64
	OrphanReferenceEdgeSeen int64
	OrphanCollateralEdges   int64
	OrphanCollateralSeen    int64
}

// Consistent reports whether every count is zero.
func (r UtxoConsistencyReport) Consistent() bool {
	return r.ProducerHashMismatches == 0 && r.OrphanProducers == 0 &&
		r.OrphanSpenders == 0 && r.OrphanReferenceEdges == 0 &&
		r.OrphanCollateralEdges == 0
}

// utxoConsistencyStatements are the five counting statements of one dialect.
// Each returns the count and the lowest offending utxo id, composed once per
// dialect from its identifier quote.
type utxoConsistencyStatements struct {
	producerHash, orphanProducer, orphanSpender string
	referenceEdge, collateralEdge               string
}

func newUtxoConsistencyStatements(quote string) utxoConsistencyStatements {
	tx := quote + "transaction" + quote
	edge := func(table string) string {
		return "SELECT COUNT(*), COALESCE(MIN(e.utxo_id), 0) FROM " + table +
			" AS e WHERE NOT EXISTS (SELECT 1 FROM " + tx +
			" AS t WHERE t.id = e.transaction_id) OR NOT EXISTS " +
			"(SELECT 1 FROM utxo AS u WHERE u.id = e.utxo_id)"
	}
	return utxoConsistencyStatements{
		producerHash: "SELECT COUNT(*), COALESCE(MIN(u.id), 0) FROM utxo AS u " +
			"JOIN " + tx + " AS t ON t.id = COALESCE(" +
			"u.transaction_id, u.collateral_return_for_tx_id) " +
			"WHERE u.tx_id IS NULL OR u.tx_id <> t.hash",
		orphanProducer: "SELECT COUNT(*), COALESCE(MIN(u.id), 0) FROM utxo AS u " +
			"WHERE (u.transaction_id IS NOT NULL OR " +
			"u.collateral_return_for_tx_id IS NOT NULL) AND NOT EXISTS (" +
			"SELECT 1 FROM " + tx + " AS t WHERE t.id = COALESCE(" +
			"u.transaction_id, u.collateral_return_for_tx_id))",
		orphanSpender: "SELECT COUNT(*), COALESCE(MIN(u.id), 0) FROM utxo AS u " +
			"WHERE u.spent_by_tx_id IS NOT NULL AND NOT EXISTS (" +
			"SELECT 1 FROM " + tx + " AS t WHERE t.id = u.spent_by_tx_id)",
		referenceEdge:  edge("utxo_reference_edge"),
		collateralEdge: edge("utxo_collateral_edge"),
	}
}

var utxoConsistencyByDialect = map[string]utxoConsistencyStatements{
	"sqlite":   newUtxoConsistencyStatements(`"`),
	"postgres": newUtxoConsistencyStatements(`"`),
	"mysql":    newUtxoConsistencyStatements("`"),
}

// CheckUtxoConsistency reads the whole utxo table and both edge tables and
// reports the identifier relationships that do not hold. It writes nothing.
func (s *Store) CheckUtxoConsistency(
	ctx context.Context,
) (UtxoConsistencyReport, error) {
	var report UtxoConsistencyReport
	statements, ok := utxoConsistencyByDialect[s.dialect.Name()]
	if !ok {
		return report, fmt.Errorf(
			"utxo consistency check: unsupported dialect %q",
			s.dialect.Name(),
		)
	}
	db, _, err := s.readDBFromTxn(nil)
	if err != nil {
		return report, err
	}
	for _, check := range []struct {
		name  string
		query string
		count *int64
		seen  *int64
	}{
		{"producer hash", statements.producerHash,
			&report.ProducerHashMismatches, &report.ProducerHashMismatchSeen},
		{"orphan producer", statements.orphanProducer,
			&report.OrphanProducers, &report.OrphanProducerSeen},
		{"orphan spender", statements.orphanSpender,
			&report.OrphanSpenders, &report.OrphanSpenderSeen},
		{"reference edge", statements.referenceEdge,
			&report.OrphanReferenceEdges, &report.OrphanReferenceEdgeSeen},
		{"collateral edge", statements.collateralEdge,
			&report.OrphanCollateralEdges, &report.OrphanCollateralSeen},
	} {
		if err := db.QueryRowContext(ctx, check.query).Scan(
			check.count, check.seen,
		); err != nil {
			return UtxoConsistencyReport{}, fmt.Errorf(
				"utxo consistency check %s: %w", check.name, err,
			)
		}
	}
	return report, nil
}
