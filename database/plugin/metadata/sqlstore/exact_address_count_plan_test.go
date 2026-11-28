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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// The exact-address count must find the address's outputs through the
// utxo_address index and the credential's candidates through an
// address_transaction key index, never by scanning a history table.
func TestExactAddressTransactionCountSearchesIndexes(t *testing.T) {
	t.Parallel()
	s := newFilePlannerStatsStore(t, prometheus.NewRegistry())
	for _, q := range []string{
		`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<20000)
INSERT INTO "transaction"(id,hash) SELECT i,randomblob(32) FROM n`,
		`INSERT INTO applied_transaction(tx_id,slot,block_index)
SELECT id,id,0 FROM "transaction"`,
		`INSERT INTO utxo(tx_id,output_idx,added_slot,deleted_slot,amount)
SELECT id,0,id,0,'1' FROM "transaction"`,
		// One output in 2,000 is at the counted address; the rest are at a
		// pointer sibling sharing its payment key.
		`INSERT INTO utxo_address(utxo_id,address)
SELECT id, CASE WHEN id % 2000 = 0 THEN x'01' ELSE x'02' END FROM utxo`,
		`INSERT INTO address_transaction(
    payment_key,payment_script,staking_key,credential_tag,
    transaction_id,slot,tx_index)
SELECT x'aa',0,NULL,0,id,id,0 FROM "transaction"`,
	} {
		_, err := s.writeDB.Exec(q)
		require.NoError(t, err)
	}
	predicate, keyArgs := addressTransactionPredicate([]byte{0xaa}, 0, nil)
	query, args := exactAddressTransactionCountQuery(
		predicate, keyArgs, []byte{0x01}, nil, nil,
	)
	for _, analyzed := range []bool{false, true} {
		if analyzed {
			_, err := s.writeDB.Exec("ANALYZE")
			require.NoError(t, err)
		}
		plan := explainQueryPlan(t, s, query, args...)
		require.Contains(t, plan,
			"SEARCH utxo_address USING COVERING INDEX idx_utxo_address_order (address=?)")
		require.Contains(t, plan, "SEARCH address_transaction USING")
		for _, table := range []string{
			"address_transaction", "utxo", "utxo_address", "applied_transaction",
		} {
			require.NotRegexp(t, `SCAN `+table+`\b`, plan, "analyzed=%v", analyzed)
		}
	}
	got, err := s.CountTransactionsByExactAddress(
		[]byte{0x01}, []byte{0xaa}, 0, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	require.Equal(t, 10, got)
}
