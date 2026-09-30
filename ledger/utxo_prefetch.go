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
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/utxoref"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// prefetchBlockUtxos resolves every spend, collateral and reference input of
// txs that no transaction in the block produces, with a single UtxosByRefs
// query. Rows that fail to decode are left out so the point lookup in
// LedgerView.UtxoById reports the error as before. A failed batch yields nil.
func (ls *LedgerState) prefetchBlockUtxos(
	txn *database.Txn,
	txs []lcommon.Transaction,
) map[utxoref.Key]lcommon.Utxo {
	produced := make(map[utxoref.Key]struct{})
	for _, tx := range txs {
		for _, utxo := range tx.Produced() {
			produced[utxoref.ForUtxo(utxo)] = struct{}{}
		}
	}
	seen := make(map[utxoref.Key]lcommon.TransactionInput)
	var refs []models.UtxoId
	add := func(inputs []lcommon.TransactionInput) {
		for _, in := range inputs {
			key := utxoref.ForInput(in)
			if _, ok := produced[key]; ok {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = in
			refs = append(refs, models.UtxoId{
				Hash: in.Id().Bytes(),
				Idx:  in.Index(),
			})
		}
	}
	for _, tx := range txs {
		add(tx.Inputs())
		add(tx.Collateral())
		add(tx.ReferenceInputs())
	}
	if len(refs) == 0 {
		return nil
	}
	ls.utxoBatchLookups.Add(1)
	rows, err := ls.db.UtxosByRefs(refs, txn)
	if err != nil {
		// Prefetching is an optimization: a failed batch must not reject a
		// block whose validators may never read the failing ref. The same
		// fault resurfaces from the point lookup if a rule does read it.
		ls.config.Logger.Debug(
			"block UTxO prefetch failed, falling back to point lookups",
			"component", "ledger",
			"error", err,
		)
		return nil
	}
	ls.utxoByRefReads.Add(uint64(len(rows)))
	out := make(map[utxoref.Key]lcommon.Utxo, len(rows))
	for i := range rows {
		output, err := rows[i].Decode()
		if err != nil || output == nil {
			continue
		}
		key := utxoref.Key{
			TxId:  lcommon.NewBlake2b256(rows[i].TxId),
			Index: rows[i].OutputIdx,
		}
		id, ok := seen[key]
		if !ok {
			continue
		}
		out[key] = lcommon.Utxo{Id: id, Output: output}
	}
	return out
}

// forgetSpentPrefetchedUtxos drops the inputs and collateral of an applied
// transaction from the prefetched set. Over-dropping is safe: a missing entry
// falls back to the database read.
func forgetSpentPrefetchedUtxos(
	prefetched map[utxoref.Key]lcommon.Utxo,
	tx lcommon.Transaction,
) {
	if len(prefetched) == 0 {
		return
	}
	for _, in := range tx.Inputs() {
		delete(prefetched, utxoref.ForInput(in))
	}
	for _, in := range tx.Collateral() {
		delete(prefetched, utxoref.ForInput(in))
	}
}
