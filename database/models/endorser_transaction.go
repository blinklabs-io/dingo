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

package models

// EndorserTransaction records that a transaction was applied to the ledger from
// a Leios endorser block (a speculative endorser-resident transaction) rather
// than from a ranking block. It exists only to distinguish a revocable
// endorser-block spend from a final ranking-block spend: on the Musashi
// prototype network, successive endorser blocks carry mutually-conflicting,
// never-confirmed mempool transactions, so when an authoritative ranking-block
// transaction later needs an input a speculative endorser-block transaction
// already spent, that endorser-block transaction is revoked rather than
// wedging the ledger with "UTxO already spent" (issue #2699).
//
// Hash is the endorser-block transaction's hash (the same hash stored in
// transaction.hash and referenced by utxo.spent_at_tx_id / utxo.tx_id, which is
// how a revoke locates the spend and outputs to undo). RbSlot is the
// referencing ranking block's slot, used to prune these rows on rollback. The
// table is populated only when endorser-block conflict resolution is enabled
// (the Musashi network); it stays empty on every other network.
type EndorserTransaction struct {
	ID     uint   `gorm:"primarykey"`
	Hash   []byte `gorm:"uniqueIndex;size:32"`
	RbSlot uint64 `gorm:"index"`
}

func (EndorserTransaction) TableName() string {
	return "endorser_transaction"
}
