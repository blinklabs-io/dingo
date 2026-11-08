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

package database

import "context"

// RebuildScriptLockedSupply replaces the running script-locked supply
// aggregate with a full scan of the live UTxOs, recorded at slot. A bulk load
// that inserts live outputs without the per-write maintenance (ledger-state
// import) calls it once the outputs are in place. A nil txn runs in its own
// read-write transaction.
func (d *Database) RebuildScriptLockedSupply(
	ctx context.Context,
	slot uint64,
	txn *Txn,
) error {
	if txn == nil {
		return d.MetadataTxn(ctx, true).Do(func(t *Txn) error {
			return d.utxoStore().RebuildScriptLockedSupply(slot, t.Metadata())
		})
	}
	return d.utxoStore().RebuildScriptLockedSupply(slot, txn.Metadata())
}
