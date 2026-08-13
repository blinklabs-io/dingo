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
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// queryShelleyUtxoWhole answers GetUTxOWhole: every live UTxO in the
// current ledger state.
//
// cardano-cli's own client-side guidance is that a whole-UTxO dump
// ("query utxo --whole-utxo") is only practical against a small network;
// dingo does not impose an additional limit here, but callers on a
// mainnet-scale chain should expect this to be slow and to return a large
// reply. This exists primarily to support LocalStateQuery-based tooling
// (e.g. the devnet cross-node ledger-state comparison,
// blinklabs-io/dingo#1900) rather than as a query aimed at a busy chain.
func (ls *LedgerState) queryShelleyUtxoWhole() (any, error) {
	ret := make(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	err := ls.db.IterateLiveUtxos(nil, func(u *models.Utxo) error {
		// IterateLiveUtxos documents that a row's Cbor buffer may be
		// reused by the next callback invocation; clone it before
		// decoding so the TransactionOutput retained in ret below never
		// aliases memory this loop is about to overwrite.
		cborCopy := bytes.Clone(u.Cbor)
		txOut, err := ledger.NewTransactionOutputFromCbor(cborCopy)
		if err != nil {
			return fmt.Errorf(
				"decode utxo %x#%d: %w", u.TxId[:8], u.OutputIdx, err,
			)
		}
		ret[olocalstatequery.UtxoId{
			Hash: ledger.NewBlake2b256(u.TxId),
			Idx:  int(u.OutputIdx),
		}] = txOut
		return nil
	})
	if err != nil {
		return nil, err
	}
	return []any{ret}, nil
}
