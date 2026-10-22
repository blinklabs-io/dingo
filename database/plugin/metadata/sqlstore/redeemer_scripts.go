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
	"fmt"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gscript "github.com/blinklabs-io/gouroboros/ledger/common/script"
)

const deleteRedeemerScriptsSQL = `-- name: DeleteRedeemerScriptsByTransaction :exec
DELETE FROM redeemer_script WHERE transaction_id = ?`

// redeemerScriptHashes resolves the script each redeemer of a transaction
// executes, keyed by redeemer pointer. A redeemer is attributed only when its
// script can be determined from the transaction and the stored outputs it
// spends; one that cannot (an input the index never saw, a pointer past the
// end of the set it indexes, a spend of an output no script locks) is left out
// rather than failing the block that carries it.
func (s *Store) redeemerScriptHashes(
	ctx context.Context,
	db queryer,
	transaction lcommon.Transaction,
) (map[lcommon.RedeemerKey]lcommon.ScriptHash, error) {
	witnesses := transaction.Witnesses()
	if witnesses == nil || witnesses.Redeemers() == nil {
		return nil, nil
	}
	// A spend redeemer indexes the inputs in ledger set order, which a body
	// need not list them in.
	inputs := gscript.SortInputs(transaction.Inputs())
	var mint lcommon.MultiAsset[lcommon.MultiAssetTypeMint]
	if assetMint := transaction.AssetMint(); assetMint != nil {
		mint = *assetMint
	}
	var zero lcommon.ScriptHash
	ret := make(map[lcommon.RedeemerKey]lcommon.ScriptHash)
	for key := range witnesses.Redeemers().Iter() {
		if key.Tag == lcommon.RedeemerTagSpend {
			hash, ok, err := s.spendScriptHash(ctx, db, inputs, key.Index)
			if err != nil {
				return nil, err
			}
			if ok {
				ret[key] = hash
			}
			continue
		}
		// None of these purposes reads a resolved input, and the script hash
		// of a purpose does not depend on the protocol version, which only
		// shapes the script context built from it.
		purpose, err := gscript.BuildScriptPurpose(
			key,
			nil,
			inputs,
			mint,
			transaction.Certificates(),
			transaction.Withdrawals(),
			transaction.VotingProcedures(),
			transaction.ProposalProcedures(),
			nil,
			0,
		)
		// A purpose that cannot be built leaves the redeemer unattributed
		// rather than failing the block that carries it.
		if err != nil || purpose == nil {
			continue
		}
		if hash := purpose.ScriptHash(); hash != zero {
			ret[key] = hash
		}
	}
	return ret, nil
}

const spendScriptHashQuery = `-- name: GetSpendScriptHashByOutput :one
SELECT payment_key, payment_script FROM utxo
WHERE tx_id = ? AND output_idx = ?`

// spendScriptHash returns the script locking the input a spend redeemer
// points at, as recorded when that output was created.
func (s *Store) spendScriptHash(
	ctx context.Context,
	db queryer,
	inputs []lcommon.TransactionInput,
	index uint32,
) (lcommon.ScriptHash, bool, error) {
	if uint64(index) >= uint64(len(inputs)) {
		return lcommon.ScriptHash{}, false, nil
	}
	input := inputs[index]
	var (
		paymentKey []byte
		isScript   sql.NullBool
	)
	err := s.queryRowCached(
		ctx, db, spendScriptHashQuery, input.Id().Bytes(), input.Index(),
	).Scan(&paymentKey, &isScript)
	if errors.Is(err, sql.ErrNoRows) {
		return lcommon.ScriptHash{}, false, nil
	}
	if err != nil {
		return lcommon.ScriptHash{}, false, fmt.Errorf(
			"get spent output %s: %w",
			input.String(),
			err,
		)
	}
	if !isScript.Valid || !isScript.Bool ||
		len(paymentKey) != lcommon.AddressHashSize {
		return lcommon.ScriptHash{}, false, nil
	}
	return lcommon.ScriptHash(paymentKey), true, nil
}

// storeRedeemerScripts records which script each redeemer of the transaction
// executes. Replaying a transaction rewrites its rows wholesale.
func (s *Store) storeRedeemerScripts(
	ctx context.Context,
	db queryer,
	rows *rowBatch,
	transactionID int64,
	transaction lcommon.Transaction,
	slot uint64,
	index uint32,
	transactionIsNew bool,
) error {
	if !transactionIsNew {
		if _, err := s.execCached(
			ctx, db, deleteRedeemerScriptsSQL, transactionID,
		); err != nil {
			return fmt.Errorf("delete existing redeemer scripts: %w", err)
		}
	}
	hashes, err := s.redeemerScriptHashes(ctx, db, transaction)
	if err != nil {
		return err
	}
	for key, hash := range hashes {
		rows.add(redeemerScriptShape,
			transactionID,
			uint8(key.Tag),
			key.Index,
			hash.Bytes(),
			slot,
			index,
		)
	}
	return nil
}
