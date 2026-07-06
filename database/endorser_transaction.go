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

import (
	"fmt"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// AddEndorserTransactions records the given transaction hashes as endorser-block
// (speculative) transactions applied under the ranking block at rbSlot, so a
// later authoritative ranking-block transaction can revoke a conflicting one
// (issue #2699). Only used when endorser-block conflict resolution is enabled
// (the Musashi network).
func (d *Database) AddEndorserTransactions(
	hashes [][]byte,
	rbSlot uint64,
	txn *Txn,
) error {
	if len(hashes) == 0 {
		return nil
	}
	owned := false
	if txn == nil {
		txn = d.Transaction(true)
		owned = true
		defer txn.Release()
	}
	if err := d.metadata.AddEndorserTransactions(
		hashes, rbSlot, txn.Metadata(),
	); err != nil {
		return fmt.Errorf("add endorser transactions: %w", err)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf("commit add endorser transactions: %w", err)
		}
	}
	return nil
}

// FilterEndorserTransactions returns the subset of the given hashes that are
// recorded endorser-block transactions.
func (d *Database) FilterEndorserTransactions(
	hashes [][]byte,
	txn *Txn,
) ([][]byte, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	return d.metadata.FilterEndorserTransactions(hashes, txn.Metadata())
}

// DeleteEndorserTransactionsAfterSlot removes endorser-transaction provenance
// rows whose referencing ranking block is after the given slot, so a chain
// rollback drops the provenance for the spends it is undoing.
func (d *Database) DeleteEndorserTransactionsAfterSlot(
	slot uint64,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = d.Transaction(true)
		owned = true
		defer txn.Release()
	}
	if err := d.metadata.DeleteEndorserTransactionsAfterSlot(
		slot, txn.Metadata(),
	); err != nil {
		return err
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf(
				"commit delete endorser transactions: %w",
				err,
			)
		}
	}
	return nil
}

// RevokeEndorserTransactionClosure revokes the endorser-block transactions in
// the given candidate root set together with their transitive
// endorser-on-endorser closure, and returns the number of transactions revoked.
//
// Roots are candidate spender hashes (e.g. the transactions that spent an input
// an authoritative ranking-block transaction now needs); non-endorser roots are
// ignored. The closure is expanded because revoking an endorser transaction
// deletes its produced outputs, so any endorser transaction that spent one of
// those outputs must also be revoked. A ranking-block transaction never spends
// an endorser output (the reference ledger forbids it), so the closure only
// ever contains endorser transactions and never revokes a ranking-block
// transaction.
func (d *Database) RevokeEndorserTransactionClosure(
	roots [][]byte,
	txn *Txn,
) (int, error) {
	if len(roots) == 0 {
		return 0, nil
	}
	owned := false
	if txn == nil {
		txn = d.Transaction(true)
		owned = true
		defer txn.Release()
	}
	// Seed the closure with the endorser transactions among the roots.
	frontier, err := d.metadata.FilterEndorserTransactions(roots, txn.Metadata())
	if err != nil {
		return 0, fmt.Errorf("revoke closure: filter roots: %w", err)
	}
	closure := make(map[string][]byte, len(frontier))
	for _, h := range frontier {
		closure[string(h)] = h
	}
	// Expand: any endorser transaction that spent an output of a transaction
	// already in the closure must also be revoked. Terminates because the
	// closure is finite and grows monotonically.
	for len(frontier) > 0 {
		spenders, err := d.metadata.UtxoSpenders(frontier, txn.Metadata())
		if err != nil {
			return 0, fmt.Errorf("revoke closure: utxo spenders: %w", err)
		}
		ebSpenders, err := d.metadata.FilterEndorserTransactions(
			spenders, txn.Metadata(),
		)
		if err != nil {
			return 0, fmt.Errorf("revoke closure: filter spenders: %w", err)
		}
		frontier = frontier[:0]
		for _, h := range ebSpenders {
			if _, seen := closure[string(h)]; seen {
				continue
			}
			closure[string(h)] = h
			frontier = append(frontier, h)
		}
	}
	if len(closure) == 0 {
		return 0, nil
	}
	revoke := make([][]byte, 0, len(closure))
	for _, h := range closure {
		revoke = append(revoke, h)
	}
	if err := d.metadata.RevokeEndorserTransactions(
		revoke, txn.Metadata(),
	); err != nil {
		return 0, fmt.Errorf("revoke endorser transactions: %w", err)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return 0, fmt.Errorf("commit revoke endorser transactions: %w", err)
		}
	}
	return len(revoke), nil
}

// CleanupSkippedEndorserTransaction removes partial writes left by a speculative
// endorser-block transaction that failed with a skippable UTxO conflict.
func (d *Database) CleanupSkippedEndorserTransaction(
	tx lcommon.Transaction,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = d.Transaction(true)
		owned = true
		defer txn.Release()
	}

	txHash := ledgerHashBytes(tx.Hash())
	if blob := d.Blob(); blob != nil && txn.Blob() != nil {
		if err := blob.DeleteTx(txn.Blob(), txHash); err != nil {
			return fmt.Errorf("cleanup skipped endorser tx blob: %w", err)
		}
		for _, utxo := range tx.Produced() {
			if err := blob.DeleteUtxo(
				txn.Blob(),
				ledgerInputIDBytes(utxo.Id),
				utxo.Id.Index(),
			); err != nil {
				return fmt.Errorf(
					"cleanup skipped endorser utxo blob: %w",
					err,
				)
			}
		}
	}
	if err := d.metadata.RevokeEndorserTransactions(
		[][]byte{txHash},
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf("cleanup skipped endorser metadata: %w", err)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf("commit skipped endorser cleanup: %w", err)
		}
	}
	return nil
}
