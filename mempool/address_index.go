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

package mempool

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/utxoref"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// UtxoResolver is the optional capability of a TxValidator without
// validation sessions that lets the pool find the address a confirmed input
// belongs to. Without it, or a ResolvingTxValidationSessionProvider, only
// output addresses and the addresses of other pool transactions' outputs are
// indexed.
type UtxoResolver interface {
	UtxosByRefs(refs []models.UtxoId) ([]models.Utxo, error)
}

// ResolvingTxValidationSessionProvider is a validation session that also
// resolves confirmed outputs, through the same storage snapshot and
// connection its validations read. The pool resolves a transaction's
// confirmed inputs there, after validating it, so an input the validation
// found is found again and one it could not have found does not exist.
// LedgerState implements it.
type ResolvingTxValidationSessionProvider interface {
	WithResolvingTxValidationSession(context.Context, func(
		validate func(
			tx gledger.Transaction,
			consumedUtxos map[utxoref.Key]struct{},
			createdUtxos map[utxoref.Key]lcommon.Utxo,
			accounts *utxoref.StateOverlay,
		) error,
		stillCurrent func() bool,
		commitIfCurrent func(func() error) (bool, error),
		resolve func(refs []models.UtxoId) ([]models.Utxo, error),
	) error) error
}

// addressIndex maps an address to the pool transactions that pay it or spend
// one of its outputs. The pool's state lock guards it, and every mutation of
// the pool's transaction list must be mirrored here.
type addressIndex struct {
	byAddress map[string]map[string]*MempoolTransaction
}

func newAddressIndex() addressIndex {
	return addressIndex{byAddress: make(map[string]map[string]*MempoolTransaction)}
}

func (i *addressIndex) add(tx *MempoolTransaction) {
	for _, address := range tx.addresses {
		txs := i.byAddress[address]
		if txs == nil {
			txs = make(map[string]*MempoolTransaction)
			i.byAddress[address] = txs
		}
		txs[tx.Hash] = tx
	}
}

func (i *addressIndex) remove(tx *MempoolTransaction) {
	for _, address := range tx.addresses {
		txs := i.byAddress[address]
		delete(txs, tx.Hash)
		if len(txs) == 0 {
			delete(i.byAddress, address)
		}
	}
}

func (i *addressIndex) rebuild(txs []*MempoolTransaction) {
	i.byAddress = make(map[string]map[string]*MempoolTransaction)
	for _, tx := range txs {
		i.add(tx)
	}
}

// outputAddresses lists the distinct addresses of the outputs the
// transaction creates when applied, in first-seen order. A transaction
// marked as failing phase 2 creates only its collateral return.
func outputAddresses(tx gledger.Transaction) []string {
	var addresses []string
	seen := make(map[string]struct{})
	for _, utxo := range tx.Produced() {
		if utxo.Output == nil {
			continue
		}
		address := utxo.Output.Address().String()
		if _, ok := seen[address]; !ok {
			seen[address] = struct{}{}
			addresses = append(addresses, address)
		}
	}
	return addresses
}

// ErrInputResolution reports that admission could not read the addresses
// of a transaction's confirmed inputs from storage. No verdict was reached
// on the transaction.
var ErrInputResolution = errors.New(
	"mempool: confirmed input address resolution failed",
)

var errNilConfirmedOutput = errors.New("decoded output is nil")

// inputAddresses lists the addresses of the inputs the transaction consumes:
// other pool transactions' outputs from created, the overlay the transaction
// was validated against, and confirmed outputs through resolve, which reads
// the snapshot the validation read. An input in neither had no live output
// when the transaction was validated and is left out. A nil resolve leaves
// confirmed inputs out. A lookup failure, or a stored output that does not
// decode, is a storage fault, as ledger validation treats it, and fails with
// ErrInputResolution.
func inputAddresses(
	tx gledger.Transaction,
	created map[utxoref.Key]lcommon.Utxo,
	resolve func(refs []models.UtxoId) ([]models.Utxo, error),
) ([]string, error) {
	consumed := tx.Consumed()
	refs := make([]models.UtxoId, 0, len(consumed))
	for _, input := range consumed {
		if _, ok := created[utxoref.ForInput(input)]; ok {
			continue
		}
		refs = append(refs, models.UtxoId{
			Hash: input.Id().Bytes(),
			Idx:  input.Index(),
		})
	}
	resolved := make(map[utxoref.Key]string, len(refs))
	if resolve != nil && len(refs) > 0 {
		utxos, err := resolve(refs)
		if err != nil {
			return nil, fmt.Errorf(
				"resolve confirmed input addresses: %w: %w",
				ErrInputResolution,
				err,
			)
		}
		for _, utxo := range utxos {
			output, err := utxo.Decode()
			if err == nil && output == nil {
				err = errNilConfirmedOutput
			}
			if err != nil || output == nil {
				return nil, fmt.Errorf(
					"decode confirmed input %s#%d: %w: %w",
					hex.EncodeToString(utxo.TxId),
					utxo.OutputIdx,
					ErrInputResolution,
					err,
				)
			}
			key := utxoref.Key{
				TxId:  lcommon.NewBlake2b256(utxo.TxId),
				Index: utxo.OutputIdx,
			}
			resolved[key] = output.Address().String()
		}
	}
	var ret []string
	for _, input := range consumed {
		key := utxoref.ForInput(input)
		if utxo, ok := created[key]; ok {
			if utxo.Output != nil {
				ret = append(ret, utxo.Output.Address().String())
			}
			continue
		}
		if address, ok := resolved[key]; ok {
			ret = append(ret, address)
		}
	}
	return ret, nil
}

// TransactionHashesByAddress returns the hashes of the pool transactions
// that pay address or spend one of its outputs, in the order Transactions
// reports them. It reads the address index and decodes nothing.
func (m *Mempool) TransactionHashesByAddress(address string) []string {
	m.RLock()
	defer m.RUnlock()
	matches := m.addresses.byAddress[address]
	if len(matches) == 0 {
		return []string{}
	}
	if m.dag != nil {
		order, err := m.dagPrefixLocked(0)
		if err == nil {
			ret := make([]string, 0, len(matches))
			for _, hash := range order {
				if _, ok := matches[hash]; ok {
					ret = append(ret, hash)
				}
			}
			return ret
		}
		m.logger.Error(
			"falling back to admission order for mempool address listing",
			"component", "mempool",
			"error", err,
		)
	}
	txs := make([]*MempoolTransaction, 0, len(matches))
	for _, tx := range matches {
		txs = append(txs, tx)
	}
	slices.SortFunc(txs, func(a, b *MempoolTransaction) int {
		switch {
		case a.seq < b.seq:
			return -1
		case a.seq > b.seq:
			return 1
		}
		return 0
	})
	ret := make([]string, len(txs))
	for i, tx := range txs {
		ret[i] = tx.Hash
	}
	return ret
}

// mergeAddresses appends to addresses each of extra it does not already hold.
func mergeAddresses(addresses, extra []string) []string {
	if len(extra) == 0 {
		return addresses
	}
	seen := make(map[string]struct{}, len(addresses)+len(extra))
	for _, address := range addresses {
		seen[address] = struct{}{}
	}
	for _, address := range extra {
		if _, ok := seen[address]; !ok {
			seen[address] = struct{}{}
			addresses = append(addresses, address)
		}
	}
	return addresses
}
