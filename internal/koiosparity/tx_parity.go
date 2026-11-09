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

package koiosparity

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
)

// EpochTransactionSampleSize bounds how many transactions per epoch are
// compared against Koios /tx_info.
const EpochTransactionSampleSize = 20

// DingoTransaction is the stored view of one transaction, reduced to the sets
// the Koios comparison covers. Every slice is sorted so two views of the same
// transaction compare equal regardless of storage order.
type DingoTransaction struct {
	Hash  string
	Valid bool
	// Inputs are the refs the ledger consumed: the body inputs of a valid
	// transaction, the collateral inputs of a phase-2-invalid one.
	Inputs []string
	// Collateral and ReferenceInputs are "<tx_hash>#<index>" refs.
	Collateral      []string
	ReferenceInputs []string
	// Outputs are the outputs the ledger produced, rendered by
	// outputAmountEntry: the body outputs of a valid transaction, the
	// collateral return of a phase-2-invalid one.
	Outputs []string
}

// TransactionParitySource is implemented by a RewardParitySource that can
// also report a bounded sample of an epoch's stored transactions. A source
// that retains no complete transaction associations returns no sample, and
// the epoch is then not compared on transactions.
type TransactionParitySource interface {
	// GetEpochTransactionSample returns up to limit transactions of epoch,
	// in a stable order, so the fetch and check phases see the same sample.
	GetEpochTransactionSample(
		ctx context.Context,
		epoch uint64,
		limit int,
	) ([]DingoTransaction, error)
}

var _ TransactionParitySource = (*DatabaseSource)(nil)

func utxoRef(hash string, idx int) string {
	return hash + "#" + strconv.Itoa(idx)
}

// outputAmountEntry renders what the comparison covers for one output: its
// index, address, lovelace and native-asset amounts. Both sides build it here
// so the encoding cannot drift between them.
func outputAmountEntry(
	idx int,
	addr, lovelace string,
	assets []KoiosTxInfoAsset,
) string {
	sorted := make([]KoiosTxInfoAsset, len(assets))
	copy(sorted, assets)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].PolicyID != sorted[j].PolicyID {
			return sorted[i].PolicyID < sorted[j].PolicyID
		}
		return sorted[i].AssetName < sorted[j].AssetName
	})
	var sb strings.Builder
	fmt.Fprintf(&sb, "%d|%s|%s", idx, addr, lovelace)
	for _, a := range sorted {
		fmt.Fprintf(&sb, "|%s.%s=%s", a.PolicyID, a.AssetName, a.Quantity)
	}
	return sb.String()
}

func sortedCopy(in []string) []string {
	out := make([]string, len(in))
	copy(out, in)
	sort.Strings(out)
	return out
}

func koiosRefs(refs []KoiosTxInfoUtxoRef) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, utxoRef(r.TxHash, r.TxIndex))
	}
	sort.Strings(out)
	return out
}

func koiosOutputs(outs []KoiosTxInfoOutput) []string {
	entries := make([]string, 0, len(outs))
	for _, o := range outs {
		entries = append(entries, outputAmountEntry(
			o.TxIndex, o.PaymentAddr.Bech32, o.Value, o.AssetList,
		))
	}
	sort.Strings(entries)
	return entries
}

// CompareEpochTransactions compares each sampled transaction's consumed
// inputs, collateral inputs, reference inputs and produced outputs (with
// amounts) against Koios /tx_info. A phase-2-invalid transaction is compared
// on what the ledger applied: the collateral it consumed and the collateral
// return it produced, as KoiosTxInfoItem.Consumed and Produced report.
// A sampled transaction with no Koios row is an ERROR, never a pass.
func CompareEpochTransactions(
	network string,
	epoch uint64,
	sample []DingoTransaction,
	koios map[string]KoiosTxInfoItem,
	now time.Time,
) []CheckMismatch {
	var out []CheckMismatch
	add := func(field, dingo, koiosVal, category string) {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			Field:      field,
			DingoValue: dingo,
			KoiosValue: koiosVal,
			Category:   category,
			CheckedAt:  now,
		})
	}
	for _, d := range sample {
		item, ok := koios[d.Hash]
		if !ok {
			add("tx_info:"+d.Hash, "present", "", CategoryDBMissing)
			continue
		}
		field := func(name string) string { return "tx:" + d.Hash + ":" + name }
		if d.Valid != item.IsValid() {
			add(
				field("valid"),
				strconv.FormatBool(d.Valid),
				strconv.FormatBool(item.IsValid()),
				CategoryValueMismatch,
			)
		}
		sets := []struct {
			name         string
			dingo, koios []string
		}{
			{"inputs", d.Inputs, koiosRefs(item.Consumed())},
			{
				"collateral_inputs",
				d.Collateral,
				koiosRefs(item.CollateralInputs),
			},
			{
				"reference_inputs",
				d.ReferenceInputs,
				koiosRefs(item.ReferenceInputs),
			},
			{"outputs", d.Outputs, koiosOutputs(item.Produced())},
		}
		for _, set := range sets {
			if !slices.Equal(sortedCopy(set.dingo), set.koios) {
				add(
					field(set.name),
					strings.Join(sortedCopy(set.dingo), ";"),
					strings.Join(set.koios, ";"),
					CategoryValueMismatch,
				)
			}
		}
	}
	return out
}

func utxoRefs(utxos []models.Utxo) []string {
	out := make([]string, 0, len(utxos))
	for _, u := range utxos {
		out = append(out, utxoRef(hex.EncodeToString(u.TxId), int(u.OutputIdx)))
	}
	sort.Strings(out)
	return out
}

func dingoOutput(u *models.Utxo) string {
	assets := make([]KoiosTxInfoAsset, 0, len(u.Assets))
	for _, a := range u.Assets {
		assets = append(assets, KoiosTxInfoAsset{
			PolicyID:  hex.EncodeToString(a.PolicyId),
			AssetName: hex.EncodeToString(a.Name),
			Quantity:  strconv.FormatUint(uint64(a.Amount), 10),
		})
	}
	return outputAmountEntry(
		int(u.OutputIdx),
		string(u.Address),
		strconv.FormatUint(uint64(u.Amount), 10),
		assets,
	)
}

// dingoTransactionFromModel reduces a stored transaction to the compared
// sets. Inputs are the stored spends, which the writer records from the
// ledger's consumed set, and a collateral return is a produced output at its
// own index.
func dingoTransactionFromModel(tx *models.Transaction) DingoTransaction {
	outputs := make([]string, 0, len(tx.Outputs)+1)
	for i := range tx.Outputs {
		outputs = append(outputs, dingoOutput(&tx.Outputs[i]))
	}
	if tx.CollateralReturn != nil {
		outputs = append(outputs, dingoOutput(tx.CollateralReturn))
	}
	sort.Strings(outputs)
	return DingoTransaction{
		Hash:            hex.EncodeToString(tx.Hash),
		Valid:           tx.Valid,
		Inputs:          utxoRefs(tx.Inputs),
		Collateral:      utxoRefs(tx.Collateral),
		ReferenceInputs: utxoRefs(tx.ReferenceInputs),
		Outputs:         outputs,
	}
}

// resolveOutputAddresses fills in the address of each produced output. The
// metadata store has no address column: the output's address lives only in
// its CBOR, which is resolved from the blob store.
func (s *DatabaseSource) resolveOutputAddresses(
	ctx context.Context,
	txn *database.Txn,
	tx *models.Transaction,
) error {
	outs := make([]*models.Utxo, 0, len(tx.Outputs)+1)
	for i := range tx.Outputs {
		outs = append(outs, &tx.Outputs[i])
	}
	if tx.CollateralReturn != nil {
		outs = append(outs, tx.CollateralReturn)
	}
	for _, u := range outs {
		cbor, err := s.db.ResolveUtxoCborWithRecovery(
			ctx, u.TxId, u.OutputIdx, txn,
		)
		if err != nil {
			return fmt.Errorf(
				"output %x#%d cbor: %w", u.TxId, u.OutputIdx, err,
			)
		}
		decoded, err := ledger.NewTransactionOutputFromCbor(cbor)
		if err != nil {
			return fmt.Errorf(
				"output %x#%d decode: %w", u.TxId, u.OutputIdx, err,
			)
		}
		u.Address = []byte(decoded.Address().String())
	}
	return nil
}

// GetEpochTransactionSample returns the first limit transactions, in block
// order, of epoch. Only API storage mode is sampled: core mode hard-deletes
// consumed UTxOs after the stability window and does not retain the
// collateral and reference associations, so an old transaction there would
// read as having lost inputs.
func (s *DatabaseSource) GetEpochTransactionSample(
	ctx context.Context,
	epoch uint64,
	limit int,
) ([]DingoTransaction, error) {
	if limit <= 0 || s.db.StorageMode() != types.StorageModeAPI {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	txn := s.db.Transaction(ctx, false)
	defer txn.Release()
	ep, err := s.db.GetEpoch(epoch, txn)
	if err != nil {
		return nil, fmt.Errorf("epoch %d lookup: %w", epoch, err)
	}
	if ep == nil || ep.LengthInSlots == 0 {
		return nil, nil
	}
	it := s.db.BlocksInRangeContext(
		ctx, ep.StartSlot, ep.StartSlot+uint64(ep.LengthInSlots)-1,
	)
	defer it.Close()
	var sample []DingoTransaction
	for len(sample) < limit {
		blk, err := it.NextRaw()
		if err != nil {
			return nil, fmt.Errorf("epoch %d blocks: %w", epoch, err)
		}
		if blk == nil {
			break
		}
		txs, err := s.db.GetTransactionsByBlockHash(ctx, blk.Hash, txn)
		if err != nil {
			return nil, err
		}
		for i := range txs {
			if len(sample) == limit {
				break
			}
			if err := s.resolveOutputAddresses(ctx, txn, &txs[i]); err != nil {
				return nil, err
			}
			sample = append(sample, dingoTransactionFromModel(&txs[i]))
		}
	}
	return sample, nil
}

// fetchTransactionsIfNeeded caches the /tx_info rows of the epoch's sampled
// transactions that are not cached yet. A Koios failure is logged and
// swallowed: the check then records each uncached transaction as a missing
// reference (an ERROR), and the rest of the epoch is still compared. Only
// cancellation is returned.
func (o *Observer) fetchTransactionsIfNeeded(
	ctx context.Context,
	epoch uint64,
) error {
	source, ok := o.cfg.Source.(TransactionParitySource)
	if !ok {
		return nil
	}
	fail := func(err error) error {
		if cancelled(ctx, err) {
			return err
		}
		o.cfg.Logger.Warn("koiosparity observer: tx_info fetch failed",
			"network", o.cfg.Network, "epoch", epoch, "error", err)
		return nil
	}
	sample, err := source.GetEpochTransactionSample(
		ctx, epoch, EpochTransactionSampleSize,
	)
	if err != nil {
		return fail(err)
	}
	hashes := make([]string, len(sample))
	for i, tx := range sample {
		hashes[i] = tx.Hash
	}
	cached, err := o.cache.GetTxInfos(o.cfg.Network, hashes)
	if err != nil {
		return fail(err)
	}
	var missing []string
	for _, h := range hashes {
		if _, ok := cached[h]; !ok {
			missing = append(missing, h)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	items, err := o.koios.GetTxInfos(ctx, missing)
	if err != nil {
		return fail(err)
	}
	if err := o.cache.UpsertTxInfos(
		o.cfg.Network, items, time.Now().UTC(),
	); err != nil {
		return fail(err)
	}
	return nil
}
