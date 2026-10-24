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

package blockfrost

import (
	"cmp"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/dingo/mempool"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// mempoolReader is the read side of the node's mempool.
type mempoolReader interface {
	GetTransaction(txHash string) (mempool.MempoolTransaction, bool)
	TransactionsBounded(
		maxItems int,
		maxBytes int64,
	) ([]mempool.MempoolTransaction, int)
}

// The node's mempool is handed to NewNodeAdapter as its submitter, and the
// adapter finds the read side by interface assertion, so a change to either
// method must break the build rather than silently disable the endpoints.
var _ mempoolReader = (*mempool.Mempool)(nil)

// mempoolAddressReader is the address index of the node's mempool.
type mempoolAddressReader interface {
	TransactionHashesByAddress(address string) []string
}

var _ mempoolAddressReader = (*mempool.Mempool)(nil)

// MempoolTransactions returns the hashes of the mempool transactions in
// admission order. An ascending page reads only the prefix of the pool it
// needs; a descending page starts at the newest transaction, so it reads the
// pool whole, which its capacity bounds.
func (a *NodeAdapter) MempoolTransactions(
	ctx context.Context,
	params PaginationParams,
) ([]string, int, error) {
	if a.mempool == nil {
		return nil, 0, ErrMempoolUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	offset, ok := paginationOffset(params)
	if !ok {
		_, total := a.mempool.TransactionsBounded(1, 0)
		return []string{}, total, nil
	}
	maxItems := 0
	if params.Order != PaginationOrderDesc &&
		offset <= math.MaxInt-params.Count {
		maxItems = offset + params.Count
	}
	txs, total := a.mempool.TransactionsBounded(maxItems, 0)
	hashes := make([]string, 0, len(txs))
	for _, tx := range txs {
		hashes = append(hashes, tx.Hash)
	}
	return orderedPage(hashes, params), total, nil
}

func (a *NodeAdapter) decodeMempoolTransaction(
	tx mempool.MempoolTransaction,
) (lcommon.Transaction, error) {
	decoded, err := safedecode.Transaction(tx.Type, tx.Cbor)
	if err != nil {
		return nil, fmt.Errorf(
			"decode mempool transaction %s: %w",
			tx.Hash,
			err,
		)
	}
	return decoded, nil
}

// confirmedInputAddresses resolves the addresses of the inputs that name a
// stored, unspent output. An input of another mempool transaction, or one
// already spent, has no confirmed output to resolve and is left out.
func (a *NodeAdapter) confirmedInputAddresses(
	ctx context.Context,
	inputs []lcommon.TransactionInput,
) (map[database.UtxoRef]string, error) {
	seen := make(map[database.UtxoRef]struct{}, len(inputs))
	refs := make([]models.UtxoId, 0, len(inputs))
	for _, input := range inputs {
		ref := transactionInputRef(input)
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		refs = append(refs, models.UtxoId{
			Hash: input.Id().Bytes(),
			Idx:  input.Index(),
		})
	}
	ret := make(map[database.UtxoRef]string, len(refs))
	if len(refs) == 0 {
		return ret, nil
	}
	utxos, err := a.ledgerState.Database().UtxosByRefs(ctx, refs, nil)
	if err != nil {
		return nil, fmt.Errorf("resolve mempool transaction inputs: %w", err)
	}
	for _, utxo := range utxos {
		ref, err := utxoRef(utxo)
		if err != nil {
			return nil, err
		}
		address, err := a.addressFromUtxo(utxo)
		if err != nil {
			return nil, fmt.Errorf(
				"resolve address of input %x#%d: %w",
				utxo.TxId,
				utxo.OutputIdx,
				err,
			)
		}
		ret[ref] = address
	}
	return ret, nil
}

func (a *NodeAdapter) mempoolDeposit(
	ctx context.Context,
	tx lcommon.Transaction,
) (uint64, error) {
	if len(tx.Certificates()) == 0 {
		return 0, nil
	}
	db := a.ledgerState.Database()
	slot := a.ledgerState.Tip().Point.Slot
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	epoch, err := db.GetEpochBySlot(slot, txn)
	if err != nil {
		return 0, fmt.Errorf("get epoch for slot %d: %w", slot, err)
	}
	if epoch == nil {
		return 0, nil
	}
	return a.transactionDeposit(ctx, tx, epoch.EraId, slot)
}

// outputAsUtxo carries the lovelace and native asset amounts of a decoded
// output in the row shape the amount helpers read.
func outputAsUtxo(output lcommon.TransactionOutput) (models.Utxo, error) {
	var utxo models.Utxo
	amount := output.Amount()
	if amount == nil || !amount.IsUint64() {
		return utxo, fmt.Errorf(
			"output lovelace amount %v is not a uint64",
			amount,
		)
	}
	utxo.Amount = dbtypes.Uint64(amount.Uint64())
	if assets := output.Assets(); assets != nil {
		converted, err := models.ConvertMultiAssetToModels(assets)
		if err != nil {
			return utxo, fmt.Errorf("output assets: %w", err)
		}
		utxo.Assets = converted
	}
	return utxo, nil
}

// producedOutput is an output a mempool transaction creates, with the index
// it will carry once applied.
type producedOutput struct {
	output lcommon.TransactionOutput
	index  uint32
}

// mempoolProducedOutputs returns the outputs a transaction creates if it is
// applied as submitted. A transaction marked as failing phase 2 discards its
// regular outputs and produces only its collateral return, which takes the
// index after the last regular output; the flag reports that case.
func mempoolProducedOutputs(
	tx lcommon.Transaction,
) ([]producedOutput, bool) {
	if !tx.IsValid() {
		output := tx.CollateralReturn()
		if output == nil {
			return nil, true
		}
		// #nosec G115 -- bounded by the transaction size
		index := uint32(len(tx.Outputs()))
		return []producedOutput{{output: output, index: index}}, true
	}
	ret := make([]producedOutput, 0, len(tx.Outputs()))
	for i, output := range tx.Outputs() {
		ret = append(ret, producedOutput{
			output: output,
			index:  uint32(i), // #nosec G115 -- bounded by the transaction size
		})
	}
	return ret, false
}

// MempoolTransaction returns one mempool transaction in the shape of a
// transaction that is not yet in a block.
func (a *NodeAdapter) MempoolTransaction(
	ctx context.Context,
	hash []byte,
) (MempoolTransactionInfo, error) {
	if a.mempool == nil {
		return MempoolTransactionInfo{}, ErrMempoolUnavailable
	}
	entry, ok := a.mempool.GetTransaction(hex.EncodeToString(hash))
	if !ok {
		return MempoolTransactionInfo{}, fmt.Errorf(
			"mempool transaction %x: %w",
			hash,
			ErrTransactionNotFound,
		)
	}
	tx, err := a.decodeMempoolTransaction(entry)
	if err != nil {
		return MempoolTransactionInfo{}, err
	}
	allInputs := make(
		[]lcommon.TransactionInput,
		0,
		len(tx.Inputs())+len(tx.Collateral())+len(tx.ReferenceInputs()),
	)
	allInputs = append(allInputs, tx.Inputs()...)
	allInputs = append(allInputs, tx.Collateral()...)
	allInputs = append(allInputs, tx.ReferenceInputs()...)
	addresses, err := a.confirmedInputAddresses(ctx, allInputs)
	if err != nil {
		return MempoolTransactionInfo{}, err
	}
	inputInfo := func(
		input lcommon.TransactionInput,
		collateral bool,
		reference *bool,
	) TransactionInputInfo {
		return TransactionInputInfo{
			Address:     addresses[transactionInputRef(input)],
			TxHash:      input.Id().String(),
			OutputIndex: input.Index(),
			Collateral:  collateral,
			Reference:   reference,
		}
	}
	inputs := make([]TransactionInputInfo, 0, len(allInputs))
	for _, input := range tx.Inputs() {
		inputs = append(inputs, inputInfo(input, false, nil))
	}
	for _, input := range tx.Collateral() {
		inputs = append(inputs, inputInfo(input, true, nil))
	}
	reference := true
	for _, input := range tx.ReferenceInputs() {
		inputs = append(inputs, inputInfo(input, false, &reference))
	}

	produced, collateralReturn := mempoolProducedOutputs(tx)
	outputs := make([]TransactionOutputInfo, 0, len(produced))
	outputUtxos := make([]models.Utxo, 0, len(produced))
	for _, out := range produced {
		output := out.output
		utxo, err := outputAsUtxo(output)
		if err != nil {
			return MempoolTransactionInfo{}, fmt.Errorf(
				"output %d of mempool transaction %x: %w",
				out.index,
				hash,
				err,
			)
		}
		outputUtxos = append(outputUtxos, utxo)
		inlineDatum, referenceScriptHash := utxoDatumAndScriptRef(output)
		var dataHash *string
		if datumHash := output.DatumHash(); datumHash != nil {
			encoded := hex.EncodeToString(datumHash.Bytes())
			dataHash = &encoded
		}
		outputs = append(outputs, TransactionOutputInfo{
			Address:             output.Address().String(),
			Amount:              addressAmountsFromUtxo(utxo),
			OutputIndex:         out.index,
			DataHash:            dataHash,
			InlineDatum:         inlineDatum,
			ReferenceScriptHash: referenceScriptHash,
			Collateral:          collateralReturn,
		})
	}

	deposit, err := a.mempoolDeposit(ctx, tx)
	if err != nil {
		return MempoolTransactionInfo{}, fmt.Errorf(
			"calculate deposit for mempool transaction %x: %w",
			hash,
			err,
		)
	}
	certs := make([]models.Certificate, 0, len(tx.Certificates()))
	for _, cert := range tx.Certificates() {
		certs = append(certs, models.Certificate{CertType: cert.Type()})
	}
	counts := transactionCertificateCounts(certs)
	assetMintBurnCount := 0
	if mint := tx.AssetMint(); mint != nil {
		for _, policy := range mint.Policies() {
			assetMintBurnCount += len(mint.Assets(policy))
		}
	}
	var invalidBefore, invalidHereafter *string
	if v := tx.ValidityIntervalStart(); v != 0 {
		encoded := strconv.FormatUint(v, 10)
		invalidBefore = &encoded
	}
	if v := tx.TTL(); v != 0 {
		encoded := strconv.FormatUint(v, 10)
		invalidHereafter = &encoded
	}

	var redeemers []MempoolRedeemerInfo
	if witnesses := tx.Witnesses(); witnesses != nil &&
		witnesses.Redeemers() != nil {
		for key, value := range witnesses.Redeemers().Iter() {
			redeemers = append(redeemers, MempoolRedeemerInfo{
				TxIndex:   int(key.Index),
				Purpose:   redeemerPurpose(key.Tag),
				UnitMem:   strconv.FormatInt(value.ExUnits.Memory, 10),
				UnitSteps: strconv.FormatInt(value.ExUnits.Steps, 10),
			})
		}
		slices.SortFunc(redeemers, func(a, b MempoolRedeemerInfo) int {
			if c := cmp.Compare(redeemerPurposeOrder[a.Purpose], redeemerPurposeOrder[b.Purpose]); c != 0 {
				return c
			}
			return cmp.Compare(a.TxIndex, b.TxIndex)
		})
	}

	return MempoolTransactionInfo{
		Tx: TransactionInfo{
			Hash:               tx.Hash().String(),
			OutputAmount:       aggregateTransactionOutputAmount(outputUtxos),
			Fees:               tx.Fee().String(),
			Deposit:            strconv.FormatUint(deposit, 10),
			Size:               len(entry.Cbor),
			InvalidBefore:      invalidBefore,
			InvalidHereafter:   invalidHereafter,
			UtxoCount:          len(allInputs) + len(produced),
			WithdrawalCount:    len(tx.Withdrawals()),
			MirCertCount:       counts.mirCerts,
			DelegationCount:    counts.delegations,
			StakeCertCount:     counts.stakeCerts,
			PoolUpdateCount:    counts.poolUpdates,
			PoolRetireCount:    counts.poolRetires,
			AssetMintBurnCount: assetMintBurnCount,
			RedeemerCount:      len(redeemers),
			ValidContract:      tx.IsValid(),
		},
		Inputs:    inputs,
		Outputs:   outputs,
		Redeemers: redeemers,
	}, nil
}

// MempoolAddressTransactions returns the hashes of the mempool transactions
// that pay to an address or spend one of its outputs, in the pool's order. A
// transaction marked as failing phase 2 pays only its collateral return and
// spends only its collateral, as when it is applied. The pool answers from
// its address index, so the request reads no storage and decodes no
// transaction.
func (a *NodeAdapter) MempoolAddressTransactions(
	ctx context.Context,
	address string,
	params PaginationParams,
) ([]string, int, error) {
	addr, isPaymentCred, err := a.parseQueryAddress(address)
	if err != nil {
		return nil, 0, err
	}
	// The endpoint takes a Bech32 address; a bare payment credential names
	// no address to compare outputs against.
	if isPaymentCred {
		return nil, 0, fmt.Errorf(
			"address %q is a payment credential: %w",
			address,
			ErrInvalidAddress,
		)
	}
	if a.mempoolAddresses == nil {
		return nil, 0, ErrMempoolUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	hashes := a.mempoolAddresses.TransactionHashesByAddress(addr.String())
	return orderedPage(hashes, params), len(hashes), nil
}

// redeemerPurposeOrder ranks purposes by redeemer tag, the order a
// transaction's redeemers are listed in.
var redeemerPurposeOrder = map[string]int{
	"spend": 0, "mint": 1, "cert": 2, "reward": 3,
	"voting": 4, "proposing": 5, "guarding": 6,
}
