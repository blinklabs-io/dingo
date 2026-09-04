// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package eras

import (
	"iter"
	"math"
	"slices"

	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// transactionWithRedeemers preserves the transaction's original CBOR and all
// other behavior while replacing the validation view of its redeemers.
type transactionWithRedeemers struct {
	lcommon.Transaction
	witnesses lcommon.TransactionWitnessSet
}

func (t transactionWithRedeemers) Witnesses() lcommon.TransactionWitnessSet {
	return t.witnesses
}

func (t transactionWithRedeemers) ValidityIntervalUpperBound() (uint64, bool) {
	return lcommon.TransactionValidityIntervalUpperBound(t.Transaction)
}

func (t transactionWithRedeemers) CurrentTreasuryValuePresent() bool {
	return lcommon.TransactionCurrentTreasuryValuePresent(t.Transaction)
}

func (t transactionWithRedeemers) SubTransactionWitnessSets() []lcommon.TransactionWitnessSet {
	return lcommon.SubTransactionWitnessSetsFromTransaction(t.Transaction)
}

func (t transactionWithRedeemers) SubTransactionBodies() []lcommon.TransactionBody {
	return lcommon.SubTransactionBodiesFromTransaction(t.Transaction)
}

func (t transactionWithRedeemers) SubTransactionOutputs() []lcommon.TransactionOutput {
	return lcommon.SubTransactionOutputsFromTransaction(t.Transaction)
}

type witnessSetWithRedeemers struct {
	lcommon.TransactionWitnessSet
	redeemers lcommon.TransactionWitnessRedeemers
}

func (w witnessSetWithRedeemers) Redeemers() lcommon.TransactionWitnessRedeemers {
	return w.redeemers
}

func (w witnessSetWithRedeemers) PlutusV4Scripts() []lcommon.PlutusV4Script {
	return lcommon.PlutusV4ScriptsFromWitnessSet(w.TransactionWitnessSet)
}

type lastWinsRedeemers struct {
	keys   []lcommon.RedeemerKey
	values map[lcommon.RedeemerKey]lcommon.RedeemerValue
}

func (r lastWinsRedeemers) Indexes(tag lcommon.RedeemerTag) []uint {
	ret := make([]uint, 0)
	for _, key := range r.keys {
		if key.Tag == tag {
			ret = append(ret, uint(key.Index))
		}
	}
	return ret
}

func (r lastWinsRedeemers) Value(
	index uint,
	tag lcommon.RedeemerTag,
) lcommon.RedeemerValue {
	if uint64(index) > math.MaxUint32 {
		return lcommon.RedeemerValue{}
	}
	return r.values[lcommon.RedeemerKey{Tag: tag, Index: uint32(index)}]
}

func (r lastWinsRedeemers) Iter() iter.Seq2[lcommon.RedeemerKey, lcommon.RedeemerValue] {
	return func(yield func(lcommon.RedeemerKey, lcommon.RedeemerValue) bool) {
		for _, key := range r.keys {
			if !yield(key, r.values[key]) {
				return
			}
		}
	}
}

// normalizeDuplicateRedeemers gives validation the same map semantics as
// cardano-ledger's list-form redeemer decoder: duplicate pointers collapse and
// the final value for a pointer wins. The transaction's raw CBOR remains
// unchanged for hashing, sizing, and persistence.
func normalizeDuplicateRedeemers(tx lcommon.Transaction) lcommon.Transaction {
	if tx == nil {
		return nil
	}
	witnesses := tx.Witnesses()
	if witnesses == nil {
		return tx
	}
	redeemers := witnesses.Redeemers()
	if redeemers == nil {
		return tx
	}

	values := make(map[lcommon.RedeemerKey]lcommon.RedeemerValue)
	entryCount := 0
	forEachRedeemerInLedgerOrder(redeemers, func(
		key lcommon.RedeemerKey,
		value lcommon.RedeemerValue,
	) {
		entryCount++
		values[key] = value
	})
	if entryCount == len(values) {
		return tx
	}

	keys := make([]lcommon.RedeemerKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, lcommon.CompareRedeemerKeys)
	normalizedRedeemers := lastWinsRedeemers{
		keys:   keys,
		values: values,
	}
	switch typedTx := tx.(type) {
	case *alonzo.AlonzoTransaction:
		ret := *typedTx
		ret.WitnessSet.WsRedeemers.Redeemers = alonzoRedeemers(
			keys,
			values,
		)
		return &ret
	case *babbage.BabbageTransaction:
		ret := *typedTx
		ret.WitnessSet.WsRedeemers.Redeemers = alonzoRedeemers(
			keys,
			values,
		)
		return &ret
	}
	return transactionWithRedeemers{
		Transaction: tx,
		witnesses: witnessSetWithRedeemers{
			TransactionWitnessSet: witnesses,
			redeemers:             normalizedRedeemers,
		},
	}
}

func forEachRedeemerInLedgerOrder(
	redeemers lcommon.TransactionWitnessRedeemers,
	yield func(lcommon.RedeemerKey, lcommon.RedeemerValue),
) {
	visit := func(redeemer alonzo.AlonzoRedeemer) {
		yield(
			lcommon.RedeemerKey{
				Tag:   redeemer.Tag,
				Index: redeemer.Index,
			},
			lcommon.RedeemerValue{
				Data:    redeemer.Data,
				ExUnits: redeemer.ExUnits,
			},
		)
	}
	switch typedRedeemers := redeemers.(type) {
	case alonzo.AlonzoRedeemers:
		for _, redeemer := range typedRedeemers.Redeemers {
			visit(redeemer)
		}
	case *alonzo.AlonzoRedeemers:
		for _, redeemer := range typedRedeemers.Redeemers {
			visit(redeemer)
		}
	default:
		for key, value := range redeemers.Iter() {
			yield(key, value)
		}
	}
}

func alonzoRedeemers(
	keys []lcommon.RedeemerKey,
	values map[lcommon.RedeemerKey]lcommon.RedeemerValue,
) []alonzo.AlonzoRedeemer {
	ret := make([]alonzo.AlonzoRedeemer, 0, len(keys))
	for _, key := range keys {
		value := values[key]
		ret = append(ret, alonzo.AlonzoRedeemer{
			Tag:     key.Tag,
			Index:   key.Index,
			Data:    value.Data,
			ExUnits: value.ExUnits,
		})
	}
	return ret
}
