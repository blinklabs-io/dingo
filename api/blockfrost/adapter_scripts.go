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
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

var (
	ErrScriptNotFound = errors.New("script not found")
	ErrDatumNotFound  = errors.New("datum not found")
)

func scriptTypeName(scriptType uint8) (string, error) {
	switch scriptType {
	case lcommon.ScriptRefTypeNativeScript:
		return "timelock", nil
	case lcommon.ScriptRefTypePlutusV1:
		return "plutusV1", nil
	case lcommon.ScriptRefTypePlutusV2:
		return "plutusV2", nil
	case lcommon.ScriptRefTypePlutusV3:
		return "plutusV3", nil
	case lcommon.ScriptRefTypePlutusV4:
		return "plutusV4", nil
	default:
		return "", fmt.Errorf("unknown script type %d", scriptType)
	}
}

func (a *NodeAdapter) storedScript(
	hash []byte,
	txn dbtypes.Txn,
) (*models.Script, error) {
	scriptHash, err := lcommon.NewBlake2b224Checked(hash)
	if err != nil {
		return nil, fmt.Errorf("script hash %x: %w", hash, err)
	}
	script, err := a.ledgerState.Database().
		Metadata().
		GetScript(scriptHash, txn)
	if err != nil {
		return nil, fmt.Errorf("get script %x: %w", hash, err)
	}
	if script == nil {
		return nil, fmt.Errorf("script %x: %w", hash, ErrScriptNotFound)
	}
	return script, nil
}

// Scripts returns the hashes of the stored scripts in the order they were
// first recorded.
func (a *NodeAdapter) Scripts(
	ctx context.Context,
	params PaginationParams,
) ([]string, int, error) {
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	meta := db.Metadata()

	total, err := meta.CountScripts(txn.Metadata())
	if err != nil {
		return nil, 0, fmt.Errorf("count scripts: %w", err)
	}
	offset, ok := paginationOffset(params)
	if !ok || offset >= total {
		return []string{}, total, nil
	}
	hashes, err := meta.ListScriptHashes(
		params.Count,
		offset,
		params.Order == PaginationOrderDesc,
		txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("list scripts: %w", err)
	}
	ret := make([]string, 0, len(hashes))
	for _, hash := range hashes {
		ret = append(ret, hex.EncodeToString(hash))
	}
	return ret, total, nil
}

// Script returns the language of a script and, for a Plutus script, the size
// of its CBOR serialisation.
func (a *NodeAdapter) Script(
	_ context.Context,
	hash []byte,
) (ScriptInfo, error) {
	script, err := a.storedScript(hash, nil)
	if err != nil {
		return ScriptInfo{}, err
	}
	typeName, err := scriptTypeName(script.Type)
	if err != nil {
		return ScriptInfo{}, fmt.Errorf("script %x: %w", hash, err)
	}
	info := ScriptInfo{Hash: hex.EncodeToString(hash), Type: typeName}
	if script.Type != lcommon.ScriptRefTypeNativeScript {
		size := len(script.Content)
		info.SerialisedSize = &size
	}
	return info, nil
}

// ScriptJSON returns the JSON form of a native script. A Plutus script has no
// JSON form and yields nil.
func (a *NodeAdapter) ScriptJSON(
	_ context.Context,
	hash []byte,
) (json.RawMessage, error) {
	script, err := a.storedScript(hash, nil)
	if err != nil {
		return nil, err
	}
	if script.Type != lcommon.ScriptRefTypeNativeScript {
		return nil, nil
	}
	value, err := nativeScriptJSON(script.Content)
	if err != nil {
		return nil, fmt.Errorf("script %x: %w", hash, err)
	}
	return value, nil
}

// ScriptCBOR returns the hex CBOR of a Plutus script. A native script yields
// nil.
func (a *NodeAdapter) ScriptCBOR(
	_ context.Context,
	hash []byte,
) (*string, error) {
	script, err := a.storedScript(hash, nil)
	if err != nil {
		return nil, err
	}
	if script.Type == lcommon.ScriptRefTypeNativeScript {
		return nil, nil
	}
	encoded := hex.EncodeToString(script.Content)
	return &encoded, nil
}

// nativeScriptJSON renders a native script in the cardano-cli simple script
// JSON form. An invalid-before bound is "after" and an invalid-hereafter
// bound is "before", named for the interval in which the script holds.
func nativeScriptJSON(raw []byte) (json.RawMessage, error) {
	var script lcommon.NativeScript
	if _, err := cbor.Decode(raw, &script); err != nil {
		return nil, fmt.Errorf("decode native script: %w", err)
	}
	node, err := nativeScriptNode(script)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(node)
	if err != nil {
		return nil, fmt.Errorf("encode native script: %w", err)
	}
	return encoded, nil
}

func nativeScriptChildren(
	scripts []lcommon.NativeScript,
) ([]any, error) {
	children := make([]any, 0, len(scripts))
	for _, child := range scripts {
		node, err := nativeScriptNode(child)
		if err != nil {
			return nil, err
		}
		children = append(children, node)
	}
	return children, nil
}

func nativeScriptNode(script lcommon.NativeScript) (map[string]any, error) {
	switch item := script.Item().(type) {
	case *lcommon.NativeScriptPubkey:
		return map[string]any{
			"type":    "sig",
			"keyHash": hex.EncodeToString(item.Hash),
		}, nil
	case *lcommon.NativeScriptAll:
		children, err := nativeScriptChildren(item.Scripts)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "all", "scripts": children}, nil
	case *lcommon.NativeScriptAny:
		children, err := nativeScriptChildren(item.Scripts)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "any", "scripts": children}, nil
	case *lcommon.NativeScriptNofK:
		children, err := nativeScriptChildren(item.Scripts)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"type":     "atLeast",
			"required": item.N,
			"scripts":  children,
		}, nil
	case *lcommon.NativeScriptInvalidBefore:
		return map[string]any{"type": "after", "slot": item.Slot}, nil
	case *lcommon.NativeScriptInvalidHereafter:
		return map[string]any{"type": "before", "slot": item.Slot}, nil
	default:
		return nil, fmt.Errorf("native script item %T has no JSON form", item)
	}
}

func (a *NodeAdapter) storedDatum(hash []byte) (*models.Datum, error) {
	datumHash, err := lcommon.NewBlake2b256Checked(hash)
	if err != nil {
		return nil, fmt.Errorf("datum hash %x: %w", hash, err)
	}
	datum, err := a.ledgerState.Database().Metadata().GetDatum(datumHash, nil)
	if err != nil {
		return nil, fmt.Errorf("get datum %x: %w", hash, err)
	}
	if datum == nil {
		return nil, fmt.Errorf("datum %x: %w", hash, ErrDatumNotFound)
	}
	return datum, nil
}

// DatumJSON returns the detailed-schema JSON of a datum.
func (a *NodeAdapter) DatumJSON(
	_ context.Context,
	hash []byte,
) (json.RawMessage, error) {
	datum, err := a.storedDatum(hash)
	if err != nil {
		return nil, err
	}
	value, err := plutusDataDetailedJSON(datum.RawDatum)
	if err != nil {
		return nil, fmt.Errorf("datum %x: %w", hash, err)
	}
	return value, nil
}

// DatumCBOR returns the hex CBOR of a datum.
func (a *NodeAdapter) DatumCBOR(
	_ context.Context,
	hash []byte,
) (string, error) {
	datum, err := a.storedDatum(hash)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(datum.RawDatum), nil
}

// redeemerSource resolves the redeemers of one transaction with their
// execution cost and datum.
type redeemerSource interface {
	TransactionRedeemers(ctx context.Context, hash []byte) ([]TransactionRedeemerInfo, error)
}

// buildScriptRedeemerInfos resolves each redeemer reference against its
// transaction. A transaction is resolved once however many references name it.
// A reference whose redeemer the transaction does not carry is an error: the
// index and the transaction disagree, which a silent skip would hide.
func buildScriptRedeemerInfos(
	ctx context.Context,
	refs []models.ScriptRedeemerRef,
	source redeemerSource,
) ([]ScriptRedeemerInfo, error) {
	byTx := make(map[string][]TransactionRedeemerInfo)
	ret := make([]ScriptRedeemerInfo, 0, len(refs))
	for _, ref := range refs {
		txKey := hex.EncodeToString(ref.TxHash)
		redeemers, ok := byTx[txKey]
		if !ok {
			var err error
			redeemers, err = source.TransactionRedeemers(ctx, ref.TxHash)
			if err != nil {
				return nil, fmt.Errorf(
					"get redeemers of transaction %s: %w",
					txKey,
					err,
				)
			}
			byTx[txKey] = redeemers
		}
		purpose := redeemerPurpose(lcommon.RedeemerTag(ref.Tag))
		found := false
		for _, redeemer := range redeemers {
			if redeemer.TxIndex != int(ref.Index) ||
				redeemer.Purpose != purpose {
				continue
			}
			ret = append(ret, ScriptRedeemerInfo{
				TxHash:           txKey,
				TxIndex:          redeemer.TxIndex,
				Purpose:          redeemer.Purpose,
				RedeemerDataHash: redeemer.RedeemerDataHash,
				DatumHash:        redeemer.DatumHash,
				UnitMem:          redeemer.UnitMem,
				UnitSteps:        redeemer.UnitSteps,
				Fee:              redeemer.Fee,
			})
			found = true
			break
		}
		if !found {
			return nil, fmt.Errorf(
				"transaction %s has no %s redeemer at index %d",
				txKey,
				purpose,
				ref.Index,
			)
		}
	}
	return ret, nil
}

// ScriptRedeemers returns the redeemers that executed a script in chain
// order. Only transactions applied since the script index existed are listed.
func (a *NodeAdapter) ScriptRedeemers(
	ctx context.Context,
	hash []byte,
	params PaginationParams,
) ([]ScriptRedeemerInfo, int, error) {
	scriptHash, err := lcommon.NewBlake2b224Checked(hash)
	if err != nil {
		return nil, 0, fmt.Errorf("script hash %x: %w", hash, err)
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	meta := db.Metadata()

	total, err := meta.CountScriptRedeemers(scriptHash, txn.Metadata())
	if err != nil {
		return nil, 0, fmt.Errorf("count script redeemers: %w", err)
	}
	if total == 0 {
		if _, err := a.storedScript(hash, txn.Metadata()); err != nil {
			return nil, 0, err
		}
		return []ScriptRedeemerInfo{}, 0, nil
	}
	offset, ok := paginationOffset(params)
	if !ok || offset >= total {
		return []ScriptRedeemerInfo{}, total, nil
	}
	refs, err := meta.GetScriptRedeemers(
		scriptHash,
		params.Count,
		offset,
		params.Order == PaginationOrderDesc,
		txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get script redeemers: %w", err)
	}
	// Each redeemer resolves through reads of its own; holding this
	// snapshot's connection across them lets concurrent requests exhaust the
	// read pool waiting on each other.
	txn.Release()
	infos, err := buildScriptRedeemerInfos(ctx, refs, a)
	if err != nil {
		return nil, 0, err
	}
	return infos, total, nil
}

// ScriptUTXOs returns the live UTxOs carrying the script as a reference
// script. Only outputs recorded since the script index existed are listed.
func (a *NodeAdapter) ScriptUTXOs(
	ctx context.Context,
	hash []byte,
	params PaginationParams,
) ([]ScriptUTXOInfo, int, error) {
	scriptHash, err := lcommon.NewBlake2b224Checked(hash)
	if err != nil {
		return nil, 0, fmt.Errorf("script hash %x: %w", hash, err)
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	meta := db.Metadata()

	total, err := meta.CountScriptReferenceUtxos(scriptHash, txn.Metadata())
	if err != nil {
		return nil, 0, fmt.Errorf("count script UTxOs: %w", err)
	}
	if total == 0 {
		if _, err := a.storedScript(hash, txn.Metadata()); err != nil {
			return nil, 0, err
		}
		return []ScriptUTXOInfo{}, 0, nil
	}
	offset, ok := paginationOffset(params)
	if !ok || offset >= total {
		return []ScriptUTXOInfo{}, total, nil
	}
	utxos, err := meta.GetScriptReferenceUtxos(
		scriptHash,
		params.Count,
		offset,
		params.Order == PaginationOrderDesc,
		txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get script UTxOs: %w", err)
	}
	// Block hashes and output CBOR resolve through reads of their own; holding
	// this snapshot's connection across them lets concurrent requests exhaust
	// the read pool waiting on each other.
	txn.Release()
	page := make([]models.UtxoWithOrdering, 0, len(utxos))
	for _, utxo := range utxos {
		page = append(page, models.UtxoWithOrdering{Utxo: utxo})
	}
	blockHashes, err := a.addressUtxoBlockHashes(ctx, page)
	if err != nil {
		return nil, 0, fmt.Errorf("get block hashes for script UTxOs: %w", err)
	}
	outputCbor, err := a.addressUtxoCbor(page)
	if err != nil {
		return nil, 0, fmt.Errorf("resolve CBOR for script UTxOs: %w", err)
	}
	ret := make([]ScriptUTXOInfo, 0, len(page))
	for _, utxo := range page {
		txKey := hex.EncodeToString(utxo.TxId)
		ref, err := utxoRef(utxo.Utxo)
		if err != nil {
			return nil, 0, err
		}
		utxo.Cbor = outputCbor[ref]
		address, err := a.addressFromUtxo(utxo.Utxo)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"resolve address of UTxO %s#%d: %w",
				txKey,
				utxo.OutputIdx,
				err,
			)
		}
		info := ScriptUTXOInfo{
			Address:             address,
			TxHash:              txKey,
			OutputIndex:         utxo.OutputIdx,
			Amount:              addressAmountsFromUtxo(utxo.Utxo),
			Block:               blockHashes[txKey],
			DataHash:            optionalHexString(utxo.DatumHash),
			ReferenceScriptHash: hex.EncodeToString(hash),
		}
		if len(utxo.Cbor) > 0 {
			if output, decodeErr := utxo.Decode(); decodeErr == nil {
				info.InlineDatum, _ = utxoDatumAndScriptRef(output)
			}
		}
		ret = append(ret, info)
	}
	return ret, total, nil
}
