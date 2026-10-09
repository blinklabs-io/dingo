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
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/labelcodec"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

func (s *Store) applyTransactionMetadataLabels(
	rows *rowBatch,
	transactionID int64,
	slot uint64,
	labels []labelcodec.Entry,
) {
	if s.storageMode != types.StorageModeAPI {
		return
	}
	// A multi-row upsert may not name one (transaction_id, label) key twice,
	// so a repeated label keeps only its last value, as one upsert per label
	// did.
	last := make(map[uint64]int, len(labels))
	for i, label := range labels {
		last[label.Label] = i
	}
	for i, label := range labels {
		if last[label.Label] != i {
			continue
		}
		var jsonValue any
		if label.JSONError == nil {
			jsonValue = label.JsonValue
		}
		rows.add(
			metadataLabelShape,
			transactionID,
			decimalUint64(types.Uint64(label.Label)),
			slot,
			label.CborValue,
			jsonValue,
		)
	}
}

func (s *Store) applyTransactionAssetMintBurn(
	transaction lcommon.Transaction,
	hash []byte,
	slot uint64,
	index uint32,
	rows *rowBatch,
) {
	if s.storageMode != types.StorageModeAPI || !transaction.IsValid() {
		return
	}
	for _, asset := range models.ConvertMintToAssetMintBurnModels(
		transaction.AssetMint(),
		hash,
		slot,
		index,
	) {
		rows.add(
			assetMintBurnShape,
			asset.TxHash,
			asset.PolicyId,
			asset.Name,
			asset.Fingerprint,
			asset.Slot,
			asset.Quantity,
			asset.TxIndex,
		)
	}
}

func (s *Store) applyTransactionAPIDetails(
	ctx context.Context,
	db queryer,
	transactionID int64,
	transaction lcommon.Transaction,
	slot uint64,
	index uint32,
	produced []models.Utxo,
	rows *rowBatch,
	transactionIsNew bool,
) error {
	if s.storageMode != types.StorageModeAPI {
		return nil
	}
	hash := transaction.Hash().Bytes()
	if err := s.markTransactionUtxoReferences(
		ctx,
		db,
		transaction.Collateral(),
		"collateral_by_tx_id",
		hash,
	); err != nil {
		return fmt.Errorf("mark collateral inputs: %w", err)
	}
	if err := s.markTransactionUtxoReferences(
		ctx,
		db,
		transaction.ReferenceInputs(),
		"referenced_by_tx_id",
		hash,
	); err != nil {
		return fmt.Errorf("mark reference inputs: %w", err)
	}
	if err := s.indexTransactionAddresses(
		ctx,
		db,
		rows,
		transactionID,
		transaction,
		slot,
		index,
		produced,
		s.dialect.ParameterLimit(),
		transactionIsNew,
	); err != nil {
		return err
	}
	if err := s.storeTransactionWitnesses(
		ctx,
		db,
		rows,
		transactionID,
		transaction,
		slot,
		transactionIsNew,
	); err != nil {
		return err
	}
	if err := storeTransactionIndexedScripts(
		ctx,
		db,
		transaction,
		slot,
	); err != nil {
		return err
	}
	if err := storeTransactionDatumIndex(rows, transaction, slot); err != nil {
		return err
	}
	return nil
}

func utxoReferencePredicate(inputCount int, alias string) string {
	predicates := make([]string, inputCount)
	for i := range predicates {
		predicates[i] = "(" + alias + "tx_id = ? AND " + alias + "output_idx = ?)"
	}
	return strings.Join(predicates, " OR ")
}

func utxoReferenceInsertBatchSQL(associationTable string, inputCount int) string {
	return `INSERT INTO ` + associationTable + ` (utxo_id, transaction_hash)
SELECT u.id, ? FROM utxo AS u
WHERE (` + utxoReferencePredicate(inputCount, "u.") + `)
  AND NOT EXISTS (
      SELECT 1 FROM ` + associationTable + ` AS r
      WHERE r.utxo_id = u.id AND r.transaction_hash = ?
  )`
}

func utxoReferenceUpdateBatchSQL(column string, inputCount int) string {
	return "UPDATE utxo SET " + column + " = ? WHERE (" +
		utxoReferencePredicate(inputCount, "") + ")"
}

func appendUtxoReferenceArgs(
	args []any,
	inputs []lcommon.TransactionInput,
	querySize int,
) []any {
	for _, input := range inputs {
		args = append(args, input.Id().Bytes(), input.Index())
	}
	for padding := len(inputs); padding < querySize; padding++ {
		args = append(args, nil, uint32(0))
	}
	return args
}

func (s *Store) markTransactionUtxoReferences(
	ctx context.Context,
	db queryer,
	inputs []lcommon.TransactionInput,
	column string,
	hash []byte,
) error {
	if column != "collateral_by_tx_id" &&
		column != "referenced_by_tx_id" {
		return fmt.Errorf("unsupported UTxO reference column %q", column)
	}
	associationTable := "utxo_collateral_input"
	if column == "referenced_by_tx_id" {
		associationTable = "utxo_reference_input"
	}
	batchSize := min(
		max(1, (s.dialect.ParameterLimit()-2)/2),
		maxCachedAddressInputQuerySize,
	)
	for start := 0; start < len(inputs); start += batchSize {
		end := min(start+batchSize, len(inputs))
		batch := inputs[start:end]
		querySize := 1
		for querySize < len(batch) {
			querySize *= 2
		}
		args := make([]any, 0, querySize*2+2)
		args = append(args, hash)
		args = appendUtxoReferenceArgs(args, batch, querySize)
		args = append(args, hash)
		if _, err := s.execCached(
			ctx, db,
			utxoReferenceInsertBatchSQL(associationTable, querySize),
			args...,
		); err != nil {
			return err
		}

		args = make([]any, 0, querySize*2+1)
		args = append(args, hash)
		args = appendUtxoReferenceArgs(args, batch, querySize)
		if _, err := s.execCached(
			ctx, db,
			utxoReferenceUpdateBatchSQL(column, querySize),
			args...,
		); err != nil {
			return err
		}
	}
	return nil
}

type addressIndexKey struct {
	payment string
	tag     uint8
	staking string
}

const (
	deleteAddressTransactionSQL    = "DELETE FROM address_transaction WHERE transaction_id = ?"
	maxCachedAddressInputQuerySize = 32
)

var cachedAddressInputQuerySizes = [...]int{1, 2, 4, 8, 16, 32}

// A wide OR of (tx_id, output_idx) pairs can make SQLite abandon
// tx_id_output_idx. Query by tx_id and filter other outputs in Go.
func addressTransactionInputQuery(size int) string {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", size), ",")
	return `
SELECT tx_id, output_idx, payment_key, credential_tag, staking_key
FROM utxo WHERE tx_id IN (` + placeholders + `)`
}

func (s *Store) indexTransactionAddresses(
	ctx context.Context,
	db queryer,
	rows *rowBatch,
	transactionID int64,
	transaction lcommon.Transaction,
	slot uint64,
	index uint32,
	produced []models.Utxo,
	parameterLimit int,
	transactionIsNew bool,
) error {
	if !transactionIsNew {
		if _, err := s.execCached(ctx, db, deleteAddressTransactionSQL,
			transactionID,
		); err != nil {
			return fmt.Errorf("delete existing address transactions: %w", err)
		}
	}
	addresses := make(map[addressIndexKey]struct{})
	add := func(payment []byte, tag uint8, staking []byte) {
		if len(payment) == 0 && len(staking) == 0 {
			return
		}
		addresses[addressIndexKey{
			payment: string(payment),
			tag:     tag,
			staking: string(staking),
		}] = struct{}{}
	}
	for _, output := range produced {
		add(output.PaymentKey, output.CredentialTag, output.StakingKey)
	}
	allInputs := make(
		[]lcommon.TransactionInput,
		0,
		len(transaction.Inputs())+
			len(transaction.Collateral())+
			len(transaction.ReferenceInputs()),
	)
	allInputs = append(allInputs, transaction.Inputs()...)
	allInputs = append(allInputs, transaction.Collateral()...)
	allInputs = append(allInputs, transaction.ReferenceInputs()...)
	refs := make([]models.UtxoId, 0, len(allInputs))
	for _, input := range allInputs {
		refs = append(refs, models.UtxoId{
			Hash: input.Id().Bytes(),
			Idx:  input.Index(),
		})
	}
	txIDs, wanted := distinctUtxoTxIDs(refs)
	inputBatchSize := 1
	for inputBatchSize*2 <= parameterLimit &&
		inputBatchSize*2 <= maxCachedAddressInputQuerySize {
		inputBatchSize *= 2
	}
	for start := 0; start < len(txIDs); start += inputBatchSize {
		end := min(start+inputBatchSize, len(txIDs))
		batch := txIDs[start:end]
		querySize := 1
		for querySize < len(batch) {
			querySize *= 2
		}
		args := make([]any, querySize)
		for i, txID := range batch {
			args[i] = txID
		}
		inputRows, err := s.queryRowsCached(
			ctx, db, addressTransactionInputQuery(querySize), args...,
		)
		if err != nil {
			return fmt.Errorf(
				"lookup input addresses for transaction %d: %w",
				transactionID,
				err,
			)
		}
		if err := func() (runErr error) {
			defer func() {
				runErr = errors.Join(runErr, inputRows.Close())
			}()
			for inputRows.Next() {
				var (
					txID    []byte
					output  uint32
					payment []byte
					staking []byte
					tag     uint8
				)
				if err := inputRows.Scan(&txID, &output, &payment, &tag, &staking); err != nil {
					return fmt.Errorf("scan input address for transaction %d: %w", transactionID, err)
				}
				outputs, ok := wanted[string(txID)]
				if !ok {
					continue
				}
				if _, ok := outputs[output]; !ok {
					continue
				}
				add(payment, tag, staking)
			}
			if err := inputRows.Err(); err != nil {
				return fmt.Errorf("iterate input addresses for transaction %d: %w", transactionID, err)
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	for address := range addresses {
		rows.add(
			addressTransactionShape,
			[]byte(address.payment),
			[]byte(address.staking),
			address.tag,
			transactionID,
			slot,
			index,
		)
	}
	return nil
}

// transactionWitnessTables are the per-transaction detail tables
// storeTransactionWitnesses rewrites wholesale on replay. Fresh transaction
// IDs have no rows to clear.
var transactionWitnessTables = []string{
	"key_witness",
	"witness_scripts",
	"redeemer",
	"plutus_data",
}

// TransactionWitnessTables lists the tables TransactionWitnessCleanupSQL is
// issued against, in the order storeTransactionWitnesses clears them.
func TransactionWitnessTables() []string {
	return slices.Clone(transactionWitnessTables)
}

// TransactionWitnessCleanupSQL is the idempotency delete
// storeTransactionWitnesses runs against one witness table when an API-mode
// transaction is replayed.
//
// Exported so a test can pin its query plan against the statement the store
// actually runs. Replay deletes can still scan while their index is deferred;
// normal import assigns a new transaction ID and skips these deletes.
func TransactionWitnessCleanupSQL(table string) string {
	return "DELETE FROM " + table + " WHERE transaction_id = ?"
}

func (s *Store) storeTransactionWitnesses(
	ctx context.Context,
	db queryer,
	rows *rowBatch,
	transactionID int64,
	transaction lcommon.Transaction,
	slot uint64,
	transactionIsNew bool,
) error {
	if !transactionIsNew {
		for _, table := range transactionWitnessTables {
			if _, err := s.execCached(
				ctx, db, TransactionWitnessCleanupSQL(table), transactionID,
			); err != nil {
				return fmt.Errorf("delete existing %s rows: %w", table, err)
			}
		}
	}
	witnesses := transaction.Witnesses()
	// Key/bootstrap witnesses and redeemers keep their existing top-level
	// transaction semantics. Nested witness sets extend only the script and
	// datum indexes consumed by chain-index APIs.
	if witnesses != nil {
		for _, witness := range witnesses.Vkey() {
			rows.add(vkeyWitnessShape,
				witness.Vkey,
				witness.Signature,
				transactionID,
				models.KeyWitnessTypeVkey,
			)
		}
		for _, witness := range witnesses.Bootstrap() {
			rows.add(bootstrapWitnessShape,
				witness.Signature,
				witness.PublicKey,
				witness.ChainCode,
				witness.Attributes,
				transactionID,
				models.KeyWitnessTypeBootstrap,
			)
		}
	}

	seenScripts := make(map[witnessScriptKey]struct{})
	seenData := make(map[string]struct{})
	for _, witnessSet := range allTransactionWitnessSets(transaction) {
		if err := storeWitnessScripts(
			ctx,
			db,
			rows,
			transactionID,
			uint8(lcommon.ScriptRefTypeNativeScript),
			witnessSet.NativeScripts(),
			slot,
			seenScripts,
		); err != nil {
			return err
		}
		if err := storeWitnessScripts(
			ctx,
			db,
			rows,
			transactionID,
			uint8(lcommon.ScriptRefTypePlutusV4),
			lcommon.PlutusV4ScriptsFromWitnessSet(witnessSet),
			slot,
			seenScripts,
		); err != nil {
			return err
		}
		if err := storeWitnessScripts(
			ctx,
			db,
			rows,
			transactionID,
			uint8(lcommon.ScriptRefTypePlutusV1),
			witnessSet.PlutusV1Scripts(),
			slot,
			seenScripts,
		); err != nil {
			return err
		}
		if err := storeWitnessScripts(
			ctx,
			db,
			rows,
			transactionID,
			uint8(lcommon.ScriptRefTypePlutusV2),
			witnessSet.PlutusV2Scripts(),
			slot,
			seenScripts,
		); err != nil {
			return err
		}
		if err := storeWitnessScripts(
			ctx,
			db,
			rows,
			transactionID,
			uint8(lcommon.ScriptRefTypePlutusV3),
			witnessSet.PlutusV3Scripts(),
			slot,
			seenScripts,
		); err != nil {
			return err
		}
		for _, datum := range witnessSet.PlutusData() {
			raw := datum.Cbor()
			key := string(raw)
			if _, ok := seenData[key]; ok {
				continue
			}
			seenData[key] = struct{}{}
			rows.add(plutusDataShape,
				raw,
				transactionID,
			)
		}
	}
	if witnesses != nil && witnesses.Redeemers() != nil {
		for key, value := range witnesses.Redeemers().Iter() {
			rows.add(redeemerShape,
				value.Data.Cbor(),
				transactionID,
				uint64(max(0, value.ExUnits.Memory)),
				uint64(max(0, value.ExUnits.Steps)),
				key.Index,
				uint8(key.Tag),
			)
		}
	}
	return nil
}

func allTransactionWitnessSets(
	transaction lcommon.Transaction,
) []lcommon.TransactionWitnessSet {
	if transaction == nil {
		return nil
	}
	subTransactionWitnesses := lcommon.SubTransactionWitnessSetsFromTransaction(
		transaction,
	)
	ret := make(
		[]lcommon.TransactionWitnessSet,
		0,
		1+len(subTransactionWitnesses),
	)
	if witnesses := transaction.Witnesses(); witnesses != nil {
		ret = append(ret, witnesses)
	}
	for _, witnesses := range subTransactionWitnesses {
		if witnesses != nil {
			ret = append(ret, witnesses)
		}
	}
	return ret
}

type witnessScriptKey struct {
	hash       lcommon.ScriptHash
	scriptType uint8
}

func storeWitnessScripts[T lcommon.Script](
	ctx context.Context,
	db queryer,
	rows *rowBatch,
	transactionID int64,
	scriptType uint8,
	scripts []T,
	slot uint64,
	seen map[witnessScriptKey]struct{},
) error {
	for _, script := range scripts {
		hash := script.Hash()
		key := witnessScriptKey{hash: hash, scriptType: scriptType}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		rows.add(witnessScriptShape,
			hash.Bytes(),
			transactionID,
			scriptType,
		)
		if err := storeScriptContent(ctx, db, script, scriptType, slot); err != nil {
			return err
		}
	}
	return nil
}

func storeScriptContent[T lcommon.Script](
	ctx context.Context,
	db queryer,
	script T,
	scriptType uint8,
	slot uint64,
) error {
	if _, err := db.ExecContext(ctx, `
INSERT INTO script (hash, content, created_slot, type)
VALUES (?, ?, ?, ?)
ON CONFLICT (hash) DO NOTHING`,
		script.Hash().Bytes(),
		script.RawScriptBytes(),
		slot,
		scriptType,
	); err != nil {
		return fmt.Errorf("create script content: %w", err)
	}
	return nil
}

func storeTransactionIndexedScripts(
	ctx context.Context,
	db queryer,
	transaction lcommon.Transaction,
	slot uint64,
) error {
	outputs := make([]lcommon.TransactionOutput, 0)
	for _, produced := range transaction.Produced() {
		if produced.Output != nil {
			outputs = append(outputs, produced.Output)
		}
	}
	outputs = append(
		outputs,
		lcommon.SubTransactionOutputsFromTransaction(transaction)...,
	)
	for _, output := range outputs {
		if output == nil {
			continue
		}
		script := output.ScriptRef()
		if script == nil {
			continue
		}
		scriptType, err := ledgerScriptType(script)
		if err != nil {
			return err
		}
		if err := storeScriptContent(ctx, db, script, scriptType, slot); err != nil {
			return fmt.Errorf("store reference script: %w", err)
		}
	}
	auxiliary := transaction.AuxiliaryData()
	if auxiliary == nil {
		return nil
	}
	type scriptsWithType struct {
		scriptType uint8
		scripts    []lcommon.Script
	}
	groups := make([]scriptsWithType, 0, 5)
	native, err := auxiliary.NativeScripts()
	if err != nil {
		return fmt.Errorf("decode auxiliary native scripts: %w", err)
	}
	groups = append(groups, scriptsWithType{
		scriptType: uint8(lcommon.ScriptRefTypeNativeScript),
		scripts:    scriptsAsInterfaces(native),
	})
	plutusV1, err := auxiliary.PlutusV1Scripts()
	if err != nil {
		return fmt.Errorf("decode auxiliary Plutus V1 scripts: %w", err)
	}
	groups = append(groups, scriptsWithType{
		scriptType: uint8(lcommon.ScriptRefTypePlutusV1),
		scripts:    scriptsAsInterfaces(plutusV1),
	})
	plutusV2, err := auxiliary.PlutusV2Scripts()
	if err != nil {
		return fmt.Errorf("decode auxiliary Plutus V2 scripts: %w", err)
	}
	groups = append(groups, scriptsWithType{
		scriptType: uint8(lcommon.ScriptRefTypePlutusV2),
		scripts:    scriptsAsInterfaces(plutusV2),
	})
	plutusV3, err := auxiliary.PlutusV3Scripts()
	if err != nil {
		return fmt.Errorf("decode auxiliary Plutus V3 scripts: %w", err)
	}
	groups = append(groups, scriptsWithType{
		scriptType: uint8(lcommon.ScriptRefTypePlutusV3),
		scripts:    scriptsAsInterfaces(plutusV3),
	})
	plutusV4, err := auxiliary.PlutusV4Scripts()
	if err != nil {
		return fmt.Errorf("decode auxiliary Plutus V4 scripts: %w", err)
	}
	groups = append(groups, scriptsWithType{
		scriptType: uint8(lcommon.ScriptRefTypePlutusV4),
		scripts:    scriptsAsInterfaces(plutusV4),
	})
	for _, group := range groups {
		for _, script := range group.scripts {
			if err := storeScriptContent(
				ctx,
				db,
				script,
				group.scriptType,
				slot,
			); err != nil {
				return fmt.Errorf("store auxiliary script: %w", err)
			}
		}
	}
	return nil
}

func scriptsAsInterfaces[T lcommon.Script](scripts []T) []lcommon.Script {
	ret := make([]lcommon.Script, len(scripts))
	for i := range scripts {
		ret[i] = scripts[i]
	}
	return ret
}

func ledgerScriptType(script lcommon.Script) (uint8, error) {
	switch script.(type) {
	case lcommon.NativeScript:
		return uint8(lcommon.ScriptRefTypeNativeScript), nil
	case lcommon.PlutusV1Script:
		return uint8(lcommon.ScriptRefTypePlutusV1), nil
	case lcommon.PlutusV2Script:
		return uint8(lcommon.ScriptRefTypePlutusV2), nil
	case lcommon.PlutusV3Script:
		return uint8(lcommon.ScriptRefTypePlutusV3), nil
	case lcommon.PlutusV4Script:
		return uint8(lcommon.ScriptRefTypePlutusV4), nil
	default:
		return 0, fmt.Errorf("unsupported script type %T", script)
	}
}

func storeTransactionDatumIndex(
	rows *rowBatch,
	transaction lcommon.Transaction,
	slot uint64,
) error {
	seen := make(map[lcommon.Blake2b256]struct{})
	for _, output := range transaction.Produced() {
		if output.Output == nil {
			continue
		}
		if err := storeDatumIndexRow(
			rows,
			output.Output.Datum(),
			slot,
			seen,
		); err != nil {
			return err
		}
	}
	for _, output := range lcommon.SubTransactionOutputsFromTransaction(transaction) {
		if output == nil {
			continue
		}
		if err := storeDatumIndexRow(rows, output.Datum(), slot, seen); err != nil {
			return err
		}
	}
	for _, witnesses := range allTransactionWitnessSets(transaction) {
		for _, datum := range witnesses.PlutusData() {
			copy := datum
			if err := storeDatumIndexRow(rows, &copy, slot, seen); err != nil {
				return err
			}
		}
	}
	return nil
}

func storeDatumIndexRow(
	rows *rowBatch,
	datum *lcommon.Datum,
	slot uint64,
	seen map[lcommon.Blake2b256]struct{},
) error {
	if datum == nil {
		return nil
	}
	raw := datum.Cbor()
	if len(raw) == 0 {
		var err error
		raw, err = datum.MarshalCBOR()
		if err != nil {
			return fmt.Errorf("marshal datum: %w", err)
		}
	}
	if len(raw) == 0 {
		return nil
	}
	hash := lcommon.Blake2b256Hash(raw)
	if _, ok := seen[hash]; ok {
		return nil
	}
	seen[hash] = struct{}{}
	rows.add(datumShape, hash.Bytes(), raw, slot)
	return nil
}
