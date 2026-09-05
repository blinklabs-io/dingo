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

package kupo

import (
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/labelcodec"
	"github.com/blinklabs-io/dingo/internal/version"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// NodeAdapter translates Kupo's package-local API contract into narrow
// LedgerState and coordinated database calls.
type NodeAdapter struct {
	ledgerState *ledger.LedgerState
}

// NewNodeAdapter builds a Kupo node adapter.
func NewNodeAdapter(ls *ledger.LedgerState) (*NodeAdapter, error) {
	if ls == nil {
		return nil, errors.New(
			"new Kupo node adapter: ledger state must not be nil",
		)
	}
	return &NodeAdapter{ledgerState: ls}, nil
}

func (a *NodeAdapter) Tip() (Point, error) {
	tip := a.ledgerState.Tip()
	return Point{
		SlotNo:     tip.Point.Slot,
		HeaderHash: hex.EncodeToString(tip.Point.Hash),
	}, nil
}

const matchPageSize = 512

type nodeMatchIterator struct {
	ctx       context.Context
	adapter   *NodeAdapter
	txn       *database.Txn
	tip       Point
	pattern   parsedPattern
	query     MatchQuery
	storage   *models.UtxoHistoryQuery
	page      []models.UtxoWithHistory
	pageIndex int
	exhausted bool
	closed    bool
	spenders  map[string]*spendingTransactionDetails
}

type spendingTransactionDetails struct {
	inputIndexes map[string]uint32
	redeemers    map[uint32]string
}

func (a *NodeAdapter) Matches(
	ctx context.Context,
	query MatchQuery,
) (MatchIterator, error) {
	pattern, err := parsePattern(query.Pattern)
	if err != nil {
		return nil, err
	}
	matchAll, addressPatterns, err := pattern.addressQuery()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	storageQuery := &models.UtxoHistoryQuery{
		MatchAllAddresses: matchAll,
		AddressPatterns:   addressPatterns,
		Descending:        !query.OldestFirst,
		TransactionID:     append([]byte(nil), query.TransactionID...),
		OutputIndex:       query.OutputIndex,
	}
	switch query.Status {
	case MatchStatusAny:
		storageQuery.Status = models.UtxoHistoryStatusAll
	case MatchStatusSpent:
		storageQuery.Status = models.UtxoHistoryStatusSpent
	case MatchStatusUnspent:
		storageQuery.Status = models.UtxoHistoryStatusUnspent
	default:
		return nil, fmt.Errorf("%w: invalid match status", ErrInvalidRequest)
	}
	if len(query.PolicyID) > 0 {
		storageQuery.FilterByAsset = true
		storageQuery.AssetPolicyID = query.PolicyID
		if query.AssetName != nil {
			storageQuery.AssetName = query.AssetName
		}
	}
	empty := applyPatternFilters(storageQuery, pattern)
	db := a.ledgerState.Database()
	txn, dbTip, err := database.NewReadSnapshotContext(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("open Kupo query snapshot: %w", err)
	}
	releaseOnError := true
	defer func() {
		if releaseOnError {
			txn.Release()
		}
	}()
	if err := a.applyPointBounds(storageQuery, query, txn); err != nil {
		return nil, err
	}
	releaseOnError = false
	return &nodeMatchIterator{
		ctx:     ctx,
		adapter: a,
		txn:     txn,
		tip: Point{
			SlotNo:     dbTip.Point.Slot,
			HeaderHash: hex.EncodeToString(dbTip.Point.Hash),
		},
		pattern:   pattern,
		query:     query,
		storage:   storageQuery,
		exhausted: empty,
	}, nil
}

func (i *nodeMatchIterator) Tip() Point {
	return i.tip
}

func (i *nodeMatchIterator) Next() (Match, bool, error) {
	for {
		if err := i.ctx.Err(); err != nil {
			return Match{}, false, err
		}
		if i.pageIndex >= len(i.page) {
			if i.exhausted {
				return Match{}, false, nil
			}
			i.storage.Limit = matchPageSize
			page, err := i.adapter.ledgerState.Database().UtxosWithHistory(
				i.storage,
				i.txn,
			)
			if err != nil {
				return Match{}, false, fmt.Errorf(
					"query historical UTxOs: %w",
					err,
				)
			}
			i.page = page
			i.pageIndex = 0
			i.exhausted = len(page) < matchPageSize
			// Bound spender hydration to the current result page. A consuming
			// transaction commonly spends several returned outputs, while a
			// full-history query may encounter an unbounded number of spenders.
			i.spenders = make(map[string]*spendingTransactionDetails)
			if len(page) == 0 {
				return Match{}, false, nil
			}
			last := page[len(page)-1]
			i.storage.After = &models.UtxoOrderingCursor{
				Slot:       last.TxSlot,
				BlockIndex: last.TxBlockIndex,
				OutputIdx:  last.OutputIdx,
				TxId:       append([]byte(nil), last.TxId...),
			}
		}

		utxo := i.page[i.pageIndex]
		i.pageIndex++
		output, err := utxo.Decode()
		if err != nil {
			return Match{}, false, fmt.Errorf(
				"decode output %x#%d: %w",
				utxo.TxId,
				utxo.OutputIdx,
				err,
			)
		}
		if !i.pattern.matchesAddress(output.Address()) ||
			!i.pattern.matchesAssets(utxo.Assets) ||
			len(utxo.CreatedBlockHash) == 0 ||
			(utxo.DeletedSlot != 0 &&
				(len(utxo.SpentBlockHash) == 0 || len(utxo.SpentAtTxId) == 0)) {
			continue
		}
		match, err := i.adapter.matchFromUtxo(
			utxo,
			i.query.ResolveHashes,
			i.txn,
		)
		if err != nil {
			return Match{}, false, err
		}
		if match.SpentAt != nil {
			inputIndex, redeemer, err := i.spentFields(utxo)
			if err != nil {
				return Match{}, false, err
			}
			match.SpentAt.InputIndex = inputIndex
			match.SpentAt.Redeemer = redeemer
		}
		return match, true, nil
	}
}

func (i *nodeMatchIterator) spentFields(
	utxo models.UtxoWithHistory,
) (*uint32, *string, error) {
	cacheKey := string(utxo.SpentAtTxId)
	details, ok := i.spenders[cacheKey]
	if !ok {
		transaction, err := i.adapter.ledgerState.Database().
			GetTransactionByHash(
				utxo.SpentAtTxId,
				i.txn,
			)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"load Kupo spending transaction %x: %w",
				utxo.SpentAtTxId,
				err,
			)
		}
		if transaction != nil {
			details = newSpendingTransactionDetails(transaction)
		}
		i.spenders[cacheKey] = details
	}
	if details == nil {
		// Older API databases can retain the output's spending point without
		// the consuming transaction associations. Kupo declares these fields
		// nullable, so preserve the match instead of inventing an ordinal.
		return nil, nil, nil
	}
	inputIndex, ok := details.inputIndexes[utxoReference(utxo.Utxo)]
	if !ok {
		return nil, nil, nil
	}
	retIndex := inputIndex
	redeemer, ok := details.redeemers[inputIndex]
	if !ok {
		return &retIndex, nil, nil
	}
	retRedeemer := redeemer
	return &retIndex, &retRedeemer, nil
}

func newSpendingTransactionDetails(
	transaction *models.Transaction,
) *spendingTransactionDetails {
	inputs := append([]models.Utxo(nil), transaction.Inputs...)
	slices.SortFunc(inputs, func(a, b models.Utxo) int {
		if comparison := bytes.Compare(a.TxId, b.TxId); comparison != 0 {
			return comparison
		}
		return cmp.Compare(a.OutputIdx, b.OutputIdx)
	})
	details := &spendingTransactionDetails{
		inputIndexes: make(map[string]uint32, len(inputs)),
		redeemers:    make(map[uint32]string),
	}
	for index := range inputs {
		details.inputIndexes[utxoReference(inputs[index])] = uint32(
			index,
		) // #nosec G115 -- a transaction input count is bounded by transaction size
	}
	for _, redeemer := range transaction.Redeemers {
		if redeemer.Tag != uint8(lcommon.RedeemerTagSpend) {
			continue
		}
		details.redeemers[redeemer.Index] = hex.EncodeToString(redeemer.Data)
	}
	return details
}

func utxoReference(utxo models.Utxo) string {
	return hex.EncodeToString(utxo.TxId) + "#" +
		strconv.FormatUint(uint64(utxo.OutputIdx), 10)
}

func (i *nodeMatchIterator) Close() {
	if i.closed {
		return
	}
	i.closed = true
	i.txn.Release()
}

func applyPatternFilters(
	query *models.UtxoHistoryQuery,
	pattern parsedPattern,
) bool {
	switch pattern.kind {
	case patternAsset:
		if query.FilterByAsset {
			// The metadata query has no asset-name-only predicate. Keep the
			// candidate set broad and apply this path pattern to hydrated
			// assets. A query-param asset filter is independent: one output
			// may contain both assets, so differing identifiers do not imply
			// an empty intersection.
			return false
		}
		query.FilterByAsset = true
		query.AssetPolicyID = pattern.policyID
		if !pattern.assetAny {
			query.AssetName = pattern.assetName
		}
	case patternReference:
		if len(query.TransactionID) > 0 &&
			!bytes.Equal(query.TransactionID, pattern.txID) {
			return true
		}
		query.TransactionID = pattern.txID
		if pattern.output != nil {
			if query.OutputIndex != nil &&
				*query.OutputIndex != *pattern.output {
				return true
			}
			query.OutputIndex = pattern.output
		}
	case patternMetadata:
		query.MetadataLabel = pattern.metadata
	case patternAll, patternShelley, patternAddress, patternCredentials:
		// These patterns are fully represented by the address query above.
	}
	return false
}

func (a *NodeAdapter) applyPointBounds(
	storage *models.UtxoHistoryQuery,
	query MatchQuery,
	txn *database.Txn,
) error {
	selectors := []struct {
		selector *PointSelector
		dest     **uint64
	}{
		{query.CreatedAfter, &storage.CreatedAfter},
		{query.CreatedBefore, &storage.CreatedBefore},
		{query.SpentAfter, &storage.SpentAfter},
		{query.SpentBefore, &storage.SpentBefore},
	}
	for _, item := range selectors {
		if item.selector == nil {
			continue
		}
		if item.selector.HeaderHash != "" {
			point, err := database.BlockPointBySlotTxn(
				txn,
				item.selector.SlotNo,
			)
			if errors.Is(err, models.ErrBlockNotFound) ||
				(err == nil && !stringsEqualFoldHex(
					point.Hash,
					item.selector.HeaderHash,
				)) {
				return fmt.Errorf(
					"%w: point is not an indexed checkpoint",
					ErrInvalidRequest,
				)
			}
			if err != nil {
				return fmt.Errorf("validate Kupo point: %w", err)
			}
		}
		slot := item.selector.SlotNo
		*item.dest = &slot
	}
	return nil
}

func stringsEqualFoldHex(raw []byte, encoded string) bool {
	decoded, err := hex.DecodeString(encoded)
	return err == nil && bytes.Equal(raw, decoded)
}

func (a *NodeAdapter) matchFromUtxo(
	utxo models.UtxoWithHistory,
	resolve bool,
	txn *database.Txn,
) (Match, error) {
	output, err := utxo.Decode()
	if err != nil {
		return Match{}, err
	}
	ret := Match{
		TransactionIndex: utxo.TxBlockIndex,
		TransactionID:    hex.EncodeToString(utxo.TxId),
		OutputIndex:      utxo.OutputIdx,
		Address:          output.Address().String(),
		Value: Value{
			Coins:  uint64(utxo.Amount),
			Assets: make(map[string]uint64, len(utxo.Assets)),
		},
		CreatedAt: Point{
			SlotNo:     utxo.TxSlot,
			HeaderHash: hex.EncodeToString(utxo.CreatedBlockHash),
		},
	}
	for _, asset := range utxo.Assets {
		assetID := hex.EncodeToString(
			asset.PolicyId,
		) + "." + hex.EncodeToString(
			asset.Name,
		)
		ret.Value.Assets[assetID] = uint64(asset.Amount)
	}
	if len(utxo.DatumHash) > 0 {
		hash := hex.EncodeToString(utxo.DatumHash)
		ret.DatumHash = &hash
		ret.DatumType = "hash"
	}
	if inline := output.Datum(); inline != nil {
		ret.DatumType = "inline"
		if resolve {
			raw := hex.EncodeToString(inline.Cbor())
			ret.Datum = &raw
		}
	}
	if scriptRef := output.ScriptRef(); scriptRef != nil {
		hash := hex.EncodeToString(scriptRef.Hash().Bytes())
		ret.ScriptHash = &hash
		if resolve {
			ret.Script = scriptFromLedger(scriptRef)
		}
	}
	if utxo.DeletedSlot != 0 {
		spender := hex.EncodeToString(utxo.SpentAtTxId)
		ret.SpentAt = &SpentPoint{
			SlotNo:        utxo.DeletedSlot,
			HeaderHash:    hex.EncodeToString(utxo.SpentBlockHash),
			TransactionID: &spender,
		}
	}
	if resolve && ret.DatumHash != nil && ret.Datum == nil {
		datum, err := a.ledgerState.Database().GetDatum(utxo.DatumHash, txn)
		if err != nil && !errors.Is(err, database.ErrDatumNotFound) {
			return Match{}, fmt.Errorf(
				"resolve datum %x: %w",
				utxo.DatumHash,
				err,
			)
		}
		if datum != nil {
			raw := hex.EncodeToString(datum.RawDatum)
			ret.Datum = &raw
		}
	}
	return ret, nil
}

func scriptFromLedger(script lcommon.Script) *Script {
	if script == nil {
		return nil
	}
	language := "native"
	switch script.(type) {
	case lcommon.PlutusV1Script:
		language = "plutus:v1"
	case lcommon.PlutusV2Script:
		language = "plutus:v2"
	case lcommon.PlutusV3Script:
		language = "plutus:v3"
	case lcommon.PlutusV4Script:
		language = "plutus:v4"
	}
	return &Script{
		Language: language,
		Script:   hex.EncodeToString(script.RawScriptBytes()),
	}
}

func (a *NodeAdapter) Datum(
	ctx context.Context,
	hash []byte,
) (*Datum, Point, error) {
	db := a.ledgerState.Database()
	txn, tip, err := database.NewReadSnapshotContext(ctx, db)
	if err != nil {
		return nil, Point{}, fmt.Errorf("open Kupo datum snapshot: %w", err)
	}
	defer txn.Release()
	snapshotTip := pointFromChainPoint(tip.Point)
	datum, err := db.GetDatum(hash, txn)
	if errors.Is(err, database.ErrDatumNotFound) {
		return nil, snapshotTip, nil
	}
	if err != nil {
		return nil, Point{}, err
	}
	return &Datum{Datum: hex.EncodeToString(datum.RawDatum)}, snapshotTip, nil
}

func (a *NodeAdapter) Script(
	ctx context.Context,
	hash []byte,
) (*Script, Point, error) {
	db := a.ledgerState.Database()
	txn, tip, err := database.NewReadSnapshotContext(ctx, db)
	if err != nil {
		return nil, Point{}, fmt.Errorf("open Kupo script snapshot: %w", err)
	}
	defer txn.Release()
	snapshotTip := pointFromChainPoint(tip.Point)
	script, err := db.GetScript(hash, txn)
	if errors.Is(err, database.ErrScriptNotFound) {
		return nil, snapshotTip, nil
	}
	if err != nil {
		return nil, Point{}, err
	}
	language, err := scriptLanguage(script.Type)
	if err != nil {
		return nil, Point{}, err
	}
	return &Script{
		Language: language,
		Script:   hex.EncodeToString(script.Content),
	}, snapshotTip, nil
}

func scriptLanguage(scriptType uint8) (string, error) {
	switch scriptType {
	case lcommon.ScriptRefTypeNativeScript:
		return "native", nil
	case lcommon.ScriptRefTypePlutusV1:
		return "plutus:v1", nil
	case lcommon.ScriptRefTypePlutusV2:
		return "plutus:v2", nil
	case lcommon.ScriptRefTypePlutusV3:
		return "plutus:v3", nil
	case lcommon.ScriptRefTypePlutusV4:
		return "plutus:v4", nil
	default:
		return "", fmt.Errorf("unsupported Kupo script type %d", scriptType)
	}
}

func (a *NodeAdapter) Checkpoints(
	ctx context.Context,
) ([]Point, Point, error) {
	db := a.ledgerState.Database()
	txn, tip, err := database.NewReadSnapshotContext(ctx, db)
	if err != nil {
		return nil, Point{}, fmt.Errorf(
			"open Kupo checkpoint snapshot: %w",
			err,
		)
	}
	defer txn.Release()
	snapshotTip := pointFromChainPoint(tip.Point)
	if len(tip.Point.Hash) == 0 {
		return []Point{}, snapshotTip, nil
	}
	tipID, err := database.BlockIDByPointLocalTxn(txn, tip.Point)
	if err != nil {
		return nil, Point{}, err
	}
	rollbackWindow := max(a.ledgerState.SecurityParam(), 1)
	offsets := []uint64{0}
	for offset := uint64(1); offset < uint64(rollbackWindow); offset *= 2 {
		offsets = append(offsets, offset)
	}
	if last := uint64(rollbackWindow); offsets[len(offsets)-1] != last {
		offsets = append(offsets, last)
	}
	ret := make([]Point, 0, len(offsets))
	lastID := ^uint64(0)
	for _, offset := range offsets {
		blockID := uint64(database.BlockInitialIndex)
		if offset < tipID {
			blockID = tipID - offset
		}
		if blockID == lastID {
			continue
		}
		point, err := db.BlockPointByIndex(blockID, txn)
		if errors.Is(err, models.ErrBlockNotFound) {
			continue
		}
		if err != nil {
			return nil, Point{}, err
		}
		ret = append(ret, Point{
			SlotNo:     point.Slot,
			HeaderHash: hex.EncodeToString(point.Hash),
		})
		lastID = blockID
	}
	return ret, snapshotTip, nil
}

func (a *NodeAdapter) Checkpoint(
	ctx context.Context,
	slot uint64,
	strict bool,
) (*Point, Point, error) {
	db := a.ledgerState.Database()
	txn, tip, err := database.NewReadSnapshotContext(ctx, db)
	if err != nil {
		return nil, Point{}, fmt.Errorf(
			"open Kupo checkpoint snapshot: %w",
			err,
		)
	}
	defer txn.Release()
	snapshotTip := pointFromChainPoint(tip.Point)
	var (
		point    ocommon.Point
		queryErr error
	)
	if strict {
		point, queryErr = database.BlockPointBySlotTxn(txn, slot)
	} else {
		point, queryErr = database.BlockPointAtOrBeforeSlotTxn(txn, slot)
	}
	if errors.Is(queryErr, models.ErrBlockNotFound) {
		return nil, snapshotTip, nil
	}
	if queryErr != nil {
		return nil, Point{}, queryErr
	}
	ret := pointFromChainPoint(point)
	return &ret, snapshotTip, nil
}

func (a *NodeAdapter) Metadata(
	ctx context.Context,
	slot uint64,
	transactionID []byte,
) ([]Metadata, string, Point, error) {
	db := a.ledgerState.Database()
	txn, tip, err := database.NewReadSnapshotContext(ctx, db)
	if err != nil {
		return nil, "", Point{}, fmt.Errorf(
			"open Kupo metadata snapshot: %w",
			err,
		)
	}
	defer txn.Release()
	snapshotTip := pointFromChainPoint(tip.Point)
	if slot == 0 {
		return []Metadata{}, "", snapshotTip, nil
	}
	block, err := database.BlockBySlotTxn(txn, slot)
	if errors.Is(err, models.ErrBlockNotFound) {
		block, err = database.BlockBeforeSlotTxn(txn, slot)
	}
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return nil, "", Point{}, fmt.Errorf(
				"%w: no indexed ancestor for slot %d",
				ErrInvalidRequest,
				slot,
			)
		}
		return nil, "", Point{}, err
	}
	decodedBlock, err := block.Decode()
	if err != nil {
		return nil, "", Point{}, fmt.Errorf("decode metadata block: %w", err)
	}
	ledgerTxs := decodedBlock.Transactions()
	ret := []Metadata{}
	for i := range ledgerTxs {
		txHash := ledgerTxs[i].Hash().Bytes()
		if len(transactionID) > 0 && !bytes.Equal(txHash, transactionID) {
			continue
		}
		auxiliary := ledgerTxs[i].AuxiliaryData()
		if auxiliary == nil || len(auxiliary.Cbor()) == 0 {
			continue
		}
		schema := map[string]any{}
		if ledgerTxs[i].Metadata() != nil {
			_, entries, err := labelcodec.EncodeAndExtract(
				ledgerTxs[i].Metadata(),
			)
			if err != nil {
				return nil, "", Point{}, fmt.Errorf(
					"decode transaction metadata: %w",
					err,
				)
			}
			for _, entry := range entries {
				metadatum, err := lcommon.DecodeMetadatumRaw(entry.CborValue)
				if err != nil {
					return nil, "", Point{}, err
				}
				schema[strconv.FormatUint(entry.Label, 10)] = metadatumSchema(
					metadatum,
				)
			}
		}
		raw := auxiliary.Cbor()
		ret = append(ret, Metadata{
			Hash:   lcommon.Blake2b256Hash(raw).String(),
			Raw:    hex.EncodeToString(raw),
			Schema: schema,
		})
	}
	return ret, hex.EncodeToString(block.Hash), snapshotTip, nil
}

func pointFromChainPoint(point ocommon.Point) Point {
	return Point{
		SlotNo:     point.Slot,
		HeaderHash: hex.EncodeToString(point.Hash),
	}
}

func metadatumSchema(value lcommon.TransactionMetadatum) any {
	switch typed := value.(type) {
	case lcommon.MetaInt:
		if typed.Value == nil {
			return map[string]any{"int": 0}
		}
		return map[string]any{"int": typed.Value}
	case lcommon.MetaText:
		return map[string]any{"string": typed.Value}
	case lcommon.MetaBytes:
		return map[string]any{"bytes": hex.EncodeToString(typed.Value)}
	case lcommon.MetaList:
		items := make([]any, len(typed.Items))
		for i := range typed.Items {
			items[i] = metadatumSchema(typed.Items[i])
		}
		return map[string]any{"list": items}
	case lcommon.MetaMap:
		pairs := make([]map[string]any, len(typed.Pairs))
		for i := range typed.Pairs {
			pairs[i] = map[string]any{
				"k": metadatumSchema(typed.Pairs[i].Key),
				"v": metadatumSchema(typed.Pairs[i].Value),
			}
		}
		return map[string]any{"map": pairs}
	default:
		return nil
	}
}

func (a *NodeAdapter) Health() (Health, Point, int, error) {
	tip := a.ledgerState.Tip()
	checkpoint := Point{
		SlotNo:     tip.Point.Slot,
		HeaderHash: hex.EncodeToString(tip.Point.Hash),
	}
	targetSlot, connected := a.ledgerState.UpstreamSyncStatus()
	ret := Health{
		ConnectionStatus: "disconnected",
		Version:          version.GetVersionString(),
	}
	ret.Configuration.Indexes = "installed"
	if len(tip.Point.Hash) > 0 {
		tipSlot := tip.Point.Slot
		ret.MostRecentCheckpoint = &tipSlot
	}
	if !connected {
		return ret, checkpoint, http.StatusServiceUnavailable, nil
	}
	ret.ConnectionStatus = "connected"
	ret.MostRecentNodeTip = &targetSlot
	progress := a.ledgerState.SyncProgress()
	if targetSlot == 0 {
		progress = 0
	}
	progress = math.Max(0, math.Min(1, progress))
	ret.NetworkSynchronization = &progress
	status := http.StatusAccepted
	if progress >= 1 {
		status = http.StatusOK
	}
	return ret, checkpoint, status, nil
}
