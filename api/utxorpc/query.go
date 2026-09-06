// Copyright 2025 Blink Labs Software
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

package utxorpc

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	utxorpcCardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	query "github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query/queryconnect"
)

// queryServiceServer implements the QueryService API
type queryServiceServer struct {
	queryconnect.UnimplementedQueryServiceHandler
	utxorpc *Utxorpc
}

// ErrByronProtocolParams reports that the ledger holds no current protocol
// parameters because the chain is still in its Byron prefix. Byron carries no
// protocol-parameter CBOR, so this is an expected state during a from-genesis
// synchronization rather than a node fault, and no Shelley-shaped parameters
// may be substituted for it.
var ErrByronProtocolParams = errors.New(
	"protocol parameters unavailable in the Byron era",
)

func extractSearchPredicatePatterns(
	predicate *query.UtxoPredicate,
) (*utxorpcCardano.AddressPattern, *utxorpcCardano.AssetPattern) {
	if predicate == nil {
		return nil, nil
	}
	match := predicate.GetMatch()
	if match == nil {
		return nil, nil
	}
	cardanoMatch := match.GetCardano()
	if cardanoMatch == nil {
		return nil, nil
	}
	return cardanoMatch.GetAddress(), cardanoMatch.GetAsset()
}

// searchUtxosMatchAllAddresses is true when the query must not filter by
// payment/stake keys: nil predicate, or Cardano predicate with an asset pattern
// but no address pattern (asset-only search).
func searchUtxosMatchAllAddresses(
	predicate *query.UtxoPredicate,
	addressPattern *utxorpcCardano.AddressPattern,
	assetPattern *utxorpcCardano.AssetPattern,
) bool {
	if predicate == nil {
		return true
	}
	if addressPattern == nil && assetPattern != nil {
		return true
	}
	return false
}

func effectiveSearchUtxosMaxItems(requested, maxAllowed int32) int32 {
	if requested != 0 {
		return requested
	}
	return maxAllowed
}

func parseSearchUtxosStartToken(
	startToken string,
) (*models.UtxoOrderingCursor, error) {
	if startToken == "" {
		return nil, nil
	}
	parts := strings.Split(startToken, ":")
	if len(parts) != 4 {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New(
				"invalid start_token: expected slot:block_index:output_idx:tx_id",
			),
		)
	}

	cursorSlot, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("invalid start_token slot"),
		)
	}

	cursorBlockIndex, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("invalid start_token block_index"),
		)
	}

	cursorOutputIdx, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("invalid start_token output_idx"),
		)
	}

	cursorTxId, err := hex.DecodeString(parts[3])
	if err != nil || len(cursorTxId) != 32 {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("invalid start_token tx_id"),
		)
	}

	return &models.UtxoOrderingCursor{
		Slot:       cursorSlot,
		BlockIndex: uint32(cursorBlockIndex),
		OutputIdx:  uint32(cursorOutputIdx),
		TxId:       cursorTxId,
	}, nil
}

func searchUtxoModelToAnyData(
	utxo *models.UtxoWithOrdering,
) (*query.AnyUtxoData, error) {
	var aud query.AnyUtxoData
	ret, err := utxo.Decode()
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, errors.New("decode returned empty utxo")
	}
	tmpUtxo, err := ret.Utxorpc()
	if err != nil {
		return nil, fmt.Errorf("failed to convert UTxO: %w", err)
	}
	audc := query.AnyUtxoData_Cardano{
		Cardano: tmpUtxo,
	}
	aud.NativeBytes = utxo.Cbor
	aud.TxoRef = &query.TxoRef{
		Hash:  utxo.TxId,
		Index: utxo.OutputIdx,
	}
	if audc.Cardano.GetDatum() != nil {
		isAllZeroes := true
		for _, b := range audc.Cardano.GetDatum().GetHash() {
			if b != 0 {
				isAllZeroes = false
				break
			}
		}
		if isAllZeroes {
			audc.Cardano.Datum = nil
		}
	}
	aud.ParsedState = &audc
	return &aud, nil
}

func searchUtxoAddressPatterns(
	addressPattern *utxorpcCardano.AddressPattern,
) ([]models.UtxoAddressPattern, error) {
	if addressPattern == nil {
		return nil, nil
	}
	pattern := models.UtxoAddressPattern{
		ExactAddress:   addressPattern.GetExactAddress(),
		PaymentPart:    addressPattern.GetPaymentPart(),
		DelegationPart: addressPattern.GetDelegationPart(),
	}
	if len(pattern.ExactAddress) > 0 {
		if _, err := lcommon.NewAddressFromBytes(
			pattern.ExactAddress,
		); err != nil {
			return nil, connect.NewError(
				connect.CodeInvalidArgument,
				fmt.Errorf("failed to decode exact address: %w", err),
			)
		}
	}
	if len(pattern.PaymentPart) > 0 &&
		len(pattern.PaymentPart) != lcommon.AddressHashSize {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			fmt.Errorf(
				"invalid payment part length %d",
				len(pattern.PaymentPart),
			),
		)
	}
	if len(pattern.DelegationPart) > 0 &&
		len(pattern.DelegationPart) != lcommon.AddressHashSize {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			fmt.Errorf(
				"invalid delegation part length %d",
				len(pattern.DelegationPart),
			),
		)
	}
	if len(pattern.ExactAddress) == 0 &&
		len(pattern.PaymentPart) == 0 &&
		len(pattern.DelegationPart) == 0 {
		return nil, nil
	}
	return []models.UtxoAddressPattern{pattern}, nil
}

// ReadParams
func (s *queryServiceServer) ReadParams(
	ctx context.Context,
	req *connect.Request[query.ReadParamsRequest],
) (*connect.Response[query.ReadParamsResponse], error) {
	fieldMask := req.Msg.GetFieldMask()

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a ReadParams request with fieldMask %v",
			fieldMask,
		),
	)
	resp := &query.ReadParamsResponse{}

	// GetCurrentPParamsForReporting omits any synthetic (not-yet-real)
	// PlutusV2 cost model from this reporting reply, matching what a real
	// cardano-node reports -- see blinklabs-io/dingo#3825.
	protoParams := s.utxorpc.config.LedgerState.GetCurrentPParamsForReporting()
	if protoParams == nil {
		// Byron carries no protocol-parameter CBOR, so a genuine Byron
		// prefix reaches this during a from-genesis sync. FailedPrecondition
		// tells the caller the chain is not yet in a state that can answer,
		// which is the truth; Unavailable would invite a retry loop across
		// what can be days of synchronization. Return before the tip lookup:
		// there is no useful tip to pair with an absent parameter set.
		return nil, connect.NewError(
			connect.CodeFailedPrecondition,
			ErrByronProtocolParams,
		)
	}

	// Get chain point (slot, hash, and height)
	br := blockRefFromTip(s.utxorpc.config.LedgerState.Tip())

	// Set up response parameters
	tmpPparams, err := protoParams.Utxorpc()
	if err != nil {
		return nil, fmt.Errorf("convert pparams: %w", err)
	}
	acpc := &query.AnyChainParams_Cardano{
		Cardano: tmpPparams,
	}
	resp.LedgerTip = &query.ChainPoint{
		Slot:   br.Slot,
		Hash:   br.Hash,
		Height: br.Height,
	}
	resp.Values = &query.AnyChainParams{
		Params: acpc,
	}
	return connect.NewResponse(resp), nil
}

// ReadEraSummary
func (s *queryServiceServer) ReadEraSummary(
	ctx context.Context,
	req *connect.Request[query.ReadEraSummaryRequest],
) (*connect.Response[query.ReadEraSummaryResponse], error) {
	fieldMask := req.Msg.GetFieldMask()

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a ReadEraSummary request with fieldMask %v",
			fieldMask,
		),
	)

	// Fetched chain system start time from shelley genesis
	systemStart, err := s.utxorpc.config.LedgerState.SystemStart()
	if err != nil {
		return nil, fmt.Errorf("get system start: %w", err)
	}
	// converts system start time to milliseconds
	systemStartMs := systemStart.UnixMilli()
	if systemStartMs < 0 {
		return nil, errors.New("system start is before unix epoch")
	}
	// Load all epochs from the database
	epochs, err := s.utxorpc.config.LedgerState.GetEpochs()
	if err != nil {
		return nil, fmt.Errorf("get epochs: %w", err)
	}
	if len(epochs) == 0 {
		return nil, errors.New("no epochs available for era summary")
	}
	// rearrange the order of epochs by start slot
	sort.Slice(epochs, func(i, j int) bool {
		return epochs[i].StartSlot < epochs[j].StartSlot
	})

	summaries := make([]*utxorpcCardano.EraSummary, 0, len(epochs))
	summaryByEra := map[uint]*utxorpcCardano.EraSummary{}
	timespanMs := uint64(0)
	baseMs := uint64(systemStartMs)
	var lastEraId uint
	var hasLastEra bool

	for _, epoch := range epochs {
		if !hasLastEra || epoch.EraId != lastEraId {
			eraDescriptor := eras.GetEraById(epoch.EraId)
			if eraDescriptor == nil {
				return nil, fmt.Errorf("unknown era ID %d", epoch.EraId)
			}
			// Build the start boundary for the era using current accumulated time and epoch metadata
			startBoundary := &utxorpcCardano.EraBoundary{
				Time:  baseMs + timespanMs,
				Slot:  epoch.StartSlot,
				Epoch: epoch.EpochId,
			}
			// Create a new era summary when era changes
			summary := &utxorpcCardano.EraSummary{
				Name:  eraDescriptor.Name,
				Start: startBoundary,
			}
			// Get the protocol params for the era epoch
			pparams, err := s.utxorpc.config.LedgerState.GetPParamsForEpoch(
				epoch.EpochId,
				*eraDescriptor,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"get protocol params for era %s epoch %d: %w",
					eraDescriptor.Name,
					epoch.EpochId,
					err,
				)
			}
			// Converts params into utxorpc form
			if pparams != nil {
				tmpParams, err := pparams.Utxorpc()
				if err != nil {
					return nil, fmt.Errorf(
						"convert protocol params for era %s: %w",
						eraDescriptor.Name,
						err,
					)
				}
				summary.ProtocolParams = tmpParams
			}
			// Sets the previous era end boundary to current era start boundary
			if hasLastEra {
				prevSummary := summaryByEra[lastEraId]
				if prevSummary != nil && prevSummary.GetEnd() == nil {
					prevSummary.End = &utxorpcCardano.EraBoundary{
						Time:  startBoundary.GetTime(),
						Slot:  startBoundary.GetSlot(),
						Epoch: startBoundary.GetEpoch(),
					}
				}
			}
			summaries = append(summaries, summary)
			summaryByEra[epoch.EraId] = summary
			lastEraId = epoch.EraId
			hasLastEra = true
		}
		epochDurationMs := uint64(
			epoch.SlotLength,
		) * uint64(
			epoch.LengthInSlots,
		)
		timespanMs += epochDurationMs
	}

	resp := &query.ReadEraSummaryResponse{
		Summary: &query.ReadEraSummaryResponse_Cardano{
			Cardano: &utxorpcCardano.EraSummaries{
				Summaries: summaries,
			},
		},
	}
	return connect.NewResponse(resp), nil
}

// utxoRefKey builds a comparable map key for a (tx hash, output index) UTxO
// reference; models.UtxoId embeds a []byte and so cannot be used as a map
// key directly.
func utxoRefKey(hash []byte, idx uint32) string {
	return string(hash) + ":" + strconv.FormatUint(uint64(idx), 10)
}

// ReadUtxos
func (s *queryServiceServer) ReadUtxos(
	ctx context.Context,
	req *connect.Request[query.ReadUtxosRequest],
) (*connect.Response[query.ReadUtxosResponse], error) {
	keys := req.Msg.GetKeys() // []*TxoRef

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf("Got a ReadUtxos request with keys %v", keys),
	)

	// Enforce request size limit
	if len(keys) > s.utxorpc.config.MaxUtxoKeys {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			fmt.Errorf(
				"too many UTxO keys: %d exceeds maximum of %d",
				len(keys),
				s.utxorpc.config.MaxUtxoKeys,
			),
		)
	}

	resp := &query.ReadUtxosResponse{}

	// Resolve all requested refs in a single batch, then correlate results
	// back to each requested key below.
	refs := make([]models.UtxoId, len(keys))
	for i, txo := range keys {
		refs[i] = models.UtxoId{Hash: txo.GetHash(), Idx: txo.GetIndex()}
	}
	utxos, err := s.utxorpc.config.LedgerState.UtxosByRefs(refs)
	if err != nil {
		return nil, err
	}
	utxoByRef := make(map[string]*models.Utxo, len(utxos))
	for i := range utxos {
		utxoByRef[utxoRefKey(utxos[i].TxId, utxos[i].OutputIdx)] = &utxos[i]
	}

	// Get UTxOs from ledger
	for _, txo := range keys {
		utxo, ok := utxoByRef[utxoRefKey(txo.GetHash(), txo.GetIndex())]
		if !ok {
			return nil, database.ErrUtxoNotFound
		}
		var aud query.AnyUtxoData
		ret, err := utxo.Decode()
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, errors.New("decode returned empty utxo")
		}
		tmpUtxo, err := ret.Utxorpc()
		if err != nil {
			return nil, fmt.Errorf("failed to convert UTxO: %w", err)
		}
		audc := query.AnyUtxoData_Cardano{
			Cardano: tmpUtxo,
		}
		aud.NativeBytes = utxo.Cbor
		aud.TxoRef = txo

		if audc.Cardano.GetDatum() != nil {
			// Check if Datum.Hash is all zeroes
			isAllZeroes := true
			for _, b := range audc.Cardano.GetDatum().GetHash() {
				if b != 0 {
					isAllZeroes = false
					break
				}
			}
			if isAllZeroes {
				// No actual datum; set Datum to nil to omit it
				audc.Cardano.Datum = nil
			}
		}
		aud.ParsedState = &audc
		resp.Items = append(resp.Items, &aud)
	}

	// Get chain point (slot, hash, and height)
	br := blockRefFromTip(s.utxorpc.config.LedgerState.Tip())

	// Set up response utxos
	resp.LedgerTip = &query.ChainPoint{
		Slot:   br.Slot,
		Hash:   br.Hash,
		Height: br.Height,
	}

	return connect.NewResponse(resp), nil
}

// searchUtxosLedgerTip returns the current tip as a ChainPoint for SearchUtxos responses.
func (s *queryServiceServer) searchUtxosLedgerTip() *query.ChainPoint {
	br := blockRefFromTip(s.utxorpc.config.LedgerState.Tip())
	return &query.ChainPoint{
		Slot:   br.Slot,
		Hash:   br.Hash,
		Height: br.Height,
	}
}

// SearchUtxos
func (s *queryServiceServer) SearchUtxos(
	ctx context.Context,
	req *connect.Request[query.SearchUtxosRequest],
) (*connect.Response[query.SearchUtxosResponse], error) {
	predicate := req.Msg.GetPredicate()   // *UtxoPredicate
	startToken := req.Msg.GetStartToken() // string
	maxItems := req.Msg.GetMaxItems()     // int32
	fieldMask := req.Msg.GetFieldMask()

	maxAllowed := int32(
		s.utxorpc.config.MaxHistoryItems,
	) // #nosec G115 -- DefaultMaxHistoryItems (10000)
	if maxItems < 0 {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			fmt.Errorf("maxItems %d must not be negative", maxItems),
		)
	}
	if maxItems > maxAllowed {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			fmt.Errorf(
				"maxItems %d exceeds maximum of %d",
				maxItems,
				maxAllowed,
			),
		)
	}
	effectiveMax := effectiveSearchUtxosMaxItems(maxItems, maxAllowed)

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a SearchUtxos request with predicate %v, startToken %s, "+
				"maxItems raw=%d effective=%d fieldMask %v",
			predicate,
			startToken,
			maxItems,
			effectiveMax,
			fieldMask,
		),
	)
	resp := &query.SearchUtxosResponse{}

	addressPattern, assetPattern := extractSearchPredicatePatterns(predicate)

	// Address resolution for the query:
	// - MatchAllAddresses → scan all live UTxOs (nil predicate, or asset-only predicate);
	//   still limited by max_items and optional asset filter.
	// - predicate != nil with address pattern but no decodable address fields → empty result.
	// - predicate != nil with one or more decoded addresses → OR of those constraints.
	matchAllAddresses := searchUtxosMatchAllAddresses(
		predicate,
		addressPattern,
		assetPattern,
	)

	addressPatterns, err := searchUtxoAddressPatterns(addressPattern)
	if err != nil {
		return nil, err
	}

	if !matchAllAddresses && len(addressPatterns) == 0 {
		resp.LedgerTip = s.searchUtxosLedgerTip()
		return connect.NewResponse(resp), nil
	}

	if effectiveMax == 0 {
		resp.LedgerTip = s.searchUtxosLedgerTip()
		return connect.NewResponse(resp), nil
	}

	filterByAsset := assetPattern != nil
	var assetPolicy []byte
	var assetName []byte
	if filterByAsset {
		assetPolicy = assetPattern.GetPolicyId()
		if len(assetPolicy) == 0 {
			return nil, connect.NewError(
				connect.CodeInvalidArgument,
				errors.New("asset pattern requires non-empty policy id"),
			)
		}
		assetName = assetPattern.GetAssetName()
	}

	after, err := parseSearchUtxosStartToken(startToken)
	if err != nil {
		return nil, err
	}

	utxoQ := &models.UtxoWithOrderingQuery{
		MatchAllAddresses: matchAllAddresses,
		AddressPatterns:   addressPatterns,
		After:             after,
		Limit:             int(effectiveMax) + 1,
		FilterByAsset:     filterByAsset,
		AssetPolicyID:     assetPolicy,
		AssetName:         assetName,
	}

	utxos, err := s.utxorpc.config.LedgerState.UtxosByAddressWithOrdering(utxoQ)
	if err != nil {
		return nil, err
	}

	pageCap := int(effectiveMax)
	hasMore := len(utxos) > pageCap
	if hasMore {
		utxos = utxos[:pageCap]
	}
	items := make([]*query.AnyUtxoData, 0, len(utxos))
	for i := range utxos {
		aud, err := searchUtxoModelToAnyData(&utxos[i])
		if err != nil {
			return nil, err
		}
		items = append(items, aud)
	}
	resp.Items = items
	if hasMore && len(utxos) > 0 {
		last := utxos[len(utxos)-1]
		resp.NextToken = fmt.Sprintf(
			"%d:%d:%d:%x",
			last.TxSlot,
			last.TxBlockIndex,
			last.OutputIdx,
			last.TxId,
		)
	}

	resp.LedgerTip = s.searchUtxosLedgerTip()
	return connect.NewResponse(resp), nil
}

// ReadData
func (s *queryServiceServer) ReadData(
	ctx context.Context,
	req *connect.Request[query.ReadDataRequest],
) (*connect.Response[query.ReadDataResponse], error) {
	keys := req.Msg.GetKeys() // [][]byte
	fieldMask := req.Msg.GetFieldMask()

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a ReadData request with keys %v and fieldMask %v",
			keys,
			fieldMask,
		),
	)

	// Enforce request size limit
	if len(keys) > s.utxorpc.config.MaxDataKeys {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			fmt.Errorf(
				"too many data keys: %d exceeds maximum of %d",
				len(keys),
				s.utxorpc.config.MaxDataKeys,
			),
		)
	}

	resp := &query.ReadDataResponse{}

	for _, key := range keys {
		datum, err := s.utxorpc.config.LedgerState.Datum(key)
		if err != nil {
			if errors.Is(err, database.ErrDatumNotFound) {
				return nil, connect.NewError(
					connect.CodeNotFound,
					fmt.Errorf("datum not found: %x", key),
				)
			}
			return nil, fmt.Errorf("get datum %x: %w", key, err)
		}
		parsed, err := plutusDatumCBORToCardano(datum.RawDatum)
		if err != nil {
			return nil, connect.NewError(
				connect.CodeInternal,
				fmt.Errorf("decode datum plutus data %x: %w", key, err),
			)
		}
		acd := &query.AnyChainDatum{
			Key:         datum.Hash,
			NativeBytes: datum.RawDatum,
		}
		if parsed != nil {
			acd.ParsedState = &query.AnyChainDatum_Cardano{Cardano: parsed}
		}
		resp.Values = append(resp.Values, acd)
	}

	// Get chain point (slot, hash, and height)
	br := blockRefFromTip(s.utxorpc.config.LedgerState.Tip())

	// Set up response utxos
	resp.LedgerTip = &query.ChainPoint{
		Slot:   br.Slot,
		Hash:   br.Hash,
		Height: br.Height,
	}

	return connect.NewResponse(resp), nil
}

// ReadTx
func (s *queryServiceServer) ReadTx(
	ctx context.Context,
	req *connect.Request[query.ReadTxRequest],
) (*connect.Response[query.ReadTxResponse], error) {
	hash := req.Msg.GetHash()
	fieldMask := req.Msg.GetFieldMask()

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a ReadTx request with hash %x and fieldMask %v",
			hash,
			fieldMask,
		),
	)

	if len(hash) == 0 {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("hash is required"),
		)
	}

	// Resolve the transaction metadata to find it's containing block.
	txRecord, err := s.utxorpc.config.LedgerState.TransactionByHash(hash)
	if err != nil {
		return nil, fmt.Errorf("lookup transaction: %w", err)
	}
	if txRecord == nil {
		return nil, connect.NewError(
			connect.CodeNotFound,
			fmt.Errorf("transaction not found: %x", hash),
		)
	}

	// Find the block blob to decode and extract the transaction.
	block, err := s.utxorpc.config.LedgerState.BlockByHash(txRecord.BlockHash)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return nil, connect.NewError(
				connect.CodeNotFound,
				fmt.Errorf("block not found: %x", txRecord.BlockHash),
			)
		}
		return nil, fmt.Errorf("lookup block: %w", err)
	}

	// Decode the block CBOR into a ledger block.
	ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
	if err != nil {
		return nil, fmt.Errorf("decode block: %w", err)
	}

	// Find the transaction by index first, then fall back to a hash scan.
	var tx lcommon.Transaction
	transactions := ledgerBlock.Transactions()
	if int(txRecord.BlockIndex) < len(transactions) {
		value := transactions[txRecord.BlockIndex]
		if bytes.Equal(value.Hash().Bytes(), hash) {
			tx = value
		}
	}
	if tx == nil {
		for _, candidate := range transactions {
			if bytes.Equal(candidate.Hash().Bytes(), hash) {
				tx = candidate
				break
			}
		}
	}
	if tx == nil {
		return nil, connect.NewError(
			connect.CodeNotFound,
			fmt.Errorf("transaction not found in block: %x", hash),
		)
	}

	tmpTx, err := tx.Utxorpc()
	if err != nil {
		return nil, fmt.Errorf("convert transaction: %w", err)
	}

	brForTx := blockRefFromModel(block)
	anyTx := &query.AnyChainTx{
		NativeBytes: tx.Cbor(),
		Chain: &query.AnyChainTx_Cardano{
			Cardano: tmpTx,
		},
		BlockRef: &query.ChainPoint{
			Slot:   brForTx.Slot,
			Hash:   brForTx.Hash,
			Height: brForTx.Height,
		},
	}

	// Get chain point (slot, hash, and height)
	br := blockRefFromTip(s.utxorpc.config.LedgerState.Tip())

	// Set up response utxos
	resp := &query.ReadTxResponse{
		Tx: anyTx,
		LedgerTip: &query.ChainPoint{
			Slot:   br.Slot,
			Hash:   br.Hash,
			Height: br.Height,
		},
	}

	return connect.NewResponse(resp), nil
}

// ReadGenesis
func (s *queryServiceServer) ReadGenesis(
	ctx context.Context,
	req *connect.Request[query.ReadGenesisRequest],
) (*connect.Response[query.ReadGenesisResponse], error) {
	fieldMask := req.Msg.GetFieldMask()
	s.utxorpc.config.Logger.Info(
		fmt.Sprintf("Got a ReadGenesis request with fieldMask %v", fieldMask),
	)

	// Pulls the Cardano node config via ledger state
	nodeConfig := s.utxorpc.config.LedgerState.CardanoNodeConfig()
	if nodeConfig == nil {
		return nil, errors.New("cardano node config is nil")
	}

	cardanoGenesis, err := buildCardanoGenesis(nodeConfig)
	if err != nil {
		return nil, err
	}

	resp := &query.ReadGenesisResponse{
		Config: &query.ReadGenesisResponse_Cardano{
			Cardano: cardanoGenesis,
		},
		Caip2: caip2FromNetworkMagic(cardanoGenesis.GetNetworkMagic()),
	}

	// Decode the hex if shelley genesis hash is configured
	if nodeConfig.ShelleyGenesisHash != "" {
		hashBytes, err := hex.DecodeString(nodeConfig.ShelleyGenesisHash)
		if err != nil {
			return nil, fmt.Errorf("decode Shelley genesis hash: %w", err)
		}
		resp.Genesis = hashBytes
	}

	return connect.NewResponse(resp), nil
}

func caip2FromNetworkMagic(networkMagic uint32) string {
	network, ok := ouroboros.NetworkByNetworkMagic(networkMagic)
	if !ok {
		return fmt.Sprintf("cardano:%d", networkMagic)
	}

	return "cardano:" + network.String()
}

func buildCardanoGenesis(
	nodeConfig *cardano.CardanoNodeConfig,
) (*utxorpcCardano.Genesis, error) {
	if nodeConfig == nil {
		return nil, errors.New("cardano node config is nil")
	}

	// Builds a utxorpc.cardano.Genesis using shelley genesis
	shelleyGenesis := nodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return nil, errors.New("shelley genesis config is nil")
	}

	// Converts active slots coefficient, pparams into rational number
	activeSlotsCoeffRat := shelleyGenesis.ActiveSlotsCoeff.Rat
	if activeSlotsCoeffRat == nil {
		return nil, errors.New("active slots coeff is nil")
	}
	activeSlotsCoeff, err := rationalToUtxorpc(
		activeSlotsCoeffRat,
		"active slots coeff",
	)
	if err != nil {
		return nil, err
	}

	slotLengthRat := shelleyGenesis.SlotLength.Rat
	if slotLengthRat == nil {
		return nil, errors.New("slot length is nil")
	}
	if slotLengthRat.Sign() < 0 {
		return nil, errors.New("slot length cannot be negative")
	}
	if slotLengthRat.Denom().Sign() == 0 {
		return nil, errors.New("slot length denominator cannot be zero")
	}
	slotMillis := new(big.Int).Mul(
		slotLengthRat.Num(),
		big.NewInt(1000),
	)
	slotMillis.Div(slotMillis, slotLengthRat.Denom())
	if !slotMillis.IsUint64() || slotMillis.Uint64() > math.MaxUint32 {
		return nil, fmt.Errorf("slot length out of range: %s", slotMillis)
	}
	slotLength := uint32(
		slotMillis.Uint64(),
	) // #nosec G115 -- bounds checked above
	pparams, err := shelleyGenesisPParams(
		shelleyGenesis.ProtocolParameters,
	)
	if err != nil {
		return nil, fmt.Errorf("shelley protocol params: %w", err)
	}

	epochLength, err := uint32FromInt(
		shelleyGenesis.EpochLength,
		"epoch length",
	)
	if err != nil {
		return nil, err
	}
	maxKesEvolutions, err := uint32FromInt(
		shelleyGenesis.MaxKESEvolutions,
		"max KES evolutions",
	)
	if err != nil {
		return nil, err
	}
	securityParam, err := uint32FromInt(
		shelleyGenesis.SecurityParam,
		"security param",
	)
	if err != nil {
		return nil, err
	}
	slotsPerKesPeriod, err := uint32FromInt(
		shelleyGenesis.SlotsPerKESPeriod,
		"slots per KES period",
	)
	if err != nil {
		return nil, err
	}
	updateQuorum, err := uint32FromInt(
		shelleyGenesis.UpdateQuorum,
		"update quorum",
	)
	if err != nil {
		return nil, err
	}

	ret := &utxorpcCardano.Genesis{
		ActiveSlotsCoeff: activeSlotsCoeff,
		EpochLength:      epochLength,
		MaxKesEvolutions: maxKesEvolutions,
		MaxLovelaceSupply: lcommon.ToUtxorpcBigInt(
			shelleyGenesis.MaxLovelaceSupply,
		),
		NetworkId:         shelleyGenesis.NetworkId,
		NetworkMagic:      shelleyGenesis.NetworkMagic,
		ProtocolParams:    pparams,
		SecurityParam:     securityParam,
		SlotLength:        slotLength,
		SlotsPerKesPeriod: slotsPerKesPeriod,
		SystemStart: shelleyGenesis.SystemStart.UTC().Format(
			time.RFC3339Nano,
		),
		UpdateQuorum: updateQuorum,
	}

	if len(shelleyGenesis.GenDelegs) > 0 {
		ret.GenDelegs = make(
			map[string]*utxorpcCardano.GenDelegs,
			len(shelleyGenesis.GenDelegs),
		)
		for k, v := range shelleyGenesis.GenDelegs {
			ret.GenDelegs[k] = &utxorpcCardano.GenDelegs{
				Delegate: v["delegate"],
				Vrf:      v["vrf"],
			}
		}
	}
	if len(shelleyGenesis.InitialFunds) > 0 {
		ret.InitialFunds = make(
			map[string]*utxorpcCardano.BigInt,
			len(shelleyGenesis.InitialFunds),
		)
		for k, v := range shelleyGenesis.InitialFunds {
			ret.InitialFunds[k] = lcommon.ToUtxorpcBigInt(v)
		}
	}

	return ret, nil
}

func shelleyGenesisPParams(
	params shelley.ShelleyGenesisProtocolParams,
) (*utxorpcCardano.PParams, error) {
	if params.A0 == nil || params.Rho == nil || params.Tau == nil {
		return nil, errors.New("missing Shelley genesis rational params")
	}
	poolInfluence, err := rationalToUtxorpc(params.A0.Rat, "pool influence")
	if err != nil {
		return nil, err
	}
	monetaryExpansion, err := rationalToUtxorpc(
		params.Rho.Rat,
		"monetary expansion",
	)
	if err != nil {
		return nil, err
	}
	treasuryExpansion, err := rationalToUtxorpc(
		params.Tau.Rat,
		"treasury expansion",
	)
	if err != nil {
		return nil, err
	}
	protocolMajor, err := uint32FromUint(
		params.ProtocolVersion.Major,
		"protocol major",
	)
	if err != nil {
		return nil, err
	}
	protocolMinor, err := uint32FromUint(
		params.ProtocolVersion.Minor,
		"protocol minor",
	)
	if err != nil {
		return nil, err
	}

	return &utxorpcCardano.PParams{
		MaxTxSize: uint64(params.MaxTxSize),
		MinFeeCoefficient: lcommon.ToUtxorpcBigInt(
			uint64(params.MinFeeA),
		),
		MinFeeConstant: lcommon.ToUtxorpcBigInt(
			uint64(params.MinFeeB),
		),
		MaxBlockBodySize:   uint64(params.MaxBlockBodySize),
		MaxBlockHeaderSize: uint64(params.MaxBlockHeaderSize),
		StakeKeyDeposit: lcommon.ToUtxorpcBigInt(
			uint64(params.KeyDeposit),
		),
		PoolDeposit: lcommon.ToUtxorpcBigInt(
			uint64(params.PoolDeposit),
		),
		PoolRetirementEpochBound: uint64(params.MaxEpoch),
		DesiredNumberOfPools:     uint64(params.NOpt),
		PoolInfluence:            poolInfluence,
		MonetaryExpansion:        monetaryExpansion,
		TreasuryExpansion:        treasuryExpansion,
		MinPoolCost: lcommon.ToUtxorpcBigInt(
			uint64(params.MinPoolCost),
		),
		ProtocolVersion: &utxorpcCardano.ProtocolVersion{
			Major: protocolMajor,
			Minor: protocolMinor,
		},
	}, nil
}

func rationalToUtxorpc(
	rat *big.Rat,
	label string,
) (*utxorpcCardano.RationalNumber, error) {
	if rat == nil {
		return nil, fmt.Errorf("%s is nil", label)
	}
	num := rat.Num()
	den := rat.Denom()
	if den.Sign() <= 0 {
		return nil, fmt.Errorf("%s denominator invalid", label)
	}
	if num.Cmp(big.NewInt(math.MinInt32)) < 0 ||
		num.Cmp(big.NewInt(math.MaxInt32)) > 0 {
		return nil, fmt.Errorf("%s numerator out of range: %s", label, num)
	}
	if den.BitLen() > 32 {
		return nil, fmt.Errorf("%s denominator out of range: %s", label, den)
	}
	return &utxorpcCardano.RationalNumber{
		Numerator:   int32(num.Int64()),  // #nosec G115 -- bounds checked above
		Denominator: uint32(den.Int64()), // #nosec G115 -- bounds checked above
	}, nil
}

func uint32FromInt(value int, label string) (uint32, error) {
	if value < 0 || value > math.MaxUint32 {
		return 0, fmt.Errorf("%s out of range: %d", label, value)
	}
	return uint32(value), nil // #nosec G115 -- bounds checked above
}

func uint32FromUint(value uint, label string) (uint32, error) {
	if value > math.MaxUint32 {
		return 0, fmt.Errorf("%s out of range: %d", label, value)
	}
	return uint32(value), nil // #nosec G115 -- bounds checked above
}
