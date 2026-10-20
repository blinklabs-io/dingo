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

package mesh

import (
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger"
)

// handleAccountBalance handles POST /account/balance.
func (s *Server) handleAccountBalance(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req AccountBalanceRequest
	if meshErr := s.decodeAndValidate(
		w, r, &req,
	); meshErr != nil {
		writeError(w, meshErr)
		return
	}

	// Resolve the point before touching the ledger: a balance pinned
	// to a block the node cannot resolve must fail rather than fall
	// back to another point.
	point, meshErr := s.resolveBalancePoint(r.Context(),
		req.BlockIdentifier,
	)
	if meshErr != nil {
		writeError(w, meshErr)
		return
	}

	addr, meshErr := s.parseAccountAddress(
		req.AccountIdentifier,
	)
	if meshErr != nil {
		writeError(w, meshErr)
		return
	}

	var (
		utxos []models.Utxo
		err   error
	)
	if point.historical {
		utxos, err = s.config.LedgerState.
			UtxosByAddressAtSlot(addr, point.slot)
	} else {
		utxos, err = s.config.LedgerState.UtxosByAddress(
			[]ledger.Address{addr},
		)
	}
	if err != nil {
		s.logger.Error(
			"failed to query UTxOs for account",
			"address",
			req.AccountIdentifier.Address,
			"slot", point.slot,
			"historical", point.historical,
			"error", err,
		)
		writeError(w, ErrInternal)
		return
	}

	writeJSON(w, http.StatusOK, &AccountBalanceResponse{
		BlockIdentifier: point.blockID,
		Balances:        aggregateBalances(utxos),
	})
}

// handleAccountCoins handles POST /account/coins.
func (s *Server) handleAccountCoins(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req AccountCoinsRequest
	if meshErr := s.decodeAndValidate(
		w, r, &req,
	); meshErr != nil {
		writeError(w, meshErr)
		return
	}

	addr, meshErr := s.parseAccountAddress(
		req.AccountIdentifier,
	)
	if meshErr != nil {
		writeError(w, meshErr)
		return
	}

	after, meshErr := parseCoinsCursor(req.Metadata)
	if meshErr != nil {
		writeError(w, meshErr)
		return
	}
	pattern, err := models.ExactUtxoAddressPattern(addr)
	if err != nil {
		writeError(w, wrapErr(ErrInvalidRequest, err))
		return
	}

	release, ok := s.acquireBulk()
	if !ok {
		writeError(w, ErrUnavailable)
		return
	}
	defer release()

	page, err := s.config.LedgerState.UtxosByAddressBoundedPage(
		r.Context(),
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{pattern},
			After:           after,
			Limit:           s.config.accountCoinsPageSize,
		},
		s.config.accountCoinsMaxCandidates,
		s.config.accountCoinsMaxBytes,
	)
	if err != nil {
		if r.Context().Err() != nil {
			// The client is gone; nothing is left to answer.
			s.logger.Debug(
				"account coins read abandoned",
				"error", err,
			)
			return
		}
		s.logger.Error(
			"failed to query UTxOs for account",
			"address",
			req.AccountIdentifier.Address,
			"error", err,
		)
		writeError(w, ErrInternal)
		return
	}

	utxos := make([]models.Utxo, 0, len(page.Utxos))
	for i := range page.Utxos {
		utxo := page.Utxos[i].Utxo
		// The bounded page skips the asset tables; the output CBOR it
		// retains carries the same holdings.
		if len(utxo.Assets) == 0 && len(utxo.Cbor) > 0 {
			assets, err := outputAssets(&utxo)
			if err != nil {
				s.logger.Error(
					"failed to decode UTxO assets",
					"address",
					req.AccountIdentifier.Address,
					"error", err,
				)
				writeError(w, ErrInternal)
				return
			}
			utxo.Assets = assets
		}
		utxos = append(utxos, utxo)
	}

	resp := &AccountCoinsResponse{
		BlockIdentifier: s.tipBlockID(s.config.Chain.Tip()),
		Coins:           utxosToCoins(utxos),
	}
	if page.Next != nil {
		resp.Metadata = map[string]any{
			"next_cursor": formatCoinsCursor(page.Next),
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// acquireBulk reserves one of the concurrent bulk-read slots without
// waiting. The returned function frees it.
func (s *Server) acquireBulk() (func(), bool) {
	select {
	case s.bulkSlots <- struct{}{}:
		return func() { <-s.bulkSlots }, true
	default:
		return nil, false
	}
}

// formatCoinsCursor encodes the last UTxO a page covered as an opaque
// slot:block_index:output_idx:tx_id token.
func formatCoinsCursor(c *models.UtxoOrderingCursor) string {
	return fmt.Sprintf(
		"%d:%d:%d:%x", c.Slot, c.BlockIndex, c.OutputIdx, c.TxId,
	)
}

// parseCoinsCursor reads the optional "cursor" request metadata.
func parseCoinsCursor(
	metadata map[string]any,
) (*models.UtxoOrderingCursor, *Error) {
	raw, ok := metadata["cursor"]
	if !ok || raw == nil {
		return nil, nil
	}
	token, ok := raw.(string)
	if !ok {
		return nil, wrapErr(
			ErrInvalidRequest, errors.New("cursor must be a string"),
		)
	}
	parts := strings.Split(token, ":")
	if len(parts) != 4 {
		return nil, wrapErr(
			ErrInvalidRequest, errors.New("malformed cursor"),
		)
	}
	slot, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil, wrapErr(ErrInvalidRequest, errors.New("malformed cursor"))
	}
	blockIndex, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return nil, wrapErr(ErrInvalidRequest, errors.New("malformed cursor"))
	}
	outputIdx, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return nil, wrapErr(ErrInvalidRequest, errors.New("malformed cursor"))
	}
	txID, err := hex.DecodeString(parts[3])
	if err != nil || len(txID) != 32 {
		return nil, wrapErr(ErrInvalidRequest, errors.New("malformed cursor"))
	}
	return &models.UtxoOrderingCursor{
		Slot:       slot,
		BlockIndex: uint32(blockIndex), // #nosec G115 -- ParseUint bitSize 32
		OutputIdx:  uint32(outputIdx),  // #nosec G115 -- ParseUint bitSize 32
		TxId:       txID,
	}, nil
}

// outputAssets decodes a UTxO's native assets from its output CBOR, ordered
// by policy then name so responses are deterministic.
func outputAssets(utxo *models.Utxo) ([]models.Asset, error) {
	output, err := utxo.Decode()
	if err != nil {
		return nil, fmt.Errorf("decode output: %w", err)
	}
	assets, err := models.ConvertMultiAssetToModels(output.Assets())
	if err != nil {
		return nil, err
	}
	slices.SortFunc(assets, func(a, b models.Asset) int {
		if c := bytes.Compare(a.PolicyId, b.PolicyId); c != 0 {
			return c
		}
		return bytes.Compare(a.Name, b.Name)
	})
	return assets, nil
}

// parseAccountAddress validates the account identifier
// and parses the Cardano address.
func (s *Server) parseAccountAddress(
	acct *AccountIdentifier,
) (ledger.Address, *Error) {
	if acct == nil || acct.Address == "" {
		return ledger.Address{},
			ErrInvalidRequest
	}
	addr, err := ledger.NewAddress(acct.Address)
	if err != nil {
		return ledger.Address{},
			wrapErr(ErrInvalidRequest, err)
	}
	return addr, nil
}

// balancePoint is the chain point a balance response is pinned to: the
// identifier echoed back to the client, the slot the UTxO set is read
// at, and whether that point is an earlier block rather than the tip.
type balancePoint struct {
	blockID    *BlockIdentifier
	slot       uint64
	historical bool
}

// resolveBalancePoint determines the point a balance request is pinned
// to. A nil identifier, or one carrying neither a hash nor an index, is
// treated as the current tip -- clients commonly send the field
// unconditionally. Anything else is resolved through the same block
// lookup /block uses, so an unknown or rolled-back point fails with
// block-not-found instead of silently answering from the tip.
func (s *Server) resolveBalancePoint(
	ctx context.Context,
	id *PartialBlockIdentifier,
) (balancePoint, *Error) {
	hasHash := id != nil && id.Hash != nil && *id.Hash != ""
	hasIndex := id != nil && id.Index != nil
	if !hasHash && !hasIndex {
		tip := s.config.Chain.Tip()
		return balancePoint{
			blockID: s.tipBlockID(tip),
			slot:    tip.Point.Slot,
		}, nil
	}

	block, meshErr := s.lookupBlock(ctx, id)
	if meshErr != nil {
		return balancePoint{}, meshErr
	}
	return balancePoint{
		blockID: &BlockIdentifier{
			Index: int64(block.Number), // #nosec G115
			Hash:  hex.EncodeToString(block.Hash),
		},
		slot:       block.Slot,
		historical: true,
	}, nil
}

// aggregateBalances sums ADA and native asset amounts
// across all UTxOs into Mesh Amount entries.
func aggregateBalances(utxos []models.Utxo) []*Amount {
	var totalLovelace uint64
	type assetKey struct {
		policyHex string
		nameHex   string
	}
	assetTotals := make(map[assetKey]uint64)

	for i := range utxos {
		totalLovelace += uint64(utxos[i].Amount)
		for j := range utxos[i].Assets {
			asset := &utxos[i].Assets[j]
			key := assetKey{
				policyHex: hex.EncodeToString(
					asset.PolicyId,
				),
				nameHex: hex.EncodeToString(
					asset.Name,
				),
			}
			assetTotals[key] += uint64(asset.Amount)
		}
	}

	balances := make(
		[]*Amount, 1, 1+len(assetTotals),
	)
	balances[0] = convertAmount(totalLovelace)

	// Sort asset keys for deterministic ordering.
	keys := make([]assetKey, 0, len(assetTotals))
	for k := range assetTotals {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b assetKey) int {
		if c := cmp.Compare(
			a.policyHex, b.policyHex,
		); c != 0 {
			return c
		}
		return cmp.Compare(a.nameHex, b.nameHex)
	})
	for _, key := range keys {
		balances = append(balances, &Amount{
			Value: strconv.FormatUint(
				assetTotals[key], 10,
			),
			Currency: nativeAssetCurrency(
				key.policyHex,
				key.nameHex,
			),
		})
	}
	return balances
}

// utxosToCoins converts UTxOs to Mesh Coin entries,
// including separate entries for each native asset.
func utxosToCoins(utxos []models.Utxo) []*Coin {
	coins := make([]*Coin, 0, len(utxos))
	for i := range utxos {
		utxo := &utxos[i]
		baseCoinID := fmt.Sprintf(
			"%s:%d",
			hex.EncodeToString(utxo.TxId),
			utxo.OutputIdx,
		)
		coins = append(coins, &Coin{
			CoinIdentifier: &CoinIdentifier{
				Identifier: baseCoinID,
			},
			Amount: convertAmount(
				uint64(utxo.Amount),
			),
		})
		for j := range utxo.Assets {
			asset := &utxo.Assets[j]
			policyHex := hex.EncodeToString(
				asset.PolicyId,
			)
			nameHex := hex.EncodeToString(
				asset.Name,
			)
			assetCoinID := fmt.Sprintf(
				"%s:%s:%s",
				baseCoinID, policyHex, nameHex,
			)
			coins = append(coins, &Coin{
				CoinIdentifier: &CoinIdentifier{
					Identifier: assetCoinID,
				},
				Amount: convertAssetAmount(
					asset.PolicyId,
					asset.Name,
					uint64(asset.Amount),
				),
			})
		}
	}
	return coins
}
