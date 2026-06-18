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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mesh

import (
	"cmp"
	"encoding/hex"
	"fmt"
	"net/http"
	"slices"
	"strconv"

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

	addr, meshErr := s.parseAccountAddress(
		req.AccountIdentifier,
	)
	if meshErr != nil {
		writeError(w, meshErr)
		return
	}

	utxos, err := s.config.LedgerState.UtxosByAddress(
		addr,
	)
	if err != nil {
		s.logger.Error(
			"failed to query UTxOs for account",
			"address",
			req.AccountIdentifier.Address,
			"error", err,
		)
		writeError(w, wrapErr(ErrInternal, err))
		return
	}

	balances := aggregateBalances(utxos)
	tip := s.config.Chain.Tip()

	writeJSON(w, http.StatusOK, &AccountBalanceResponse{
		BlockIdentifier: s.tipBlockID(tip),
		Balances:        balances,
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

	utxos, err := s.config.LedgerState.UtxosByAddress(
		addr,
	)
	if err != nil {
		s.logger.Error(
			"failed to query UTxOs for account",
			"address",
			req.AccountIdentifier.Address,
			"error", err,
		)
		writeError(w, wrapErr(ErrInternal, err))
		return
	}

	coins := utxosToCoins(utxos)
	tip := s.config.Chain.Tip()

	writeJSON(w, http.StatusOK, &AccountCoinsResponse{
		BlockIdentifier: s.tipBlockID(tip),
		Coins:           coins,
	})
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
