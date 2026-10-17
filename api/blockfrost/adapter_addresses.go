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
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// lovelaceDecimals is the number of decimal places Blockfrost reports for
// lovelace.
const lovelaceDecimals = 6

// addressTotalBatch bounds how many output rows one step of the address
// totals scan loads.
const addressTotalBatch = 1000

// AddressAssetUTXOs returns the paginated current UTxOs of an address that
// hold the given asset.
func (a *NodeAdapter) AddressAssetUTXOs(
	ctx context.Context,
	address string,
	policyID string,
	assetName []byte,
	params PaginationParams,
) ([]AddressUTXOInfo, int, error) {
	policy, err := hex.DecodeString(policyID)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"decode asset policy ID %q: %w",
			policyID,
			err,
		)
	}
	return a.addressUTXOs(ctx, address, params, &assetFilter{
		policyID:  policy,
		assetName: assetName,
	})
}

// AddressExtended returns the address summary with registry decimals and the
// on-chain metadata flag of every asset held.
func (a *NodeAdapter) AddressExtended(
	ctx context.Context,
	address string,
) (AddressExtendedInfo, error) {
	info, err := a.addressSummary(ctx, address)
	if err != nil {
		return AddressExtendedInfo{}, err
	}
	amounts := make([]AddressExtendedAmountInfo, 0, len(info.Amount))
	for _, amount := range info.Amount {
		extended := AddressExtendedAmountInfo{
			Unit:     amount.Unit,
			Quantity: amount.Quantity,
		}
		if amount.Unit == "lovelace" {
			decimals := lovelaceDecimals
			extended.Decimals = &decimals
			amounts = append(amounts, extended)
			continue
		}
		// Each held asset costs three point reads, so stop as soon as the
		// client is gone rather than finish a long balance.
		if err := ctx.Err(); err != nil {
			return AddressExtendedInfo{}, err
		}
		policyID, assetName, err := parseAssetIdentifier(amount.Unit)
		if err != nil {
			return AddressExtendedInfo{}, fmt.Errorf(
				"parse asset unit %q: %w",
				amount.Unit,
				err,
			)
		}
		entry, err := a.ledgerState.Database().Metadata().
			GetTokenRegistryEntry(
				tokenRegistrySubjectFor(policyID, assetName),
				nil,
			)
		if err != nil {
			return AddressExtendedInfo{}, fmt.Errorf(
				"get token registry metadata for asset %s: %w",
				amount.Unit,
				err,
			)
		}
		if entry != nil && entry.Decimals != nil {
			decimals := *entry.Decimals
			extended.Decimals = &decimals
		}
		extended.HasNFTOnchainMetadata, err = a.assetHasOnchainMetadata(
			ctx,
			policyID,
			assetName,
		)
		if err != nil {
			return AddressExtendedInfo{}, err
		}
		amounts = append(amounts, extended)
	}
	return AddressExtendedInfo{
		Address:      info.Address,
		Amount:       amounts,
		StakeAddress: info.StakeAddress,
		Type:         info.Type,
		Script:       info.Script,
	}, nil
}

// assetHasOnchainMetadata reports whether the initial mint transaction of an
// asset carries CIP-25 metadata for it. An asset with no recorded mint, as in
// a database populated by a snapshot import, has none.
func (a *NodeAdapter) assetHasOnchainMetadata(
	ctx context.Context,
	policyID string,
	assetName []byte,
) (bool, error) {
	policyBytes, err := hex.DecodeString(policyID)
	if err != nil {
		return false, fmt.Errorf("decode asset policy ID %q: %w", policyID, err)
	}
	policyHash, err := lcommon.NewBlake2b224Checked(policyBytes)
	if err != nil {
		return false, fmt.Errorf("asset policy ID %q: %w", policyID, err)
	}
	mintTxHash, _, err := a.ledgerState.Database().Metadata().
		GetAssetMintBurnInfo(policyHash, assetName, nil)
	if err != nil {
		return false, fmt.Errorf(
			"get asset mint info for %s%x: %w",
			policyID,
			assetName,
			err,
		)
	}
	if len(mintTxHash) == 0 {
		return false, nil
	}
	var info AssetInfo
	if err := a.populateAssetOnchainMetadata(
		ctx,
		&info,
		mintTxHash,
		policyID,
		assetName,
	); err != nil {
		return false, err
	}
	return info.OnchainMetadata != nil, nil
}

// unitSums accumulates lovelace and native asset quantities without overflow.
type unitSums struct {
	lovelace *big.Int
	assets   map[string]*big.Int
}

func newUnitSums() *unitSums {
	return &unitSums{lovelace: new(big.Int), assets: map[string]*big.Int{}}
}

func (s *unitSums) add(utxo models.Utxo) {
	s.lovelace.Add(s.lovelace, new(big.Int).SetUint64(uint64(utxo.Amount)))
	for _, asset := range utxo.Assets {
		unit := hex.EncodeToString(asset.PolicyId) +
			hex.EncodeToString(asset.Name)
		sum, ok := s.assets[unit]
		if !ok {
			sum = new(big.Int)
			s.assets[unit] = sum
		}
		sum.Add(sum, new(big.Int).SetUint64(uint64(asset.Amount)))
	}
}

// amounts lists lovelace first, then every asset in unit order, which is the
// order of the balance endpoints.
func (s *unitSums) amounts() []AddressAmountInfo {
	units := make([]string, 0, len(s.assets))
	for unit := range s.assets {
		units = append(units, unit)
	}
	slices.SortFunc(units, strings.Compare)
	ret := make([]AddressAmountInfo, 0, len(units)+1)
	ret = append(ret, AddressAmountInfo{
		Unit:     "lovelace",
		Quantity: s.lovelace.String(),
	})
	for _, unit := range units {
		ret = append(ret, AddressAmountInfo{
			Unit:     unit,
			Quantity: s.assets[unit].String(),
		})
	}
	return ret
}

// AddressTotal sums everything an address has ever received and sent. Every
// output paid to the address counts as received and every such output that
// has since been spent counts as sent, so the figures rest on the spent
// output history API storage mode retains.
func (a *NodeAdapter) AddressTotal(
	ctx context.Context,
	address string,
) (AddressTotalInfo, error) {
	addr, isPaymentCred, err := a.parseQueryAddress(address)
	if err != nil {
		return AddressTotalInfo{}, err
	}
	var pattern models.UtxoAddressPattern
	if isPaymentCred {
		// Key and script hashes share one namespace; the credential's
		// prefix says which one it names.
		paymentScript := models.PaymentScriptFromAddress(addr)
		pattern = models.UtxoAddressPattern{
			PaymentPart:   addr.PaymentKeyHash().Bytes(),
			PaymentScript: &paymentScript,
		}
	} else {
		pattern, err = models.ExactUtxoAddressPattern(addr)
		if err != nil {
			return AddressTotalInfo{}, err
		}
	}

	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()

	received := newUnitSums()
	sent := newUnitSums()
	outputs := 0
	query := &models.UtxoHistoryQuery{
		AddressPatterns: []models.UtxoAddressPattern{pattern},
		Status:          models.UtxoHistoryStatusAll,
		Limit:           addressTotalBatch,
	}
	for {
		if err := ctx.Err(); err != nil {
			return AddressTotalInfo{}, err
		}
		// The read runs in txn, which carries ctx.
		//nolint:contextcheck // UtxosWithHistory takes its context from txn
		batch, err := db.UtxosWithHistory(query, txn)
		if err != nil {
			return AddressTotalInfo{}, fmt.Errorf(
				"get output history for %q: %w",
				address,
				err,
			)
		}
		for i := range batch {
			outputs++
			received.add(batch[i].Utxo)
			if batch[i].DeletedSlot != 0 {
				sent.add(batch[i].Utxo)
			}
		}
		if len(batch) < addressTotalBatch {
			break
		}
		last := batch[len(batch)-1]
		query.After = &models.UtxoOrderingCursor{
			Slot:       last.TxSlot,
			BlockIndex: last.TxBlockIndex,
			OutputIdx:  last.OutputIdx,
			TxId:       last.TxId,
		}
	}

	var txCount int
	if isPaymentCred {
		txCount, err = db.CountTransactionsByPaymentCred(
			ctx,
			addr.PaymentKeyHash().Bytes(),
			models.PaymentScriptFromAddress(addr),
			txn,
		)
	} else {
		txCount, err = db.CountTransactionsByAddress(ctx, addr, nil, nil, txn)
	}
	if err != nil {
		return AddressTotalInfo{}, fmt.Errorf(
			"count address transactions for %q: %w",
			address,
			err,
		)
	}
	if outputs == 0 && txCount == 0 {
		return AddressTotalInfo{}, fmt.Errorf(
			"address %q: %w",
			address,
			ErrAddressNotFound,
		)
	}
	return AddressTotalInfo{
		Address:     address,
		ReceivedSum: received.amounts(),
		SentSum:     sent.amounts(),
		TxCount:     txCount,
	}, nil
}
