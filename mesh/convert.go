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
	"encoding/hex"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// adaCurrency returns the ADA currency descriptor.
// 1 ADA = 1,000,000 lovelace (6 decimals).
func adaCurrency() *Currency {
	return &Currency{
		Symbol:   "ADA",
		Decimals: 6,
	}
}

// nativeAssetCurrency returns a Currency for a Cardano
// native asset identified by hex-encoded policy ID and
// asset name.
func nativeAssetCurrency(
	policyHex, nameHex string,
) *Currency {
	return &Currency{
		Symbol:   nameHex,
		Decimals: 0,
		Metadata: map[string]any{
			"policyId":  policyHex,
			"assetName": nameHex,
		},
	}
}

// convertBlock converts a database block model and its
// transactions into a Mesh Block.
func convertBlock(
	block models.Block,
	txs []models.Transaction,
	addrNetworkID uint8,
	slotToTimestamp func(uint64) int64,
) *Block {
	meshTxs := make([]*Transaction, 0, len(txs))
	for i := range txs {
		meshTxs = append(
			meshTxs,
			convertTransaction(txs[i], addrNetworkID),
		)
	}

	blockHash := hex.EncodeToString(block.Hash)

	// Per Mesh spec, genesis block's parent is itself.
	var parentID *BlockIdentifier
	if block.Number == 0 {
		parentID = &BlockIdentifier{
			Index: 0,
			Hash:  blockHash,
		}
	} else {
		parentID = &BlockIdentifier{
			// #nosec G115
			Index: int64(block.Number - 1),
			Hash: hex.EncodeToString(
				block.PrevHash,
			),
		}
	}

	return &Block{
		BlockIdentifier: &BlockIdentifier{
			Index: int64(block.Number), // #nosec G115
			Hash:  blockHash,
		},
		ParentBlockIdentifier: parentID,
		Timestamp:             slotToTimestamp(block.Slot),
		Transactions:          meshTxs,
	}
}

// convertTransaction converts a database transaction
// into a Mesh Transaction with input/output operations.
func convertTransaction(
	tx models.Transaction,
	addrNetworkID uint8,
) *Transaction {
	txHash := hex.EncodeToString(tx.Hash)
	status := txStatus(tx.Valid)

	ops := make(
		[]*Operation,
		0,
		len(tx.Inputs)+len(tx.Outputs),
	)
	var opIdx int64

	// Track only the primary ADA operation index for
	// each input (not native asset sub-operations).
	// Per Mesh spec, outputs reference the spent coin
	// operations, not individual asset movements.
	inputOpIDs := make(
		[]*OperationIdentifier, 0, len(tx.Inputs),
	)

	for i := range tx.Inputs {
		input := &tx.Inputs[i]
		coinID := fmt.Sprintf(
			"%s:%d",
			hex.EncodeToString(input.TxId),
			input.OutputIdx,
		)
		inputOpIDs = append(inputOpIDs,
			&OperationIdentifier{Index: opIdx},
		)
		opIdx = appendUtxoOps(
			&ops, opIdx, status,
			utxoAddress(input, addrNetworkID),
			coinID, uint64(input.Amount),
			input.Assets,
			OpInput, CoinSpent, true, nil,
		)
	}

	for i := range tx.Outputs {
		output := &tx.Outputs[i]
		coinID := fmt.Sprintf(
			"%s:%d", txHash, output.OutputIdx,
		)
		opIdx = appendUtxoOps(
			&ops, opIdx, status,
			utxoAddress(output, addrNetworkID),
			coinID, uint64(output.Amount),
			output.Assets,
			OpOutput, CoinCreated, false,
			inputOpIDs,
		)
	}

	return &Transaction{
		TransactionIdentifier: &TransactionIdentifier{
			Hash: txHash,
		},
		Operations: ops,
	}
}

// txStatus returns the operation status string pointer
// for a transaction's validity.
func txStatus(valid bool) *string {
	if valid {
		s := StatusSuccess
		return &s
	}
	s := StatusInvalid
	return &s
}

// utxoAddress reconstructs a bech32 Cardano address from
// the payment and staking key hashes stored in the
// metadata DB. The address type depends on which keys
// are present:
//
//   - PaymentKey + StakingKey → AddressTypeKeyKey
//   - PaymentKey only        → AddressTypeKeyNone
//   - Neither (Byron-era)    → "byron:<txId hex>"
//
// The networkID determines the address prefix
// (addr / addr_test1).
func utxoAddress(
	utxo *models.Utxo,
	networkID uint8,
) string {
	if len(utxo.PaymentKey) == 0 {
		// Byron-era or unresolvable — return a
		// distinguishable placeholder so the address
		// field is never empty (Mesh spec requires
		// it).
		return "byron:" +
			hex.EncodeToString(utxo.TxId)
	}

	var addrType uint8
	var stakingKey []byte
	if len(utxo.StakingKey) > 0 {
		addrType = lcommon.AddressTypeKeyKey
		stakingKey = utxo.StakingKey
	} else {
		addrType = lcommon.AddressTypeKeyNone
	}

	addr, err := lcommon.NewAddressFromParts(
		addrType, networkID,
		utxo.PaymentKey, stakingKey,
	)
	if err != nil {
		// Fallback: should not happen with valid
		// 28-byte key hashes, but don't panic.
		return hex.EncodeToString(utxo.PaymentKey)
	}
	return addr.String()
}

// appendUtxoOps appends an ADA operation and any native
// asset operations for a single UTxO to the ops slice.
// When negate is true, the ADA value is prefixed with "-"
// (for inputs). relatedOps, if non-nil, is attached to
// each operation (used for outputs to reference inputs).
// Returns the next operation index.
func appendUtxoOps(
	ops *[]*Operation,
	opIdx int64,
	status *string,
	address string,
	coinID string,
	lovelace uint64,
	assets []models.Asset,
	opType string,
	coinAction string,
	negate bool,
	relatedOps []*OperationIdentifier,
) int64 {
	acct := &AccountIdentifier{Address: address}
	coinChange := &CoinChange{
		CoinIdentifier: &CoinIdentifier{
			Identifier: coinID,
		},
		CoinAction: coinAction,
	}

	value := strconv.FormatUint(lovelace, 10)
	if negate {
		value = "-" + value
	}

	*ops = append(*ops, &Operation{
		OperationIdentifier: &OperationIdentifier{
			Index: opIdx,
		},
		RelatedOperations: relatedOps,
		Type:              opType,
		Status:            status,
		Account:           acct,
		Amount: &Amount{
			Value:    value,
			Currency: adaCurrency(),
		},
		CoinChange: coinChange,
	})
	opIdx++

	for i := range assets {
		asset := &assets[i]
		assetValue := strconv.FormatUint(
			uint64(asset.Amount), 10,
		)
		if negate {
			assetValue = "-" + assetValue
		}
		policyHex := hex.EncodeToString(
			asset.PolicyId,
		)
		nameHex := hex.EncodeToString(
			asset.Name,
		)
		assetCoinID := fmt.Sprintf(
			"%s:%s:%s",
			coinID, policyHex, nameHex,
		)
		*ops = append(*ops, &Operation{
			OperationIdentifier: &OperationIdentifier{
				Index: opIdx,
			},
			RelatedOperations: relatedOps,
			Type:              opType,
			Status:            status,
			Account:           acct,
			Amount: &Amount{
				Value: assetValue,
				Currency: nativeAssetCurrency(
					policyHex,
					nameHex,
				),
			},
			CoinChange: &CoinChange{
				CoinIdentifier: &CoinIdentifier{
					Identifier: assetCoinID,
				},
				CoinAction: coinAction,
			},
		})
		opIdx++
	}

	return opIdx
}

// convertAmount converts a lovelace value to a Mesh
// Amount with ADA currency.
func convertAmount(lovelace uint64) *Amount {
	return &Amount{
		Value:    strconv.FormatUint(lovelace, 10),
		Currency: adaCurrency(),
	}
}

// convertAssetAmount converts a native asset to a Mesh
// Amount with currency metadata.
func convertAssetAmount(
	policyID []byte,
	assetName []byte,
	amount uint64,
) *Amount {
	return &Amount{
		Value: strconv.FormatUint(amount, 10),
		Currency: nativeAssetCurrency(
			hex.EncodeToString(policyID),
			hex.EncodeToString(assetName),
		),
	}
}

// decodeTxCbor hex-decodes a transaction string and
// parses it into a gouroboros Transaction. Shared by
// hash, parse, and combine endpoints.
func decodeTxCbor(
	hexStr string,
) (gledger.Transaction, *Error) {
	txBytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, wrapErr(
			ErrInvalidTransaction,
			fmt.Errorf("hex decode: %w", err),
		)
	}
	txType, err := gledger.DetermineTransactionType(
		txBytes,
	)
	if err != nil {
		return nil, wrapErr(
			ErrInvalidTransaction,
			fmt.Errorf(
				"determine tx type: %w", err,
			),
		)
	}
	tx, err := gledger.NewTransactionFromCbor(
		txType, txBytes,
	)
	if err != nil {
		return nil, wrapErr(
			ErrInvalidTransaction,
			fmt.Errorf("decode tx: %w", err),
		)
	}
	return tx, nil
}

// convertParsedTransaction converts a gouroboros
// Transaction to Mesh operations and signer accounts.
// When signed is true, VKey witnesses are hashed to
// produce signer identifiers.
func convertParsedTransaction(
	tx gledger.Transaction,
	signed bool,
) ([]*Operation, []*AccountIdentifier) {
	txHash := tx.Hash().String()
	ops := convertBodyToOps(
		tx.Inputs(), tx.Outputs(),
		tx.Certificates(), tx.Withdrawals(),
		txHash,
	)

	// Signers from VKey witnesses
	var signers []*AccountIdentifier
	if signed {
		witnesses := tx.Witnesses()
		for _, vkw := range witnesses.Vkey() {
			keyHash := lcommon.Blake2b224Hash(
				vkw.Vkey,
			)
			signers = append(
				signers,
				&AccountIdentifier{
					Address: hex.EncodeToString(
						keyHash[:],
					),
				},
			)
		}
	}

	return ops, signers
}

// convertBodyToOps builds Mesh operations from
// transaction body components. Used by both signed TX
// parsing and unsigned body parsing. The txHash is used
// to construct output coin identifiers; pass empty
// string if unknown.
func convertBodyToOps(
	inputs []gledger.TransactionInput,
	outputs []gledger.TransactionOutput,
	certs []gledger.Certificate,
	withdrawals map[*lcommon.Address]*big.Int,
	txHash string,
) []*Operation {
	var ops []*Operation
	var opIdx int64

	// Inputs — no Account since we don't have UTxO
	// lookup in the construction flow (standard for
	// Cardano Mesh implementations).
	for _, input := range inputs {
		coinID := fmt.Sprintf(
			"%s:%d",
			input.Id().String(),
			input.Index(),
		)
		ops = append(ops, &Operation{
			OperationIdentifier: &OperationIdentifier{
				Index: opIdx,
			},
			Type: OpInput,
			CoinChange: &CoinChange{
				CoinIdentifier: &CoinIdentifier{
					Identifier: coinID,
				},
				CoinAction: CoinSpent,
			},
		})
		opIdx++
	}

	// Outputs
	for i, output := range outputs {
		addr := output.Address().String()
		amount := output.Amount()
		coinID := fmt.Sprintf("%s:%d", txHash, i)

		ops = append(ops, &Operation{
			OperationIdentifier: &OperationIdentifier{
				Index: opIdx,
			},
			Type: OpOutput,
			Account: &AccountIdentifier{
				Address: addr,
			},
			Amount: &Amount{
				Value:    amount.String(),
				Currency: adaCurrency(),
			},
			CoinChange: &CoinChange{
				CoinIdentifier: &CoinIdentifier{
					Identifier: coinID,
				},
				CoinAction: CoinCreated,
			},
		})
		opIdx++

		// Native assets
		assets := output.Assets()
		if assets != nil {
			for _, policyID := range assets.Policies() {
				for _, name := range assets.Assets(
					policyID,
				) {
					val := assets.Asset(policyID, name)
					pHex := hex.EncodeToString(
						policyID[:],
					)
					nHex := hex.EncodeToString(name)
					assetCoinID := fmt.Sprintf(
						"%s:%s:%s",
						coinID, pHex, nHex,
					)
					ops = append(ops, &Operation{
						OperationIdentifier: &OperationIdentifier{
							Index: opIdx,
						},
						Type: OpOutput,
						Account: &AccountIdentifier{
							Address: addr,
						},
						Amount: &Amount{
							Value: val.String(),
							Currency: nativeAssetCurrency(
								pHex, nHex,
							),
						},
						CoinChange: &CoinChange{
							CoinIdentifier: &CoinIdentifier{
								Identifier: assetCoinID,
							},
							CoinAction: CoinCreated,
						},
					})
					opIdx++
				}
			}
		}
	}

	// Certificates
	for _, cert := range certs {
		opType := certToOpType(cert)
		if opType == "" {
			continue
		}
		ops = append(ops, &Operation{
			OperationIdentifier: &OperationIdentifier{
				Index: opIdx,
			},
			Type: opType,
		})
		opIdx++
	}

	// Withdrawals — sort by address for deterministic
	// operation indices.
	type withdrawal struct {
		addr   string
		amount *big.Int
	}
	wdrls := make([]withdrawal, 0, len(withdrawals))
	for addr, amount := range withdrawals {
		wdrls = append(wdrls, withdrawal{
			addr:   addr.String(),
			amount: amount,
		})
	}
	slices.SortFunc(wdrls, func(a, b withdrawal) int {
		return strings.Compare(a.addr, b.addr)
	})
	for _, w := range wdrls {
		ops = append(ops, &Operation{
			OperationIdentifier: &OperationIdentifier{
				Index: opIdx,
			},
			Type: OpWithdrawal,
			Account: &AccountIdentifier{
				Address: w.addr,
			},
			Amount: &Amount{
				Value:    w.amount.String(),
				Currency: adaCurrency(),
			},
		})
		opIdx++
	}

	return ops
}

// certToOpType maps a gouroboros Certificate type to a
// Mesh operation type string.
func certToOpType(
	cert gledger.Certificate,
) string {
	switch lcommon.CertificateType(cert.Type()) {
	case lcommon.CertificateTypeStakeRegistration,
		lcommon.CertificateTypeRegistration:
		return OpStakeKeyRegistration
	case lcommon.CertificateTypeStakeDeregistration,
		lcommon.CertificateTypeDeregistration:
		return OpStakeKeyDeregistration
	case lcommon.CertificateTypeStakeDelegation:
		return OpStakeDelegation
	case lcommon.CertificateTypePoolRegistration:
		return OpPoolRegistration
	case lcommon.CertificateTypePoolRetirement:
		return OpPoolRetirement
	case lcommon.CertificateTypeVoteDelegation:
		return OpVoteDRepDelegation
	case lcommon.CertificateTypeGenesisKeyDelegation,
		lcommon.CertificateTypeMoveInstantaneousRewards,
		lcommon.CertificateTypeStakeVoteDelegation,
		lcommon.CertificateTypeStakeRegistrationDelegation,
		lcommon.CertificateTypeVoteRegistrationDelegation,
		lcommon.CertificateTypeStakeVoteRegistrationDelegation,
		lcommon.CertificateTypeAuthCommitteeHot,
		lcommon.CertificateTypeResignCommitteeCold,
		lcommon.CertificateTypeRegistrationDrep,
		lcommon.CertificateTypeDeregistrationDrep,
		lcommon.CertificateTypeUpdateDrep,
		lcommon.CertificateTypeLeiosEb:
		return ""
	}
	return ""
}
