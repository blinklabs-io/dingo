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
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"golang.org/x/crypto/blake2b"
)

const (
	// avgTxSize is the average transaction size in bytes,
	// used to estimate the suggested fee.
	avgTxSize = 300
)

// handleConstructionDerive derives an account identifier
// (address) from a public key.
func (s *Server) handleConstructionDerive(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req ConstructionDeriveRequest
	if meshErr := s.decodeAndValidate(
		w, r, &req,
	); meshErr != nil {
		writeError(w, meshErr)
		return
	}

	if req.PublicKey == nil {
		writeError(w, wrapErr(
			ErrInvalidPublicKey,
			errors.New("public_key is required"),
		))
		return
	}
	if req.PublicKey.CurveType != Edwards25519 {
		writeError(w, wrapErr(
			ErrInvalidPublicKey,
			fmt.Errorf(
				"unsupported curve type: %s",
				req.PublicKey.CurveType,
			),
		))
		return
	}

	pubKeyBytes, err := hex.DecodeString(
		req.PublicKey.HexBytes,
	)
	if err != nil {
		writeError(w, wrapErr(
			ErrInvalidPublicKey,
			fmt.Errorf("hex decode: %w", err),
		))
		return
	}
	if len(pubKeyBytes) != 32 {
		writeError(w, wrapErr(
			ErrInvalidPublicKey,
			fmt.Errorf(
				"expected 32 bytes, got %d",
				len(pubKeyBytes),
			),
		))
		return
	}

	keyHash := lcommon.Blake2b224Hash(pubKeyBytes)

	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		s.addrNetworkID,
		keyHash[:],
		nil,
	)
	if err != nil {
		writeError(w, wrapErr(
			ErrInternal,
			fmt.Errorf("build address: %w", err),
		))
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		&ConstructionDeriveResponse{
			AccountIdentifier: &AccountIdentifier{
				Address: addr.String(),
			},
		},
	)
}

// handleConstructionHash computes the transaction hash
// for a signed transaction.
func (s *Server) handleConstructionHash(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req ConstructionHashRequest
	if meshErr := s.decodeAndValidate(
		w, r, &req,
	); meshErr != nil {
		writeError(w, meshErr)
		return
	}

	tx, meshErr := decodeTxCbor(req.SignedTransaction)
	if meshErr != nil {
		writeError(w, meshErr)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		&ConstructionHashResponse{
			TransactionIdentifier: &TransactionIdentifier{
				Hash: tx.Hash().String(),
			},
		},
	)
}

// handleConstructionParse parses a signed or unsigned
// transaction and returns the operations within it.
func (s *Server) handleConstructionParse(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req ConstructionParseRequest
	if meshErr := s.decodeAndValidate(
		w, r, &req,
	); meshErr != nil {
		writeError(w, meshErr)
		return
	}

	if req.Signed {
		s.parseSignedTransaction(w, req.Transaction)
	} else {
		s.parseUnsignedTransaction(w, req.Transaction)
	}
}

// parseSignedTransaction decodes a full signed TX and
// returns its operations and signers.
func (s *Server) parseSignedTransaction(
	w http.ResponseWriter,
	txHex string,
) {
	tx, meshErr := decodeTxCbor(txHex)
	if meshErr != nil {
		writeError(w, meshErr)
		return
	}

	ops, signers := convertParsedTransaction(tx, true)

	writeJSON(
		w,
		http.StatusOK,
		&ConstructionParseResponse{
			Operations:               ops,
			AccountIdentifierSigners: signers,
		},
	)
}

// parseUnsignedTransaction decodes an unsigned TX body
// CBOR (as produced by /construction/payloads) and
// returns its operations. DetermineTransactionType only
// works on signed TXs (CBOR arrays), so unsigned bodies
// (CBOR maps) are decoded directly as ConwayTransactionBody.
func (s *Server) parseUnsignedTransaction(
	w http.ResponseWriter,
	bodyHex string,
) {
	bodyBytes, err := hex.DecodeString(bodyHex)
	if err != nil {
		writeError(w, wrapErr(
			ErrInvalidTransaction,
			fmt.Errorf("hex decode: %w", err),
		))
		return
	}

	var body conway.ConwayTransactionBody
	if _, err := cbor.Decode(bodyBytes, &body); err != nil {
		writeError(w, wrapErr(
			ErrInvalidTransaction,
			fmt.Errorf("decode body: %w", err),
		))
		return
	}

	// Compute the body hash for output coin IDs.
	bodyHash := blake2b.Sum256(bodyBytes)
	txHash := hex.EncodeToString(bodyHash[:])

	ops := convertBodyToOps(
		body.Inputs(), body.Outputs(),
		body.Certificates(), body.Withdrawals(),
		txHash,
	)

	writeJSON(
		w,
		http.StatusOK,
		&ConstructionParseResponse{
			Operations: ops,
		},
	)
}

// handleConstructionPreprocess is called prior to
// /construction/payloads to construct a request for any
// metadata needed for transaction construction.
func (s *Server) handleConstructionPreprocess(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req ConstructionPreprocessRequest
	if meshErr := s.decodeAndValidate(
		w, r, &req,
	); meshErr != nil {
		writeError(w, meshErr)
		return
	}

	var inputRefs []string
	var requiredKeys []*AccountIdentifier
	for _, op := range req.Operations {
		if op.Type != OpInput {
			continue
		}
		if op.CoinChange != nil &&
			op.CoinChange.CoinIdentifier != nil {
			inputRefs = append(
				inputRefs,
				op.CoinChange.CoinIdentifier.Identifier,
			)
		}
		if op.Account != nil &&
			op.Account.Address != "" {
			requiredKeys = append(
				requiredKeys,
				&AccountIdentifier{
					Address: op.Account.Address,
				},
			)
		}
	}

	writeJSON(
		w,
		http.StatusOK,
		&ConstructionPreprocessResponse{
			Options: map[string]any{
				"input_refs": inputRefs,
			},
			RequiredPublicKeys: requiredKeys,
		},
	)
}

// handleConstructionMetadata returns metadata required
// for transaction construction.
func (s *Server) handleConstructionMetadata(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req ConstructionMetadataRequest
	if meshErr := s.decodeAndValidate(
		w, r, &req,
	); meshErr != nil {
		writeError(w, meshErr)
		return
	}

	pparams := s.config.LedgerState.GetCurrentPParams()
	if pparams == nil {
		writeError(w, wrapErr(
			ErrUnavailable,
			errors.New(
				"protocol parameters not available",
			),
		))
		return
	}

	pp, err := pparams.Utxorpc()
	if err != nil {
		writeError(w, wrapErr(
			ErrInternal,
			fmt.Errorf(
				"convert protocol params: %w", err,
			),
		))
		return
	}
	if pp == nil {
		writeError(w, wrapErr(
			ErrUnavailable,
			errors.New(
				"protocol parameters conversion "+
					"returned nil",
			),
		))
		return
	}

	var coefficient, constant int64
	if c := pp.GetMinFeeCoefficient(); c != nil {
		coefficient = c.GetInt()
	}
	if c := pp.GetMinFeeConstant(); c != nil {
		constant = c.GetInt()
	}
	suggestedFee := coefficient*avgTxSize + constant

	writeJSON(
		w,
		http.StatusOK,
		&ConstructionMetadataResponse{
			Metadata: map[string]any{
				"min_fee_coefficient": coefficient,
				"min_fee_constant":    constant,
			},
			SuggestedFee: []*Amount{
				{
					Value: strconv.FormatInt(
						suggestedFee, 10,
					),
					Currency: adaCurrency(),
				},
			},
		},
	)
}

// handleConstructionPayloads generates an unsigned
// transaction and signing payloads from operations.
func (s *Server) handleConstructionPayloads(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req ConstructionPayloadsRequest
	if meshErr := s.decodeAndValidate(
		w, r, &req,
	); meshErr != nil {
		writeError(w, meshErr)
		return
	}

	var inputs []shelley.ShelleyTransactionInput
	type outputEntry struct {
		addr   lcommon.Address
		amount mary.MaryTransactionOutputValue
	}
	outputMap := make(map[int]outputEntry)
	var maxOutputIdx int

	for _, op := range req.Operations {
		switch op.Type {
		case OpInput:
			if op.CoinChange == nil ||
				op.CoinChange.CoinIdentifier == nil {
				writeError(w, wrapErr(
					ErrInvalidRequest,
					errors.New(
						"input op missing coin_change",
					),
				))
				return
			}
			parts := strings.SplitN(
				op.CoinChange.CoinIdentifier.Identifier,
				":", 2,
			)
			if len(parts) != 2 {
				writeError(w, wrapErr(
					ErrInvalidRequest,
					fmt.Errorf(
						"invalid coin ID: %s",
						op.CoinChange.CoinIdentifier.Identifier,
					),
				))
				return
			}
			// Validate hex and length before calling
			// NewShelleyTransactionInput, which panics
			// on invalid input.
			hashBytes, err := hex.DecodeString(
				parts[0],
			)
			if err != nil {
				writeError(w, wrapErr(
					ErrInvalidRequest,
					fmt.Errorf(
						"invalid tx hash hex: %w",
						err,
					),
				))
				return
			}
			if len(hashBytes) != 32 {
				writeError(w, wrapErr(
					ErrInvalidRequest,
					fmt.Errorf(
						"tx hash must be 32 bytes, "+
							"got %d",
						len(hashBytes),
					),
				))
				return
			}
			idx, err := strconv.Atoi(parts[1])
			if err != nil || idx < 0 {
				writeError(w, wrapErr(
					ErrInvalidRequest,
					fmt.Errorf(
						"invalid output index: %s",
						parts[1],
					),
				))
				return
			}
			inputs = append(
				inputs,
				shelley.NewShelleyTransactionInput(
					parts[0], idx,
				),
			)
		case OpOutput:
			if op.Account == nil || op.Amount == nil {
				writeError(w, wrapErr(
					ErrInvalidRequest,
					errors.New(
						"output op missing account "+
							"or amount",
					),
				))
				return
			}
			if op.OperationIdentifier == nil {
				writeError(w, wrapErr(
					ErrInvalidRequest,
					errors.New(
						"output op missing "+
							"operation_identifier",
					),
				))
				return
			}
			if op.OperationIdentifier.Index < 0 ||
				op.OperationIdentifier.Index > 10000 {
				writeError(w, wrapErr(
					ErrInvalidRequest,
					fmt.Errorf(
						"operation index out "+
							"of range: %d",
						op.OperationIdentifier.Index,
					),
				))
				return
			}
			outIdx := int(
				op.OperationIdentifier.Index,
			)
			if _, exists := outputMap[outIdx]; exists {
				writeError(w, wrapErr(
					ErrInvalidRequest,
					fmt.Errorf(
						"duplicate output index: %d",
						outIdx,
					),
				))
				return
			}
			addr, err := lcommon.NewAddress(
				op.Account.Address,
			)
			if err != nil {
				writeError(w, wrapErr(
					ErrInvalidRequest,
					fmt.Errorf(
						"invalid address: %w",
						err,
					),
				))
				return
			}
			var entry outputEntry
			entry.addr = addr
			if op.Amount.Currency == nil ||
				op.Amount.Currency.Symbol != "ADA" {
				writeError(w, wrapErr(
					ErrInvalidRequest,
					errors.New(
						"only ADA outputs "+
							"supported in "+
							"construction",
					),
				))
				return
			}
			val, err := strconv.ParseUint(
				op.Amount.Value, 10, 64,
			)
			if err != nil {
				writeError(w, wrapErr(
					ErrInvalidRequest,
					fmt.Errorf(
						"invalid amount: %w",
						err,
					),
				))
				return
			}
			entry.amount.Amount = val
			outputMap[outIdx] = entry
			if outIdx > maxOutputIdx {
				maxOutputIdx = outIdx
			}
		default:
			writeError(w, wrapErr(
				ErrInvalidRequest,
				fmt.Errorf(
					"unsupported operation type: %s",
					op.Type,
				),
			))
			return
		}
	}

	// Build outputs in insertion order by iterating
	// up to the maximum index seen.
	outputs := make(
		[]babbage.BabbageTransactionOutput, 0,
		len(outputMap),
	)
	for i := range maxOutputIdx + 1 {
		entry, ok := outputMap[i]
		if !ok {
			continue
		}
		outputs = append(
			outputs,
			babbage.BabbageTransactionOutput{
				OutputAddress: entry.addr,
				OutputAmount:  entry.amount,
			},
		)
	}

	// Fee from metadata (required for valid TX)
	feeStr, ok := req.Metadata["fee"].(string)
	if !ok || feeStr == "" {
		writeError(w, wrapErr(
			ErrInvalidRequest,
			errors.New(
				"metadata must include \"fee\"",
			),
		))
		return
	}
	fee, err := strconv.ParseUint(feeStr, 10, 64)
	if err != nil {
		writeError(w, wrapErr(
			ErrInvalidRequest,
			fmt.Errorf("invalid fee: %w", err),
		))
		return
	}

	// TTL from metadata (optional)
	var ttl uint64
	if ttlVal, ok := req.Metadata["ttl"]; ok {
		switch v := ttlVal.(type) {
		case string:
			var err error
			ttl, err = strconv.ParseUint(v, 10, 64)
			if err != nil {
				writeError(w, wrapErr(
					ErrInvalidRequest,
					fmt.Errorf(
						"invalid ttl: %w", err,
					),
				))
				return
			}
		case float64:
			if v < 0 || v != float64(uint64(v)) {
				writeError(w, wrapErr(
					ErrInvalidRequest,
					fmt.Errorf(
						"invalid ttl: %v "+
							"(must be a non-negative "+
							"integer)", v,
					),
				))
				return
			}
			ttl = uint64(v) // #nosec G115
		default:
			writeError(w, wrapErr(
				ErrInvalidRequest,
				fmt.Errorf(
					"unsupported ttl type: %T", v,
				),
			))
			return
		}
	}

	body := conway.ConwayTransactionBody{
		TxInputs: conway.NewConwayTransactionInputSet(
			inputs,
		),
		TxOutputs: outputs,
		TxFee:     fee,
		Ttl:       ttl,
	}

	bodyCbor, err := cbor.Encode(&body)
	if err != nil {
		writeError(w, wrapErr(
			ErrInternal,
			fmt.Errorf("encode body: %w", err),
		))
		return
	}

	bodyHash := blake2b.Sum256(bodyCbor)
	bodyHashHex := hex.EncodeToString(bodyHash[:])

	// Build one SigningPayload per unique signer.
	// When public keys are provided, derive the
	// address for each to set AccountIdentifier.
	var payloads []*SigningPayload
	if len(req.PublicKeys) > 0 {
		seen := make(map[string]struct{})
		for _, pk := range req.PublicKeys {
			if _, dup := seen[pk.HexBytes]; dup {
				continue
			}
			seen[pk.HexBytes] = struct{}{}
			pkBytes, pkErr := hex.DecodeString(
				pk.HexBytes,
			)
			if pkErr != nil || len(pkBytes) != 32 {
				writeError(w, wrapErr(
					ErrInvalidPublicKey,
					fmt.Errorf(
						"invalid public key: %s",
						pk.HexBytes,
					),
				))
				return
			}
			keyHash := lcommon.Blake2b224Hash(
				pkBytes,
			)
			addr, addrErr := lcommon.NewAddressFromParts(
				lcommon.AddressTypeKeyNone,
				s.addrNetworkID,
				keyHash[:], nil,
			)
			if addrErr != nil {
				writeError(w, wrapErr(
					ErrInternal, addrErr,
				))
				return
			}
			payloads = append(payloads,
				&SigningPayload{
					AccountIdentifier: &AccountIdentifier{
						Address: addr.String(),
					},
					HexBytes:      bodyHashHex,
					SignatureType: Ed25519,
				},
			)
		}
	} else {
		// No public keys — single payload without
		// AccountIdentifier.
		payloads = []*SigningPayload{
			{
				HexBytes:      bodyHashHex,
				SignatureType: Ed25519,
			},
		}
	}

	writeJSON(
		w,
		http.StatusOK,
		&ConstructionPayloadsResponse{
			UnsignedTransaction: hex.EncodeToString(
				bodyCbor,
			),
			Payloads: payloads,
		},
	)
}

// handleConstructionCombine combines an unsigned
// transaction with signatures to produce a signed
// transaction.
func (s *Server) handleConstructionCombine(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req ConstructionCombineRequest
	if meshErr := s.decodeAndValidate(
		w, r, &req,
	); meshErr != nil {
		writeError(w, meshErr)
		return
	}

	bodyBytes, err := hex.DecodeString(
		req.UnsignedTransaction,
	)
	if err != nil {
		writeError(w, wrapErr(
			ErrInvalidTransaction,
			fmt.Errorf("hex decode body: %w", err),
		))
		return
	}

	witnesses := make(
		[]lcommon.VkeyWitness, 0, len(req.Signatures),
	)
	for _, sig := range req.Signatures {
		if sig.PublicKey == nil {
			writeError(w, wrapErr(
				ErrInvalidPublicKey,
				errors.New(
					"signature missing public_key",
				),
			))
			return
		}
		vkeyBytes, err := hex.DecodeString(
			sig.PublicKey.HexBytes,
		)
		if err != nil {
			writeError(w, wrapErr(
				ErrInvalidPublicKey,
				fmt.Errorf(
					"hex decode public key: %w", err,
				),
			))
			return
		}
		if len(vkeyBytes) != 32 {
			writeError(w, wrapErr(
				ErrInvalidPublicKey,
				fmt.Errorf(
					"public key must be 32 bytes, "+
						"got %d",
					len(vkeyBytes),
				),
			))
			return
		}
		sigBytes, err := hex.DecodeString(sig.HexBytes)
		if err != nil {
			writeError(w, wrapErr(
				ErrInvalidTransaction,
				fmt.Errorf(
					"hex decode signature: %w", err,
				),
			))
			return
		}
		if len(sigBytes) != 64 {
			writeError(w, wrapErr(
				ErrInvalidTransaction,
				fmt.Errorf(
					"signature must be 64 bytes, "+
						"got %d",
					len(sigBytes),
				),
			))
			return
		}
		witnesses = append(
			witnesses,
			lcommon.VkeyWitness{
				Vkey:      vkeyBytes,
				Signature: sigBytes,
			},
		)
	}

	// Build signed TX as CBOR 4-element array:
	// [body, witness_set, is_valid, auxiliary_data]
	// Use RawMessage for body to preserve exact bytes.
	witnessMap := map[int]any{
		0: witnesses,
	}
	signedTx := []any{
		cbor.RawMessage(bodyBytes),
		witnessMap,
		true,
		nil,
	}

	signedCbor, err := cbor.Encode(signedTx)
	if err != nil {
		writeError(w, wrapErr(
			ErrInternal,
			fmt.Errorf(
				"encode signed tx: %w", err,
			),
		))
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		&ConstructionCombineResponse{
			SignedTransaction: hex.EncodeToString(
				signedCbor,
			),
		},
	)
}

// handleConstructionSubmit submits a signed transaction
// to the network via the mempool.
func (s *Server) handleConstructionSubmit(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req ConstructionSubmitRequest
	if meshErr := s.decodeAndValidate(
		w, r, &req,
	); meshErr != nil {
		writeError(w, meshErr)
		return
	}

	txBytes, err := hex.DecodeString(
		req.SignedTransaction,
	)
	if err != nil {
		writeError(w, wrapErr(
			ErrInvalidTransaction,
			fmt.Errorf("hex decode: %w", err),
		))
		return
	}

	txType, err := gledger.DetermineTransactionType(
		txBytes,
	)
	if err != nil {
		writeError(w, wrapErr(
			ErrInvalidTransaction,
			fmt.Errorf(
				"determine tx type: %w", err,
			),
		))
		return
	}

	tx, err := gledger.NewTransactionFromCbor(
		txType, txBytes,
	)
	if err != nil {
		writeError(w, wrapErr(
			ErrInvalidTransaction,
			fmt.Errorf("decode tx: %w", err),
		))
		return
	}

	if err := s.config.Mempool.AddTransaction(
		txType, txBytes,
	); err != nil {
		writeError(w, wrapErr(
			ErrSubmitFailed,
			fmt.Errorf(
				"mempool submit: %w", err,
			),
		))
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		&ConstructionSubmitResponse{
			TransactionIdentifier: &TransactionIdentifier{
				Hash: tx.Hash().String(),
			},
		},
	)
}
