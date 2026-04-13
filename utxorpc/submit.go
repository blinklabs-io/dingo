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
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sync"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mempool"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	submit "github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit/submitconnect"
)

// submitServiceServer implements the SubmitService API
type submitServiceServer struct {
	submitconnect.UnimplementedSubmitServiceHandler
	utxorpc *Utxorpc
}

// SubmitTx
func (s *submitServiceServer) SubmitTx(
	ctx context.Context,
	req *connect.Request[submit.SubmitTxRequest],
) (*connect.Response[submit.SubmitTxResponse], error) {
	txRaw := req.Msg.GetTx()

	s.utxorpc.config.Logger.Info("Got a SubmitTx request")
	resp := &submit.SubmitTxResponse{}

	txRawBytes := txRaw.GetRaw()
	txType, err := gledger.DetermineTransactionType(txRawBytes)
	if err != nil {
		return nil, fmt.Errorf("failed decoding tx: %w", err)
	}
	tx, err := gledger.NewTransactionFromCbor(txType, txRawBytes)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to decode transaction from CBOR: %w",
			err,
		)
	}
	if tx == nil {
		return nil, errors.New("decoded transaction is nil")
	}
	txHash := tx.Hash()
	// Add transaction to mempool
	err = s.utxorpc.config.Mempool.AddTransaction(txType, txRawBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to add tx to mempool: %w", err)
	}
	resp.Ref = txHash.Bytes()
	return connect.NewResponse(resp), nil
}

// WaitForTx subscribes to block events and streams confirmation responses
// for the requested transaction hashes. It blocks until all requested
// transactions are confirmed or the client disconnects.
func (s *submitServiceServer) WaitForTx(
	ctx context.Context,
	req *connect.Request[submit.WaitForTxRequest],
	stream *connect.ServerStream[submit.WaitForTxResponse],
) error {
	ref := req.Msg.GetRef() // [][]byte

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Received WaitForTx request with %d transactions",
			len(ref),
		),
	)

	if len(ref) == 0 {
		return nil
	}

	// Build a set of pending transaction hashes for O(1) lookup
	var mu sync.Mutex
	pending := make(map[string][]byte, len(ref))
	for _, r := range ref {
		pending[hex.EncodeToString(r)] = r
	}

	// Channel to signal all transactions have been confirmed
	doneCh := make(chan struct{}, 1)
	// Channel to propagate errors from the event handler
	errCh := make(chan error, 1)

	// Mutex to protect stream.Send which is not goroutine-safe.
	// The stopped flag prevents sends after the function returns.
	var streamMu sync.Mutex
	var stopped bool

	subId := s.utxorpc.config.EventBus.SubscribeFunc(
		ledger.BlockfetchEventType,
		func(evt event.Event) {
			defer func() {
				if r := recover(); r != nil {
					s.utxorpc.config.Logger.Error(
						"panic in WaitForTx event handler",
						"panic",
						r,
					)
				}
			}()
			e, ok := evt.Data.(ledger.BlockfetchEvent)
			if !ok {
				s.utxorpc.config.Logger.Warn(
					"unexpected event data type in WaitForTx",
				)
				return
			}
			// Skip non-block events (e.g. BatchDone) which have
			// no block data and cannot confirm transactions
			if e.Block == nil {
				return
			}
			for _, tx := range e.Block.Transactions() {
				txHash := tx.Hash().String()
				mu.Lock()
				refBytes, found := pending[txHash]
				if !found {
					mu.Unlock()
					continue
				}
				// Remove from pending before sending
				delete(pending, txHash)
				remaining := len(pending)
				mu.Unlock()

				// Send confirmation response
				streamMu.Lock()
				if stopped {
					streamMu.Unlock()
					return
				}
				err := stream.Send(
					&submit.WaitForTxResponse{
						Ref:   refBytes,
						Stage: submit.Stage_STAGE_CONFIRMED,
					},
				)
				streamMu.Unlock()
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				s.utxorpc.config.Logger.Debug(
					"Confirmation response sent",
					"transaction_hash", txHash,
				)
				// Signal done when all txs confirmed
				if remaining == 0 {
					select {
					case doneCh <- struct{}{}:
					default:
					}
					return
				}
			}
		},
	)
	defer s.utxorpc.config.EventBus.Unsubscribe(
		ledger.BlockfetchEventType,
		subId,
	)
	// Prevent event handler from calling stream.Send
	// after this function returns and the stream is torn
	// down. Registered after the Unsubscribe defer so
	// LIFO ordering sets stopped=true before Unsubscribe.
	defer func() {
		streamMu.Lock()
		stopped = true
		streamMu.Unlock()
	}()

	// Block until all transactions are confirmed, an error
	// occurs, or the client disconnects
	select {
	case <-doneCh:
		return nil
	case err := <-errCh:
		if ctx.Err() != nil {
			s.utxorpc.config.Logger.Warn(
				"Client disconnected",
				"error", ctx.Err(),
			)
			return ctx.Err()
		}
		return err
	case <-ctx.Done():
		s.utxorpc.config.Logger.Debug(
			"WaitForTx client disconnected",
		)
		return ctx.Err()
	}
}

// EvalTx
func (s *submitServiceServer) EvalTx(
	ctx context.Context,
	req *connect.Request[submit.EvalTxRequest],
) (*connect.Response[submit.EvalTxResponse], error) {
	s.utxorpc.config.Logger.Info("Got an EvalTx request")
	txRaw := req.Msg.GetTx()
	resp := &submit.EvalTxResponse{}

	txRawBytes := txRaw.GetRaw()
	// Decode TX
	txType, err := gledger.DetermineTransactionType(txRawBytes)
	if err != nil {
		return nil, fmt.Errorf(
			"could not parse transaction to determine type: %w",
			err,
		)
	}
	tx, err := gledger.NewTransactionFromCbor(txType, txRawBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction CBOR: %w", err)
	}
	if tx == nil {
		return nil, errors.New("decoded transaction is nil")
	}
	// Evaluate TX
	fee, totalExUnits, redeemerExUnits, err := s.utxorpc.config.LedgerState.EvaluateTx(
		tx,
	)
	// Populate response
	redeemerData := redeemerPlutusDataByKey(tx)
	tmpRedeemers := make([]*cardano.Redeemer, 0, len(redeemerExUnits))
	for key, val := range redeemerExUnits {
		r := &cardano.Redeemer{
			Purpose: cardano.RedeemerPurpose(key.Tag + 1), // gouroboros tags are 0-based, cardano tags are offset by 1
			Index:   key.Index,
			ExUnits: &cardano.ExUnits{
				Steps:  uint64(val.Steps),  // nolint:gosec
				Memory: uint64(val.Memory), // nolint:gosec
			},
		}
		if pd, ok := redeemerData[key]; ok {
			r.Payload = plutusDataToCardano(pd)
		}
		tmpRedeemers = append(tmpRedeemers, r)
	}
	var txEval *cardano.TxEval
	if err != nil {
		txEval = &cardano.TxEval{
			Errors: []*cardano.EvalError{
				{
					Msg: err.Error(),
				},
			},
		}
	} else {
		var feeBigInt *cardano.BigInt
		if fee <= math.MaxInt64 {
			feeBigInt = &cardano.BigInt{
				BigInt: &cardano.BigInt_Int{
					Int: int64(fee),
				},
			}
		} else {
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, fee)
			feeBigInt = &cardano.BigInt{
				BigInt: &cardano.BigInt_BigUInt{
					BigUInt: buf,
				},
			}
		}
		txEval = &cardano.TxEval{
			Fee: feeBigInt,
			ExUnits: &cardano.ExUnits{
				Steps:  uint64(totalExUnits.Steps),  // nolint:gosec
				Memory: uint64(totalExUnits.Memory), // nolint:gosec
			},
			Redeemers: tmpRedeemers,
		}
	}
	resp.Report = &submit.AnyChainEval{
		Chain: &submit.AnyChainEval_Cardano{
			Cardano: txEval,
		},
	}
	return connect.NewResponse(resp), nil
}

// ReadMempool
func (s *submitServiceServer) ReadMempool(
	ctx context.Context,
	req *connect.Request[submit.ReadMempoolRequest],
) (*connect.Response[submit.ReadMempoolResponse], error) {
	s.utxorpc.config.Logger.Info("Got a ReadMempool request")
	resp := &submit.ReadMempoolResponse{}

	txs := s.utxorpc.config.Mempool.Transactions()
	mempoolItems := make([]*submit.TxInMempool, 0, len(txs))
	for _, tx := range txs {
		record := &submit.TxInMempool{
			NativeBytes: tx.Cbor,
			Stage:       submit.Stage_STAGE_MEMPOOL,
		}
		mempoolItems = append(mempoolItems, record)
	}
	resp.Items = mempoolItems

	return connect.NewResponse(resp), nil
}

// WatchMempool subscribes to mempool add-transaction events and streams
// matching transactions to the client. It blocks until the client
// disconnects.
func (s *submitServiceServer) WatchMempool(
	ctx context.Context,
	req *connect.Request[submit.WatchMempoolRequest],
	stream *connect.ServerStream[submit.WatchMempoolResponse],
) error {
	predicate := req.Msg.GetPredicate() // Predicate
	fieldMask := req.Msg.GetFieldMask()

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a WatchMempool request with predicate %v and fieldMask %v",
			predicate,
			fieldMask,
		),
	)

	var predTree *txPredicateNode
	if predicate != nil {
		predTree = txPredicateFromSubmit(predicate)
	}
	// Channel to propagate errors from the event handler
	errCh := make(chan error, 1)

	// Mutex to protect stream.Send which is not goroutine-safe.
	// The stopped flag prevents sends after the function returns.
	var streamMu sync.Mutex
	var stopped bool

	// Subscribe to mempool add-transaction events
	subId := s.utxorpc.config.EventBus.SubscribeFunc(
		mempool.AddTransactionEventType,
		func(evt event.Event) {
			defer func() {
				if r := recover(); r != nil {
					s.utxorpc.config.Logger.Error(
						"panic in WatchMempool event handler",
						"panic",
						r,
					)
				}
			}()
			addEvt, ok := evt.Data.(mempool.AddTransactionEvent)
			if !ok {
				return
			}
			txRawBytes := addEvt.Body
			txType, err := gledger.DetermineTransactionType(
				txRawBytes,
			)
			if err != nil {
				s.utxorpc.config.Logger.Error(
					"failed to determine tx type",
					"error", err,
				)
				return
			}
			tx, err := gledger.NewTransactionFromCbor(
				txType,
				txRawBytes,
			)
			if err != nil {
				s.utxorpc.config.Logger.Error(
					"failed to decode tx in WatchMempool",
					"error", err,
				)
				return
			}
			if tx == nil {
				s.utxorpc.config.Logger.Error(
					"decoded transaction is nil in WatchMempool",
				)
				return
			}
			resp := &submit.WatchMempoolResponse{}
			record := &submit.TxInMempool{
				NativeBytes: txRawBytes,
				Stage:       submit.Stage_STAGE_MEMPOOL,
			}
			resp.Tx = record

			shouldSend := predicate == nil ||
				s.utxorpc.matchTxPredicateNode(tx, predTree)
			if !shouldSend {
				return
			}

			streamMu.Lock()
			if stopped {
				streamMu.Unlock()
				return
			}
			err = stream.Send(resp)
			streamMu.Unlock()
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		},
	)
	defer s.utxorpc.config.EventBus.Unsubscribe(
		mempool.AddTransactionEventType,
		subId,
	)
	// Prevent event handler from calling stream.Send
	// after this function returns and the stream is torn
	// down. Registered after the Unsubscribe defer so
	// LIFO ordering sets stopped=true before Unsubscribe.
	defer func() {
		streamMu.Lock()
		stopped = true
		streamMu.Unlock()
	}()

	// Block until client disconnects or an error occurs
	select {
	case err := <-errCh:
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	case <-ctx.Done():
		s.utxorpc.config.Logger.Debug(
			"WatchMempool client disconnected",
		)
		return ctx.Err()
	}
}

// matchesTxPattern classifies a tx against a Cardano TxPattern.
// predUnevaluable is returned when the filter cannot be applied (no usable
// address constraint, bad address bytes, etc.), so composite not() does not
// treat it as a definite non-match.
//
// Set fields are combined with AND: consumes ∧ produces ∧ has_address ∧
// mints_asset ∧ moves_asset ∧ has_certificate (each present sub-pattern must
// match).
func (u *Utxorpc) matchesTxPattern(
	tx gledger.Transaction,
	pattern *cardano.TxPattern,
) predOutcome {
	if pattern == nil {
		return predUnevaluable
	}
	var parts []predOutcome
	if p := pattern.GetConsumes(); p != nil {
		parts = append(parts, u.txPatternMatchConsumes(tx, p))
	}
	if p := pattern.GetProduces(); p != nil {
		parts = append(parts, u.txPatternMatchProduces(tx, p))
	}
	if pattern.GetHasAddress() != nil {
		parts = append(parts, u.txPatternMatchHasAddress(tx, pattern))
	}
	if p := pattern.GetMintsAsset(); p != nil {
		parts = append(parts, u.txPatternMatchAsset(tx, p))
	}
	if p := pattern.GetMovesAsset(); p != nil {
		parts = append(parts, u.txPatternMatchAsset(tx, p))
	}
	if p := pattern.GetHasCertificate(); p != nil {
		parts = append(parts, u.txPatternMatchHasCertificate(tx, p))
	}
	if len(parts) == 0 {
		return predUnevaluable
	}
	return combineANDBranches(parts)
}

func (u *Utxorpc) txPatternMatchConsumes(
	tx gledger.Transaction,
	pat *cardano.TxOutputPattern,
) predOutcome {
	return u.matchConsumesWithLookup(tx, pat, u.lookupSpentOutput)
}

func (u *Utxorpc) matchConsumesWithLookup(
	tx gledger.Transaction,
	pat *cardano.TxOutputPattern,
	lookup func(gledger.TransactionInput) (gledger.TransactionOutput, error),
) predOutcome {
	var sawUnevaluable bool
	for _, in := range tx.Consumed() {
		out, err := lookup(in)
		if err != nil {
			u.config.Logger.Error(
				"failed to look up input for consumes predicate",
				"error", err,
			)
			sawUnevaluable = true
			continue
		}
		switch u.txOutputPatternMatches(out, pat) {
		case predMatch:
			return predMatch
		case predUnevaluable:
			sawUnevaluable = true
		case predNoMatch:
		}
	}
	if sawUnevaluable {
		return predUnevaluable
	}
	return predNoMatch
}

func (u *Utxorpc) txPatternMatchProduces(
	tx gledger.Transaction,
	pat *cardano.TxOutputPattern,
) predOutcome {
	// Body outputs only; collateral return is not part of "produces" here.
	outs := tx.Outputs()
	var sawUnevaluable bool
	for _, out := range outs {
		switch u.txOutputPatternMatches(out, pat) {
		case predMatch:
			return predMatch
		case predUnevaluable:
			sawUnevaluable = true
		case predNoMatch:
		}
	}
	if sawUnevaluable {
		return predUnevaluable
	}
	return predNoMatch
}

// txOutputPatternMatches tests one ledger output against TxOutputPattern
// (optional AddressPattern + optional AssetPattern; both must hold when set).
func (u *Utxorpc) txOutputPatternMatches(
	out gledger.TransactionOutput,
	pat *cardano.TxOutputPattern,
) predOutcome {
	if pat == nil {
		return predUnevaluable
	}
	addrPat := pat.GetAddress()
	assetPat := pat.GetAsset()
	if addrPat == nil && assetPat == nil {
		return predMatch
	}
	parts := make([]predOutcome, 0, 2)
	if addrPat != nil {
		addrMatch := u.addressPatternMatchesOutput(out, addrPat)
		parts = append(parts, addrMatch)
	}
	if assetPat != nil {
		assetMatch := u.txOutputHasAssetPattern(out, assetPat)
		if assetMatch {
			parts = append(parts, predMatch)
		} else {
			parts = append(parts, predNoMatch)
		}
	}
	return combineANDBranches(parts)
}

func (u *Utxorpc) addressPatternMatchesOutput(
	out gledger.TransactionOutput,
	ap *cardano.AddressPattern,
) predOutcome {
	if ap == nil {
		return predUnevaluable
	}
	hasConstraint := ap.GetExactAddress() != nil ||
		ap.GetPaymentPart() != nil ||
		ap.GetDelegationPart() != nil
	if !hasConstraint {
		return predUnevaluable
	}
	addr := out.Address()
	sawUnevaluable := false
	if b := ap.GetExactAddress(); b != nil {
		patAddr, err := lcommon.NewAddressFromBytes(b)
		if err != nil {
			u.config.Logger.Error("failed to decode exact address", "error", err)
			sawUnevaluable = true
		} else if addr.String() != patAddr.String() {
			return predNoMatch
		}
	}
	if b := ap.GetPaymentPart(); b != nil {
		if len(b) != lcommon.AddressHashSize {
			u.config.Logger.Error(
				"invalid payment_part length",
				"length", len(b),
			)
			sawUnevaluable = true
		} else if !bytes.Equal(addr.PaymentKeyHash().Bytes(), b) {
			return predNoMatch
		}
	}
	if b := ap.GetDelegationPart(); b != nil {
		if len(b) != lcommon.AddressHashSize {
			u.config.Logger.Error(
				"invalid delegation_part length",
				"length", len(b),
			)
			sawUnevaluable = true
		} else if !bytes.Equal(addr.StakeKeyHash().Bytes(), b) {
			return predNoMatch
		}
	}
	if sawUnevaluable {
		return predUnevaluable
	}
	return predMatch
}

func (u *Utxorpc) txOutputHasAssetPattern(
	out gledger.TransactionOutput,
	ap *cardano.AssetPattern,
) bool {
	if ap == nil {
		return true
	}
	assets := out.Assets()
	if assets == nil {
		return false
	}
	for _, policyID := range assets.Policies() {
		if !bytes.Equal(policyID.Bytes(), ap.GetPolicyId()) {
			continue
		}
		for _, asset := range assets.Assets(policyID) {
			if bytes.Equal(asset, ap.GetAssetName()) {
				return true
			}
		}
	}
	return false
}

func (u *Utxorpc) lookupSpentOutput(
	in gledger.TransactionInput,
) (gledger.TransactionOutput, error) {
	if u.config.LedgerState == nil {
		return nil, errors.New("ledger state not available")
	}
	rec, err := u.config.LedgerState.UtxoByRef(
		in.Id().Bytes(),
		in.Index(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"UtxoByRef(%x, %d): %w", in.Id().Bytes(), in.Index(), err,
		)
	}
	out, err := rec.Decode()
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if out == nil {
		return nil, errors.New("decoded utxo is nil")
	}
	return out, nil
}

// collectOutputsForHasAddress returns outputs, collateral return, and UTxOs
// resolved from ordinary, reference, and collateral inputs (same set as before).
func (u *Utxorpc) collectOutputsForHasAddress(
	tx gledger.Transaction,
) ([]gledger.TransactionOutput, bool) {
	var sawUnevaluable bool
	utxos := make([]gledger.TransactionOutput, 0, len(tx.Outputs())+8)
	utxos = append(utxos, tx.Outputs()...)
	if cr := tx.CollateralReturn(); cr != nil {
		utxos = append(utxos, cr)
	}
	txInputs := tx.Inputs()
	refInputs := tx.ReferenceInputs()
	collateral := tx.Collateral()
	inputs := make(
		[]gledger.TransactionInput,
		0,
		len(txInputs)+len(refInputs)+len(collateral),
	)
	inputs = append(inputs, txInputs...)
	inputs = append(inputs, refInputs...)
	inputs = append(inputs, collateral...)
	for _, input := range inputs {
		out, err := u.lookupSpentOutput(input)
		if err != nil {
			u.config.Logger.Error(
				"failed to look up input for predicate",
				"error", err,
			)
			sawUnevaluable = true
			continue
		}
		utxos = append(utxos, out)
	}
	return utxos, sawUnevaluable
}

func (u *Utxorpc) txPatternMatchHasAddress(
	tx gledger.Transaction,
	pattern *cardano.TxPattern,
) predOutcome {
	addressPattern := pattern.GetHasAddress()
	if addressPattern == nil {
		return predUnevaluable
	}

	utxos, sawUnevaluable := u.collectOutputsForHasAddress(tx)

	for _, utxo := range utxos {
		st := u.addressPatternMatchesOutput(utxo, addressPattern)
		if st == predUnevaluable {
			sawUnevaluable = true
			continue
		}
		if st == predMatch {
			return predMatch
		}
	}
	if sawUnevaluable {
		return predUnevaluable
	}
	return predNoMatch
}

func (u *Utxorpc) txPatternMatchAsset(
	tx gledger.Transaction,
	assetPattern *cardano.AssetPattern,
) predOutcome {
	if assetPattern == nil {
		return predUnevaluable
	}
	utxos, sawUnevaluable := u.collectOutputsForHasAddress(tx)
	for _, utxo := range utxos {
		if u.txOutputHasAssetPattern(utxo, assetPattern) {
			return predMatch
		}
	}
	if sawUnevaluable {
		return predUnevaluable
	}
	return predNoMatch
}
