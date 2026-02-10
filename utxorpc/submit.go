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
	tmpRedeemers := make([]*cardano.Redeemer, 0, len(redeemerExUnits))
	for key, val := range redeemerExUnits {
		tmpRedeemers = append(
			tmpRedeemers,
			&cardano.Redeemer{
				Purpose: cardano.RedeemerPurpose(key.Tag),
				Index:   key.Index,
				ExUnits: &cardano.ExUnits{
					Steps:  uint64(val.Steps),  // nolint:gosec
					Memory: uint64(val.Memory), // nolint:gosec
				},
				// TODO: Payload
			},
		)
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
				s.utxorpc.matchesTxPattern(
					tx,
					predicate.GetMatch().GetCardano(),
				)
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

// matchesTxPattern checks whether a transaction matches the given
// TxPattern (address and asset filters). This is shared between
// WatchMempool (submit) and WatchTx (watch) endpoints.
func (u *Utxorpc) matchesTxPattern(
	tx gledger.Transaction,
	pattern *cardano.TxPattern,
) bool {
	addressPattern := pattern.GetHasAddress()
	mintAssetPattern := pattern.GetMintsAsset()
	moveAssetPattern := pattern.GetMovesAsset()

	var addresses []gledger.Address
	if addressPattern != nil {
		// Handle Exact Address
		exactAddressBytes := addressPattern.GetExactAddress()
		if exactAddressBytes != nil {
			addr, err := lcommon.NewAddressFromBytes(
				exactAddressBytes,
			)
			if err != nil {
				u.config.Logger.Error(
					"failed to decode exact address",
					"error", err,
				)
				return false
			}
			addresses = append(addresses, addr)
		}

		// Handle Payment Part
		paymentPart := addressPattern.GetPaymentPart()
		if paymentPart != nil {
			paymentAddr, err := lcommon.NewAddressFromBytes(
				paymentPart,
			)
			if err != nil {
				u.config.Logger.Error(
					"failed to decode payment part",
					"error", err,
				)
				return false
			}
			addresses = append(addresses, paymentAddr)
		}

		// Handle Delegation Part
		delegationPart := addressPattern.GetDelegationPart()
		if delegationPart != nil {
			delegationAddr, err := lcommon.NewAddressFromBytes(
				delegationPart,
			)
			if err != nil {
				u.config.Logger.Error(
					"failed to decode delegation part",
					"error", err,
				)
				return false
			}
			addresses = append(addresses, delegationAddr)
		}
	}

	var assetPatterns []*cardano.AssetPattern
	if mintAssetPattern != nil {
		assetPatterns = append(assetPatterns, mintAssetPattern)
	}
	if moveAssetPattern != nil {
		assetPatterns = append(assetPatterns, moveAssetPattern)
	}

	// If no address filter specified, no match via predicate
	if len(addresses) == 0 {
		return false
	}

	// Convert everything to utxos for matching
	var utxos []gledger.TransactionOutput
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
		utxo, err := u.config.LedgerState.UtxoByRef(
			input.Id().Bytes(),
			input.Index(),
		)
		if err != nil {
			u.config.Logger.Error(
				"failed to look up input for predicate",
				"error", err,
			)
			continue
		}
		ret, err := utxo.Decode()
		if err != nil {
			u.config.Logger.Error(
				"failed to decode utxo for predicate",
				"error", err,
			)
			continue
		}
		if ret == nil {
			continue
		}
		utxos = append(utxos, ret)
	}

	// Check UTxOs for matching addresses and assets
	for _, address := range addresses {
		for _, utxo := range utxos {
			if utxo.Address().String() != address.String() {
				continue
			}
			// Address matched; check asset patterns
			if len(assetPatterns) == 0 {
				return true
			}
			for _, assetPattern := range assetPatterns {
				if assetPattern == nil {
					return true
				}
				for _, policyId := range utxo.Assets().Policies() {
					if !bytes.Equal(
						policyId.Bytes(),
						assetPattern.GetPolicyId(),
					) {
						continue
					}
					for _, asset := range utxo.Assets().Assets(
						policyId,
					) {
						if bytes.Equal(
							asset,
							assetPattern.GetAssetName(),
						) {
							return true
						}
					}
				}
			}
		}
	}
	return false
}
