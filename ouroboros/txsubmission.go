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

package ouroboros

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/blinklabs-io/dingo/mempool"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/blinklabs-io/gouroboros/protocol/txsubmission"
)

const (
	txsubmissionRequestTxIdsCount = 10 // Number of TxIds to request from peer at one time

	// txsubmissionBlockingTimeout is how long we wait for a transaction
	// from the mempool before returning an empty response. This must be
	// well under gouroboros' 120-second protocol timeout to keep the
	// tx-submission state machine alive during initial sync when the
	// mempool is empty.
	txsubmissionBlockingTimeout = 90 * time.Second
)

func (o *Ouroboros) txsubmissionServerConnOpts() []txsubmission.TxSubmissionOptionFunc {
	return []txsubmission.TxSubmissionOptionFunc{
		txsubmission.WithInitFunc(o.txsubmissionServerInit),
	}
}

func (o *Ouroboros) txsubmissionClientConnOpts() []txsubmission.TxSubmissionOptionFunc {
	return []txsubmission.TxSubmissionOptionFunc{
		txsubmission.WithRequestTxIdsFunc(o.txsubmissionClientRequestTxIds),
		txsubmission.WithRequestTxsFunc(o.txsubmissionClientRequestTxs),
	}
}

func (o *Ouroboros) txsubmissionClientStart(
	connId ouroboros.ConnectionId,
) error {
	// Register mempool consumer
	// We don't bother capturing the consumer because we can easily look it up later by connection ID
	_ = o.Mempool.AddConsumer(connId)
	// Start TxSubmission loop
	conn := o.ConnManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf("failed to lookup connection ID: %s", connId.String())
	}
	tx := conn.TxSubmission()
	if tx == nil {
		return fmt.Errorf(
			"TxSubmission protocol not available on connection: %s",
			connId.String(),
		)
	}
	tx.Client.Init()
	return nil
}

func (o *Ouroboros) txsubmissionServerInit(
	ctx txsubmission.CallbackContext,
) error {
	// Start async loop to request transactions from the peer's mempool
	go func() {
		conn := o.ConnManager.GetConnectionById(ctx.ConnectionId)
		if conn == nil {
			return
		}
		for {
			done := make(chan struct{})
			var txIds []txsubmission.TxIdAndSize
			var err error
			go func() {
				defer close(done)
				txIds, err = ctx.Server.RequestTxIds(
					true,
					txsubmissionRequestTxIdsCount,
				)
			}()
			select {
			case <-done:
			case <-conn.ErrorChan():
				return
			}
			if err != nil {
				// Peer requested shutdown
				if errors.Is(err, txsubmission.ErrStopServerProcess) {
					return
				}
				// Don't log on connection close
				if errors.Is(err, protocol.ErrProtocolShuttingDown) {
					return
				}
				o.config.Logger.Error(
					fmt.Sprintf(
						"failed to get TxIds: %s",
						err,
					),
					"component", "network",
					"protocol", "tx-submission",
					"role", "server",
					"connection_id", ctx.ConnectionId.String(),
				)
				return
			}
			if len(txIds) > 0 {
				// Check per-peer rate limit before processing
				if o.txSubmissionRateLimiter != nil &&
					!o.txSubmissionRateLimiter.Allow(
						ctx.ConnectionId,
						len(txIds),
					) {
					waitDur := o.txSubmissionRateLimiter.WaitDuration(
						ctx.ConnectionId,
						len(txIds),
					)
					o.config.Logger.Warn(
						"tx submission rate limit exceeded, waiting",
						"component", "network",
						"protocol", "tx-submission",
						"role", "server",
						"connection_id", ctx.ConnectionId.String(),
						"tx_count", len(txIds),
						"wait_duration", waitDur,
					)
					// Wait for tokens to refill instead of
					// spinning in a tight loop
					select {
					case <-time.After(waitDur):
						// Consume tokens before proceeding
						o.txSubmissionRateLimiter.Allow(
							ctx.ConnectionId,
							len(txIds),
						)
					case <-conn.ErrorChan():
						return
					}
				}
				// Unwrap inner TxId from TxIdAndSize
				var requestTxIds []txsubmission.TxId
				for _, txId := range txIds {
					requestTxIds = append(requestTxIds, txId.TxId)
				}
				// Request TX content for TxIds from above
				txs, err := ctx.Server.RequestTxs(requestTxIds)
				if err != nil {
					o.config.Logger.Error(
						fmt.Sprintf(
							"failed to get Txs: %s",
							err,
						),
						"component", "network",
						"protocol", "tx-submission",
						"role", "server",
						"connection_id", ctx.ConnectionId.String(),
					)
					return
				}
				for _, txBody := range txs {
					// Decode TX from CBOR
					tx, err := ledger.NewTransactionFromCbor(
						uint(txBody.EraId),
						txBody.TxBody,
					)
					if err != nil {
						o.config.Logger.Error(
							fmt.Sprintf(
								"failed to parse transaction CBOR: %s",
								err,
							),
							"component", "network",
							"protocol", "tx-submission",
							"role", "server",
							"connection_id", ctx.ConnectionId.String(),
						)
						return
					}
					o.config.Logger.Debug(
						"received tx",
						"tx_hash", tx.Hash(),
						"protocol", "tx-submission",
						"role", "server",
						"connection_id", ctx.ConnectionId.String(),
					)
					// Add transaction to mempool
					err = o.Mempool.AddTransaction(
						uint(txBody.EraId),
						txBody.TxBody,
					)
					if err != nil {
						o.config.Logger.Error(
							fmt.Sprintf(
								"failed to add tx %x to mempool: %s",
								tx.Hash(),
								err,
							),
							"component", "network",
							"protocol", "tx-submission",
							"role", "server",
							"connection_id", ctx.ConnectionId.String(),
						)
						return
					}
				}
			}
		}
	}()
	return nil
}

func (o *Ouroboros) txsubmissionClientRequestTxIds(
	ctx txsubmission.CallbackContext,
	blocking bool,
	ack uint16,
	req uint16,
) ([]txsubmission.TxIdAndSize, error) {
	connId := ctx.ConnectionId
	ret := []txsubmission.TxIdAndSize{}
	consumer := o.Mempool.Consumer(connId)
	// Clear TX cache
	if ack > 0 {
		consumer.ClearCache()
	}
	// Get available TXs
	var tmpTxs []*mempool.MempoolTransaction
	for {
		// nolint:staticcheck
		// The linter wants to move this up into the loop condition, but it's more readable this way
		if len(tmpTxs) >= int(req) {
			break
		}
		if blocking && len(tmpTxs) == 0 {
			// Wait until we see a TX, but with a timeout to prevent
			// blocking forever when the mempool is empty (e.g. during
			// initial sync). Without this, the protocol's 120s hard
			// timeout fires and kills the peer connection.
			type nextTxResult struct {
				tx *mempool.MempoolTransaction
			}
			resultCh := make(chan nextTxResult, 1)
			// Goroutine is safe: consumer.Close() (called on
			// connection teardown) closes the done channel,
			// unblocking NextTx(true).
			go func() {
				resultCh <- nextTxResult{tx: consumer.NextTx(true)}
			}()
			select {
			case r := <-resultCh:
				if r.tx == nil {
					break
				}
				tmpTxs = append(tmpTxs, r.tx)
				continue
			case <-time.After(txsubmissionBlockingTimeout):
				o.config.Logger.Info(
					"tx-submission blocking request timed out, returning empty",
					"component", "network",
					"protocol", "tx-submission",
					"role", "client",
					"connection_id", connId.String(),
					"timeout", txsubmissionBlockingTimeout.String(),
				)
				// Return empty to keep the protocol state machine alive.
				// The goroutine reading NextTx will eventually complete
				// when a transaction arrives or the mempool shuts down.
			}
			break
		} else {
			// Return immediately if no TX is available
			tmpTx := consumer.NextTx(false)
			if tmpTx == nil {
				break
			}
			tmpTxs = append(tmpTxs, tmpTx)
		}
	}
	for _, tmpTx := range tmpTxs {
		// Add to return value
		txHashBytes, err := hex.DecodeString(tmpTx.Hash)
		if err != nil {
			return nil, err
		}
		if len(txHashBytes) != 32 {
			return nil, fmt.Errorf(
				"unexpected tx hash length %d for %s",
				len(txHashBytes),
				tmpTx.Hash,
			)
		}
		var txIdArr [32]byte
		copy(txIdArr[:], txHashBytes)
		txSize := len(tmpTx.Cbor)
		if txSize > math.MaxUint32 {
			return nil, errors.New("tx impossibly large")
		}
		ret = append(
			ret,
			txsubmission.TxIdAndSize{
				TxId: txsubmission.TxId{
					EraId: uint16(tmpTx.Type), // #nosec G115
					TxId:  txIdArr,
				},
				Size: uint32(txSize),
			},
		)
	}
	return ret, nil
}

func (o *Ouroboros) txsubmissionClientRequestTxs(
	ctx txsubmission.CallbackContext,
	txIds []txsubmission.TxId,
) ([]txsubmission.TxBody, error) {
	connId := ctx.ConnectionId
	ret := []txsubmission.TxBody{}
	consumer := o.Mempool.Consumer(connId)
	for _, txId := range txIds {
		txHash := hex.EncodeToString(txId.TxId[:])
		tx := consumer.GetTxFromCache(txHash)
		if tx != nil {
			ret = append(
				ret,
				txsubmission.TxBody{
					EraId:  uint16(tx.Type), // #nosec G115
					TxBody: tx.Cbor,
				},
			)
		}
		consumer.RemoveTxFromCache(txHash)
	}
	return ret, nil
}
