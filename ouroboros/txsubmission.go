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
	txsubmissionRequestTxIdsCount        = 10              // Number of TxIds to request from peer at one time
	txsubmissionMaxConsecutiveRateLimits = 3               // Drop TxIds after this many consecutive hits
	txsubmissionMaxBackoff               = 5 * time.Second // Cap on exponential backoff wait
	txsubmissionBaseBackoff              = 150 * time.Millisecond
	txsubmissionLogEvery                 = 10 // Log every Nth rate limit hit after the 1st
)

// txsubmissionBackoffDuration returns the exponential backoff duration
// for the given number of consecutive rate limit hits, capped at max.
func txsubmissionBackoffDuration(consecutiveHits int) time.Duration {
	if consecutiveHits <= 0 {
		return txsubmissionBaseBackoff
	}
	d := txsubmissionBaseBackoff
	for i := 1; i < consecutiveHits; i++ {
		d *= 2
		if d >= txsubmissionMaxBackoff {
			return txsubmissionMaxBackoff
		}
	}
	return d
}

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
	o.Mempool.AddConsumer(connId)
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
		var consecutiveRateLimits int
		var rateLimitTotal int
		backoffTimer := time.NewTimer(0)
		backoffTimer.Stop()
		defer backoffTimer.Stop()

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
					consecutiveRateLimits++
					rateLimitTotal++

					// Log throttle: 1st hit + every Nth
					if consecutiveRateLimits == 1 ||
						rateLimitTotal%txsubmissionLogEvery == 0 {
						o.config.Logger.Warn(
							"tx submission rate limit exceeded",
							"component", "network",
							"protocol", "tx-submission",
							"role", "server",
							"connection_id", ctx.ConnectionId.String(),
							"tx_count", len(txIds),
							"consecutive_hits", consecutiveRateLimits,
							"total_hits", rateLimitTotal,
						)
					}

					// Drop after N consecutive hits — peer will
					// re-offer, goroutine parks on blocking
					// RequestTxIds (zero CPU).
					if consecutiveRateLimits > txsubmissionMaxConsecutiveRateLimits {
						o.config.Logger.Info(
							"dropping txids after sustained rate limiting",
							"component", "network",
							"protocol", "tx-submission",
							"role", "server",
							"connection_id", ctx.ConnectionId.String(),
							"dropped_count", len(txIds),
							"consecutive_hits", consecutiveRateLimits,
						)
						continue
					}

					// Exponential backoff with reused timer
					wait := txsubmissionBackoffDuration(
						consecutiveRateLimits,
					)
					backoffTimer.Reset(wait)
					select {
					case <-backoffTimer.C:
						// Re-check after backoff; if still
						// limited, loop back for next offer
						if !o.txSubmissionRateLimiter.Allow(
							ctx.ConnectionId,
							len(txIds),
						) {
							continue
						}
					case <-conn.ErrorChan():
						return
					}
				} else {
					consecutiveRateLimits = 0
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
	if consumer == nil {
		return nil, fmt.Errorf(
			"no mempool consumer for connection: %s",
			connId.String(),
		)
	}
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
			// Wait until we see a TX
			tmpTx := consumer.NextTx(true)
			if tmpTx == nil {
				break
			}
			tmpTxs = append(tmpTxs, tmpTx)
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
	if consumer == nil {
		return nil, fmt.Errorf(
			"no mempool consumer for connection: %s",
			connId.String(),
		)
	}
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
