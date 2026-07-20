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
		txsubmission.WithInitFunc(
			o.instrumentTxsubmissionInit(o.txsubmissionServerInit),
		),
	}
}

func (o *Ouroboros) txsubmissionClientConnOpts() []txsubmission.TxSubmissionOptionFunc {
	return []txsubmission.TxSubmissionOptionFunc{
		txsubmission.WithRequestTxIdsFunc(
			o.instrumentTxsubmissionRequestTxIds(o.txsubmissionClientRequestTxIds),
		),
		txsubmission.WithRequestTxsFunc(
			o.instrumentTxsubmissionRequestTxs(o.txsubmissionClientRequestTxs),
		),
	}
}

// instrumentTxsubmissionInit wraps the Init callback. txsubmissionServerInit
// returns immediately after launching the per-peer mempool-pump goroutine.
// The metric outcome reflects only that synchronous handoff; errors from
// the long-running goroutine (rate-limit failures, peer disconnects) are
// not visible here.
func (o *Ouroboros) instrumentTxsubmissionInit(
	fn func(txsubmission.CallbackContext) error,
) func(txsubmission.CallbackContext) error {
	return func(ctx txsubmission.CallbackContext) error {
		start := time.Now()
		err := fn(ctx)
		o.recordProtocolMessage("txsubmission", err, time.Since(start))
		return err
	}
}

// instrumentTxsubmissionRequestTxIds wraps the RequestTxIds callback. When
// the peer requests blocking=true and the mempool is empty, the underlying
// callback waits inside consumer.NextTx(true) until a tx arrives, which on
// quiet networks can be seconds to minutes. Those waits land in the
// duration histogram and dominate p95/p99 — operators reading the
// histogram should treat long tails for txsubmission as expected idle
// time, not callback work.
func (o *Ouroboros) instrumentTxsubmissionRequestTxIds(
	fn func(txsubmission.CallbackContext, bool, uint16, uint16) ([]txsubmission.TxIdAndSize, error),
) func(txsubmission.CallbackContext, bool, uint16, uint16) ([]txsubmission.TxIdAndSize, error) {
	return func(
		ctx txsubmission.CallbackContext,
		blocking bool,
		ack uint16,
		req uint16,
	) ([]txsubmission.TxIdAndSize, error) {
		start := time.Now()
		ids, err := fn(ctx, blocking, ack, req)
		o.recordProtocolMessage("txsubmission", err, time.Since(start))
		return ids, err
	}
}

func (o *Ouroboros) instrumentTxsubmissionRequestTxs(
	fn func(txsubmission.CallbackContext, []txsubmission.TxId) ([]txsubmission.TxBody, error),
) func(txsubmission.CallbackContext, []txsubmission.TxId) ([]txsubmission.TxBody, error) {
	return func(
		ctx txsubmission.CallbackContext,
		txIds []txsubmission.TxId,
	) ([]txsubmission.TxBody, error) {
		start := time.Now()
		bodies, err := fn(ctx, txIds)
		o.recordProtocolMessage("txsubmission", err, time.Since(start))
		return bodies, err
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
		var consecutiveImpossibleOffers int
		backoffTimer := time.NewTimer(0)
		backoffTimer.Stop()
		defer backoffTimer.Stop()
		minHeadroomBytes := int64(1)

		for {
			if !o.Mempool.WaitForAdmissionHeadroom(
				minHeadroomBytes,
				conn.ErrorChan(),
			) {
				return
			}
			minHeadroomBytes = 1
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

				// Process this offer: keep trying until we can request
				// at least some transactions, or determine the offer is
				// impossible. This loop retries the SAME offer after
				// waiting for headroom, avoiding the protocol bug where
				// calling RequestTxIds again would acknowledge/discard
				// the current offer.
				for {
					availableBytes := o.Mempool.AdmissionHeadroomBytes()
					maxHeadroomBytes := o.Mempool.MaxAdmissionHeadroomBytes()
					if availableBytes <= 0 {
						// No headroom at all; wait for any space
						if !o.Mempool.WaitForAdmissionHeadroom(
							1,
							conn.ErrorChan(),
						) {
							return
						}
						continue
					}
					requestTxIds, nextHeadroomBytes := requestableTxIdsWithinHeadroom(
						txIds,
						availableBytes,
					)
					if len(requestTxIds) > 0 {
						// We can request at least some txs from this offer
						consecutiveImpossibleOffers = 0
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
						// Successfully processed this offer; break inner loop
						// to request the next offer
						break
					}
					// No txIds fit in available headroom
					if nextHeadroomBytes > maxHeadroomBytes {
						// Impossible offer: first tx is larger than max headroom
						consecutiveImpossibleOffers++
						o.config.Logger.Debug(
							"skipping impossible tx offer under mempool admission limits",
							"component", "network",
							"protocol", "tx-submission",
							"role", "server",
							"connection_id", ctx.ConnectionId.String(),
							"required_headroom_bytes", nextHeadroomBytes,
							"max_headroom_bytes", maxHeadroomBytes,
						)
						wait := txsubmissionBackoffDuration(
							consecutiveImpossibleOffers,
						)
						backoffTimer.Reset(wait)
						select {
						case <-backoffTimer.C:
						case <-conn.ErrorChan():
							return
						}
						// After backoff, abandon this impossible offer
						break
					}
					// Deferred offer: first tx needs more headroom than available
					consecutiveImpossibleOffers = 0
					o.config.Logger.Debug(
						"deferring tx offer until mempool headroom increases",
						"component", "network",
						"protocol", "tx-submission",
						"role", "server",
						"connection_id", ctx.ConnectionId.String(),
						"required_headroom_bytes", nextHeadroomBytes,
						"available_bytes", availableBytes,
					)
					// Wait for the required headroom, then retry this offer
					if !o.Mempool.WaitForAdmissionHeadroom(
						nextHeadroomBytes,
						conn.ErrorChan(),
					) {
						return
					}
					// Loop back to retry the same offer with new headroom
				}
			}
		}
	}()
	return nil
}

func requestableTxIdsWithinHeadroom(
	txIds []txsubmission.TxIdAndSize,
	availableBytes int64,
) ([]txsubmission.TxId, int64) {
	if availableBytes <= 0 {
		return nil, 0
	}
	// Build the longest contiguous prefix of txIds that fits within
	// availableBytes. Stop at the first tx that doesn't fit to preserve
	// order and avoid breaking dependency chains (a later tx may depend
	// on an earlier one).
	ret := make([]txsubmission.TxId, 0, len(txIds))
	var requestedBytes int64
	var nextHeadroomBytes int64
	for _, txId := range txIds {
		txSize := int64(txId.Size)
		if requestedBytes+txSize > availableBytes {
			// First tx that doesn't fit; record the headroom needed
			// for this tx and stop (don't skip to later txs).
			nextHeadroomBytes = txSize
			break
		}
		ret = append(ret, txId.TxId)
		requestedBytes += txSize
	}
	// If we successfully requested at least one tx, return 0 for
	// nextHeadroomBytes (we'll get a fresh offer next). Only return
	// non-zero nextHeadroomBytes when we couldn't request anything
	// (ret is empty), indicating the caller should wait for more headroom.
	if len(ret) > 0 {
		return ret, 0
	}
	return ret, nextHeadroomBytes
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
