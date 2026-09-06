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

package ouroboros

import (
	"bytes"
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
	txsubmissionMaxAdmissionRetryStreak  = 3               // Drop one offered TX after this many lost headroom races
	txsubmissionMaxBackoff               = 5 * time.Second // Cap on exponential backoff wait
	txsubmissionBaseBackoff              = 150 * time.Millisecond
	txsubmissionLogEvery                 = 10 // Log every Nth rate limit hit after the 1st
	// Give up on a peer only after this many consecutive rejected replies.
	// A single bad reply drops that reply and keeps the pull loop running.
	txsubmissionMaxConsecutiveReplyMismatches = 3
)

var (
	errTxsubmissionAdmissionRetriesExhausted = errors.New(
		"txsubmission admission retries exhausted",
	)
	errTxsubmissionAdmissionStopped = errors.New(
		"txsubmission admission wait stopped",
	)
	errTxsubmissionReplySizeMismatch = errors.New(
		"txsubmission reply size mismatch",
	)
)

// cborHeadLen returns the encoded length in bytes of a CBOR head (the
// initial byte plus any following argument bytes) whose argument is v.
func cborHeadLen(v uint64) uint64 {
	switch {
	case v <= 0x17:
		return 1
	case v <= 0xff:
		return 2
	case v <= 0xffff:
		return 3
	case v <= 0xffffffff:
		return 5
	default:
		return 9
	}
}

// txsubmissionWireSize returns the size of one MsgReplyTxs item as it
// appears on the wire, given the era ID and the length of the unwrapped
// transaction body.
//
// gouroboros decodes each item -- [eraId, #6.24(bytes)] -- and keeps only
// the tag-24 payload, so TxBody.TxBody is the inner transaction CBOR. A
// cardano-node peer advertises the spec-correct wire size in MsgReplyTxIds
// (ouroboros-consensus SupportsMempool.txWireSize, which is deliberately
// distinct from txInBlockSize), covering the whole wrapped item:
//
//	array(2) header + era ID uint  2 bytes for era IDs <= 23
//	tag 24 (0xd8 0x18)             2 bytes
//	byte-string length header      1, 2, 3, 5 or 9 bytes
//
// so the advertised value exceeds len(TxBody) by 6 bytes for a 24..255 byte
// body and by 7 bytes for a 256..65535 byte body.
func txsubmissionWireSize(eraId uint16, bodyLen int) uint64 {
	body := uint64(0)
	if bodyLen > 0 {
		body = uint64(bodyLen)
	}
	// 1 byte for the definite-length array(2) header, the era ID head, the
	// two-byte tag 24 head, then the byte string itself.
	return 1 + cborHeadLen(uint64(eraId)) + 2 + cborHeadLen(body) + body
}

type validatedTxsubmissionBody struct {
	body txsubmission.TxBody
	tx   ledger.Transaction
	// wireSizeAdvertised records that the peer advertised the wrapped wire
	// size for this body rather than the unwrapped body size.
	wireSizeAdvertised bool
}

// validateTxsubmissionReply verifies the complete reply before its first
// transaction is admitted. The advertised sizes are part of the request
// budget, so each body must have exactly one of the two sizes the peer can
// legitimately have advertised for it: the unwrapped body size, or the
// wrapped wire size that cardano-node advertises (see txsubmissionWireSize).
// Anything else is a mismatch.
func validateTxsubmissionReply(
	requested []txsubmission.TxIdAndSize,
	returned []txsubmission.TxBody,
) ([]validatedTxsubmissionBody, error) {
	if len(returned) > len(requested) {
		return nil, fmt.Errorf(
			"txsubmission reply count exceeds request: requested %d, received %d",
			len(requested),
			len(returned),
		)
	}
	ret := make([]validatedTxsubmissionBody, 0, len(returned))
	var requestedBytes uint64
	for _, requestedTx := range requested {
		requestedBytes += uint64(requestedTx.Size)
	}
	var returnedBytes uint64
	nextRequested := 0
	for i, txBody := range returned {
		tx, err := ledger.NewTransactionFromCbor(
			uint(txBody.EraId),
			txBody.TxBody,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"txsubmission reply transaction %d decode failed: %w",
				i,
				err,
			)
		}
		txHash := tx.Hash()
		matched := -1
		for requestedIdx := nextRequested; requestedIdx < len(requested); requestedIdx++ {
			if bytes.Equal(txHash[:], requested[requestedIdx].TxId.TxId[:]) {
				matched = requestedIdx
				break
			}
		}
		if matched < 0 {
			return nil, fmt.Errorf(
				"txsubmission reply hash or order mismatch at index %d: received %x",
				i,
				txHash,
			)
		}
		want := requested[matched]
		if txBody.EraId != want.TxId.EraId {
			return nil, fmt.Errorf(
				"txsubmission reply era mismatch at index %d: requested %d, received %d",
				i,
				want.TxId.EraId,
				txBody.EraId,
			)
		}
		bodySize := uint64(len(txBody.TxBody))
		wireSize := txsubmissionWireSize(txBody.EraId, len(txBody.TxBody))
		var wireSizeAdvertised bool
		switch uint64(want.Size) {
		case bodySize:
			// Peer advertised the unwrapped body size, as Dingo's own
			// client did before it was corrected to advertise the wire
			// size. Still accepted so that a mixed fleet interoperates.
		case wireSize:
			wireSizeAdvertised = true
		default:
			return nil, fmt.Errorf(
				"%w at index %d: advertised %d, body %d, wire %d, era %d",
				errTxsubmissionReplySizeMismatch,
				i,
				want.Size,
				bodySize,
				wireSize,
				txBody.EraId,
			)
		}
		// The aggregate budget is checked after the per-body size, so an
		// advertisement smaller than the body it describes is reported and
		// counted as the size mismatch it is instead of surfacing as an
		// unattributed batch-budget error. Every accepted body is no larger
		// than its own advertisement and each advertisement is consumed at
		// most once, so this is an invariant backstop rather than a check
		// a size advertisement alone can trip.
		returnedBytes += bodySize
		if returnedBytes > requestedBytes {
			return nil, fmt.Errorf(
				"txsubmission reply exceeds byte limit: requested %d, received at least %d",
				requestedBytes,
				returnedBytes,
			)
		}
		nextRequested = matched + 1
		ret = append(ret, validatedTxsubmissionBody{
			body:               txBody,
			tx:                 tx,
			wireSizeAdvertised: wireSizeAdvertised,
		})
	}
	return ret, nil
}

// recordTxsubmissionReplyOutcome records the size-advertisement outcome of
// one reply. Both outcomes are counted in reply BODIES, never in replies:
// accepted_wire_size counts the bodies whose advertisement was the wrapped
// wire size, and a reply dropped for a size mismatch contributes every body
// it carried to rejected, because the whole reply is dropped.
func (o *Ouroboros) recordTxsubmissionReplyOutcome(
	validated []validatedTxsubmissionBody,
	replyBodies int,
	err error,
) {
	if errors.Is(err, errTxsubmissionReplySizeMismatch) {
		o.recordTxsubmissionReplySize(
			txsubmissionReplySizeRejected,
			replyBodies,
		)
		return
	}
	var wireSized int
	for _, validatedTx := range validated {
		if validatedTx.wireSizeAdvertised {
			wireSized++
		}
	}
	o.recordTxsubmissionReplySize(
		txsubmissionReplySizeAcceptedWire,
		wireSized,
	)
}

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

func retryTxsubmissionAdmission(
	add func() error,
	waitForHeadroom func() bool,
	recordRetry func(int),
) error {
	for retryStreak := 0; ; {
		err := add()
		if err == nil {
			return nil
		}
		var fullErr *mempool.MempoolFullError
		if !errors.As(err, &fullErr) {
			return err
		}
		retryStreak++
		recordRetry(retryStreak)
		if retryStreak >= txsubmissionMaxAdmissionRetryStreak {
			return fmt.Errorf(
				"%w after %d capacity failures: %w",
				errTxsubmissionAdmissionRetriesExhausted,
				retryStreak,
				err,
			)
		}
		if !waitForHeadroom() {
			return errTxsubmissionAdmissionStopped
		}
	}
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
			o.instrumentTxsubmissionRequestTxIds(
				o.txsubmissionClientRequestTxIds,
			),
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
	conn := o.connManager.GetConnectionById(connId)
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
	// Register only after all required connection state has been verified. This
	// avoids leaving a stale consumer behind when startup cannot proceed.
	if consumer := o.mempool.NewConsumer(connId); consumer == nil {
		return mempool.ErrMempoolStopped
	}
	tx.Client.Init()
	return nil
}

func (o *Ouroboros) txsubmissionServerInit(
	ctx txsubmission.CallbackContext,
) error {
	// Start async loop to request transactions from the peer's mempool
	go func() {
		conn := o.connManager.GetConnectionById(ctx.ConnectionId)
		if conn == nil {
			return
		}
		var consecutiveRateLimits int
		var rateLimitTotal int
		var consecutiveImpossibleOffers int
		var consecutiveReplyMismatches int
		backoffTimer := time.NewTimer(0)
		backoffTimer.Stop()
		defer backoffTimer.Stop()

		for {
			headroom, limitAdmission := o.mempool.(mempool.AdmissionHeadroom)
			requestCount := txsubmissionRequestTxIdsCount
			if limitAdmission {
				if !headroom.WaitForAdmissionHeadroom(
					1,
					conn.ErrorChan(),
				) {
					return
				}
				// Request one ID at a time because gouroboros acknowledges every
				// ID in a reply on the next request. Requesting a larger batch
				// and fetching only the prefix that fits would silently discard
				// the unrequested remainder.
				// TODO: Restore batched requests when gouroboros can
				// acknowledge only the fetched prefix.
				requestCount = 1
			}
			done := make(chan struct{})
			var txIds []txsubmission.TxIdAndSize
			var err error
			go func() {
				defer close(done)
				txIds, err = ctx.Server.RequestTxIds(
					true,
					requestCount,
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
				requestedTxs := make(
					[]txsubmission.TxIdAndSize,
					0,
					len(txIds),
				)
				if limitAdmission {
					// The advertised size is the wrapped wire size for a
					// spec-conformant peer, so it is an upper bound on the
					// body that will be admitted. Reserving against it is
					// conservative by the few bytes of wrapper.
					if int64(txIds[0].Size) >
						headroom.MaxAdmissionHeadroomBytes() {
						consecutiveImpossibleOffers++
						o.config.Logger.Warn(
							"peer offered transaction larger than mempool admission capacity",
							"component",
							"network",
							"protocol",
							"tx-submission",
							"role",
							"server",
							"connection_id",
							ctx.ConnectionId.String(),
							"tx_size",
							txIds[0].Size,
							"max_admission_bytes",
							headroom.MaxAdmissionHeadroomBytes(),
						)
						backoffTimer.Reset(txsubmissionBackoffDuration(
							consecutiveImpossibleOffers,
						))
						select {
						case <-backoffTimer.C:
						case <-conn.ErrorChan():
							return
						}
						continue
					}
					consecutiveImpossibleOffers = 0
					if !headroom.WaitForAdmissionHeadroom(
						int64(txIds[0].Size),
						conn.ErrorChan(),
					) {
						return
					}
					requestedTxs = append(requestedTxs, txIds[0])
				} else {
					requestedTxs = append(requestedTxs, txIds...)
				}
				if len(requestedTxs) == 0 {
					continue
				}
				requestTxIds := make(
					[]txsubmission.TxId,
					0,
					len(requestedTxs),
				)
				for _, requestedTx := range requestedTxs {
					requestTxIds = append(requestTxIds, requestedTx.TxId)
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
				validatedTxs, err := validateTxsubmissionReply(
					requestedTxs,
					txs,
				)
				o.recordTxsubmissionReplyOutcome(
					validatedTxs,
					len(txs),
					err,
				)
				if err != nil {
					consecutiveReplyMismatches++
					o.config.Logger.Error(
						"rejected mismatched txsubmission reply",
						"component", "network",
						"protocol", "tx-submission",
						"role", "server",
						"connection_id", ctx.ConnectionId.String(),
						"consecutive_mismatches", consecutiveReplyMismatches,
						"error", err,
					)
					// One bad reply must not end tx ingest for the life of
					// the connection. Drop the whole reply -- a partially
					// valid batch is still not trusted -- and keep pulling,
					// giving up only on a peer that returns nothing else.
					if consecutiveReplyMismatches >= txsubmissionMaxConsecutiveReplyMismatches {
						o.config.Logger.Error(
							"stopping tx ingest after repeated mismatched txsubmission replies",
							"component", "network",
							"protocol", "tx-submission",
							"role", "server",
							"connection_id", ctx.ConnectionId.String(),
							"consecutive_mismatches", consecutiveReplyMismatches,
						)
						return
					}
					backoffTimer.Reset(
						txsubmissionBackoffDuration(
							consecutiveReplyMismatches,
						),
					)
					select {
					case <-backoffTimer.C:
					case <-conn.ErrorChan():
						return
					}
					continue
				}
				consecutiveReplyMismatches = 0
				for _, validatedTx := range validatedTxs {
					txBody := validatedTx.body
					tx := validatedTx.tx
					o.config.Logger.Debug(
						"received tx",
						"tx_hash", tx.Hash(),
						"protocol", "tx-submission",
						"role", "server",
						"connection_id", ctx.ConnectionId.String(),
					)
					// Admission headroom can be consumed by another peer
					// between the wait above and this commit. Retry only
					// capacity failures, and bound the retry streak so one
					// contested offer cannot stall this peer indefinitely.
					if limitAdmission {
						err = retryTxsubmissionAdmission(
							func() error {
								return o.mempool.AddTransaction(
									uint(txBody.EraId),
									txBody.TxBody,
								)
							},
							func() bool {
								return headroom.WaitForAdmissionHeadroom(
									int64(len(txBody.TxBody)),
									conn.ErrorChan(),
								)
							},
							o.recordTxsubmissionAdmissionRetry,
						)
					} else {
						err = o.mempool.AddTransaction(
							uint(txBody.EraId),
							txBody.TxBody,
						)
					}
					if errors.Is(
						err,
						errTxsubmissionAdmissionStopped,
					) {
						return
					}
					if errors.Is(err, mempool.ErrMempoolStopped) {
						return
					}
					if errors.Is(
						err,
						errTxsubmissionAdmissionRetriesExhausted,
					) {
						o.config.Logger.Warn(
							"dropping peer transaction after repeated mempool admission contention",
							"component",
							"network",
							"protocol",
							"tx-submission",
							"role",
							"server",
							"connection_id",
							ctx.ConnectionId.String(),
							"tx_hash",
							tx.Hash(),
							"retry_streak",
							txsubmissionMaxAdmissionRetryStreak,
							"error",
							err,
						)
						continue
					}
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
						continue
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
	consumer := o.mempool.FindConsumer(connId)
	if consumer == nil {
		return nil, fmt.Errorf(
			"no mempool consumer for connection: %s",
			connId.String(),
		)
	}
	// Forget only the acknowledged prefix of previously offered transaction
	// bodies. TxSubmission acknowledges the offered-id window in FIFO order;
	// clearing the whole cache here would also drop bodies for ids offered
	// after that prefix that the peer has not acknowledged and may still
	// request.
	if ack > 0 {
		consumer.AcknowledgeOffered(int(ack))
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
		eraId := uint16(tmpTx.Type) // #nosec G115
		// Advertise the size of the transaction as it appears on the wire,
		// matching cardano-node's txWireSize. Peers use the advertised size
		// for flow control against the bytes they will actually read off
		// the wire, which includes the tx-submission wrapper.
		txSize := txsubmissionWireSize(eraId, len(tmpTx.Cbor))
		if txSize > math.MaxUint32 {
			return nil, errors.New("tx impossibly large")
		}
		ret = append(
			ret,
			txsubmission.TxIdAndSize{
				TxId: txsubmission.TxId{
					EraId: eraId,
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
	consumer := o.mempool.FindConsumer(connId)
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
