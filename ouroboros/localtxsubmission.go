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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/blinklabs-io/dingo/mempool"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	olocaltxsubmission "github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
)

// errLocalTxSubmissionMempoolUnavailable is returned by the LocalTxSubmission
// callback when this Ouroboros was built without a mempool. It is a fault of
// the node, not of the submitted transaction, so it carries no CBOR rejection.
var errLocalTxSubmissionMempoolUnavailable = errors.New(
	"local-tx-submission: mempool unavailable",
)

// errLocalTxSubmissionBusy is returned when every in-flight validation slot is
// held. It is a node condition, not a verdict on the transaction.
var errLocalTxSubmissionBusy = errors.New(
	"local-tx-submission: validation capacity exhausted",
)

// localTxSubmissionMaxInFlight bounds validations that are queued behind or
// running under the mempool mutation gate across all NtC connections. The gate
// serializes validation, so extra slots only add waiters; the bound exists to
// cap what a client that reconnects can park, not to add throughput. A slot is
// held until the callback returns, never until the socket closes.
const localTxSubmissionMaxInFlight = 8

func (o *Ouroboros) localtxsubmissionServerConnOpts(
	trackActivity ...bool,
) []olocaltxsubmission.LocalTxSubmissionOptionFunc {
	return []olocaltxsubmission.LocalTxSubmissionOptionFunc{
		olocaltxsubmission.WithSubmitTxFunc(
			o.instrumentLocaltxsubmissionSubmitTx(
				o.localtxsubmissionServerSubmitTx,
				activityTrackingEnabled(trackActivity),
			),
		),
	}
}

func (o *Ouroboros) instrumentLocaltxsubmissionSubmitTx(
	fn func(olocaltxsubmission.CallbackContext, olocaltxsubmission.MsgSubmitTxTransaction) error,
	trackActivity bool,
) func(olocaltxsubmission.CallbackContext, olocaltxsubmission.MsgSubmitTxTransaction) error {
	return func(
		ctx olocaltxsubmission.CallbackContext,
		tx olocaltxsubmission.MsgSubmitTxTransaction,
	) error {
		endActivity := o.beginTrustedNtCActivity(
			trackActivity,
			ctx.ConnectionId,
			olocaltxsubmission.ProtocolId,
		)
		defer endActivity()
		start := time.Now()
		err := fn(ctx, tx)
		o.recordProtocolMessage("localtxsubmission", err, time.Since(start))
		return err
	}
}

func (o *Ouroboros) localtxsubmissionServerSubmitTx(
	ctx olocaltxsubmission.CallbackContext,
	tx olocaltxsubmission.MsgSubmitTxTransaction,
) error {
	rawTx, ok := tx.Raw.Content.([]byte)
	if !ok {
		o.config.Logger.Warn(
			fmt.Sprintf(
				"received local-tx-submission payload with unexpected content type: %T",
				tx.Raw.Content,
			),
			"component", "network",
			"protocol", "local-tx-submission",
			"role", "server",
			"connection_id", ctx.ConnectionId.String(),
		)
		return errors.New(
			"local-tx-submission: unexpected transaction content type",
		)
	}
	if o.mempool == nil {
		return errLocalTxSubmissionMempoolUnavailable
	}
	// The slot is released only when AddTransaction returns, so a closed
	// connection cannot free capacity that its still-queued validation uses.
	select {
	case o.localTxSubmissionSlots <- struct{}{}:
		defer func() { <-o.localTxSubmissionSlots }()
	default:
		return localTxSubmissionRejectReason(
			tx.EraId,
			errLocalTxSubmissionBusy,
		)
	}
	// The listener starts protocols before registering the connection, so the
	// manager's teardown signal can be absent here. The protocol's stop signal
	// closes on every bearer teardown regardless of registration; its done
	// signal cannot be used, as it closes only after this callback returns.
	connDone := o.localTxSubmissionConnDone(ctx.ConnectionId)
	var protoStop <-chan struct{}
	if ctx.Server != nil && ctx.Server.Protocol != nil {
		protoStop = ctx.Server.StopChan()
	}
	valCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if connDone != nil || protoStop != nil {
		go func() {
			select {
			case <-connDone:
				cancel()
			case <-protoStop:
				cancel()
			case <-valCtx.Done():
			}
		}()
	}
	err := o.mempool.AddTransaction(valCtx, uint(tx.EraId), rawTx)
	if err != nil {
		o.config.Logger.Error(
			fmt.Sprintf(
				"failed to add transaction to mempool: %s",
				err,
			),
			"component", "network",
			"protocol", "local-tx-submission",
			"role", "server",
			"connection_id", ctx.ConnectionId.String(),
		)
		return localTxSubmissionRejectReason(tx.EraId, err)
	}
	return nil
}

type cborRejectReason interface {
	error
	MarshalCBOR() ([]byte, error)
}

type hardForkApplyTxError struct {
	era           uint16
	err           error
	inputSetEmpty bool
}

type unrepresentableTxSubmissionError struct {
	err error
}

func localTxSubmissionRejectReason(eraId uint16, err error) error {
	if isLocalTxSubmissionInfrastructureError(err) {
		return errors.New("local transaction submission unavailable")
	}
	return newLocalTxSubmissionRejectReason(eraId, err)
}

func newLocalTxSubmissionRejectReason(
	eraId uint16,
	err error,
) error {
	if err == nil {
		return errors.New("transaction rejection has no cause")
	}
	if _, ok := errors.AsType[cborRejectReason](err); ok {
		return err
	}
	var inputSetEmpty shelley.InputSetEmptyUtxoError
	var inputSetEmptyPtr *shelley.InputSetEmptyUtxoError
	isInputSetEmpty := errors.As(err, &inputSetEmpty) ||
		errors.As(err, &inputSetEmptyPtr)
	switch eraId {
	case gledger.EraIdConway, gledger.EraIdDijkstra:
		return &hardForkApplyTxError{
			era:           eraId,
			err:           err,
			inputSetEmpty: isInputSetEmpty,
		}
	default:
		if isInputSetEmpty {
			switch eraId {
			case gledger.EraIdShelley, gledger.EraIdAllegra, gledger.EraIdMary,
				gledger.EraIdAlonzo, gledger.EraIdBabbage:
				return &hardForkApplyTxError{
					era:           eraId,
					err:           err,
					inputSetEmpty: true,
				}
			}
		}
	}
	return &unrepresentableTxSubmissionError{err: err}
}

// localTxSubmissionConnDone returns the connection manager's teardown signal
// for the connection, or nil when the connection is not registered yet or no
// manager is wired.
func (o *Ouroboros) localTxSubmissionConnDone(
	connId ouroboros.ConnectionId,
) <-chan struct{} {
	if o.localTxSubmissionConnDoneFunc != nil {
		return o.localTxSubmissionConnDoneFunc(connId)
	}
	if o.connManager == nil {
		return nil
	}
	conn, done := o.connManager.GetConnectionWithDone(connId)
	if conn == nil {
		return nil
	}
	return done
}

func isLocalTxSubmissionInfrastructureError(err error) bool {
	var fullErr *mempool.MempoolFullError
	return errors.Is(err, errLocalTxSubmissionBusy) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, mempool.ErrNilValidator) ||
		errors.Is(err, mempool.ErrMempoolStopped) ||
		errors.Is(err, mempool.ErrPendingStateMoved) ||
		errors.Is(err, mempool.ErrInputResolution) ||
		errors.As(err, &fullErr)
}

func (e *hardForkApplyTxError) Error() string {
	if e.err == nil {
		return "transaction rejection has no cause"
	}
	return e.err.Error()
}

func (e *hardForkApplyTxError) Unwrap() error {
	return e.err
}

func (e *hardForkApplyTxError) MarshalCBOR() ([]byte, error) {
	var failure []any
	switch {
	case e.inputSetEmpty && e.era == gledger.EraIdDijkstra:
		failure = []any{
			dijkstraMempoolLedgerFailure,
			[]any{dijkstraLedgerUtxowFailure, e.utxowFailure()},
		}
	case e.inputSetEmpty && e.era == gledger.EraIdConway:
		failure = []any{gledger.ConwayLedgerUtxowFailure, e.utxowFailure()}
	case e.inputSetEmpty:
		failure = []any{gledger.ApplyTxErrorUtxowFailure, e.utxowFailure()}
	case e.era == gledger.EraIdDijkstra:
		failure = []any{dijkstraMempoolFailure, e.err.Error()}
	case e.era == gledger.EraIdConway:
		// ConwayApplyTxError contains ledger predicate failures directly. The
		// ConwayMempoolFailure constructor is tag 7; it is not a UTXOW failure.
		failure = []any{conwayLedgerMempoolFailure, e.err.Error()}
	default:
		return nil, errors.New(
			"transaction rejection cause is not representable as a ledger UTXOW failure",
		)
	}
	return cbor.Encode([]any{[]any{e.era, []any{failure}}})
}

func (e *unrepresentableTxSubmissionError) Error() string {
	if e.err == nil {
		return "transaction rejection has no cause"
	}
	return e.err.Error()
}

func (e *unrepresentableTxSubmissionError) Unwrap() error {
	return e.err
}

func (e *unrepresentableTxSubmissionError) MarshalCBOR() ([]byte, error) {
	return nil, fmt.Errorf(
		"transaction rejection is not representable in the local-tx-submission protocol: %w",
		e.err,
	)
}

const (
	conwayLedgerMempoolFailure   = 7
	dijkstraMempoolFailure       = 2
	dijkstraMempoolLedgerFailure = 1
	dijkstraLedgerUtxowFailure   = 1
)

func (e *hardForkApplyTxError) utxowFailure() []any {
	switch e.era {
	case gledger.EraIdShelley, gledger.EraIdAllegra, gledger.EraIdMary:
		return []any{
			gledger.ShelleyUtxowUtxoFailure,
			[]any{gledger.UtxoFailureInputSetEmpty},
		}
	case gledger.EraIdAlonzo:
		return []any{
			gledger.AlonzoUtxowShelleyInAlonzo,
			[]any{
				gledger.ShelleyUtxowUtxoFailure,
				[]any{gledger.UtxoFailureInputSetEmpty},
			},
		}
	case gledger.EraIdBabbage:
		return []any{
			gledger.BabbageUtxowUtxoFailure,
			[]any{
				gledger.BabbageUtxoAlonzoInBabbage,
				[]any{gledger.UtxoFailureInputSetEmpty},
			},
		}
	case gledger.EraIdConway:
		return []any{
			gledger.ConwayUtxowUtxoFailure,
			[]any{gledger.ConwayUtxoInputSetEmptyUTxO},
		}
	case gledger.EraIdDijkstra:
		return []any{
			gledger.ConwayUtxowUtxoFailure,
			[]any{gledger.ConwayUtxoInputSetEmptyUTxO},
		}
	default:
		return nil
	}
}
