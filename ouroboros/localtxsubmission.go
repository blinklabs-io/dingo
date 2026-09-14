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
	"errors"
	"fmt"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	olocaltxsubmission "github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
)

func (o *Ouroboros) localtxsubmissionServerConnOpts() []olocaltxsubmission.LocalTxSubmissionOptionFunc {
	return []olocaltxsubmission.LocalTxSubmissionOptionFunc{
		olocaltxsubmission.WithSubmitTxFunc(
			o.instrumentLocaltxsubmissionSubmitTx(
				o.localtxsubmissionServerSubmitTx,
			),
		),
	}
}

func (o *Ouroboros) instrumentLocaltxsubmissionSubmitTx(
	fn func(olocaltxsubmission.CallbackContext, olocaltxsubmission.MsgSubmitTxTransaction) error,
) func(olocaltxsubmission.CallbackContext, olocaltxsubmission.MsgSubmitTxTransaction) error {
	return func(
		ctx olocaltxsubmission.CallbackContext,
		tx olocaltxsubmission.MsgSubmitTxTransaction,
	) error {
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
		return newLocalTxSubmissionRejectReason(tx.EraId, errors.New(
			"local-tx-submission: unexpected transaction content type",
		))
	}
	// Add transaction to mempool
	err := o.mempool.AddTransaction(
		uint(tx.EraId),
		rawTx,
	)
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
		return newLocalTxSubmissionRejectReason(tx.EraId, err)
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

func newLocalTxSubmissionRejectReason(
	eraId uint16,
	err error,
) error {
	if _, ok := errors.AsType[cborRejectReason](err); ok {
		return err
	}
	var inputSetEmpty shelley.InputSetEmptyUtxoError
	var inputSetEmptyPtr *shelley.InputSetEmptyUtxoError
	if errors.As(err, &inputSetEmpty) ||
		errors.As(err, &inputSetEmptyPtr) {
		switch eraId {
		case gledger.EraIdShelley, gledger.EraIdAllegra, gledger.EraIdMary,
			gledger.EraIdAlonzo, gledger.EraIdBabbage, gledger.EraIdConway,
			gledger.EraIdDijkstra:
			return &hardForkApplyTxError{
				era:           eraId,
				err:           err,
				inputSetEmpty: true,
			}
		}
	}
	if eraId == gledger.EraIdConway || eraId == gledger.EraIdDijkstra {
		return &hardForkApplyTxError{
			era: eraId,
			err: err,
		}
	}
	return &unrepresentableTxSubmissionError{err: err}
}

func (e *hardForkApplyTxError) Error() string {
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
	conwayLedgerMempoolFailure    = 7
	dijkstraMempoolFailure        = 2
	dijkstraMempoolLedgerFailure  = 1
	dijkstraLedgerUtxowFailure    = 1
	dijkstraUtxoFailureInputEmpty = 4
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
			0,
			[]any{dijkstraUtxoFailureInputEmpty},
		}
	default:
		return nil
	}
}
