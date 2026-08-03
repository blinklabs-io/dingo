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
	olocaltxsubmission "github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
)

func (o *Ouroboros) localtxsubmissionServerConnOpts() []olocaltxsubmission.LocalTxSubmissionOptionFunc {
	return []olocaltxsubmission.LocalTxSubmissionOptionFunc{
		olocaltxsubmission.WithSubmitTxFunc(
			o.instrumentLocaltxsubmissionSubmitTx(o.localtxsubmissionServerSubmitTx),
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
		return errors.New("local-tx-submission: unexpected transaction content type")
	}
	// Add transaction to mempool
	err := o.Mempool.AddTransaction(
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
	era uint8
	err error
}

func newLocalTxSubmissionRejectReason(
	eraId uint16,
	err error,
) error {
	if _, ok := errors.AsType[cborRejectReason](err); ok {
		return err
	}
	return &hardForkApplyTxError{
		era: uint8(eraId), //nolint:gosec // Cardano era IDs fit in uint8
		err: err,
	}
}

func (e *hardForkApplyTxError) Error() string {
	return e.err.Error()
}

func (e *hardForkApplyTxError) Unwrap() error {
	return e.err
}

func (e *hardForkApplyTxError) MarshalCBOR() ([]byte, error) {
	return cbor.Encode([]any{
		[]any{
			e.era,
			[]any{
				[]any{
					gledger.ApplyTxErrorUtxowFailure,
					e.utxowFailure(),
				},
			},
		},
	})
}

func (e *hardForkApplyTxError) utxowFailure() []any {
	utxoFailure := []any{
		e.era,
		e.inputSetEmptyFailure(),
	}

	switch e.era {
	case gledger.EraIdShelley, gledger.EraIdAllegra, gledger.EraIdMary:
		return []any{
			gledger.ShelleyUtxowUtxoFailure,
			utxoFailure,
		}
	case gledger.EraIdAlonzo:
		return []any{
			gledger.AlonzoUtxowShelleyInAlonzo,
			[]any{
				gledger.ShelleyUtxowUtxoFailure,
				utxoFailure,
			},
		}
	case gledger.EraIdBabbage:
		return []any{
			gledger.BabbageUtxowUtxoFailure,
			[]any{
				gledger.BabbageUtxoAlonzoInBabbage,
				utxoFailure,
			},
		}
	case gledger.EraIdConway:
		return []any{
			gledger.ConwayUtxowUtxoFailure,
			utxoFailure,
		}
	default:
		return []any{
			gledger.ConwayUtxowUtxoFailure,
			[]any{
				gledger.EraIdConway,
				[]any{gledger.ConwayUtxoInputSetEmptyUTxO},
			},
		}
	}
}

func (e *hardForkApplyTxError) inputSetEmptyFailure() []any {
	switch e.era {
	case gledger.EraIdConway:
		return []any{gledger.ConwayUtxoInputSetEmptyUTxO}
	default:
		return []any{gledger.UtxoFailureInputSetEmpty}
	}
}
