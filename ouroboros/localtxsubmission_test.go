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
	"errors"
	"fmt"
	"log/slog"
	"net"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/connection"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	olocaltxsubmission "github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalTxSubmissionServerSubmitTx_NonByteContentReturnsError(t *testing.T) {
	o := &Ouroboros{
		config: OuroborosConfig{
			Logger: slog.New(slog.DiscardHandler),
		},
	}
	ctx := olocaltxsubmission.CallbackContext{
		ConnectionId: connection.ConnectionId{
			LocalAddr:  &net.TCPAddr{},
			RemoteAddr: &net.TCPAddr{},
		},
	}
	tx := olocaltxsubmission.MsgSubmitTxTransaction{
		EraId: uint16(gledger.EraIdConway),
		Raw: cbor.Tag{
			Number:  24,
			Content: "not-bytes",
		},
	}

	require.NotPanics(t, func() {
		err := o.localtxsubmissionServerSubmitTx(ctx, tx)
		require.Error(t, err)
	})
}

func TestLocalTxSubmissionRejectReason_FallbackIsHardForkApplyTxErr(t *testing.T) {
	for _, era := range []uint16{
		gledger.EraIdShelley,
		gledger.EraIdAllegra,
		gledger.EraIdMary,
		gledger.EraIdAlonzo,
		gledger.EraIdBabbage,
		gledger.EraIdConway,
	} {
		t.Run(gledger.GetEraById(uint8(era)).Name, func(t *testing.T) {
			err := newLocalTxSubmissionRejectReason(
				era,
				errors.New("plain validation failure"),
			)
			reason, ok := err.(cborRejectReason)
			require.True(t, ok)

			wireBytes, marshalErr := reason.MarshalCBOR()
			require.NoError(t, marshalErr)

			decoded, decodeErr := gledger.NewTxSubmitErrorFromCbor(wireBytes)
			require.NoError(t, decodeErr)

			var validationErr *gledger.ShelleyTxValidationError
			require.ErrorAs(t, decoded, &validationErr)
			assert.Equal(t, uint8(era), validationErr.Era)
		})
	}
}

func TestLocalTxSubmissionRejectReason_PreservesTypedReason(t *testing.T) {
	typed := &gledger.EraMismatch{
		OtherEra: gledger.EraInfo{
			Index: gledger.EraIdShelley,
			Name:  "Shelley",
		},
		LedgerEra: gledger.EraInfo{
			Index: gledger.EraIdByron,
			Name:  "Byron",
		},
	}
	wrapped := fmt.Errorf("validate transaction: %w", typed)

	err := newLocalTxSubmissionRejectReason(gledger.EraIdShelley, wrapped)
	assert.Same(t, wrapped, err)

	var reason cborRejectReason
	require.ErrorAs(t, err, &reason)
	require.NotNil(t, reason)

	wireBytes, marshalErr := reason.MarshalCBOR()
	require.NoError(t, marshalErr)

	decoded, decodeErr := gledger.NewTxSubmitErrorFromCbor(wireBytes)
	require.NoError(t, decodeErr)

	var eraMismatch *gledger.EraMismatch
	require.ErrorAs(t, decoded, &eraMismatch)
	require.NotNil(t, eraMismatch)
	assert.Equal(t, typed.OtherEra, eraMismatch.OtherEra)
	assert.Equal(t, typed.LedgerEra, eraMismatch.LedgerEra)
}
