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

package bark

import (
	"errors"
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/bark/eventtype"
	databasev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/database"
	eventv1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/event"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func testProtocolParams() *shelley.ShelleyProtocolParameters {
	return &shelley.ShelleyProtocolParameters{
		MaxTxSize:     16384,
		ProtocolMajor: 10,
		ProtocolMinor: 2,
		A0:            &cbor.Rat{Rat: big.NewRat(3, 10)},
		Rho:           &cbor.Rat{Rat: big.NewRat(3, 1000)},
		Tau:           &cbor.Rat{Rat: big.NewRat(2, 10)},
	}
}

func TestEventAdapterConvertsSyncState(t *testing.T) {
	t.Parallel()
	out := (&eventAdapter{}).convert(
		event.NewEvent(ledger.SyncStateEventType, ledger.SyncStateEvent{
			Previous:    ledger.SyncStateCaughtUp,
			Current:     ledger.SyncStateSyncing,
			TipSlot:     700,
			SlotsBehind: 42,
		}),
		discardLogger(),
	)
	require.Len(t, out, 1)
	require.Equal(t, eventtype.LedgerSyncState, out[0].GetEventType())
	var d eventv1alpha1.SyncStateEventData
	require.NoError(t, out[0].GetTypedData().UnmarshalTo(&d))
	require.Equal(
		t,
		eventv1alpha1.SyncState_SYNC_STATE_CAUGHT_UP,
		d.GetPreviousState(),
	)
	require.Equal(
		t,
		eventv1alpha1.SyncState_SYNC_STATE_SYNCING,
		d.GetCurrentState(),
	)
	require.Equal(t, uint64(700), d.GetTipSlot())
	require.Equal(t, uint64(42), d.GetSlotsBehind())
}

// TestEventAdapterConvertsProtocolParams checks the payload carries the
// epoch, its era, the protocol version, and JSON that decodes back to the
// same parameters.
func TestEventAdapterConvertsProtocolParams(t *testing.T) {
	t.Parallel()
	a := &eventAdapter{
		eraNameForEpoch: func(epoch uint64) (string, error) {
			require.Equal(t, uint64(12), epoch)
			return "conway", nil
		},
	}
	params := testProtocolParams()
	out := a.convert(
		event.NewEvent(
			ledger.ProtocolParamsEventType,
			ledger.ProtocolParamsEvent{
				Epoch:  12,
				Params: params,
			},
		),
		discardLogger(),
	)
	require.Len(t, out, 1)
	require.Equal(t, eventtype.LedgerProtocolParams, out[0].GetEventType())
	var d eventv1alpha1.ProtocolParametersEventData
	require.NoError(t, out[0].GetTypedData().UnmarshalTo(&d))
	require.Equal(t, uint64(12), d.GetEpoch())
	require.Equal(t, "conway", d.GetEra())
	require.Equal(t, uint64(10), d.GetProtocolMajor())
	require.Equal(t, uint64(2), d.GetProtocolMinor())

	want, err := params.Utxorpc()
	require.NoError(t, err)
	var got cardano.PParams
	require.NoError(t, protojson.Unmarshal([]byte(d.GetParametersJson()), &got))
	require.True(
		t,
		proto.Equal(want, &got),
		"parameters_json does not round-trip",
	)
}

// TestEventAdapterDropsUnconvertibleProtocolParams checks parameters that
// cannot be converted produce no event rather than an empty payload.
func TestEventAdapterDropsUnconvertibleProtocolParams(t *testing.T) {
	t.Parallel()
	a := &eventAdapter{
		eraNameForEpoch: func(uint64) (string, error) {
			return "", errors.New("unknown epoch")
		},
	}
	bad := testProtocolParams()
	bad.A0 = nil
	for _, params := range []*shelley.ShelleyProtocolParameters{bad, nil} {
		evt := ledger.ProtocolParamsEvent{Epoch: 12}
		if params != nil {
			evt.Params = params
		}
		out := a.convert(
			event.NewEvent(ledger.ProtocolParamsEventType, evt),
			discardLogger(),
		)
		require.Empty(t, out)
	}
}

func TestEventAdapterConvertsDatabaseOperation(t *testing.T) {
	t.Parallel()
	out := (&eventAdapter{}).convert(
		event.NewEvent(DatabaseOperationEventType, DatabaseOperationEvent{
			OperationID: "op-1",
			Type:        databasev1alpha1.OperationType_OPERATION_TYPE_RESTORE,
			Status:      databasev1alpha1.OperationStatus_OPERATION_STATUS_FAILED,
			Error:       "disk full",
		}),
		discardLogger(),
	)
	require.Len(t, out, 1)
	require.Equal(t, eventtype.DatabaseOperation, out[0].GetEventType())
	var d eventv1alpha1.DatabaseOperationEventData
	require.NoError(t, out[0].GetTypedData().UnmarshalTo(&d))
	require.Equal(t, "op-1", d.GetOperationId())
	require.Equal(t,
		databasev1alpha1.OperationType_OPERATION_TYPE_RESTORE, d.GetType())
	require.Equal(t,
		databasev1alpha1.OperationStatus_OPERATION_STATUS_FAILED, d.GetStatus())
	require.Equal(t, "disk full", d.GetError())
}

// TestEventSubscribeDeliversNodeStateEvents checks the three internal event
// types reach a subscriber through the hub.
func TestEventSubscribeDeliversNodeStateEvents(t *testing.T) {
	f := newEventFixture(t, nil, false)
	stream := openSubscribe(t, f,
		eventtype.LedgerSyncState,
		eventtype.LedgerProtocolParams,
		eventtype.DatabaseOperation,
	)
	f.bus.Publish(ledger.SyncStateEventType, event.NewEvent(
		ledger.SyncStateEventType,
		ledger.SyncStateEvent{
			Previous: ledger.SyncStateSyncing,
			Current:  ledger.SyncStateCaughtUp,
		},
	))
	f.bus.Publish(ledger.ProtocolParamsEventType, event.NewEvent(
		ledger.ProtocolParamsEventType,
		ledger.ProtocolParamsEvent{Epoch: 3, Params: testProtocolParams()},
	))
	f.bus.Publish(DatabaseOperationEventType, event.NewEvent(
		DatabaseOperationEventType,
		DatabaseOperationEvent{OperationID: "op-2"},
	))
	got := map[string]bool{}
	for range 3 {
		ev := recvEvent(t, func() (*eventv1alpha1.Event, error) {
			if !stream.Receive() {
				return nil, stream.Err()
			}
			return stream.Msg().GetEvent(), nil
		})
		got[ev.GetEventType()] = true
	}
	require.Equal(t, map[string]bool{
		eventtype.LedgerSyncState:      true,
		eventtype.LedgerProtocolParams: true,
		eventtype.DatabaseOperation:    true,
	}, got)
}
