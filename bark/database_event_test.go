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
	"testing"
	"time"

	databasev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func requireOperationEvent(
	t *testing.T,
	ch <-chan event.Event,
	want DatabaseOperationEvent,
) {
	t.Helper()
	evt := testutil.RequireReceive(t, ch, 2*time.Second, "operation event")
	require.Equal(t, want, evt.Data)
}

// TestDatabaseOperationPublishesEachStatus checks an operation reports
// pending, running and its terminal status in that order, with the error
// only on failure.
func TestDatabaseOperationPublishesEachStatus(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		err    error
		cancel bool
		status databasev1alpha1.OperationStatus
		errMsg string
	}{
		{
			name:   "completed",
			status: databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED,
		},
		{
			name:   "failed",
			err:    errors.New("disk full"),
			status: databasev1alpha1.OperationStatus_OPERATION_STATUS_FAILED,
			errMsg: "disk full",
		},
		{
			name:   "cancelled",
			err:    errors.New("interrupted"),
			cancel: true,
			status: databasev1alpha1.OperationStatus_OPERATION_STATUS_CANCELLED,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := event.NewEventBus(nil, nil)
			t.Cleanup(bus.Close)
			_, ch := bus.Subscribe(DatabaseOperationEventType)
			h := &databaseServiceHandler{
				bark:       &Bark{config: BarkConfig{EventBus: bus}},
				operations: make(map[string]*operation),
			}
			opType := databasev1alpha1.OperationType_OPERATION_TYPE_TRUNCATE
			op, _, err := h.startOperation(opType)
			require.NoError(t, err)
			want := DatabaseOperationEvent{OperationID: op.id, Type: opType}

			want.Status = databasev1alpha1.OperationStatus_OPERATION_STATUS_PENDING
			requireOperationEvent(t, ch, want)
			op.setRunning()
			want.Status = databasev1alpha1.OperationStatus_OPERATION_STATUS_RUNNING
			requireOperationEvent(t, ch, want)
			if tc.cancel {
				op.requestCancel()
			}
			h.completeOperation(op, tc.err, 0)
			want.Status, want.Error = tc.status, tc.errMsg
			requireOperationEvent(t, ch, want)
			testutil.RequireNoReceive(t, ch, 50*time.Millisecond, "extra event")
		})
	}
}

// TestDatabaseOperationWithoutEventBus checks an operation runs with the
// EventService off.
func TestDatabaseOperationWithoutEventBus(t *testing.T) {
	t.Parallel()
	h := &databaseServiceHandler{
		bark:       &Bark{},
		operations: make(map[string]*operation),
	}
	op, _, err := h.startOperation(
		databasev1alpha1.OperationType_OPERATION_TYPE_SNAPSHOT,
	)
	require.NoError(t, err)
	op.setRunning()
	h.completeOperation(op, nil, 0)
	require.Equal(t,
		databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED,
		op.record().Status)
}
