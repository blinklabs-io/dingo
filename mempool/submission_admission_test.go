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

package mempool

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	dingotestutil "github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func newSubmissionAdmissionPool(t *testing.T) *Mempool {
	t.Helper()
	m, err := NewMempool(MempoolConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        event.NewEventBus(nil, nil),
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       newMockValidator(),
		MempoolCapacity: 1 << 20,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Stop(context.Background()) })
	return m
}

// fillSubmissionSlots reserves every shared submission slot directly.
func fillSubmissionSlots(
	t *testing.T,
	m *Mempool,
) []func() {
	t.Helper()
	var releases []func()
	for range cap(m.submissionSlots) {
		m.submissionSlots <- struct{}{}
		releases = append(releases, m.submissionSlotRelease())
	}
	t.Cleanup(func() {
		for _, release := range releases {
			release()
		}
	})
	return releases
}

func TestAdmitSubmissionFailsFastWhenSlotsAreFull(t *testing.T) {
	t.Parallel()
	m := newSubmissionAdmissionPool(t)
	fillSubmissionSlots(t, m)
	_, _, err := m.AdmitSubmission(context.Background())
	require.ErrorIs(t, err, ErrSubmissionBusy)
	require.Empty(t, m.apiSubmissionSlots)
}

func TestAPISubmissionSaturationPreservesDirectSlot(t *testing.T) {
	t.Parallel()
	m := newSubmissionAdmissionPool(t)
	var releases []func()
	for {
		_, release, err := m.AdmitSubmission(context.Background())
		if errors.Is(err, ErrSubmissionBusy) {
			break
		}
		require.NoError(t, err)
		releases = append(releases, release)
		require.LessOrEqual(t, len(releases), cap(m.submissionSlots))
	}
	t.Cleanup(func() {
		for _, release := range releases {
			release()
		}
	})
	if got, want := len(releases), cap(m.submissionSlots)-1; got != want {
		t.Errorf("API admission held %d slots, want %d", got, want)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(
		t,
		m.AddTransaction(ctx, uint(conway.EraIdConway), getTestTxBytes(t)),
	)
}

type singleSlotValidator struct {
	*mockValidator
}

func (singleSlotValidator) MempoolSubmissionCapacity() int { return 1 }

// With one process-wide slot the API share is zero, so API admission must
// refuse while the slot is free and direct submission must still validate.
func TestAPISubmissionUnavailableWithSingleSlot(t *testing.T) {
	t.Parallel()
	m, err := NewMempool(MempoolConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        event.NewEventBus(nil, nil),
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       singleSlotValidator{newMockValidator()},
		MempoolCapacity: 1 << 20,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Stop(context.Background()) })
	require.Equal(t, 1, cap(m.submissionSlots))

	_, release, err := m.AdmitSubmission(context.Background())
	if release != nil {
		release()
	}
	require.ErrorIs(t, err, ErrSubmissionBusy)
	require.Empty(t, m.submissionSlots)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(
		t,
		m.AddTransaction(ctx, uint(conway.EraIdConway), getTestTxBytes(t)),
	)
}

// Direct callers (node-to-node and node-to-client submission, rolled-back
// transaction restoration) have no client to answer with a busy status, so a
// full gate must delay them rather than drop the transaction.
func TestAddTransactionWaitsForSubmissionSlot(t *testing.T) {
	t.Parallel()
	m := newSubmissionAdmissionPool(t)
	releases := fillSubmissionSlots(t, m)
	txBytes := getTestTxBytes(t)
	result := make(chan error, 1)
	go func() {
		result <- m.AddTransaction(
			context.Background(),
			uint(conway.EraIdConway),
			txBytes,
		)
	}()
	dingotestutil.RequireNoReceive(
		t,
		result,
		100*time.Millisecond,
		"AddTransaction returned while every slot was held",
	)
	releases[0]()
	require.NoError(t, dingotestutil.RequireReceive(
		t,
		result,
		5*time.Second,
		"AddTransaction did not proceed after a slot was released",
	))
	require.Len(t, m.Transactions(), 1)
}

func TestAddTransactionWaitHonorsContext(t *testing.T) {
	t.Parallel()
	m := newSubmissionAdmissionPool(t)
	fillSubmissionSlots(t, m)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		100*time.Millisecond,
	)
	defer cancel()
	err := m.AddTransaction(ctx, uint(conway.EraIdConway), getTestTxBytes(t))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, m.Transactions())
}

func TestAddTransactionWaitEndsOnStop(t *testing.T) {
	t.Parallel()
	m := newSubmissionAdmissionPool(t)
	fillSubmissionSlots(t, m)
	result := make(chan error, 1)
	go func() {
		result <- m.AddTransaction(
			context.Background(),
			uint(conway.EraIdConway),
			getTestTxBytes(t),
		)
	}()
	dingotestutil.RequireNoReceive(
		t,
		result,
		50*time.Millisecond,
		"AddTransaction returned while every slot was held",
	)
	require.NoError(t, m.Stop(context.Background()))
	require.ErrorIs(t, dingotestutil.RequireReceive(
		t,
		result,
		5*time.Second,
		"AddTransaction kept waiting for a slot after Stop",
	), ErrMempoolStopped)
}

// A handler that reserved a slot before decoding must not need a second one
// in AddTransaction, or a full gate would refuse its own reservation.
func TestAddTransactionReusesAdmittedSlot(t *testing.T) {
	t.Parallel()
	m := newSubmissionAdmissionPool(t)
	admitted, release, err := m.AdmitSubmission(context.Background())
	require.NoError(t, err)
	t.Cleanup(release)
	for len(m.submissionSlots) < cap(m.submissionSlots) {
		m.submissionSlots <- struct{}{}
		t.Cleanup(m.submissionSlotRelease())
	}
	ctx, cancel := context.WithTimeout(admitted, 5*time.Second)
	defer cancel()
	require.NoError(
		t,
		m.AddTransaction(ctx, uint(conway.EraIdConway), getTestTxBytes(t)),
	)
}
