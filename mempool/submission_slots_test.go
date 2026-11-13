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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func newSlotTestPool(t *testing.T, apiSlots int) *Mempool {
	t.Helper()
	m, err := NewMempool(MempoolConfig{
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:          event.NewEventBus(nil, nil),
		PromRegistry:      prometheus.NewRegistry(),
		Validator:         newMockValidator(),
		MempoolCapacity:   1 << 20,
		MaxAPISubmissions: apiSlots,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Stop(context.Background()) })
	return m
}

func fillSlots(t *testing.T, slots chan struct{}) {
	t.Helper()
	n := cap(slots)
	for range n {
		slots <- struct{}{}
	}
	t.Cleanup(func() {
		for range n {
			select {
			case <-slots:
			default:
			}
		}
	})
}

func TestExhaustedAPISlotsDoNotBlockPeerSubmission(t *testing.T) {
	t.Parallel()
	m := newSlotTestPool(t, 0)
	fillSlots(t, m.apiSubmissionSlots)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(
		t,
		m.AddTransaction(ctx, uint(conway.EraIdConway), getTestTxBytes(t)),
	)
}

func TestExhaustedPeerSlotsDoNotBlockAPISubmission(t *testing.T) {
	t.Parallel()
	m := newSlotTestPool(t, 0)
	fillSlots(t, m.submissionSlots)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(
		t,
		m.AddTransaction(
			WithAPISubmission(ctx),
			uint(conway.EraIdConway),
			getTestTxBytes(t),
		),
	)
}

func TestAPISubmissionWaitsWhenAPISlotsAreExhausted(t *testing.T) {
	t.Parallel()
	m := newSlotTestPool(t, 1)
	fillSlots(t, m.apiSubmissionSlots)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := m.AddTransaction(
		WithAPISubmission(ctx),
		uint(conway.EraIdConway),
		getTestTxBytes(t),
	)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestMaxAPISubmissionsSizesAPIPool(t *testing.T) {
	t.Parallel()
	m := newSlotTestPool(t, 9)
	require.Equal(t, 9, cap(m.apiSubmissionSlots))
	require.Equal(t, DefaultMaxConcurrentSubmissions, cap(m.submissionSlots))

	m = newSlotTestPool(t, 0)
	require.Equal(t, cap(m.submissionSlots), cap(m.apiSubmissionSlots))

	_, err := NewMempool(MempoolConfig{
		Validator:         newMockValidator(),
		MaxAPISubmissions: -1,
	})
	require.Error(t, err)
}
