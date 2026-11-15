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

package utxoshadow

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestShadowRestartsOnTheSameRegistry(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	start := func() *Shadow {
		s, err := Start(Config{
			Dir:        t.TempDir(),
			LedgerDB:   &stubLedgerDB{tip: pt(1)},
			EventBus:   event.NewEventBus(nil, nil),
			Registerer: reg,
		})
		require.NoError(t, err)
		return s
	}
	require.NoError(t, start().Stop())
	s := start()
	require.NoError(t, s.Stop())
}

// A handler the bus dispatched before Unsubscribe can run after Stop; it
// must leave the closed store alone rather than fail against it.
func TestShadowIgnoresEventsAfterStop(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	s, err := Start(Config{
		Dir:      t.TempDir(),
		LedgerDB: &stubLedgerDB{tip: pt(1)},
		EventBus: event.NewEventBus(nil, nil),
		Logger:   slog.New(slog.NewTextHandler(&logs, nil)),
	})
	require.NoError(t, err)
	require.NoError(t, s.Stop())
	send(s, txChange(2, []ledger.UtxoOutput{output(2, 0)}))
	require.Zero(t, s.Applied())
	require.Empty(t, logs.String())
	require.NoError(t, s.Stop(), "a second Stop is a no-op")
}

func TestShadowVerifyAllNeedsABootstrap(t *testing.T) {
	t.Parallel()
	s := newTestShadow(t, &stubLedgerDB{
		tip:  pt(1),
		live: map[badgerutxo.Ref][]byte{ref(1, 0): {1, 0}},
	})
	_, err := s.VerifyAll(context.Background())
	require.ErrorIs(t, err, errShadowNotBootstrapped)
	require.NoError(t, s.Bootstrap())
	rep, err := s.VerifyAll(context.Background())
	require.NoError(t, err)
	require.True(t, rep.Clean())
}

func TestShadowPrunesItsJournalBehindTheDiffWindow(t *testing.T) {
	t.Parallel()
	s, err := Start(Config{
		Dir:             t.TempDir(),
		LedgerDB:        &stubLedgerDB{tip: pt(1)},
		EventBus:        event.NewEventBus(nil, nil),
		DiffWindowSlots: 10,
		SampleEvery:     1 << 30,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Stop() })
	for slot := uint64(2); slot <= pruneEvery+2; slot++ {
		send(s, txChange(slot, []ledger.UtxoOutput{output(byte(slot), 0)}))
	}
	_, err = s.store.RollbackPreview(pt(2))
	require.Error(t, err, "the journal still reaches far behind the window")
}
