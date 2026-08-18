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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/event"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestCloseAllowsReconstructionWithSameRegistry is the prerequisite for
// constructor injection. Once Ouroboros takes its dependencies at
// construction it can no longer be retained across a live snapshot/restore —
// the restore rebuilds those dependencies, so it must rebuild Ouroboros too.
// Today that is impossible: the metric constructors go through
// promauto.With(PromRegistry), which panics on duplicate registration, and no
// collector handles are kept to unregister. Close must make a replacement
// instance constructible against the same registry.
func TestCloseAllowsReconstructionWithSameRegistry(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	registry := prometheus.NewRegistry()
	newInstance := func() *Ouroboros {
		return newOuroboros(OuroborosConfig{
			Logger:       logger,
			EventBus:     event.NewEventBus(nil, logger),
			PromRegistry: registry,
			EnableLeios:  true,
		})
	}

	first := newInstance()
	require.NoError(t, first.Close())
	require.NotPanics(t, func() {
		second := newInstance()
		require.NoError(t, second.Close())
	})
}

// TestCloseUnsubscribesFromEventBus covers the second half of the same
// prerequisite. The EventBus outlives a live restore, so every subscription
// an Ouroboros makes on its own behalf has to come back off on Close.
// Otherwise each restore cycle leaves a stale handler permanently attached
// and a single published event is handled once per accumulated cycle — the
// same leak node_leios.go captures subscription IDs to avoid.
func TestCloseUnsubscribesFromEventBus(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	o := newOuroboros(OuroborosConfig{
		Logger:      logger,
		EventBus:    bus,
		EnableLeios: true,
	})
	// The constructor subscribes the Leios announcement retry handler, and
	// the node subscribes the chainsync resync handler during startup.
	o.SubscribeChainsyncResync(t.Context())
	require.NotEmpty(t, o.subscriptions)

	require.NoError(t, o.Close())
	require.Empty(t, o.subscriptions)
}

// TestCloseIsIdempotent keeps shutdown paths simple: Run()'s deferred cleanup
// and an explicit live-restore teardown can both fire without the second
// call double-unregistering collectors owned by a replacement instance.
func TestCloseIsIdempotent(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:       logger,
		EventBus:     event.NewEventBus(nil, logger),
		PromRegistry: prometheus.NewRegistry(),
		EnableLeios:  true,
	})
	require.NoError(t, o.Close())
	require.NoError(t, o.Close())
}
