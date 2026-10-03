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

package dingo

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingService counts starts and stops and keeps the environment it was
// given.
type recordingService struct {
	starts, stops int
	env           NodeServiceEnv
	err           error
}

func (s *recordingService) run(
	_ context.Context,
	env NodeServiceEnv,
) (func(), error) {
	if s.err != nil {
		return nil, s.err
	}
	s.starts++
	s.env = env
	return func() { s.stops++ }, nil
}

func TestStartNodeServicesHandsServicesTheNodeEnvironment(t *testing.T) {
	t.Parallel()
	n := newStartupCleanupProducerNode(t)
	service := &recordingService{}
	WithNodeService(service.run)(&n.config)

	started, err := n.startNodeServices(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, started, 1)
	require.Equal(t, 1, service.starts)

	genesis, err := n.blockProducerShelleyGenesis()
	require.NoError(t, err)
	assert.Same(t, genesis, service.env.ShelleyGenesis)
	assert.Same(t, n.config.logger, service.env.Logger)
	assert.Equal(t, n.config.promRegistry, service.env.PromRegistry)
	// The ledger view and slot clock are the node's own: a pool with no
	// registration reports none, and the clock answers as the ledger's does.
	_, found, err := service.env.LedgerView.PoolRegistrationVRFKeyHash(
		[28]byte{1},
	)
	require.NoError(t, err)
	assert.False(t, found)
	slot, supported, err := service.env.WallClockSlot()
	require.NoError(t, err)
	wantSlot, wantSupported, err := n.ledgerState.WallClockSlotFromConfirmedHistory()
	require.NoError(t, err)
	assert.Equal(t, wantSupported, supported)
	assert.InDelta(t, wantSlot, slot, 5)

	// The single stop returned for rollback stops the service once.
	started[0]()
	assert.Equal(t, 1, service.stops)
	started[0]()
	assert.Equal(t, 1, service.stops)
}

func TestStartNodeServicesStopsStartedServicesWhenALaterOneFails(
	t *testing.T,
) {
	t.Parallel()
	n := newStartupCleanupProducerNode(t)
	first := &recordingService{}
	failing := &recordingService{err: errors.New("no aggregator")}
	WithNodeService(first.run)(&n.config)
	WithNodeService(failing.run)(&n.config)

	started, err := n.startNodeServices(t.Context(), nil)
	require.ErrorContains(t, err, "node service startup failed: no aggregator")
	assert.Empty(t, started)
	assert.Equal(t, 1, first.stops)
	assert.Empty(t, n.serviceStops)
}

func TestStartNodeServicesRequiresLedgerState(t *testing.T) {
	t.Parallel()
	n := newStartupCleanupProducerNode(t)
	n.ledgerState = nil
	WithNodeService((&recordingService{}).run)(&n.config)

	_, err := n.startNodeServices(t.Context(), nil)
	require.ErrorContains(t, err, "require ledger state")
}

func TestStartNodeServicesWithoutServicesStartsNothing(t *testing.T) {
	t.Parallel()
	n := &Node{}
	started, err := n.startNodeServices(t.Context(), nil)
	require.NoError(t, err)
	assert.Empty(t, started)
	assert.Empty(t, n.quiesceComponentStops())
}

func TestNodeServicesStopWithTheLiveLifecycleQuiesce(t *testing.T) {
	t.Parallel()
	n := newStartupCleanupProducerNode(t)
	service := &recordingService{}
	WithNodeService(service.run)(&n.config)
	_, err := n.startNodeServices(t.Context(), nil)
	require.NoError(t, err)

	// Services read the ledger, so they are stopped with the other
	// components before storage is closed, and first among them.
	stops := n.quiesceComponentStops()
	require.NotEmpty(t, stops)
	require.Equal(t, "node services", stops[0].name)
	require.NoError(t, stops[0].stop())
	assert.Equal(t, 1, service.stops)
	assert.Empty(t, n.quiesceComponentStops())

	// A restore then starts them again against the rebuilt node.
	_, err = n.startNodeServices(t.Context(), nil)
	require.NoError(t, err)
	assert.Equal(t, 2, service.starts)
}
