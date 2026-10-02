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
	"fmt"
	"log/slog"
	"slices"

	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/prometheus/client_golang/prometheus"
)

// NodeServiceEnv is what a NodeService may read from the node it runs in.
type NodeServiceEnv struct {
	Logger         *slog.Logger
	PromRegistry   prometheus.Registerer
	ShelleyGenesis *shelley.ShelleyGenesis
	// LedgerView reads pool registrations and operational certificate
	// counters from the ledger.
	LedgerView forging.LedgerView
	// WallClockSlot returns the slot for the current time. supported is
	// false while the ledger's confirmed era history does not yet span the
	// wall clock, when the slot cannot be placed on the chain.
	WallClockSlot func() (slot uint64, supported bool, err error)
}

// NodeService is a component that runs for the lifetime of the node. It
// starts after every built-in component, so a failure stops the node, and its
// stop function runs first at shutdown. It is supplied from outside this
// package because the component's own package depends on it.
//
// The returned stop function must wait for the service to finish; it may be
// nil when there is nothing to stop.
type NodeService func(ctx context.Context, env NodeServiceEnv) (stop func(), err error)

// WithNodeService adds a service to run alongside the node.
func WithNodeService(service NodeService) ConfigOptionFunc {
	return func(c *Config) {
		c.services = append(c.services, service)
	}
}

// startNodeServices starts the configured services in order. Their stops are
// kept on the node so a live restore can stop and restart them, and one
// combined stop is appended to started for startup rollback.
func (n *Node) startNodeServices(
	ctx context.Context,
	started []func(),
) ([]func(), error) {
	if len(n.config.services) == 0 {
		return started, nil
	}
	if n.ledgerState == nil {
		return started, errors.New("node services require ledger state")
	}
	genesis, err := n.blockProducerShelleyGenesis()
	if err != nil {
		return started, fmt.Errorf("node services: %w", err)
	}
	env := NodeServiceEnv{
		Logger:         n.config.logger,
		PromRegistry:   n.config.promRegistry,
		ShelleyGenesis: genesis,
		LedgerView:     blockProducerLedgerView{ls: n.ledgerState},
		WallClockSlot:  n.ledgerState.WallClockSlotFromConfirmedHistory,
	}
	for _, service := range n.config.services {
		stop, err := service(ctx, env)
		if err != nil {
			n.stopNodeServices()
			return started, fmt.Errorf("node service startup failed: %w", err)
		}
		if stop != nil {
			n.serviceStops = append(n.serviceStops, stop)
		}
	}
	return append(started, n.stopNodeServices), nil
}

// stopNodeServices stops the running services, last started first. It is a
// no-op when none are running.
func (n *Node) stopNodeServices() {
	stops := n.serviceStops
	n.serviceStops = nil
	for _, stop := range slices.Backward(stops) {
		stop()
	}
}
