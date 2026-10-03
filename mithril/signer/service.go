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

package signer

import (
	"context"
	"fmt"
	"sync"

	dingo "github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/mithril"
)

// Service runs the Mithril signer for the node's lifetime. It is a
// dingo.NodeService, supplied to the node from outside because the root
// package cannot import this one.
func Service(cfg *config.Config) dingo.NodeService {
	return serviceWithProgress(cfg, &roundProgress{})
}

func serviceWithProgress(
	cfg *config.Config,
	progress *roundProgress,
) dingo.NodeService {
	return func(
		ctx context.Context,
		env dingo.NodeServiceEnv,
	) (func(), error) {
		endpoint, err := signerEndpoint(cfg.Mithril, cfg.Network)
		if err != nil {
			return nil, err
		}
		var clientOpts []mithril.ClientOption
		if cfg.Mithril.AllowInsecureHTTP {
			clientOpts = append(clientOpts, mithril.WithAllowInsecureHTTP())
		}
		s, err := newSigner(Config{
			KESKeyPath:          cfg.Mithril.Signer.KESKey,
			OperationalCertPath: cfg.Mithril.Signer.OperationalCert,
			ColdVKeyPath:        cfg.Mithril.Signer.ColdVKey,
			STMKeyPath:          cfg.Mithril.Signer.STMKey,
			Genesis:             env.ShelleyGenesis,
			Client:              mithril.NewClient(endpoint, clientOpts...),
			Slot: func() (uint64, error) {
				slot, supported, err := env.WallClockSlot()
				if err != nil {
					return 0, fmt.Errorf(
						"wall-clock slot from confirmed history: %w",
						err,
					)
				}
				if !supported {
					// The slot cannot be placed on the chain until the
					// confirmed era history spans the wall clock.
					return 0, fmt.Errorf(
						"%w: confirmed era history does not span the wall clock",
						ErrSlotUnavailable,
					)
				}
				return slot, nil
			},
			Ledger:       env.LedgerView,
			Logger:       env.Logger,
			PromRegistry: env.PromRegistry,
		}, progress)
		if err != nil {
			return nil, fmt.Errorf("mithril signer: %w", err)
		}
		env.Logger.Info(
			"mithril signer starting",
			"component", "node",
			"party_id", s.PartyID(),
		)
		runCtx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		wg.Go(func() {
			if err := s.Run(runCtx); err != nil {
				env.Logger.Error(
					"mithril signer stopped",
					"component", "node",
					"error", err,
				)
			}
		})
		return func() {
			cancel()
			wg.Wait()
		}, nil
	}
}

// signerEndpoint selects the aggregator for the signer: its own
// endpoint, else the bootstrap aggregator URL, else the network's default.
func signerEndpoint(
	cfg config.MithrilConfig,
	network string,
) (string, error) {
	switch {
	case cfg.Signer.AggregatorEndpoint != "":
		return cfg.Signer.AggregatorEndpoint, nil
	case cfg.AggregatorURL != "":
		return cfg.AggregatorURL, nil
	}
	endpoint, err := mithril.AggregatorURLForNetwork(network)
	if err != nil {
		return "", fmt.Errorf(
			"mithril.signer.aggregatorEndpoint is required on network %q: %w",
			network,
			err,
		)
	}
	return endpoint, nil
}
