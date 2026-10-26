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
	"errors"
	"fmt"
	"math/big"

	nodeconsensus "github.com/blinklabs-io/dingo/consensus"
	"github.com/blinklabs-io/dingo/database"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

type consensusIdentityStore interface {
	GetSyncState(string, *database.Txn) (string, error)
	SetSyncState(string, string, *database.Txn) error
	GetTip(*database.Txn) (ochainsync.Tip, error)
}

func consensusNetworkMagic(
	base uint32,
	provider nodeconsensus.Provider,
) uint32 {
	if provider == nil {
		return base
	}
	return nodeconsensus.NetworkMagic(base, provider.NetworkIdentity())
}

func (n *Node) consensusNetworkMagic() uint32 {
	return consensusNetworkMagic(
		n.config.networkMagic,
		n.selectedConsensusProvider(),
	)
}

func validateConsensusConfiguration(
	cfg *Config,
	provider nodeconsensus.Provider,
) error {
	if provider == nil || provider.Name() != nodeconsensus.TachysProviderName {
		return nil
	}
	if cfg == nil {
		return errors.New("tachys configuration is unavailable")
	}
	if cfg.genesisBootstrap {
		return errors.New("tachys does not support Genesis bootstrap")
	}
	if cfg.Mithril().Enabled {
		return errors.New("tachys does not support Mithril bootstrap")
	}
	if cfg.RunMode() == internalconfig.RunModeLeios {
		return errors.New("tachys does not support Leios mode")
	}
	if cfg.StartEra().IsDijkstra() {
		return errors.New("tachys does not support a Dijkstra start era")
	}
	nodeCfg := cfg.CardanoNodeConfig()
	if nodeCfg == nil || nodeCfg.ShelleyGenesis() == nil {
		return errors.New("tachys requires Shelley genesis configuration")
	}
	for _, era := range []string{
		"shelley", "allegra", "mary", "alonzo", "babbage",
	} {
		epoch, ok := nodeCfg.HardForkEpoch(era)
		if !ok || epoch != 0 {
			return fmt.Errorf(
				"tachys requires %s at genesis (hard-fork epoch 0)",
				era,
			)
		}
	}
	if epoch, ok := nodeCfg.HardForkEpoch("dijkstra"); ok {
		return fmt.Errorf(
			"tachys does not support the configured Dijkstra transition at epoch %d",
			epoch,
		)
	}
	activeSlotsCoeff := nodeCfg.ShelleyGenesis().ActiveSlotsCoeff.Rat
	if activeSlotsCoeff == nil || activeSlotsCoeff.Cmp(big.NewRat(1, 1)) != 0 {
		return errors.New("tachys requires activeSlotsCoeff 1/1")
	}
	return nil
}

func (n *Node) enforceConsensusDatabaseIdentity() error {
	if n == nil || n.db == nil {
		return errors.New("consensus database is unavailable")
	}
	provider := n.selectedConsensusProvider()
	identity := provider.DatabaseIdentity()
	if provider.Name() == "" || identity == "" ||
		provider.NetworkIdentity() == "" || provider.ScheduleIdentity() == "" {
		return errors.New("consensus provider identity is incomplete")
	}
	return enforceConsensusIdentity(n.db, identity)
}

func enforceConsensusIdentity(
	store consensusIdentityStore,
	identity string,
) error {
	persisted, err := store.GetSyncState(nodeconsensus.DatabaseIdentitySyncKey, nil)
	if err != nil {
		return fmt.Errorf("read consensus database identity: %w", err)
	}
	if persisted == "" {
		tip, err := store.GetTip(nil)
		if err != nil {
			return fmt.Errorf("read consensus database tip: %w", err)
		}
		empty := tip.Point.Slot == 0 && len(tip.Point.Hash) == 0 &&
			tip.BlockNumber == 0
		// A nonempty database created before provider selection is Praos.
		// Fresh databases may adopt the configured provider before importing
		// or validating any chain state.
		if !empty && identity != nodeconsensus.PraosIdentity {
			return fmt.Errorf(
				"unmarked database uses consensus %q, selected provider uses %q",
				nodeconsensus.PraosIdentity,
				identity,
			)
		}
		if err := store.SetSyncState(
			nodeconsensus.DatabaseIdentitySyncKey,
			identity,
			nil,
		); err != nil {
			return fmt.Errorf("persist consensus database identity: %w", err)
		}
		return nil
	}
	if persisted != identity {
		return fmt.Errorf(
			"database consensus identity %q does not match selected provider %q",
			persisted,
			identity,
		)
	}
	return nil
}
