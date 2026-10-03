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
	"time"

	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/dmq"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// configPopulateDMQNetworkMagic resolves the DMQ network magic from the
// topic and Cardano network when none was configured. Only an enabled DMQ
// stack needs one.
func (n *Node) configPopulateDMQNetworkMagic() error {
	dmqCfg := &n.config.cfg.DMQ
	if !dmqCfg.Enabled || dmqCfg.NetworkMagic != 0 {
		return nil
	}
	magic, ok := dmq.TopicNetworkMagic(dmqCfg.Topic, n.config.cfg.Network)
	if !ok {
		return fmt.Errorf(
			"dmq: no network magic for topic %q on network %q: "+
				"set dmq.networkMagic",
			dmqCfg.Topic,
			n.config.cfg.Network,
		)
	}
	dmqCfg.NetworkMagic = magic
	return nil
}

// dmqStakeAuthority reports a pool's stake from the snapshot Praos uses for
// leader election, so a DMQ message is accepted from exactly the pools that
// may currently forge blocks. It reads the ledger on every call, so it follows
// a ledger that is rebuilt underneath it.
type dmqStakeAuthority struct {
	node *Node
}

func (a dmqStakeAuthority) PoolActiveStake(
	pool ocommon.PoolKeyHash,
) (uint64, error) {
	ls := a.node.ledgerState
	if ls == nil {
		return 0, errors.New("ledger state unavailable")
	}
	stake, _, err := (&stakeDistributionAdapter{ledgerState: ls}).
		GetPoolAndTotalActiveStake(
			praos.StakeSnapshotEpoch(ls.CurrentEpoch()),
			pool[:],
		)
	return stake, err
}

// startDMQ starts the DMQ stack when it is enabled. Its collectors register
// against the retained registry because the stack is not rebuilt by a live
// restore, which unregisters everything the rebuildable wrapper holds.
func (n *Node) startDMQ() error {
	cfg := n.config.cfg.DMQ
	if !cfg.Enabled {
		return nil
	}
	authenticator, err := ocommon.NewMessageAuthenticator(
		ocommon.MessageAuthenticatorConfig{
			StakeAuthority:    dmqStakeAuthority{node: n},
			SlotsPerKESPeriod: n.ledgerState.SlotsPerKESPeriod(),
			MaxKESEvolutions:  n.config.MaxKESEvolutions(),
			Logger:            n.config.logger,
		},
	)
	if err != nil {
		return fmt.Errorf("creating dmq authenticator: %w", err)
	}
	stack, err := dmq.NewStack(dmq.StackConfig{
		Logger:          n.config.logger,
		PromRegistry:    n.retainedComponentPromRegistry(),
		NetworkMagic:    cfg.NetworkMagic,
		SocketPath:      cfg.SocketPath,
		MessageTTL:      time.Duration(cfg.MessageTTL) * time.Second, // #nosec G115 -- seconds
		MaxMempoolBytes: int64(cfg.MaxMempoolSize) << 20,             // #nosec G115 -- MB count
		Authenticator:   authenticator,
	})
	if err != nil {
		return fmt.Errorf("creating dmq stack: %w", err)
	}
	if err := stack.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf("starting dmq stack: %w", err)
	}
	n.dmqStack = stack
	return nil
}
