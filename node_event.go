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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/bark"
	"github.com/blinklabs-io/dingo/ledger/eras"
)

// eventNodeID names this node in Event.origin_node_id. Every subscriber sees
// it, so it comes from configuration and never from the host. Unconfigured, a
// random identifier is drawn once per Node, so it is still unique, which
// Publish relies on to refuse events that originated here.
func (n *Node) eventNodeID() string {
	n.eventNodeIDOnce.Do(func() {
		n.eventNodeIDValue = n.config.cfg.BarkEventNodeID
		if n.eventNodeIDValue != "" {
			return
		}
		var b [8]byte
		_, _ = rand.Read(b[:])
		n.eventNodeIDValue = "dingo-" + hex.EncodeToString(b[:])
	})
	return n.eventNodeIDValue
}

var errEventLedgerUnavailable = errors.New("ledger state is not available")

// eventEpochForSlot gives the Event Service the epoch a block's slot falls in.
func (n *Node) eventEpochForSlot(slot uint64) (uint64, error) {
	ls := n.wallClockLedger.Load()
	if ls == nil {
		return 0, errEventLedgerUnavailable
	}
	epoch, err := ls.SlotToEpoch(slot)
	if err != nil {
		return 0, err
	}
	return epoch.EpochId, nil
}

// eventEraNameForEpoch gives the Event Service the era name of an epoch.
func (n *Node) eventEraNameForEpoch(epoch uint64) (string, error) {
	ls := n.wallClockLedger.Load()
	if ls == nil {
		return "", errEventLedgerUnavailable
	}
	id, err := ls.EraIDForEpoch(epoch)
	if err != nil {
		return "", err
	}
	desc := eras.GetEraById(id)
	if desc == nil {
		return "", fmt.Errorf("unknown era id %d", id)
	}
	return desc.Name, nil
}

// startRemoteEvents follows the EventService of the configured sources and
// republishes what they send on this node's EventBus. It does nothing when no
// source is configured, and does not depend on this node serving its own
// EventService.
func (n *Node) startRemoteEvents() error {
	cfgSources := n.config.cfg.BarkEventSources
	if len(cfgSources) == 0 {
		return nil
	}
	sources := make([]bark.RemoteEventSource, 0, len(cfgSources))
	for _, src := range cfgSources {
		sources = append(sources, bark.RemoteEventSource(src))
	}
	remote, err := bark.NewRemoteEvents(bark.RemoteEventsConfig{
		Logger:   n.config.logger,
		EventBus: n.eventBus,
		NodeID:   n.eventNodeID(),
		Sources:  sources,
	})
	if err != nil {
		return fmt.Errorf("failed to create remote event subscriber: %w", err)
	}
	if err := remote.Start(n.ctx); err != nil {
		return fmt.Errorf("failed to start remote event subscriber: %w", err)
	}
	n.remoteEvents = remote
	return nil
}
