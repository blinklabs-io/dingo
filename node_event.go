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
	"os"

	"github.com/blinklabs-io/dingo/ledger/eras"
)

// eventNodeID names this node in Event.origin_node_id. The hostname is stable
// across restarts, which a subscriber de-duplicating on origin relies on.
func (n *Node) eventNodeID() string {
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	return "dingo"
}

var errEventLedgerUnavailable = errors.New("ledger state is not available")

// eventEpochForSlot gives the Event Service the epoch a block's slot falls in.
func (n *Node) eventEpochForSlot(slot uint64) (uint64, error) {
	ls := n.ledgerState
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
	ls := n.ledgerState
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
