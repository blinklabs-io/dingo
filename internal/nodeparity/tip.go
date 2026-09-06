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

package nodeparity

import (
	"encoding/hex"
	"errors"
	"fmt"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

// Tip is a comparison-friendly snapshot of a node's chain tip: enough to
// tell whether two nodes agree on the same block (Slot and Hash) without
// carrying gouroboros's wire types into this package's exported API.
type Tip struct {
	Slot        uint64
	Hash        string // hex-encoded block hash; empty at the origin point
	BlockNumber uint64
}

// Equal reports whether two tips name the same point on chain.
func (t Tip) Equal(other Tip) bool {
	return t.Slot == other.Slot && t.Hash == other.Hash
}

// ReadTip asks conn's ChainSync mini-protocol for the node's current tip.
// This is a single request/reply call (MsgFindIntersect with no points,
// which cardano-node answers with just its tip), not a chain-following
// subscription, so it is cheap to call once per check cycle.
func ReadTip(conn *ouroboros.Connection) (Tip, error) {
	cs := conn.ChainSync()
	if cs == nil || cs.Client == nil {
		return Tip{}, errors.New("ChainSync client unavailable")
	}
	tip, err := cs.Client.GetCurrentTip()
	if err != nil {
		return Tip{}, fmt.Errorf("get current tip: %w", err)
	}
	return Tip{
		Slot:        tip.Point.Slot,
		Hash:        hex.EncodeToString(tip.Point.Hash),
		BlockNumber: tip.BlockNumber,
	}, nil
}
