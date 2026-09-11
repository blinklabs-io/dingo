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
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// Tip is a comparison-friendly snapshot of a node's chain tip: enough to
// tell whether two nodes agree on the same block (Slot and Hash) without
// carrying gouroboros's wire types into this package's exported API.
type Tip struct {
	Slot uint64 `json:"slot"`
	// Hash is hex-encoded; empty at the origin point.
	Hash        string `json:"hash"`
	BlockNumber uint64 `json:"blockNumber"`
}

// Equal reports whether two tips name the same point on chain.
func (t Tip) Equal(other Tip) bool {
	return t.Slot == other.Slot && t.Hash == other.Hash
}

// point converts the tip into gouroboros's wire Point type, suitable for
// localstatequery.Client.Acquire, by hex-decoding Hash back into raw bytes.
func (t Tip) point() (pcommon.Point, error) {
	hashBytes, err := hex.DecodeString(t.Hash)
	if err != nil {
		return pcommon.Point{}, fmt.Errorf(
			"decode tip hash %q: %w",
			t.Hash,
			err,
		)
	}
	return pcommon.NewPoint(t.Slot, hashBytes), nil
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
