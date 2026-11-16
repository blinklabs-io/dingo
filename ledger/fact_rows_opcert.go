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

package ledger

import (
	"bytes"
	"context"
	"errors"
	"slices"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
)

var _ metadata.AnchorOpCertSource = (*LedgerState)(nil)

// AnchorOpCertCounters returns the op-cert counters a snapshot import
// established at anchor: the counter of each pool whose latest observation as
// of anchor is at anchor itself, since an import writes them there and no
// block is applied at or below it. When the op-cert family is drained they
// come from the live UTxO set, which keeps each pool's observations only from
// its load and within its journal; a pool whose observation at the anchor it
// no longer holds is left out and logged, as the metadata store it replaced
// would have held it.
func (ls *LedgerState) AnchorOpCertCounters(
	ctx context.Context,
	anchor uint64,
) ([]metadata.OpCertCounter, error) {
	drained, err := ls.anchorFamilyDrained(ctx, nodesettings.FamilyOpCerts)
	if err != nil {
		return nil, err
	}
	if !drained {
		store, ok := ls.db.Metadata().(metadata.AnchorStateStore)
		if !ok {
			return nil, errors.New(
				"ledger metadata store cannot export op-cert counters",
			)
		}
		return store.ReadOpCertCountersAt(ctx, anchor, nil)
	}
	r := ls.opCertReader(nil)
	if r == nil {
		return nil, drainedUnserved(nodesettings.FamilyOpCerts)
	}
	pools, err := ls.opCertPools(r, nil)
	if err != nil {
		return nil, err
	}
	var out []metadata.OpCertCounter
	var lost int
	for _, p := range pools {
		st, known, err := ls.opCertView(r, nil, p, &anchor)
		if err != nil {
			return nil, err
		}
		if !known {
			lost++
			continue
		}
		if st != nil && st.Slot == anchor {
			out = append(out, metadata.OpCertCounter{
				PoolKeyHash: bytes.Clone(p), Sequence: st.LastSequence,
			})
		}
	}
	if lost > 0 {
		ls.config.Logger.Warn(
			"the live UTxO set no longer holds the op-cert counters certified "+
				"at the anchor for some pools",
			"component", "ledger",
			"pools", lost,
			"anchor_slot", anchor,
		)
	}
	slices.SortFunc(out, func(a, b metadata.OpCertCounter) int {
		return bytes.Compare(a.PoolKeyHash, b.PoolKeyHash)
	})
	return out, nil
}
