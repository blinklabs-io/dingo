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

package database

import (
	"bytes"
	"errors"
	"fmt"
)

// AdoptImmutableTip records that every block through blockIndex, the last of
// which is the block at slot and hash, has its body in chunk files that are
// already in place and synced. It is how a bulk import that moved whole chunk
// files into the ImmutableDB, rather than appending blocks through
// PromoteThrough, advances the recorded tip.
//
// The import records a chunk's last block before any of that chunk's markers
// commit: startup reconciliation cuts chunk bytes beyond the recorded tip, and
// the blocks of an interrupted chunk still need their markers, so the tip
// must already cover them. Markers therefore never exceed the recorded tip,
// and the tip may briefly cover blocks the chain has not yet stored. The tip
// only moves forward, and repeating the call for the recorded tip is a no-op,
// so a resumed import repeats it safely. The caller holds LockImmutable.
func (d *Database) AdoptImmutableTip(
	slot uint64,
	hash []byte,
	blockIndex uint64,
) error {
	rt := d.immutable
	if rt == nil {
		return ErrImmutableNotEnabled
	}
	st := rt.snapshot()
	if st.HasTip {
		if blockIndex == st.TipIndex &&
			slot == st.TipSlot && bytes.Equal(hash, st.TipHash) {
			return nil
		}
		if blockIndex <= st.TipIndex || slot <= st.TipSlot {
			return fmt.Errorf(
				"adopt immutable tip: block index %d, slot %d does not "+
					"follow the recorded tip index %d, slot %d",
				blockIndex, slot, st.TipIndex, st.TipSlot,
			)
		}
	}
	if len(hash) == 0 {
		return errors.New("adopt immutable tip: empty block hash")
	}
	next := ImmutableState{
		Geometry: st.Geometry,
		HasTip:   true,
		TipSlot:  slot,
		TipHash:  bytes.Clone(hash),
		TipIndex: blockIndex,
	}
	if err := d.writeImmutableState(next); err != nil {
		return fmt.Errorf("adopt immutable tip: %w", err)
	}
	rt.setState(next)
	return nil
}
