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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestSplitUtxoEventBoundsEveryPartAndKeepsEveryOutput(t *testing.T) {
	t.Parallel()
	evt := UtxoEvent{
		Point: ocommon.Point{Slot: 7, Hash: bytes.Repeat([]byte{7}, 32)},
		Cause: UtxoChangeGenesis,
	}
	// Few large outputs cross the byte bound, many small ones the count
	// bound.
	big := bytes.Repeat([]byte{1}, MaxUtxoEventBytes/3)
	for i := range uint32(7) {
		evt.Added = append(evt.Added, UtxoOutput{
			Ref: models.UtxoId{Hash: bytes.Repeat([]byte{1}, 32), Idx: i}, Cbor: big,
		})
	}
	for i := range uint32(2*MaxUtxoEventEntries + 5) {
		evt.Added = append(evt.Added, UtxoOutput{
			Ref: models.UtxoId{Hash: bytes.Repeat([]byte{2}, 32), Idx: i}, Cbor: []byte{0x80},
		})
	}
	evt.Removed = []models.UtxoId{{Hash: bytes.Repeat([]byte{3}, 32)}}

	parts := splitUtxoEvent(evt)
	require.Greater(t, len(parts), 3)
	var added, removed int
	for _, p := range parts {
		entries, size := UtxoEventCost(p)
		require.LessOrEqual(t, entries, MaxUtxoEventEntries)
		require.LessOrEqual(t, size, MaxUtxoEventBytes)
		require.Equal(t, evt.Point, p.Point)
		require.Equal(t, evt.Cause, p.Cause)
		added += len(p.Added)
		removed += len(p.Removed)
	}
	require.Equal(t, len(evt.Added), added)
	require.Equal(t, 1, removed)
	require.Len(t, parts[0].Removed, 1, "removals apply first")

	small := UtxoEvent{Added: evt.Added[len(evt.Added)-3:]}
	require.Equal(t, []UtxoEvent{small}, splitUtxoEvent(small))
}
