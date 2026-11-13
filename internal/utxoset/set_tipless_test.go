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

package utxoset_test

import (
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// A tipless batch as large as a big genesis set holds more entries than one
// Badger transaction takes, so the set has to write it in several.
func TestApplyUtxoChangesWritesALargeTiplessBatchInGroups(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })

	const events = 20
	var changes []ledger.UtxoEvent
	hash := make([]byte, 32)
	for e := range events {
		evt := ledger.UtxoEvent{
			Point: ocommon.Point{Slot: 0, Hash: make([]byte, 32)},
			Cause: ledger.UtxoChangeGenesis,
		}
		for i := range ledger.MaxUtxoEventEntries {
			n := uint32(e*ledger.MaxUtxoEventEntries + i)
			binary.BigEndian.PutUint32(hash, n)
			evt.Added = append(evt.Added, ledger.UtxoOutput{
				Ref:  models.UtxoId{Hash: append([]byte(nil), hash...)},
				Cbor: []byte{0x80},
			})
		}
		changes = append(changes, evt)
	}
	require.NoError(t, set.ApplyUtxoChanges(nil, changes))

	binary.BigEndian.PutUint32(hash, uint32(events*ledger.MaxUtxoEventEntries-1))
	_, ok, err := set.Get(hash, 0)
	require.NoError(t, err)
	require.True(t, ok)
	binary.BigEndian.PutUint32(hash, 0)
	_, ok, err = set.Get(hash, 0)
	require.NoError(t, err)
	require.True(t, ok)
}
