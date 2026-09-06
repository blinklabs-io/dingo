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

package ledgerstate

import (
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
)

// Pool performance is beta/sigma_a, with beta the pool's share of the blocks
// minted in the performance epoch. A node bootstrapped from a snapshot has no
// local history for the epochs preceding its anchor and so can count none of
// those blocks -- but the snapshot itself carries them, in the two BlocksMade
// fields the ledger keeps for exactly this calculation. Discarding them at
// import is what leaves the first reward round with nothing to compute from.
func TestSnapshotCarriesBlocksMadeForBothEpochs(t *testing.T) {
	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	require.Equal(t, uint64(4), state.Epoch)

	const (
		poolA = "2b00dcd8850e3baa26295ce80c9a36898566e26665e14fe10950a6f7"
		poolB = "b0f3f3effa2365ab4937cfd7dea054cb3fb7a5b1fb65bb99c436527c"
	)
	decode := func(hexKey string) string {
		raw, err := hex.DecodeString(hexKey)
		require.NoError(t, err)
		return string(raw)
	}

	require.Len(t, state.BlocksPrev, 2)
	assert.Equal(t, uint64(63), state.BlocksPrev[decode(poolA)])
	assert.Equal(t, uint64(104), state.BlocksPrev[decode(poolB)])
	assert.Equal(t, uint64(167), sumBlocksMade(state.BlocksPrev))

	require.Len(t, state.BlocksCur, 2)
	assert.Equal(t, uint64(63), state.BlocksCur[decode(poolA)])
	assert.Equal(t, uint64(81), state.BlocksCur[decode(poolB)])
	assert.Equal(t, uint64(144), sumBlocksMade(state.BlocksCur))
}

// A dropped entry lowers one pool's beta and the epoch total every other
// pool's beta divides by, so a partially decoded map produces a
// complete-looking distribution at the wrong amount for every pool at once.
// The stake and delegation maps can afford to skip an entry; this one cannot.
func TestParseBlocksMadeRejectsMalformedEntries(t *testing.T) {
	poolKey := make([]byte, credentialHashSize)
	for i := range poolKey {
		poolKey[i] = byte(i)
	}
	// BlocksMade is a bare CBOR map from a 28-byte pool key hash to a count,
	// so the entries are built here rather than encoded from a Go map, whose
	// string keys would become text strings instead of byte strings.
	bstr := func(value []byte) []byte {
		return append([]byte{byte(0x40 | len(value))}, value...)
	}
	longBstr := func(value []byte) []byte {
		return append([]byte{0x58, byte(len(value))}, value...)
	}
	entry := func(key, value []byte) cbor.RawMessage {
		out := []byte{0xa1}
		out = append(out, key...)
		out = append(out, value...)
		return out
	}

	for _, tc := range []struct {
		name    string
		encoded cbor.RawMessage
		wantErr string
	}{
		{
			name:    "short pool key hash",
			encoded: entry(bstr(poolKey[:20]), []byte{0x03}),
			wantErr: "pool key hash is 20 bytes",
		},
		{
			name: "non-numeric block count",
			encoded: entry(
				longBstr(poolKey),
				append([]byte{0x65}, []byte("three")...),
			),
			wantErr: "decoding block count",
		},
		{
			name:    "non-bytestring key",
			encoded: entry([]byte{0x07}, []byte{0x03}),
			wantErr: "decoding pool key hash",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blocks, err := parseBlocksMade(tc.encoded)
			require.Error(t, err)
			require.Nil(t, blocks)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}

	blocks, err := parseBlocksMade(entry(longBstr(poolKey), []byte{0x03}))
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{string(poolKey): 3}, blocks)
}

// nesBprev describes the epoch before the snapshot's and nesBcur the
// snapshot's own, so the two maps land on the two epochs whose blocks a
// bootstrapped node can never count: the performance epoch of the first reward
// round it crosses, and the pre-anchor half of the second one's.
func TestImportBlocksMadePersistsBothEpochs(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	require.NotNil(t, state.Tip)

	store := db.Metadata()
	require.NoError(t, importBlocksMade(
		store,
		state.Epoch,
		state.BlocksPrev,
		state.BlocksCur,
		state.Tip.Slot,
		nil,
	))

	prev, err := store.GetImportedPoolBlockCounts(state.Epoch-1, nil)
	require.NoError(t, err)
	assert.Equal(t, state.BlocksPrev, prev)

	cur, err := store.GetImportedPoolBlockCounts(state.Epoch, nil)
	require.NoError(t, err)
	assert.Equal(t, state.BlocksCur, cur)

	// An epoch the snapshot says nothing about must stay empty, so the reward
	// round can tell "not imported" from "imported as zero".
	older, err := store.GetImportedPoolBlockCounts(state.Epoch-2, nil)
	require.NoError(t, err)
	assert.Empty(t, older)
}

// A catch-up import carries a later anchor and a later nesBcur. Merging the
// new map into the old rows would leave one epoch holding counts taken at two
// different anchors, so the epoch is replaced rather than added to.
func TestImportBlocksMadeReplacesAnEpochRatherThanMerging(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	store := db.Metadata()

	poolA := string(make([]byte, credentialHashSize))
	poolBKey := make([]byte, credentialHashSize)
	poolBKey[0] = 0x01
	poolB := string(poolBKey)

	require.NoError(t, importBlocksMade(
		store,
		9,
		map[string]uint64{poolA: 4},
		map[string]uint64{poolB: 2},
		100,
		nil,
	))
	require.NoError(t, importBlocksMade(
		store,
		9,
		map[string]uint64{poolA: 4},
		map[string]uint64{poolA: 5},
		200,
		nil,
	))

	cur, err := store.GetImportedPoolBlockCounts(9, nil)
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{poolA: 5}, cur)
}
