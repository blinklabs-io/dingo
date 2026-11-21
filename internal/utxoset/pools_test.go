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

package utxoset

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type legacyPoolStateEnc struct {
	cbor.StructAsArray
	Params      []legacyPoolRegEnc
	Future      []legacyPoolRegEnc
	Retiring    []uint64
	DepositHeld []uint64
}

func TestPoolLeiosKeyEpochFollowsStateAndRollback(t *testing.T) {
	t.Parallel()
	set, err := Open(Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	pool := bytes.Repeat([]byte{0x11}, 28)
	apply := func(slot, epoch uint64) {
		t.Helper()
		registration := ledger.CleanPoolRegistration(&models.PoolRegistration{
			PoolKeyHash:               pool,
			LeiosKeyPublic:            bytes.Repeat([]byte{0x22}, 96),
			LeiosKeyPossessionProof:   bytes.Repeat([]byte{0x33}, 48),
			LeiosKeyRegistrationEpoch: &epoch,
			AddedSlot:                 slot,
		})
		point := ocommon.NewPoint(slot, bytes.Repeat([]byte{byte(slot)}, 32))
		require.NoError(t, set.ApplyLedgerChanges(&point, ledger.LedgerChanges{
			Pools: []ledger.PoolChange{{
				Slot:  slot,
				State: ledger.PoolState{Pool: pool, Params: registration},
			}},
		}))
	}
	readEpoch := func() uint64 {
		t.Helper()
		state, err := set.PoolState(pool)
		require.NoError(t, err)
		require.NotNil(t, state)
		require.NotNil(t, state.Params)
		require.NotNil(t, state.Params.LeiosKeyRegistrationEpoch)
		return *state.Params.LeiosKeyRegistrationEpoch
	}

	apply(10, 4)
	require.Equal(t, uint64(4), readEpoch())
	apply(12, 8)
	require.Equal(t, uint64(8), readEpoch())
	_, err = set.Rollback(
		ocommon.NewPoint(10, bytes.Repeat([]byte{10}, 32)),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(4), readEpoch())
}

func TestPoolStateDecodesRecordWithoutLeiosKeyEpoch(t *testing.T) {
	t.Parallel()
	pool := bytes.Repeat([]byte{0x44}, 28)
	value, err := cbor.Encode(&legacyPoolStateEnc{
		Params: []legacyPoolRegEnc{{
			Vrf:         bytes.Repeat([]byte{0x55}, 32),
			LeiosPublic: bytes.Repeat([]byte{0x66}, 96),
			LeiosProof:  bytes.Repeat([]byte{0x77}, 48),
			AddedSlot:   9,
		}},
	})
	require.NoError(t, err)

	state, err := decodePoolState(pool, value)
	require.NoError(t, err)
	require.Equal(t, pool, state.Params.PoolKeyHash)
	require.Equal(t, bytes.Repeat([]byte{0x55}, 32), state.Params.VrfKeyHash)
	require.Equal(
		t,
		bytes.Repeat([]byte{0x66}, 96),
		state.Params.LeiosKeyPublic,
	)
	require.Nil(t, state.Params.LeiosKeyRegistrationEpoch)
}
