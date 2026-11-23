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
	"math"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// The registrations the boundary snapshot freezes a Leios key from carry the
// epoch the key was registered in: a re-registration that keeps the key keeps
// its epoch, and a key imported without an age stays without one rather than
// being dated from its registration's slot.
func TestSnapshotRegistrationsCarryTheLeiosKeyAge(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	placeStakePoolStateTip(t, ls, 360)
	kept := repeatedBytes(28, 0x11)
	imported := repeatedBytes(28, 0x12)

	seedStakePoolBlsRegistration(t, ls, kept, 0x66, 120)
	seedStakePoolBlsRegistration(t, ls, kept, 0x66, 250)
	require.NoError(t, fxImportPool(t, ls.db, t.Context(), nil, &models.Pool{
		PoolKeyHash:   imported,
		VrfKeyHash:    repeatedBytes(32, 0xAA),
		RewardAccount: repeatedBytes(28, 0x22),
	}, &models.PoolRegistration{
		PoolKeyHash:                    imported,
		VrfKeyHash:                     repeatedBytes(32, 0xAA),
		RewardAccount:                  repeatedBytes(28, 0x22),
		AddedSlot:                      5,
		LeiosKeyPublic:                 repeatedBytes(96, 0x66),
		LeiosKeyPossessionProof:        repeatedBytes(48, 0x67),
		LeiosKeyRegistrationAgeUnknown: true,
	}))

	// The registrations in effect in epoch 3, as the capture at the
	// boundary into epoch 4 reads them.
	regs, err := ls.ledgerDB().GetPoolRegistrationsEffectiveForEpoch(
		[]lcommon.PoolKeyHash{
			lcommon.PoolKeyHash(lcommon.NewBlake2b224(kept)),
			lcommon.PoolKeyHash(lcommon.NewBlake2b224(imported)),
		},
		300, 3, math.MaxInt64, nil,
	)
	require.NoError(t, err)
	byPool := map[string]models.PoolRegistration{}
	for _, reg := range regs {
		byPool[string(reg.PoolKeyHash)] = reg
	}
	require.Len(t, byPool, 2)

	keptReg := byPool[string(kept)]
	require.Equal(t, uint64(250), keptReg.AddedSlot)
	require.NotNil(t, keptReg.LeiosKeyRegistrationEpoch)
	require.Equal(t, uint64(1), *keptReg.LeiosKeyRegistrationEpoch)
	require.False(t, keptReg.LeiosKeyRegistrationAgeUnknown)

	importedReg := byPool[string(imported)]
	require.Nil(t, importedReg.LeiosKeyRegistrationEpoch)
	require.True(t, importedReg.LeiosKeyRegistrationAgeUnknown)
}

// A rollback below a key rotation restores the pool record with the key and
// registration epoch it held before.
func TestLeiosKeyRegistrationEpochRollsBackWithThePoolRecord(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	placeStakePoolStateTip(t, ls, 360)
	pool := repeatedBytes(28, 0x11)

	registered := fxPoint(120)
	tx, _ := stakePoolBlsRegistrationTx(t, pool, 0x66, 120)
	fxApplyTx(t, ls.db, tx, registered, 0, map[int]uint64{0: 500_000_000})
	tx, _ = stakePoolBlsRegistrationTx(t, pool, 0x68, 260)
	fxApplyTx(t, ls.db, tx, fxPoint(260), 0, nil)

	state, err := ls.poolReader(nil).PoolState(pool)
	require.NoError(t, err)
	require.NotNil(t, state.Params.LeiosKeyRegistrationEpoch)
	require.Equal(t, uint64(1), *state.Params.LeiosKeyRegistrationEpoch)
	require.NotNil(t, state.Future)
	require.Equal(t, repeatedBytes(96, 0x68), state.Future.LeiosKeyPublic)
	require.NotNil(t, state.Future.LeiosKeyRegistrationEpoch)
	require.Equal(t, uint64(3), *state.Future.LeiosKeyRegistrationEpoch)

	fxRollback(t, ls, ocommon.Point(registered))

	state, err = ls.poolReader(nil).PoolState(pool)
	require.NoError(t, err)
	require.Nil(t, state.Future)
	require.Equal(t, repeatedBytes(96, 0x66), state.Params.LeiosKeyPublic)
	require.NotNil(t, state.Params.LeiosKeyRegistrationEpoch)
	require.Equal(t, uint64(1), *state.Params.LeiosKeyRegistrationEpoch)
	events, err := ls.poolHistoryReader(nil).PoolHistory(pool)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.NotNil(t, events[0].Registration.LeiosKeyRegistrationEpoch)
	require.Equal(t, uint64(1), *events[0].Registration.LeiosKeyRegistrationEpoch)
}

// A genesis pool's Leios key is registered in the initial epoch.
func TestGenesisPoolLeiosKeyIsRegisteredInTheInitialEpoch(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	pool := repeatedBytes(28, 0x11)
	cert := lcommon.PoolRegistrationCertificate{
		CertType: uint(lcommon.CertificateTypePoolRegistration),
		Operator: lcommon.PoolKeyHash(lcommon.NewBlake2b224(pool)),
		VrfKeyHash: lcommon.VrfKeyHash(
			lcommon.NewBlake2b256(repeatedBytes(32, 0xAA)),
		),
		Margin: cbor.Rat{Rat: big.NewRat(3, 10)},
		RewardAccount: lcommon.AddrKeyHash(
			lcommon.NewBlake2b224(repeatedBytes(28, 0x22)),
		),
		LeiosKey: &lcommon.LeiosKey{
			PublicKey:       repeatedBytes(96, 0x66),
			PossessionProof: repeatedBytes(48, 0x67),
		},
	}
	ls.stageGenesisStaking(
		nil,
		map[string]lcommon.PoolRegistrationCertificate{"pool": cert},
		nil, 0, 500_000_000,
	)
	require.False(t, ls.utxoSetFailed.Load())

	state, err := ls.poolReader(nil).PoolState(pool)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.NotNil(t, state.Params.LeiosKeyRegistrationEpoch)
	require.Equal(t, uint64(0), *state.Params.LeiosKeyRegistrationEpoch)
	events, err := ls.poolHistoryReader(nil).PoolHistory(pool)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.NotNil(t, events[0].Registration.LeiosKeyRegistrationEpoch)
}
