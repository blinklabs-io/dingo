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

	"github.com/stretchr/testify/require"
)

func hash28(b byte) []byte {
	out := make([]byte, 28)
	for i := range out {
		out[i] = b
	}
	return out
}

func hex28(b byte) string { return hex.EncodeToString(hash28(b)) }

// twoPoolSnapshot is a snapshot with two pools: one whose owner also
// delegates to it, and one with only outside delegators.
func twoPoolSnapshot() *ParsedSnapShot {
	ownerA := hex28(0x11)
	delegA := hex28(0x12)
	delegB := hex28(0x21)
	return &ParsedSnapShot{
		Stake: map[string]uint64{
			ownerA: 1_000,
			delegA: 4_000,
			delegB: 7_000,
		},
		Delegations: map[string][]byte{
			ownerA: hash28(0xAA),
			delegA: hash28(0xAA),
			delegB: hash28(0xBB),
		},
		PoolParams: map[string]*ParsedPool{
			hex28(0xAA): {
				PoolKeyHash:   hash28(0xAA),
				Pledge:        500,
				Cost:          340,
				MarginNum:     1,
				MarginDen:     50,
				RewardAccount: hash28(0x11),
				Owners:        [][]byte{hash28(0x11)},
			},
			hex28(0xBB): {
				PoolKeyHash:   hash28(0xBB),
				Pledge:        0,
				Cost:          170,
				MarginNum:     3,
				MarginDen:     100,
				RewardAccount: hash28(0x21),
			},
		},
	}
}

// The derived basis has to reconcile, because the ledger reads it back through
// a path that returns an error rather than skipping when it does not — which
// would fail the epoch rollover outright.
func TestDeriveRewardInputsReconciles(t *testing.T) {
	bundle := deriveRewardInputs(twoPoolSnapshot(), 1385, 119_750_400, 0)
	require.NotNil(t, bundle)
	require.NoError(t, bundle.validate())

	require.Equal(t, uint64(1385), bundle.snapshot.Epoch)
	require.Equal(t, uint64(2), bundle.snapshot.TotalPoolCount)
	require.Equal(t, uint64(3), bundle.snapshot.TotalDelegators)
	require.Equal(t, uint64(12_000), uint64(bundle.snapshot.TotalActiveStake))
	require.Len(t, bundle.stakeInputs, 3)

	byPool := map[string]*struct {
		delegated, owner uint64
		delegators       uint64
	}{}
	for _, p := range bundle.poolInputs {
		byPool[hex.EncodeToString(p.PoolKeyHash)] = &struct {
			delegated, owner uint64
			delegators       uint64
		}{uint64(p.DelegatedStake), uint64(p.OwnerStake), p.DelegatorCount}
	}
	a := byPool[hex28(0xAA)]
	require.NotNil(t, a)
	require.Equal(t, uint64(5_000), a.delegated)
	// Only the credential that is a registered owner counts toward owner
	// stake; the other delegator to the same pool must not.
	require.Equal(t, uint64(1_000), a.owner)
	require.Equal(t, uint64(2), a.delegators)

	b := byPool[hex28(0xBB)]
	require.NotNil(t, b)
	require.Equal(t, uint64(7_000), b.delegated)
	require.Zero(t, b.owner, "a pool with no owner delegating has no owner stake")
	require.Equal(t, uint64(1), b.delegators)
}

// A credential delegated to a pool the snapshot carries no parameters for
// cannot be paid, and counting it would break the reconciliation the ledger
// enforces.
func TestDeriveRewardInputsDropsUnknownPoolDelegations(t *testing.T) {
	snap := twoPoolSnapshot()
	orphan := hex28(0x31)
	snap.Stake[orphan] = 9_000
	snap.Delegations[orphan] = hash28(0xCC) // no PoolParams entry

	bundle := deriveRewardInputs(snap, 1385, 1, 0)
	require.NotNil(t, bundle)
	require.NoError(t, bundle.validate())
	require.Equal(t, uint64(12_000), uint64(bundle.snapshot.TotalActiveStake),
		"the orphaned delegation must not inflate the active stake")
	require.Len(t, bundle.stakeInputs, 3)
}

// Zero-stake credentials are rejected by the ledger's validator, so they must
// never reach it.
func TestDeriveRewardInputsDropsZeroStake(t *testing.T) {
	snap := twoPoolSnapshot()
	idle := hex28(0x41)
	snap.Stake[idle] = 0
	snap.Delegations[idle] = hash28(0xAA)

	bundle := deriveRewardInputs(snap, 1385, 1, 0)
	require.NotNil(t, bundle)
	require.NoError(t, bundle.validate())
	require.Len(t, bundle.stakeInputs, 3)
}

// The gate exists to stop an unusable basis reaching the database. A margin
// the snapshot reports above 1 is the kind of thing it must catch: the ledger
// rejects it on read, and on that path a rejection fails the rollover.
func TestDerivedRewardInputsGateRejectsBadMargin(t *testing.T) {
	snap := twoPoolSnapshot()
	snap.PoolParams[hex28(0xAA)].MarginNum = 3
	snap.PoolParams[hex28(0xAA)].MarginDen = 2

	bundle := deriveRewardInputs(snap, 1385, 1, 0)
	require.NotNil(t, bundle)
	require.ErrorContains(t, bundle.validate(), "margin outside [0,1]")
}

// A pool key of the wrong length is likewise refused rather than written.
func TestDerivedRewardInputsGateRejectsBadPoolKey(t *testing.T) {
	snap := twoPoolSnapshot()
	snap.PoolParams[hex28(0xAA)].PoolKeyHash = []byte{0x01, 0x02}

	bundle := deriveRewardInputs(snap, 1385, 1, 0)
	require.NotNil(t, bundle)
	require.ErrorContains(t, bundle.validate(), "pool key hash")
}

// The gate must catch a totals mismatch, which is the failure mode a future
// edit to the derivation is most likely to introduce.
func TestDerivedRewardInputsGateRejectsTotalsMismatch(t *testing.T) {
	bundle := deriveRewardInputs(twoPoolSnapshot(), 1385, 1, 0)
	require.NotNil(t, bundle)
	require.NoError(t, bundle.validate())

	bundle.snapshot.TotalActiveStake++
	require.ErrorContains(t, bundle.validate(), "does not match snapshot")
}
