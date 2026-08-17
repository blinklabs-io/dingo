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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// The snapshots carry each epoch's pool parameters in full, and reading them
// is what closes issue #3165.
//
// The seeding's remaining gap was a pool that held stake in the go or set
// snapshot and retired before the snapshot's own epoch: gone from cert state
// and from the active pool distribution, so no registration described it, its
// delegators' stake could not be attributed, and the gate dropped that whole
// epoch's reward basis. The parameters were in the snapshot the entire time.
// parsePoolDistrEntry took only the VRF key from these records and its
// comment said the rest was "not present in the legacy order" -- they are
// present, at an offset it did not look at.
//
// Cert state is the cross-check because it decodes full pool parameters
// through a different path. Every pool the snapshot and cert state both
// describe must agree, or the field mapping below is wrong somewhere it
// happens not to show.
func TestSnapshotPoolParamsMatchCertState(t *testing.T) {
	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err)
	certState, err := ParseCertState(state.CertStateData)
	require.NoError(t, err)
	require.NotEmpty(t, certState.Pools)

	live := make(map[string]*ParsedPool, len(certState.Pools))
	for i := range certState.Pools {
		pool := certState.Pools[i]
		live[hex.EncodeToString(pool.PoolKeyHash)] = &pool
	}

	var compared int
	for _, snap := range []*ParsedSnapShot{
		&snapshots.Mark, &snapshots.Set, &snapshots.Go,
	} {
		require.NotEmpty(t, snap.PoolParams)
		for key, got := range snap.PoolParams {
			want, ok := live[key]
			if !ok {
				continue
			}
			compared++
			require.Equal(t, want.VrfKeyHash, got.VrfKeyHash, "pool %s vrf", key)
			require.Equal(t, want.Pledge, got.Pledge, "pool %s pledge", key)
			require.Equal(t, want.Cost, got.Cost, "pool %s cost", key)
			require.Equal(t, want.MarginNum, got.MarginNum,
				"pool %s margin numerator", key)
			require.Equal(t, want.MarginDen, got.MarginDen,
				"pool %s margin denominator", key)
			require.Equal(t, want.RewardAccount, got.RewardAccount,
				"pool %s reward account", key)
			require.Equal(t, want.RewardAccountCredentialTag,
				got.RewardAccountCredentialTag,
				"pool %s reward account credential type", key)
			requireOwnersConsistent(t, key, want, got, snap)
		}
	}
	require.Positive(t, compared,
		"no pool appears in both the snapshots and cert state, so this "+
			"comparison checked nothing")

	// The reward account is what the gate rejects a basis for, so it is the
	// field that decides whether an epoch is seeded at all.
	for _, pool := range snapshots.Go.PoolParams {
		require.NotEmpty(t, pool.RewardAccount,
			"a snapshot pool with no reward account cannot be seeded, which "+
				"is the failure this parsing exists to remove")
	}
}

// The fixture is a two-pool DevNet with zero pledge, zero cost, a zero margin
// and no owners -- every field that would distinguish a correct mapping from a
// coincidental one is at its zero value. This runs the same comparison against
// a real network, where pledges, costs, margins and owner sets differ per
// pool, so a mapping that only works on zeros fails here.
//
//	ZZ_PREVIEW_SNAPSHOT=<path to a cardano-node ledger state file>
func TestSnapshotPoolParamsMatchCertStateOnRealNetwork(t *testing.T) {
	path := os.Getenv("ZZ_PREVIEW_SNAPSHOT")
	if path == "" {
		t.Skip("set ZZ_PREVIEW_SNAPSHOT to cross-check against a real network")
	}
	state, err := ParseSnapshot(path)
	require.NoError(t, err)
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err)
	certState, err := ParseCertState(state.CertStateData)
	require.NoError(t, err)

	live := make(map[string]*ParsedPool, len(certState.Pools))
	for i := range certState.Pools {
		pool := certState.Pools[i]
		live[hex.EncodeToString(pool.PoolKeyHash)] = &pool
	}

	var compared, withMargin, withPledge, withOwners int
	for key, got := range snapshots.Mark.PoolParams {
		want, ok := live[key]
		if !ok {
			continue
		}
		compared++
		if got.MarginNum != 0 {
			withMargin++
		}
		if got.Pledge != 0 {
			withPledge++
		}
		if len(got.Owners) > 0 {
			withOwners++
		}
		require.Equal(t, want.VrfKeyHash, got.VrfKeyHash, "pool %s vrf", key)
		require.Equal(t, want.Pledge, got.Pledge, "pool %s pledge", key)
		require.Equal(t, want.Cost, got.Cost, "pool %s cost", key)
		require.Equal(t, want.MarginNum, got.MarginNum, "pool %s margin num", key)
		require.Equal(t, want.MarginDen, got.MarginDen, "pool %s margin den", key)
		require.Equal(t, want.RewardAccount, got.RewardAccount,
			"pool %s reward account", key)
		requireOwnersConsistent(t, key, want, got, &snapshots.Mark)
	}
	t.Logf("compared %d pools: %d with a non-zero margin, %d with a "+
		"non-zero pledge, %d with owners",
		compared, withMargin, withPledge, withOwners)
	require.Positive(t, compared)
	require.Positive(t, withMargin,
		"no pool has a non-zero margin, so this run cannot tell a correct "+
			"margin field from a zero one")
	require.Positive(t, withOwners,
		"no pool has owners, so this run cannot tell a correct owner field "+
			"from an empty one")
}

// requireOwnersConsistent checks the snapshot's owner set against the
// registration's.
//
// They are not the same list, and should not be asserted equal. A
// registration names every owner; the snapshot names the owners that actually
// held stake when it was taken, and records their combined stake alongside.
// Across a real network 62 of 715 pools differ, always in the same direction
// -- the snapshot omits an owner the registration names, never the reverse --
// and every omitted owner has no stake delegated to that pool in that
// snapshot.
//
// That difference does not reach a reward: an owner with no stake contributes
// nothing to owner stake either way. What matters is that the omission is
// always justified, which is what this asserts. Equality would fail for a
// correct parse, and a bare subset check would pass for one that dropped
// owners at random.
func requireOwnersConsistent(
	t *testing.T,
	poolHex string,
	want, got *ParsedPool,
	snap *ParsedSnapShot,
) {
	t.Helper()
	registered := make(map[string]struct{}, len(want.Owners))
	for _, owner := range want.Owners {
		registered[hex.EncodeToString(owner)] = struct{}{}
	}
	inSnapshot := make(map[string]struct{}, len(got.Owners))
	for _, owner := range got.Owners {
		key := hex.EncodeToString(owner)
		inSnapshot[key] = struct{}{}
		require.Contains(t, registered, key,
			"pool %s: the snapshot names an owner the registration does not",
			poolHex)
	}
	for owner := range registered {
		if _, ok := inSnapshot[owner]; ok {
			continue
		}
		// Omitted, so it must hold no stake here -- otherwise the parse has
		// dropped stake that belongs in the pool's owner stake.
		delegated, isDelegated := snap.Delegations[owner]
		if isDelegated && hex.EncodeToString(delegated) == poolHex {
			require.Zero(t, snap.Stake[owner],
				"pool %s: owner %s is omitted from the snapshot's owner set "+
					"but holds stake in it", poolHex, owner)
		}
	}
}

// The resolution of issue #3165, stated as the property that was failing.
//
// A pool that held stake in one of the three snapshots and retired before the
// snapshot's own epoch is absent from cert state and from the current pool
// distribution, so no registration in an imported database describes it. Its
// delegators' stake could not be attributed to any pool, and rather than seed
// a basis that understates every other pool's share, the gate dropped that
// epoch entirely -- so a bootstrapped node skipped the reward rounds for all
// three epochs, stayed short on reward balances, and with them on the
// leadership stake those balances feed.
//
// Measured on preview at the time this was written: mark and set each had one
// such pool holding 1,218,574,660 lovelace, and go had two holding
// 19,782,849,212. All three epochs were rejected. Reading the parameters the
// snapshots carry attributes every pool in all three.
//
//	ZZ_PREVIEW_SNAPSHOT=<path to a cardano-node ledger state file>
func TestEveryDelegatedPoolIsAttributableFromTheSnapshot(t *testing.T) {
	path := os.Getenv("ZZ_PREVIEW_SNAPSHOT")
	if path == "" {
		t.Skip("set ZZ_PREVIEW_SNAPSHOT to check against a real network")
	}
	state, err := ParseSnapshot(path)
	require.NoError(t, err)
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err)
	certState, err := ParseCertState(state.CertStateData)
	require.NoError(t, err)

	// Cert state alone is what the seeding used to have, and is still the
	// fallback for a snapshot that cannot describe its own pools.
	registered := make(map[string]*ParsedPool, len(certState.Pools))
	for i := range certState.Pools {
		pool := certState.Pools[i]
		registered[hex.EncodeToString(pool.PoolKeyHash)] = &pool
	}

	for _, c := range []struct {
		name  string
		snap  *ParsedSnapShot
		epoch uint64
	}{
		{"mark", &snapshots.Mark, state.Epoch},
		{"set", &snapshots.Set, state.Epoch - 1},
		{"go", &snapshots.Go, state.Epoch - 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			bundle := deriveRewardInputs(
				c.snap,
				effectiveRewardPoolParams(c.snap, registered),
				c.epoch,
				1,
				0,
			)
			require.NotNil(t, bundle)
			require.Zero(t, bundle.unattributedPools,
				"%d pools holding %d lovelace have no parameters, so this "+
					"epoch's reward round is still dropped",
				bundle.unattributedPools, bundle.unattributedStake)
			require.NoError(t, bundle.validate(),
				"the basis for this epoch must be seedable")
			require.NotEmpty(t, bundle.poolInputs)
			t.Logf("%s: %d pools, %d delegators, all attributable",
				c.name, len(bundle.poolInputs), len(bundle.stakeInputs))

			// Cert state alone must still fall short, or the fixture no
			// longer contains the case this exists for and the assertion
			// above proves nothing.
			old := deriveRewardInputs(c.snap, registered, c.epoch, 1, 0)
			require.NotNil(t, old)
			require.Positive(t, old.unattributedPools,
				"this network no longer has a pool that retired inside the "+
					"snapshot window, so this run cannot show the fix")
		})
	}
}
