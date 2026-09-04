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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// A pool synthesized from the current active distribution can be absent from
// mark and go while still being present, with authoritative parameters, in
// set. Registration lookup is intentionally shared across the three epochs,
// so the synthetic fallback must be scoped back to the pools delegated to in
// each target snapshot before that snapshot's complete parameters overlay it.
//
// The two-pool shape mirrors preview snapshot i27926: the affected pools were
// delegated to only in set, absent from cert state, and therefore synthesized
// with no historical reward account. Before the fix that irrelevant fallback
// made mark and go fail validation while set passed because its own complete
// parameters replaced the synthetic entry.
func TestSeedImportedRewardInputsScopesFallbackToTargetSnapshot(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	poolA := scopedRewardTestPool(0xA1, 0x11)
	poolB := scopedRewardTestPoolFromKey(
		t,
		"102e9ff50bee440b1ef337f58d1760a5475f3ce716f2aab60e6ef424",
		0x22,
	)
	poolC := scopedRewardTestPoolFromKey(
		t,
		"1fc372fdce61f42d31be7ddfc2bf8e343b08a54e4d3e6d64b2e328ff",
		0x23,
	)
	compactA := &ParsedPool{
		PoolKeyHash: poolA.PoolKeyHash,
		VrfKeyHash:  poolA.VrfKeyHash,
	}
	compactB := &ParsedPool{
		PoolKeyHash: poolB.PoolKeyHash,
		VrfKeyHash:  poolB.VrfKeyHash,
	}
	compactC := &ParsedPool{
		PoolKeyHash: poolC.PoolKeyHash,
		VrfKeyHash:  poolC.VrfKeyHash,
	}
	setCredentialB := hex28(0x32)
	setCredentialC := hex28(0x34)
	snapshots := &ParsedSnapShots{
		Mark: scopedRewardTestSnapshot(0x31, 1_000, poolA, compactA),
		Set: ParsedSnapShot{
			Stake: map[string]uint64{
				setCredentialB: 2_000,
				setCredentialC: 2_500,
			},
			Delegations: map[string][]byte{
				setCredentialB: poolB.PoolKeyHash,
				setCredentialC: poolC.PoolKeyHash,
			},
			PoolParams: map[string]*ParsedPool{
				hex.EncodeToString(poolB.PoolKeyHash): poolB,
				hex.EncodeToString(poolC.PoolKeyHash): poolC,
			},
		},
		Go: scopedRewardTestSnapshot(0x33, 3_000, poolA, compactA),
	}
	// poolB and poolC model the synthesized registrations: they identify the
	// exact two pools reported in issue #3313 but have none of the economics
	// needed for rewards. They are valid fallback inputs to consider for set,
	// and must not contaminate mark or go.
	registered := map[string]*ParsedPool{
		hex.EncodeToString(poolA.PoolKeyHash): poolA,
		hex.EncodeToString(poolB.PoolKeyHash): compactB,
		hex.EncodeToString(poolC.PoolKeyHash): compactC,
	}

	txn := db.MetadataTxn(true)
	require.NoError(t, seedImportedRewardInputs(
		db.Metadata(),
		txn.Metadata(),
		snapshots,
		func(uint64) (map[string]*ParsedPool, error) {
			return registered, nil
		},
		nil,
		100,
		9_999,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))
	require.NoError(t, txn.Commit())

	want := map[uint64]struct {
		pools map[string]uint64
	}{
		100: {pools: map[string]uint64{
			hex.EncodeToString(poolA.PoolKeyHash): 1_000,
		}},
		99: {pools: map[string]uint64{
			hex.EncodeToString(poolB.PoolKeyHash): 2_000,
			hex.EncodeToString(poolC.PoolKeyHash): 2_500,
		}},
		98: {pools: map[string]uint64{
			hex.EncodeToString(poolA.PoolKeyHash): 3_000,
		}},
	}
	for epoch, expected := range want {
		var totalStake uint64
		for _, stake := range expected.pools {
			totalStake += stake
		}
		snapshot, err := db.Metadata().GetRewardSnapshot(epoch, "mark", nil)
		require.NoError(t, err)
		require.NotNil(t, snapshot,
			"epoch %d must not be dropped because another snapshot refers "+
				"to a synthesized pool", epoch)
		require.Equal(t, uint64(len(expected.pools)), snapshot.TotalPoolCount)
		require.Equal(t, totalStake, uint64(snapshot.TotalActiveStake))
		require.Equal(t, uint64(len(expected.pools)), snapshot.TotalDelegators)

		poolInputs, err := db.Metadata().GetRewardPoolInputs(epoch, nil)
		require.NoError(t, err)
		require.Len(t, poolInputs, len(expected.pools))
		for _, input := range poolInputs {
			key := hex.EncodeToString(input.PoolKeyHash)
			require.Equal(t, expected.pools[key],
				uint64(input.DelegatedStake))
			require.Equal(t, uint64(1), input.DelegatorCount)
		}

		stakeInputs, err := db.Metadata().GetRewardStakeInputs(epoch, nil)
		require.NoError(t, err)
		require.Len(t, stakeInputs, len(expected.pools))
		for _, input := range stakeInputs {
			key := hex.EncodeToString(input.PoolKeyHash)
			require.Equal(t, expected.pools[key], uint64(input.Stake))
		}
	}
}

// Scoping must never turn an actually delegated-to pool into an omission.
// If neither the target snapshot nor the registration fallback has complete
// parameters, validation must still reject the whole epoch rather than seed
// a partial basis that understates every other pool's reward share.
func TestEffectiveRewardPoolParamsKeepsIncompleteReferencedPool(t *testing.T) {
	pool := scopedRewardTestPool(0xC3, 0x41)
	compact := &ParsedPool{
		PoolKeyHash: pool.PoolKeyHash,
		VrfKeyHash:  pool.VrfKeyHash,
	}
	snapshot := scopedRewardTestSnapshot(0x42, 4_000, pool, compact)
	key := hex.EncodeToString(pool.PoolKeyHash)

	params := effectiveRewardPoolParams(
		&snapshot,
		map[string]*ParsedPool{key: compact},
	)
	require.Contains(t, params, key,
		"a referenced pool must remain visible to validation")

	bundle := deriveRewardInputs(&snapshot, params, 100, 9_999, 0)
	require.ErrorContains(t, bundle.validate(), "has no reward account")
}

// When a target snapshot genuinely delegates stake to incomplete pools, the
// safe behavior is still to reject the epoch. The diagnostic must name every
// such pool and its delegated stake, though, so operators can see the whole
// blast radius instead of whichever map entry validation visited first.
func TestDerivedRewardInputsReportsAllIncompleteReferencedPools(t *testing.T) {
	poolA := scopedRewardTestPoolFromKey(
		t,
		"102e9ff50bee440b1ef337f58d1760a5475f3ce716f2aab60e6ef424",
		0x51,
	)
	poolB := scopedRewardTestPoolFromKey(
		t,
		"1fc372fdce61f42d31be7ddfc2bf8e343b08a54e4d3e6d64b2e328ff",
		0x52,
	)
	credentialA := hex28(0x61)
	credentialB := hex28(0x62)
	snapshot := &ParsedSnapShot{
		Stake: map[string]uint64{
			credentialA: 393_520_844,
			credentialB: 397_411_504,
		},
		Delegations: map[string][]byte{
			credentialA: poolA.PoolKeyHash,
			credentialB: poolB.PoolKeyHash,
		},
	}
	params := map[string]*ParsedPool{
		hex.EncodeToString(poolA.PoolKeyHash): {
			PoolKeyHash: poolA.PoolKeyHash,
			VrfKeyHash:  poolA.VrfKeyHash,
		},
		hex.EncodeToString(poolB.PoolKeyHash): {
			PoolKeyHash: poolB.PoolKeyHash,
			VrfKeyHash:  poolB.VrfKeyHash,
		},
	}

	bundle := deriveRewardInputs(snapshot, params, 1_395, 120_644_200, 0)
	err := bundle.validate()
	require.Error(t, err)
	require.ErrorContains(t, err,
		"102e9ff50bee440b1ef337f58d1760a5475f3ce716f2aab60e6ef424")
	require.ErrorContains(t, err, "393520844 lovelace delegated stake")
	require.ErrorContains(t, err,
		"1fc372fdce61f42d31be7ddfc2bf8e343b08a54e4d3e6d64b2e328ff")
	require.ErrorContains(t, err, "397411504 lovelace delegated stake")
}

func TestDerivedRewardInputsBoundsIncompletePoolDiagnostic(t *testing.T) {
	const poolCount = maxRewardSeedFailurePools + 8
	snapshot := &ParsedSnapShot{
		Stake:       make(map[string]uint64, poolCount),
		Delegations: make(map[string][]byte, poolCount),
	}
	params := make(map[string]*ParsedPool, poolCount)
	for i := 0; i < poolCount; i++ {
		credential := hash28(byte(i + 1))
		poolKey := hash28(byte(i + 100))
		credentialHex := hex.EncodeToString(credential)
		poolHex := hex.EncodeToString(poolKey)
		snapshot.Stake[credentialHex] = 1
		snapshot.Delegations[credentialHex] = poolKey
		params[poolHex] = &ParsedPool{PoolKeyHash: poolKey}
	}

	bundle := deriveRewardInputs(snapshot, params, 1, 1, 0)
	err := bundle.validate()
	require.Error(t, err)
	require.ErrorContains(t, err, "additional pools omitted")
	require.LessOrEqual(t, len(err.Error()), 4_096)
}

func scopedRewardTestPool(poolByte, rewardByte byte) *ParsedPool {
	return &ParsedPool{
		PoolKeyHash:   hash28(poolByte),
		VrfKeyHash:    make([]byte, 32),
		Pledge:        1_000,
		Cost:          75_000_000,
		MarginNum:     1,
		MarginDen:     5,
		RewardAccount: hash28(rewardByte),
		Owners:        [][]byte{hash28(rewardByte)},
	}
}

func scopedRewardTestPoolFromKey(
	t *testing.T,
	poolHex string,
	rewardByte byte,
) *ParsedPool {
	t.Helper()
	pool := scopedRewardTestPool(0, rewardByte)
	key, err := hex.DecodeString(poolHex)
	require.NoError(t, err)
	require.Len(t, key, credentialHashSize)
	pool.PoolKeyHash = key
	return pool
}

func scopedRewardTestSnapshot(
	credentialByte byte,
	stake uint64,
	pool *ParsedPool,
	params *ParsedPool,
) ParsedSnapShot {
	credential := hex28(credentialByte)
	poolKey := hex.EncodeToString(pool.PoolKeyHash)
	return ParsedSnapShot{
		Stake:       map[string]uint64{credential: stake},
		Delegations: map[string][]byte{credential: pool.PoolKeyHash},
		PoolParams:  map[string]*ParsedPool{poolKey: params},
	}
}
