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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lbabbage "github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	lconway "github.com/blinklabs-io/gouroboros/ledger/conway"
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
	t.Parallel()

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
			require.Equal(
				t,
				want.VrfKeyHash,
				got.VrfKeyHash,
				"pool %s vrf",
				key,
			)
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
	t.Parallel()

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
		require.Equal(
			t,
			want.MarginNum,
			got.MarginNum,
			"pool %s margin num",
			key,
		)
		require.Equal(
			t,
			want.MarginDen,
			got.MarginDen,
			"pool %s margin den",
			key,
		)
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
	t.Parallel()

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

func TestFindLedgerStateFileLegacy(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ledgerDir := filepath.Join(dir, "ledger")
	err := os.MkdirAll(ledgerDir, 0o750)
	require.NoError(t, err)

	// Create a legacy .lstate file
	lstatePath := filepath.Join(ledgerDir, "12345.lstate")
	err = os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	found, err := FindLedgerStateFile(dir)
	require.NoError(t, err)
	require.Equal(t, lstatePath, found)
}

func TestFindLedgerStateFileUTxOHD(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	slotDir := filepath.Join(dir, "ledger", "67890")
	err := os.MkdirAll(slotDir, 0o750)
	require.NoError(t, err)

	statePath := filepath.Join(slotDir, "state")
	err = os.WriteFile(statePath, []byte("data"), 0o640)
	require.NoError(t, err)

	found, err := FindLedgerStateFile(dir)
	require.NoError(t, err)
	require.Equal(t, statePath, found)
}

func TestFindLedgerStateFilePreferUTxOHD(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ledgerDir := filepath.Join(dir, "ledger")
	err := os.MkdirAll(ledgerDir, 0o750)
	require.NoError(t, err)

	// Create both legacy and UTxO-HD files
	lstatePath := filepath.Join(ledgerDir, "12345.lstate")
	err = os.WriteFile(lstatePath, []byte("legacy"), 0o640)
	require.NoError(t, err)

	slotDir := filepath.Join(ledgerDir, "67890")
	err = os.MkdirAll(slotDir, 0o750)
	require.NoError(t, err)

	statePath := filepath.Join(slotDir, "state")
	err = os.WriteFile(statePath, []byte("utxohd"), 0o640)
	require.NoError(t, err)

	// Should prefer UTxO-HD format
	found, err := FindLedgerStateFile(dir)
	require.NoError(t, err)
	require.Equal(t, statePath, found)
}

func TestFindLedgerStateFileDBSubdir(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ledgerDir := filepath.Join(dir, "db", "ledger")
	err := os.MkdirAll(ledgerDir, 0o750)
	require.NoError(t, err)

	lstatePath := filepath.Join(ledgerDir, "55555.lstate")
	err = os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	found, err := FindLedgerStateFile(dir)
	require.NoError(t, err)
	require.Equal(t, lstatePath, found)
}

func TestFindLedgerStateFileUTxOHDHighestSlot(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	// Create two UTxO-HD slot subdirectories with state files
	for _, slot := range []string{"100", "200"} {
		slotDir := filepath.Join(dir, "ledger", slot)
		err := os.MkdirAll(slotDir, 0o750)
		require.NoError(t, err)

		statePath := filepath.Join(slotDir, "state")
		err = os.WriteFile(statePath, []byte("data"), 0o640)
		require.NoError(t, err)
	}

	// FindLedgerStateFile must return the higher-numbered slot
	found, err := FindLedgerStateFile(dir)
	require.NoError(t, err)

	expectedPath := filepath.Join(dir, "ledger", "200", "state")
	require.Equal(t, expectedPath, found)
}

func TestFindLedgerStateFileAtOrBefore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	for _, slot := range []string{"100", "200", "300"} {
		slotDir := filepath.Join(dir, "ledger", slot)
		require.NoError(t, os.MkdirAll(slotDir, 0o750))
		require.NoError(
			t,
			os.WriteFile(
				filepath.Join(slotDir, "state"),
				[]byte("data"),
				0o640,
			),
		)
	}

	found, err := FindLedgerStateFileAtOrBefore(dir, 250)
	require.NoError(t, err)
	require.Equal(
		t,
		filepath.Join(dir, "ledger", "200", "state"),
		found,
	)
}

func TestFindLedgerStateFileAtOrBeforeRejectsVolatileOnlyState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	slotDir := filepath.Join(dir, "ledger", "300")
	require.NoError(t, os.MkdirAll(slotDir, 0o750))
	require.NoError(
		t,
		os.WriteFile(
			filepath.Join(slotDir, "state"),
			[]byte("data"),
			0o640,
		),
	)

	_, err := FindLedgerStateFileAtOrBefore(dir, 250)
	require.ErrorContains(t, err, "at or before slot 250")
}

func TestFindLedgerStateFileNotFound(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	_, err := FindLedgerStateFile(dir)
	require.ErrorIs(t, err, ErrLedgerDirNotFound)
}

func TestFindUTxOTableFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	tablesDir := filepath.Join(dir, "ledger", "99999", "tables")
	err := os.MkdirAll(tablesDir, 0o750)
	require.NoError(t, err)

	tvarPath := filepath.Join(tablesDir, "tvar")
	err = os.WriteFile(tvarPath, []byte("data"), 0o640)
	require.NoError(t, err)

	found := FindUTxOTableFile(dir)
	require.Equal(t, tvarPath, found)
}

func TestFindUTxOTableFileForState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	slotDir := filepath.Join(dir, "ledger", "99999")
	require.NoError(t, os.MkdirAll(slotDir, 0o750))
	statePath := filepath.Join(slotDir, "state")
	require.NoError(t, os.WriteFile(statePath, []byte("state"), 0o640))
	tablesPath := filepath.Join(slotDir, "tables")
	require.NoError(t, os.WriteFile(tablesPath, []byte("table"), 0o640))

	require.Equal(t, tablesPath, FindUTxOTableFileForState(statePath))
	require.Empty(
		t,
		FindUTxOTableFileForState(
			filepath.Join(dir, "ledger", "99999.lstate"),
		),
	)
}

func TestFindUTxOTableFileCurrentTablesFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	slotDir := filepath.Join(dir, "ledger", "99999")
	err := os.MkdirAll(slotDir, 0o750)
	require.NoError(t, err)

	tablesPath := filepath.Join(slotDir, "tables")
	err = os.WriteFile(tablesPath, []byte("data"), 0o640)
	require.NoError(t, err)

	found := FindUTxOTableFile(dir)
	require.Equal(t, tablesPath, found)
}

func TestFindUTxOTableFileNotFound(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ledgerDir := filepath.Join(dir, "ledger")
	err := os.MkdirAll(ledgerDir, 0o750)
	require.NoError(t, err)

	found := FindUTxOTableFile(dir)
	require.Empty(t, found)
}

func TestIsLedgerStateFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filename string
		expected bool
	}{
		{"numeric slot", "12345", true},
		{"checksum file", "12345.checksum", false},
		{"lock file", "12345.lock", false},
		{"tmp file", "12345.tmp", false},
		{"non-numeric", "state", false},
		{"empty", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(
				t,
				tt.expected,
				isLedgerStateFile(tt.filename),
			)
		})
	}
}

func TestEraName(t *testing.T) {
	t.Parallel()

	require.Equal(t, "Byron", EraName(EraByron))
	require.Equal(t, "Shelley", EraName(EraShelley))
	require.Equal(t, "Allegra", EraName(EraAllegra))
	require.Equal(t, "Mary", EraName(EraMary))
	require.Equal(t, "Alonzo", EraName(EraAlonzo))
	require.Equal(t, "Babbage", EraName(EraBabbage))
	require.Equal(t, "Conway", EraName(EraConway))
	require.Contains(t, EraName(99), "Unknown")
}

func TestParseSnapShotsAcceptsTwoElementSnapshots(t *testing.T) {
	t.Parallel()

	emptyMap, err := cbor.Encode(map[uint64]uint64{})
	require.NoError(t, err)

	snapshot, err := cbor.Encode([]any{
		cbor.RawMessage(emptyMap),
		cbor.RawMessage(emptyMap),
	})
	require.NoError(t, err)

	data, err := cbor.Encode([]any{
		cbor.RawMessage(snapshot),
		cbor.RawMessage(snapshot),
		cbor.RawMessage(snapshot),
	})
	require.NoError(t, err)

	snapshots, err := ParseSnapShots(data)
	require.NoError(t, err)
	require.NotNil(t, snapshots)
	require.Empty(t, snapshots.Mark.PoolParams)
	require.Empty(t, snapshots.Set.PoolParams)
	require.Empty(t, snapshots.Go.PoolParams)
}

func TestParseSnapShotsAcceptsUTxOHDStakeWithPool(t *testing.T) {
	t.Parallel()

	credHash := toFixed28([]byte("credential hash for snapshot"))
	poolHash := toFixed28([]byte("pool hash for snapshot"))
	stakeMap := encodeCredentialMapEntry(
		t,
		[]any{uint64(0), credHash[:]},
		[]any{uint64(42), poolHash[:]},
	)

	emptyMap, err := cbor.Encode(map[uint64]uint64{})
	require.NoError(t, err)

	snapshot, err := cbor.Encode([]any{
		cbor.RawMessage(stakeMap),
		cbor.RawMessage(emptyMap),
	})
	require.NoError(t, err)

	data, err := cbor.Encode([]any{
		cbor.RawMessage(snapshot),
		cbor.RawMessage(snapshot),
		cbor.RawMessage(snapshot),
	})
	require.NoError(t, err)

	snapshots, err := ParseSnapShots(data)
	require.NoError(t, err)

	credKey := hex.EncodeToString(credHash[:])
	require.Equal(t, uint64(42), snapshots.Mark.Stake[credKey])
	require.Equal(t, poolHash[:], snapshots.Mark.Delegations[credKey])
	require.Equal(t, uint64(42), snapshots.Set.Stake[credKey])
	require.Equal(t, poolHash[:], snapshots.Set.Delegations[credKey])
	require.Equal(t, uint64(42), snapshots.Go.Stake[credKey])
	require.Equal(t, poolHash[:], snapshots.Go.Delegations[credKey])
}

func TestParsePoolParamsMapAcceptsPoolDistrEntry(t *testing.T) {
	t.Parallel()

	poolHash := toFixed28([]byte("pool hash for distribution"))
	vrfHash := [32]byte{}
	copy(vrfHash[:], []byte("vrf hash for distribution entry"))

	poolMap := encodeCredentialMapEntry(
		t,
		poolHash[:],
		[]any{
			uint64(0),
			[]any{uint64(0), uint64(1)},
			[]any{},
			uint64(0),
			vrfHash[:],
			uint64(0),
			uint64(0),
			[]any{uint64(0), uint64(1)},
			uint64(0),
			[]any{},
		},
	)

	pools, err := parsePoolParamsMap(poolMap)
	require.NoError(t, err)

	pool := pools[hex.EncodeToString(poolHash[:])]
	require.NotNil(t, pool)
	require.Equal(t, poolHash[:], pool.PoolKeyHash)
	require.Equal(t, vrfHash[:], pool.VrfKeyHash)
}

func TestParseW32SnapshotPoolParamsCarriesLeiosKeyIntoStakeRows(
	t *testing.T,
) {
	t.Parallel()

	poolHash := toFixed28([]byte("w32 snapshot pool key hash"))
	credentialHash := toFixed28([]byte("w32 snapshot credential"))
	rewardHash := toFixed28([]byte("w32 snapshot reward account"))
	vrfHash := [32]byte{}
	copy(vrfHash[:], []byte("w32 snapshot vrf key hash"))
	publicKey := make([]byte, 96)
	copy(publicKey, []byte("w32 snapshot Leios public key"))
	possessionProof := make([]byte, 48)
	copy(possessionProof, []byte("w32 snapshot Leios possession proof"))

	poolMap := encodeCredentialMapEntry(
		t,
		poolHash[:],
		[]any{
			uint64(42),
			&cbor.Rat{Rat: big.NewRat(1, 1)},
			[]any{},
			uint64(0),
			vrfHash[:],
			[]any{[]any{publicKey, possessionProof}, uint64(7)},
			uint64(1),
			uint64(2),
			&cbor.Rat{Rat: big.NewRat(1, 100)},
			uint64(1),
			[]any{uint64(0), rewardHash[:]},
		},
	)
	pools, err := parsePoolParamsMap(poolMap)
	require.NoError(t, err)
	pool := pools[hex.EncodeToString(poolHash[:])]
	require.NotNil(t, pool)
	require.Equal(t, publicKey, pool.LeiosKeyPublic)
	require.Equal(t, possessionProof, pool.LeiosKeyPossessionProof)
	require.NotNil(t, pool.LeiosKeyRegistrationEpoch)
	require.Equal(t, uint64(7), *pool.LeiosKeyRegistrationEpoch)

	credentialHex := hex.EncodeToString(credentialHash[:])
	rows := AggregatePoolStake(&ParsedSnapShot{
		Stake:       map[string]uint64{credentialHex: 42},
		Delegations: map[string][]byte{credentialHex: poolHash[:]},
		PoolParams:  pools,
	}, 9, "mark", 99)
	require.Len(t, rows, 1)
	require.NotNil(t, rows[0].LeiosKeyRegistrationEpoch)
	require.Equal(t, uint64(7), *rows[0].LeiosKeyRegistrationEpoch)
	require.Equal(t, publicKey, rows[0].LeiosKeyPublic)
	require.Equal(t, possessionProof, rows[0].LeiosKeyPossessionProof)

	// The row owns its bytes independently of the decoded pool parameters.
	pool.LeiosKeyPublic[0] ^= 0xff
	pool.LeiosKeyPossessionProof[0] ^= 0xff
	require.Equal(t, publicKey, rows[0].LeiosKeyPublic)
	require.Equal(t, possessionProof, rows[0].LeiosKeyPossessionProof)
}

// TestAggregatePoolStakeKeepsZeroStakePool is the blinklabs-io/dingo#4152
// regression: a pool whose only delegator has zero stake at snapshot time
// (registered and delegated, but with no lovelace behind the credential --
// e.g. its UTxOs spent and no reward balance) must still get a
// PoolStakeSnapshot row. A real cardano-node reports such a pool in
// GetStakeDistribution with an explicit zero fraction rather than omitting
// it (confirmed live against a real Preview cardano-node during #4152's
// investigation), and dingo's own live snapshot-rotation path
// (calculateLiveStakeDistributionInTxn) already does the same. Before the
// fix, AggregatePoolStake silently dropped this pool's row entirely --
// exactly the "pool present on a real node, completely absent from dingo's
// answer" symptom #4152 reported for 36 real Preview pools after a Mithril
// bootstrap.
func TestAggregatePoolStakeKeepsZeroStakePool(t *testing.T) {
	t.Parallel()

	zeroStakePool := toFixed28([]byte("4152 zero stake pool"))
	zeroStakeCred := toFixed28([]byte("4152 zero stake delegator"))

	mixedPool := toFixed28([]byte("4152 mixed pool"))
	mixedZeroCred := toFixed28([]byte("4152 mixed pool zero delegator"))
	mixedPaidCred := toFixed28([]byte("4152 mixed pool paid delegator"))

	zeroStakeCredHex := hex.EncodeToString(zeroStakeCred[:])
	mixedZeroCredHex := hex.EncodeToString(mixedZeroCred[:])
	mixedPaidCredHex := hex.EncodeToString(mixedPaidCred[:])

	snap := &ParsedSnapShot{
		// zeroStakeCredHex is deliberately absent from Stake entirely (as
		// well as being reachable with an explicit 0 entry, exercised by
		// mixedZeroCredHex below) -- both are ways a real snapshot can leave
		// a delegated credential with nothing behind it.
		Stake: map[string]uint64{
			mixedZeroCredHex: 0,
			mixedPaidCredHex: 42,
		},
		Delegations: map[string][]byte{
			zeroStakeCredHex: zeroStakePool[:],
			mixedZeroCredHex: mixedPool[:],
			mixedPaidCredHex: mixedPool[:],
		},
	}

	rows := AggregatePoolStake(snap, 9, "mark", 99)

	byPool := make(map[string]*models.PoolStakeSnapshot, len(rows))
	for _, row := range rows {
		byPool[hex.EncodeToString(row.PoolKeyHash)] = row
	}

	zeroRow := byPool[hex.EncodeToString(zeroStakePool[:])]
	require.NotNil(
		t,
		zeroRow,
		"pool with a single zero-stake delegator must still get a row",
	)
	require.Equal(t, uint64(0), uint64(zeroRow.TotalStake))
	require.Equal(t, uint64(1), zeroRow.DelegatorCount)

	mixedRow := byPool[hex.EncodeToString(mixedPool[:])]
	require.NotNil(t, mixedRow)
	require.Equal(t, uint64(42), uint64(mixedRow.TotalStake))
	require.Equal(
		t,
		uint64(2),
		mixedRow.DelegatorCount,
		"a zero-stake delegator must still be counted alongside a paid one",
	)
}

func TestParseW32SnapshotPoolParamsRejectsMalformedLeiosKey(t *testing.T) {
	t.Parallel()

	poolHash := toFixed28([]byte("malformed snapshot pool key"))
	vrfHash := make([]byte, 32)
	rewardHash := toFixed28([]byte("malformed snapshot reward"))
	record, err := cbor.Encode([]any{
		uint64(42),
		&cbor.Rat{Rat: big.NewRat(1, 1)},
		[]any{},
		uint64(0),
		vrfHash,
		[]any{[]any{[]byte{0x01}, make([]byte, 48)}},
		uint64(1),
		uint64(2),
		&cbor.Rat{Rat: big.NewRat(1, 100)},
		uint64(1),
		[]any{uint64(0), rewardHash[:]},
	})
	require.NoError(t, err)
	fields, err := decodeRawArray(record)
	require.NoError(t, err)
	_, err = parseSnapshotPoolParams(poolHash[:], fields)
	require.ErrorContains(t, err, "decoding Leios key")
}

func TestParseActivePoolDistribution(t *testing.T) {
	t.Parallel()

	poolHash := toFixed28([]byte("active pool distribution"))
	vrfHash := [32]byte{}
	copy(vrfHash[:], []byte("active vrf key hash"))

	data := encodeCredentialMapEntry(
		t,
		poolHash[:],
		[]any{
			&cbor.Rat{Rat: big.NewRat(3, 10)},
			vrfHash[:],
		},
	)

	pools, err := ParseActivePoolDistribution(data)
	require.NoError(t, err)
	require.Len(t, pools, 1)
	require.Equal(t, poolHash[:], pools[0].PoolKeyHash)
	require.Equal(t, uint64(3), pools[0].StakeNumerator)
	require.Equal(t, uint64(10), pools[0].StakeDenominator)
	require.Equal(t, vrfHash[:], pools[0].VrfKeyHash)
}

func TestParseActivePoolDistributionContainer(t *testing.T) {
	t.Parallel()

	poolHash := toFixed28([]byte("active pool distribution"))
	vrfHash := [32]byte{}
	copy(vrfHash[:], []byte("active vrf key hash"))

	poolMap := encodeCredentialMapEntry(
		t,
		poolHash[:],
		[]any{
			&cbor.Rat{Rat: big.NewRat(3, 10)},
			uint64(3),
			vrfHash[:],
		},
	)
	totalStake, err := cbor.Encode(uint64(10))
	require.NoError(t, err)
	data := append([]byte{0x82}, poolMap...)
	data = append(data, totalStake...)

	pools, err := ParseActivePoolDistribution(data)
	require.NoError(t, err)
	require.Len(t, pools, 1)
	require.Equal(t, poolHash[:], pools[0].PoolKeyHash)
	require.Equal(t, uint64(3), pools[0].StakeNumerator)
	require.Equal(t, uint64(10), pools[0].StakeDenominator)
	require.Equal(t, vrfHash[:], pools[0].VrfKeyHash)
}

func TestParseW32ActivePoolDistributionCarriesLeiosKey(t *testing.T) {
	t.Parallel()

	poolHash := toFixed28([]byte("w32 active pool distribution"))
	vrfHash := make([]byte, 32)
	copy(vrfHash, []byte("w32 active vrf key hash"))
	publicKey := make([]byte, 96)
	copy(publicKey, []byte("w32 active Leios public key"))
	possessionProof := make([]byte, 48)
	copy(possessionProof, []byte("w32 active Leios proof"))

	poolMap := encodeCredentialMapEntry(
		t,
		poolHash[:],
		[]any{
			&cbor.Rat{Rat: big.NewRat(3, 10)},
			uint64(3),
			vrfHash,
			[]any{[]any{publicKey, possessionProof}, uint64(7)},
		},
	)
	totalStake, err := cbor.Encode(uint64(10))
	require.NoError(t, err)
	data := append([]byte{0x82}, poolMap...)
	data = append(data, totalStake...)

	pools, err := ParseActivePoolDistribution(data)
	require.NoError(t, err)
	require.Len(t, pools, 1)
	require.Equal(t, publicKey, pools[0].LeiosKeyPublic)
	require.Equal(t, possessionProof, pools[0].LeiosKeyPossessionProof)
	require.NotNil(t, pools[0].LeiosKeyRegistrationEpoch)
	require.Equal(t, uint64(7), *pools[0].LeiosKeyRegistrationEpoch)
	rows := ActivePoolDistributionSnapshots(pools, 12, 120)
	require.Len(t, rows, 1)
	require.Equal(t, publicKey, rows[0].LeiosKeyPublic)
	require.Equal(t, possessionProof, rows[0].LeiosKeyPossessionProof)
	require.NotNil(t, rows[0].LeiosKeyRegistrationEpoch)
	require.Equal(t, uint64(7), *rows[0].LeiosKeyRegistrationEpoch)
}

func TestParseActivePoolDistributionContainerRejectsStakeMismatch(
	t *testing.T,
) {
	t.Parallel()

	poolHash := toFixed28([]byte("active pool distribution"))
	vrfHash := [32]byte{}
	copy(vrfHash[:], []byte("active vrf key hash"))

	poolMap := encodeCredentialMapEntry(
		t,
		poolHash[:],
		[]any{
			&cbor.Rat{Rat: big.NewRat(3, 10)},
			uint64(4),
			vrfHash[:],
		},
	)
	totalStake, err := cbor.Encode(uint64(10))
	require.NoError(t, err)
	data := append([]byte{0x82}, poolMap...)
	data = append(data, totalStake...)

	_, err = ParseActivePoolDistribution(data)
	require.ErrorContains(t, err, "does not match active stake")
}

func TestParseActivePoolDistributionContainerAllowsZeroStake(
	t *testing.T,
) {
	t.Parallel()

	poolHash := toFixed28([]byte("active pool distribution"))
	vrfHash := [32]byte{}
	copy(vrfHash[:], []byte("active vrf key hash"))

	poolMap := encodeCredentialMapEntry(
		t,
		poolHash[:],
		[]any{
			&cbor.Rat{Rat: big.NewRat(0, 1)},
			uint64(0),
			vrfHash[:],
		},
	)
	totalStake, err := cbor.Encode(uint64(10))
	require.NoError(t, err)
	data := append([]byte{0x82}, poolMap...)
	data = append(data, totalStake...)

	pools, err := ParseActivePoolDistribution(data)
	require.NoError(t, err)
	require.Len(t, pools, 1)
	require.Equal(t, uint64(0), pools[0].StakeNumerator)
	require.Equal(t, uint64(10), pools[0].StakeDenominator)
}

func TestParseActivePoolDistributionRejectsMalformedEntry(t *testing.T) {
	t.Parallel()

	poolHash := toFixed28([]byte("active pool distribution"))
	data := encodeCredentialMapEntry(
		t,
		poolHash[:],
		[]any{uint64(1)},
	)

	_, err := ParseActivePoolDistribution(data)
	require.ErrorContains(t, err, "expected 2, 3, or 4")
}

func TestVerifySnapshotDigest(t *testing.T) {
	t.Parallel()

	content := []byte("test snapshot content for hashing")
	h := sha256.Sum256(content)
	expectedDigest := hex.EncodeToString(h[:])

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "test.tar.zst")
	err := os.WriteFile(archivePath, content, 0o640)
	require.NoError(t, err)

	err = VerifySnapshotDigest(archivePath, expectedDigest)
	require.NoError(t, err)
}

func TestVerifySnapshotDigestMismatch(t *testing.T) {
	t.Parallel()

	content := []byte("test snapshot content")

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "test.tar.zst")
	err := os.WriteFile(archivePath, content, 0o640)
	require.NoError(t, err)

	err = VerifySnapshotDigest(
		archivePath,
		"0000000000000000000000000000000"+
			"000000000000000000000000000000000",
	)
	require.ErrorContains(t, err, "mismatch")
}

func TestVerifyChecksumFileNoChecksum(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	// No .checksum file - should succeed (not an error)
	err = VerifyChecksumFile(lstatePath)
	require.NoError(t, err)
}

func TestVerifyChecksumFileEmptyChecksum(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(checksumPath, []byte("  \n"), 0o640)
	require.NoError(t, err)

	// Empty checksum - should succeed (skip verification)
	err = VerifyChecksumFile(lstatePath)
	require.NoError(t, err)
}

func TestVerifyChecksumFileMismatch(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(
		checksumPath,
		[]byte("00000000"),
		0o640,
	)
	require.NoError(t, err)

	err = VerifyChecksumFile(lstatePath)
	require.ErrorContains(t, err, "mismatch")
}

func TestExtractPParamsDataBabbageGovState(t *testing.T) {
	t.Parallel()

	pparams := testBabbagePParams()
	pparamsData, err := cbor.Encode(pparams)
	require.NoError(t, err)
	previous := *pparams
	previous.MinFeeA++
	previousData, err := cbor.Encode(&previous)
	require.NoError(t, err)

	govStateData, err := cbor.Encode([]any{
		uint64(1),
		uint64(2),
		cbor.RawMessage(pparamsData),
		cbor.RawMessage(previousData),
	})
	require.NoError(t, err)

	got, gotPrevious, err := extractPParamsData(EraBabbage, govStateData)
	require.NoError(t, err)
	require.Equal(t, pparamsData, []byte(got))
	require.Equal(t, previousData, []byte(gotPrevious))
}

func TestExtractPParamsDataConwayGovStateMap(t *testing.T) {
	t.Parallel()

	pparams := testConwayPParams()
	pparamsData, err := cbor.Encode(pparams)
	require.NoError(t, err)
	previous := *pparams
	previous.MinFeeA++
	previousData, err := cbor.Encode(&previous)
	require.NoError(t, err)

	govStateData, err := cbor.Encode(map[uint64]any{
		0: uint64(1),
		1: uint64(2),
		2: uint64(3),
		3: cbor.RawMessage(pparamsData),
		4: cbor.RawMessage(previousData),
	})
	require.NoError(t, err)

	got, gotPrevious, err := extractPParamsData(EraConway, govStateData)
	require.NoError(t, err)
	require.Equal(t, pparamsData, []byte(got))
	require.Equal(t, previousData, []byte(gotPrevious))
}

func TestExtractPParamsDataDetectsEraSpecificType(t *testing.T) {
	t.Parallel()

	alonzoData, err := cbor.Encode(testBabbagePParams())
	require.NoError(t, err)
	conwayData, err := cbor.Encode(testConwayPParams())
	require.NoError(t, err)

	govStateData, err := cbor.Encode([]any{
		uint64(1),
		uint64(2),
		cbor.RawMessage(alonzoData),
		cbor.RawMessage(conwayData),
	})
	require.NoError(t, err)

	got, _, err := extractPParamsData(EraConway, govStateData)
	require.NoError(t, err)
	require.Equal(t, conwayData, []byte(got))
}

func testBabbagePParams() *lbabbage.BabbageProtocolParameters {
	return &lbabbage.BabbageProtocolParameters{
		MinFeeA:            44,
		MinFeeB:            155381,
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
		KeyDeposit:         2000000,
		PoolDeposit:        500000000,
		MaxEpoch:           18,
		NOpt:               500,
		A0:                 &cbor.Rat{Rat: big.NewRat(3, 10)},
		Rho:                &cbor.Rat{Rat: big.NewRat(3, 1000)},
		Tau:                &cbor.Rat{Rat: big.NewRat(1, 5)},
		ProtocolMajor:      8,
		ProtocolMinor:      0,
		MinPoolCost:        340000000,
		AdaPerUtxoByte:     4310,
		CostModels:         map[uint][]int64{1: {0}},
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  &cbor.Rat{Rat: big.NewRat(577, 10000)},
			StepPrice: &cbor.Rat{Rat: big.NewRat(721, 10000000)},
		},
		MaxTxExUnits: lcommon.ExUnits{
			Memory: 10000000,
			Steps:  10000000000,
		},
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 50000000,
			Steps:  40000000000,
		},
		MaxValueSize:         5000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
	}
}

func testConwayPParams() *lconway.ConwayProtocolParameters {
	return &lconway.ConwayProtocolParameters{
		MinFeeA:            44,
		MinFeeB:            155381,
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
		KeyDeposit:         2000000,
		PoolDeposit:        500000000,
		MaxEpoch:           18,
		NOpt:               500,
		A0:                 &cbor.Rat{Rat: big.NewRat(3, 10)},
		Rho:                &cbor.Rat{Rat: big.NewRat(3, 1000)},
		Tau:                &cbor.Rat{Rat: big.NewRat(1, 5)},
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 10,
			Minor: 0,
		},
		MinPoolCost:    340000000,
		AdaPerUtxoByte: 4310,
		CostModels:     map[uint][]int64{1: {0}},
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  &cbor.Rat{Rat: big.NewRat(577, 10000)},
			StepPrice: &cbor.Rat{Rat: big.NewRat(721, 10000000)},
		},
		MaxTxExUnits: lcommon.ExUnits{
			Memory: 10000000,
			Steps:  10000000000,
		},
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 50000000,
			Steps:  40000000000,
		},
		MaxValueSize:         5000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
		PoolVotingThresholds: lconway.PoolVotingThresholds{
			MotionNoConfidence:    cbor.Rat{Rat: big.NewRat(1, 2)},
			CommitteeNormal:       cbor.Rat{Rat: big.NewRat(1, 2)},
			CommitteeNoConfidence: cbor.Rat{Rat: big.NewRat(1, 2)},
			HardForkInitiation:    cbor.Rat{Rat: big.NewRat(1, 2)},
			PpSecurityGroup:       cbor.Rat{Rat: big.NewRat(1, 2)},
		},
		DRepVotingThresholds: lconway.DRepVotingThresholds{
			MotionNoConfidence:    cbor.Rat{Rat: big.NewRat(1, 2)},
			CommitteeNormal:       cbor.Rat{Rat: big.NewRat(1, 2)},
			CommitteeNoConfidence: cbor.Rat{Rat: big.NewRat(1, 2)},
			UpdateToConstitution:  cbor.Rat{Rat: big.NewRat(1, 2)},
			HardForkInitiation:    cbor.Rat{Rat: big.NewRat(1, 2)},
			PpNetworkGroup:        cbor.Rat{Rat: big.NewRat(1, 2)},
			PpEconomicGroup:       cbor.Rat{Rat: big.NewRat(1, 2)},
			PpTechnicalGroup:      cbor.Rat{Rat: big.NewRat(1, 2)},
			PpGovGroup:            cbor.Rat{Rat: big.NewRat(1, 2)},
			TreasuryWithdrawal:    cbor.Rat{Rat: big.NewRat(1, 2)},
		},
		MinCommitteeSize:        5,
		CommitteeTermLimit:      146,
		GovActionValidityPeriod: 20,
		GovActionDeposit:        100000000000,
		DRepDeposit:             500000000,
		DRepInactivityPeriod:    20,
		MinFeeRefScriptCostPerByte: &cbor.Rat{
			Rat: big.NewRat(1, 1),
		},
	}
}

func TestVerifyChecksumFileWrongLength(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	// Valid hex but wrong length (5 bytes instead of 4)
	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(
		checksumPath,
		[]byte("aabbccdd00"),
		0o640,
	)
	require.NoError(t, err)

	err = VerifyChecksumFile(lstatePath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected 8")
}

func TestVerifyChecksumFileInvalidHex(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	// Valid length but invalid hex characters
	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(
		checksumPath,
		[]byte("GGGGGGGG"),
		0o640,
	)
	require.NoError(t, err)

	err = VerifyChecksumFile(lstatePath)
	require.ErrorContains(t, err, "invalid hex")
}

func TestVerifyChecksumFileValid(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	content := []byte("test data for crc32")
	err := os.WriteFile(lstatePath, content, 0o640)
	require.NoError(t, err)

	// Compute the actual CRC32 for the content
	h := crc32.NewIEEE()
	_, err = h.Write(content)
	require.NoError(t, err)
	checksum := fmt.Sprintf("%08x", h.Sum32())

	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(
		checksumPath,
		[]byte(checksum),
		0o640,
	)
	require.NoError(t, err)

	err = VerifyChecksumFile(lstatePath)
	require.NoError(t, err)
}
