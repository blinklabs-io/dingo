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
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

func newSkippedEntriesImportConfig(
	t *testing.T,
	state *RawLedgerState,
) ImportConfig {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })
	return ImportConfig{
		Database: db,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		State:    state,
	}
}

// badCredentialMap is a one-entry map whose key is not a credential, so a
// credential-keyed parser skips it.
func badCredentialMap(t *testing.T, value any) []byte {
	t.Helper()
	return encodeCredentialMapEntry(t, uint64(1), value)
}

// TestSeedImportedRewardBasisRejectsMalformedDelegationPoolKey covers the
// pool set the imported reward basis is derived for. Dropping a delegation
// whose pool key is malformed would leave that pool out of the basis.
func TestSeedImportedRewardBasisRejectsMalformedDelegationPoolKey(
	t *testing.T,
) {
	t.Parallel()
	cfg := newSkippedEntriesImportConfig(t, &RawLedgerState{Epoch: 2})
	snapshots := &ParsedSnapShots{
		Mark: ParsedSnapShot{Delegations: map[string][]byte{
			"aa": bytes.Repeat([]byte{0x01}, credentialHashSize-1),
		}},
	}
	err := seedImportedRewardBasis(context.Background(), cfg, snapshots, 2, 100)
	require.ErrorContains(t, err, "seeding imported reward basis")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
}

// TestSynthesizeRetiredScheduledPoolsRejectsMalformedEntry covers the active
// pool distribution a retired-but-scheduled pool is synthesized from. A
// skipped entry would leave a pool the leader schedule elects with no VRF
// key to check its blocks against.
func TestSynthesizeRetiredScheduledPoolsRejectsMalformedEntry(t *testing.T) {
	t.Parallel()
	poolKey := bytes.Repeat([]byte{0x02}, credentialHashSize)
	vrfKey := bytes.Repeat([]byte{0x03}, 32)
	for _, tc := range []struct {
		name  string
		entry ParsedActivePoolStake
		want  string
	}{
		{
			name: "pool key hash",
			entry: ParsedActivePoolStake{
				PoolKeyHash: poolKey[:credentialHashSize-1],
				VrfKeyHash:  vrfKey,
			},
			want: "pool key hash: invalid blake2b-224 hash",
		},
		{
			name: "VRF key hash",
			entry: ParsedActivePoolStake{
				PoolKeyHash: poolKey,
				VrfKeyHash:  vrfKey[:31],
			},
			want: "VRF key hash: invalid blake2b-256 hash",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := newSkippedEntriesImportConfig(t, &RawLedgerState{Epoch: 2})
			err := synthesizeRetiredScheduledPools(
				context.Background(),
				cfg,
				[]ParsedActivePoolStake{tc.entry},
				2,
				100,
			)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// TestImportSnapShotsRejectsSkippedStakeEntry drives the stake snapshot
// import over a mark snapshot with one undecodable stake entry. Skipping it
// drops that credential's stake from the leader schedule and rewards.
func TestImportSnapShotsRejectsSkippedStakeEntry(t *testing.T) {
	t.Parallel()
	emptyMap, err := cbor.Encode(map[uint64]uint64{})
	require.NoError(t, err)
	snapshot, err := cbor.Encode([]any{
		cbor.RawMessage(badCredentialMap(t, []any{
			uint64(5),
			bytes.Repeat([]byte{0x04}, credentialHashSize),
		})),
		cbor.RawMessage(emptyMap),
	})
	require.NoError(t, err)
	data, err := cbor.Encode([]any{
		cbor.RawMessage(snapshot),
		cbor.RawMessage(snapshot),
		cbor.RawMessage(snapshot),
	})
	require.NoError(t, err)
	cfg := newSkippedEntriesImportConfig(
		t,
		&RawLedgerState{Epoch: 2, SnapShotsData: data},
	)

	err = importSnapShots(
		context.Background(),
		cfg,
		100,
		func(ImportProgress) {},
		false,
	)
	require.ErrorContains(t, err, "stake-with-pool map: skipped 1 of 1 entries")
	require.ErrorContains(t, err, "cannot be imported with skipped entries")
}

// TestImportCertStateRejectsSkippedAccount drives the cert state import over
// a DState with one undecodable account. Skipping it drops that account's
// reward balance, deposit and delegation.
func TestImportCertStateRejectsSkippedAccount(t *testing.T) {
	t.Parallel()
	emptyMap, err := cbor.Encode(map[uint64]uint64{})
	require.NoError(t, err)
	vstate, err := cbor.Encode([]any{cbor.RawMessage(emptyMap)})
	require.NoError(t, err)
	pstate, err := cbor.Encode([]any{})
	require.NoError(t, err)
	dstate, err := cbor.Encode([]any{
		cbor.RawMessage(badCredentialMap(t, []any{uint64(0), uint64(0)})),
	})
	require.NoError(t, err)
	data, err := cbor.Encode([]any{
		cbor.RawMessage(vstate),
		cbor.RawMessage(pstate),
		cbor.RawMessage(dstate),
	})
	require.NoError(t, err)
	cfg := newSkippedEntriesImportConfig(
		t,
		&RawLedgerState{Epoch: 2, CertStateData: data},
	)

	_, err = importCertState(
		context.Background(),
		cfg,
		100,
		func(ImportProgress) {},
	)
	require.ErrorContains(t, err, "skipped 1")
	require.ErrorContains(t, err, "skipped or partially decoded entries")
}

func TestParseSnapShotsReportsUndecodableFee(t *testing.T) {
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
		"not a coin",
	})
	require.NoError(t, err)

	snapshots, err := ParseSnapShots(data)
	require.ErrorContains(t, err, "decoding fee")
	require.NotNil(t, snapshots)
}

func TestParsePoolRegistrationFieldsRejectMalformedValues(t *testing.T) {
	t.Parallel()
	owner := bytes.Repeat([]byte{0x05}, credentialHashSize)
	t.Run("owner", func(t *testing.T) {
		t.Parallel()
		data, err := cbor.Encode([]any{owner, owner[:credentialHashSize-1]})
		require.NoError(t, err)
		_, err = parsePoolOwners(data)
		require.ErrorContains(t, err, "owner 1 hash is 27 bytes")
	})
	t.Run("margin", func(t *testing.T) {
		t.Parallel()
		// A margin read as 0/1 would pay the pool operator no margin.
		data, err := cbor.Encode([]any{
			owner,
			bytes.Repeat([]byte{0x07}, 32),
			uint64(1),
			uint64(1),
			"not a rational",
			append([]byte{0xe0}, owner...),
			[]any{},
		})
		require.NoError(t, err)
		_, err = parsePoolParams(owner, data)
		require.ErrorContains(t, err, "decoding margin")
	})
	t.Run("relay", func(t *testing.T) {
		t.Parallel()
		data, err := cbor.Encode([]any{[]any{}})
		require.NoError(t, err)
		_, err = parseRelays(data)
		require.ErrorContains(t, err, "relay 0")
	})
	t.Run("short metadata hash is preserved", func(t *testing.T) {
		t.Parallel()
		data, err := cbor.Encode([]any{
			"https://example.invalid/pool.json",
			bytes.Repeat([]byte{0x06}, 31),
		})
		require.NoError(t, err)
		var pool ParsedPool
		err = parsePoolMetadata(data, &pool)
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte{0x06}, 31), pool.MetadataHash)
	})
	t.Run("absent metadata", func(t *testing.T) {
		t.Parallel()
		data, err := cbor.Encode(nil)
		require.NoError(t, err)
		var pool ParsedPool
		require.NoError(t, parsePoolMetadata(data, &pool))
	})
}

// TestDevnetSnapshotParsesWithoutSkippedEntries is the control for the
// fail-closed import: a real cardano-node ledger state parses with no
// skipped entry in any of the sections the importer now refuses to import
// partially.
func TestDevnetSnapshotParsesWithoutSkippedEntries(t *testing.T) {
	t.Parallel()
	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)

	_, err = ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err)
	_, err = ParseCertState(state.CertStateData)
	require.NoError(t, err)
	govState, err := ParseGovState(state.GovStateData, state.EraIndex)
	require.NoError(t, err)
	require.NotNil(t, govState)
	_, err = ParseActivePoolDistribution(state.PoolDistrData)
	require.NoError(t, err)
}

// encodePoolUint64Map builds a pool key hash -> uint64 map with entries in
// the given order.
func encodePoolUint64Map(t *testing.T, keys [][]byte, value uint64) []byte {
	t.Helper()
	data := []byte{0xa0 + byte(len(keys))}
	for _, key := range keys {
		k, err := cbor.Encode(key)
		require.NoError(t, err)
		v, err := cbor.Encode(value)
		require.NoError(t, err)
		data = append(data, k...)
		data = append(data, v...)
	}
	return data
}

// TestPoolScalarMapsRejectMalformedKeys covers the PState deposit and
// retirement maps. A malformed key used to be dropped from the deposit map,
// and made the whole retirement map unrecognizable, so every scheduled
// retirement in it was lost.
func TestPoolScalarMapsRejectMalformedKeys(t *testing.T) {
	t.Parallel()
	poolKey := bytes.Repeat([]byte{0x08}, credentialHashSize)
	short := poolKey[:credentialHashSize-1]
	placeholder, err := cbor.Encode(map[uint64]uint64{})
	require.NoError(t, err)

	t.Run("deposits", func(t *testing.T) {
		t.Parallel()
		pools := []ParsedPool{{PoolKeyHash: poolKey}}
		err := mergePoolDeposits(
			pools,
			[][]byte{
				placeholder,
				encodePoolUint64Map(t, [][]byte{poolKey, short}, 500_000_000),
			},
			0,
		)
		require.ErrorContains(t, err, "pool deposit map: 1 malformed entries")
	})
	t.Run("retirements", func(t *testing.T) {
		t.Parallel()
		pools := []ParsedPool{{PoolKeyHash: poolKey}}
		_, err := mergePoolRetirements(
			pools,
			[][]byte{
				placeholder,
				encodePoolUint64Map(t, [][]byte{poolKey, short}, 10),
			},
			[]int{1},
		)
		require.ErrorContains(
			t,
			err,
			"pool retirement map: 1 malformed entries",
		)
	})
	t.Run("well-formed retirements", func(t *testing.T) {
		t.Parallel()
		pools := []ParsedPool{{PoolKeyHash: poolKey}}
		retirements, err := mergePoolRetirements(
			pools,
			[][]byte{
				placeholder,
				encodePoolUint64Map(t, [][]byte{poolKey}, 10),
			},
			[]int{1},
		)
		require.NoError(t, err)
		require.Equal(t, map[uint64][][]byte{10: {poolKey}}, retirements)
	})
}

// TestParseDRepMapRejectsMalformedState covers DRepState fields that used to
// be zeroed when they failed to decode, importing a DRep whose expiry,
// anchor or deposit differ from the ledger's.
func TestParseDRepMapRejectsMalformedState(t *testing.T) {
	t.Parallel()
	credential := []any{
		uint64(0),
		bytes.Repeat([]byte{0x09}, credentialHashSize),
	}
	anchor := func(hashLen int) []any {
		return []any{
			"https://example.invalid/drep.json",
			bytes.Repeat([]byte{0x0a}, hashLen),
		}
	}
	for _, tc := range []struct {
		name  string
		state []any
		ok    bool
	}{
		{name: "deposit", state: []any{uint64(10), nil, "not a coin"}},
		{name: "expiry", state: []any{"not an epoch", nil, uint64(1)}},
		{name: "anchor hash", state: []any{uint64(10), anchor(31), uint64(1)}},
		{name: "short state", state: []any{uint64(10), nil}},
		{
			name:  "well-formed",
			state: []any{uint64(10), anchor(32), uint64(500_000_000), []any{}},
			ok:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dreps, err := parseDRepMap(
				encodeCredentialMapEntry(t, credential, tc.state),
			)
			if tc.ok {
				require.NoError(t, err)
				require.Len(t, dreps, 1)
				require.Equal(t, uint64(10), dreps[0].ExpiryEpoch)
				require.Equal(t, uint64(500_000_000), dreps[0].Deposit)
				require.Len(t, dreps[0].AnchorHash, 32)
				return
			}
			require.ErrorContains(t, err, "drep map: skipped 1 of 1 entries")
			require.Empty(t, dreps)
		})
	}
}

func TestParseRelayDecodesStrictly(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		relay []any
		want  string
	}{
		{name: "null port", relay: []any{uint64(0), nil, []byte{127, 0, 0, 1}, nil}},
		{name: "ipv4 length", relay: []any{uint64(0), uint64(3001), []byte{127, 0, 1}}, want: "relay ipv4 is 3 bytes"},
		{name: "port", relay: []any{uint64(1), "not a port", "relay.example"}, want: "relay port"},
		{name: "unknown type", relay: []any{uint64(9)}, want: "unknown relay type 9"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data, err := cbor.Encode(tc.relay)
			require.NoError(t, err)
			_, err = parseRelay(data)
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.want)
		})
	}
}
