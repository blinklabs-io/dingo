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
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

// importTestEpochLength is the epoch length, in slots, of the fixtures.
const importTestEpochLength uint = 1_000

const (
	fixtureAnchorSlot  = uint64(100_000)
	fixtureCatchUpSlot = uint64(200_000)
)

var (
	fixtureStakingKey = bytes.Repeat([]byte{0xbb}, 28)
	// fixtureAnchorKey is the one account of the anchor snapshot, which
	// the later snapshots no longer list.
	fixtureAnchorKey = bytes.Repeat([]byte{0xcc}, 28)
)

// snapshotConfig is the import of the snapshot at tipSlot. The anchor
// snapshot has one output and a small reward; the later one has two outputs
// and a larger reward, so a catch-up that kept the anchor's state differs.
func snapshotConfig(
	t *testing.T,
	db *database.Database,
	tipSlot uint64,
	reconcile bool,
	set UtxoSetLoader,
) ImportConfig {
	t.Helper()
	addr := buildShelleyAddr(
		0, 1,
		bytes.Repeat([]byte{0x83}, 28),
		bytes.Repeat([]byte{0x84}, 28),
	)
	amounts := []uint64{5_000_000}
	reward := uint64(4_000_000)
	if tipSlot == fixtureCatchUpSlot {
		amounts = []uint64{5_000_000, 7_000_000}
		reward = 9_000_000
	}
	nonce := bytes.Repeat([]byte{byte(tipSlot / 100_000)}, 32)
	stakingKey := fixtureStakingKey
	if tipSlot == fixtureAnchorSlot {
		stakingKey = fixtureAnchorKey
	}
	return ImportConfig{
		Database:  db,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Reconcile: reconcile,
		UtxoSet:   set,
		State: &RawLedgerState{
			UTxOData:      inlineUTxOMap(t, addr, amounts),
			CertStateData: accountCertStateData(t, stakingKey, reward, 2_000_000),
			GovStateData: testGovStateData(
				t, bytes.Repeat([]byte{0x95}, 32), tipSlot/1_000,
			),
			Epoch:               tipSlot / 1_000,
			EraIndex:            EraConway,
			EraBounds:           make([]EraBound, EraConway+1),
			EpochNonce:          nonce,
			EvolvingNonce:       nonce,
			CandidateNonce:      nonce,
			LastEpochBlockNonce: nonce,
			Tip: &SnapshotTip{
				Slot:      tipSlot,
				BlockHash: bytes.Repeat([]byte{byte(tipSlot / 100_000)}, 32),
			},
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, importTestEpochLength, nil
		},
	}
}

func openFixtureSet(t *testing.T) *utxoset.Set {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	return set
}

// importSetFor returns the live UTxO set of db in this test, opening it on
// first use, so every import into db loads the set a node keeps beside it.
// A nil db names no database to share a set with: every such test gets its
// own set, since parallel tests sharing one would bootstrap it concurrently
// and close it under each other.
func importSetFor(t *testing.T, db *database.Database) *utxoset.Set {
	t.Helper()
	if db == nil {
		return openFixtureSet(t)
	}
	importSetsMu.Lock()
	defer importSetsMu.Unlock()
	if set, ok := importSets[db]; ok {
		return set
	}
	set := openFixtureSet(t)
	importSets[db] = set
	t.Cleanup(func() {
		importSetsMu.Lock()
		delete(importSets, db)
		importSetsMu.Unlock()
	})
	return set
}

var (
	importSetsMu sync.Mutex
	importSets   = map[*database.Database]*utxoset.Set{}
)

func newFixtureDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	return db
}

var richAnchorDormant = uint64(3)

var (
	richPool1 = bytes.Repeat([]byte{0x51}, 28)
	richPool2 = bytes.Repeat([]byte{0x52}, 28)
	richDrep1 = bytes.Repeat([]byte{0x61}, 28)
	richDrep2 = bytes.Repeat([]byte{0x62}, 28)
)

func richCertState(t *testing.T, anchor bool) cbor.RawMessage {
	t.Helper()
	return richCertStateOpts(t, anchor, false, false)
}

func richCertStateFuture(t *testing.T, anchor, future bool) cbor.RawMessage {
	t.Helper()
	return richCertStateOpts(t, anchor, future, false)
}

// richCertStateOpts builds the rich cert state. With future set the
// non-anchor snapshot stages a re-registration of pool 0x52 in
// futurePoolParams; with dangling set the anchor's account stays and still delegates to the
// DRep the anchor held and the snapshot no longer registers.
func richCertStateOpts(t *testing.T, anchor, future, dangling bool) cbor.RawMessage {
	t.Helper()
	acct := fixtureStakingKey
	pool, drep := richPool2, richDrep2
	if dangling && !anchor {
		acct, drep = fixtureAnchorKey, richDrep1
	}
	if anchor {
		acct, pool, drep = fixtureAnchorKey, richPool1, richDrep1
	}
	credMap := encodeCredentialMapEntry(t,
		testCredentialKey{Type: 0, Hash: toFixed28(acct)},
		[]any{uint64(4_000_000), uint64(2_000_000), pool, []any{uint64(0), drep}},
	)
	emptyMap := encodeCborMap(t)
	dAccts, err := cbor.Encode([]any{cbor.RawMessage(credMap), cbor.RawMessage(emptyMap)})
	require.NoError(t, err)
	dState, err := cbor.Encode([]any{cbor.RawMessage(dAccts)})
	require.NoError(t, err)
	var poolParams, deposits []byte
	if anchor {
		poolParams = encodeCborMap(t, richPool1, testPoolParams(0x51), richPool2, testPoolParams(0x52))
		deposits = encodeCborMap(t, richPool1, testPoolDeposit, richPool2, testPoolDeposit)
	} else {
		poolParams = encodeCborMap(t, richPool2, testPoolParams(0x52))
		deposits = encodeCborMap(t, richPool2, testPoolDeposit)
	}
	futureParams := emptyMap
	if future && !anchor {
		futureParams = encodeCborMap(t, richPool2, testPoolParams(0x70))
	}
	pState := encodeTestPState(t, poolParams, futureParams, emptyMap, deposits)
	drepState := func(delegator []byte) []any {
		return []any{uint64(500), nil, uint64(500_000_000), []any{[]any{uint64(0), delegator}}}
	}
	var drepMap []byte
	if anchor {
		drepMap = encodeCborMap(t,
			[]any{uint64(0), richDrep1}, drepState(acct),
			[]any{uint64(0), richDrep2}, []any{uint64(500), nil, uint64(500_000_000)},
		)
	} else {
		delegator := drepState(acct)
		if dangling {
			delegator = []any{uint64(500), nil, uint64(500_000_000)}
		}
		drepMap = encodeCborMap(t, []any{uint64(0), richDrep2}, delegator)
	}
	dormant := uint64(0)
	if anchor {
		dormant = richAnchorDormant
	}
	vState, err := cbor.Encode([]any{cbor.RawMessage(drepMap), map[any]any{}, dormant})
	require.NoError(t, err)
	out, err := cbor.Encode([]any{cbor.RawMessage(vState), cbor.RawMessage(pState), cbor.RawMessage(dState)})
	require.NoError(t, err)
	return out
}

func richConfig(t *testing.T, db *database.Database, slot uint64, anchor, reconcile bool, set UtxoSetLoader) ImportConfig {
	cfg := snapshotConfig(t, db, slot, reconcile, set)
	cfg.State.CertStateData = richCertState(t, anchor)
	return cfg
}

func richConfigFuture(t *testing.T, db *database.Database, slot uint64, anchor, reconcile bool, set UtxoSetLoader) ImportConfig {
	cfg := snapshotConfig(t, db, slot, reconcile, set)
	cfg.State.CertStateData = richCertStateFuture(t, anchor, true)
	return cfg
}

func richConfigDangling(t *testing.T, db *database.Database, slot uint64, anchor, reconcile bool, set UtxoSetLoader) ImportConfig {
	cfg := snapshotConfig(t, db, slot, reconcile, set)
	cfg.State.CertStateData = richCertStateOpts(t, anchor, false, true)
	return cfg
}

// importWholeState is ImportLedgerState for a fixture that builds only the
// part of a ledger state its test reads: the UTxO map and certificate state
// it leaves out are filled in with empty ones, which a snapshot carries.
func importWholeState(
	t *testing.T,
	ctx context.Context,
	cfg ImportConfig,
) error {
	t.Helper()
	if cfg.State != nil {
		state := *cfg.State
		emptyMap := cbor.RawMessage{0xa0}
		if state.UTxOTablePath == "" && state.UTxOData == nil {
			state.UTxOData = emptyMap
		}
		if state.CertStateData == nil {
			// [VState, PState, DState], each with no entries.
			accounts, err := cbor.Encode([]any{emptyMap, emptyMap})
			require.NoError(t, err)
			certState, err := cbor.Encode([]any{
				[]any{emptyMap},
				[]any{emptyMap},
				[]any{cbor.RawMessage(accounts)},
			})
			require.NoError(t, err)
			state.CertStateData = certState
		}
		cfg.State = &state
	}
	return importLedgerStateForTest(ctx, cfg)
}

// TestImportRefusesAPartialLedgerState requires an import of a ledger state
// with no UTxO map, or with no certificate state, to refuse before it writes:
// the LedgerDB set keeps the state it held and no import is left pending.
// Without a UTxO map the set is never replaced, so the import would write
// the snapshot's families over the state the set held.
func TestImportRefusesAPartialLedgerState(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		strip func(*RawLedgerState)
		want  string
	}{
		"no UTxO map": {
			strip: func(s *RawLedgerState) { s.UTxOData = nil },
			want:  "ledger state has no UTxO map",
		},
		"no certificate state": {
			strip: func(s *RawLedgerState) { s.CertStateData = nil },
			want:  "ledger state has no certificate state",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := newFixtureDB(t)
			set := openFixtureSet(t)
			require.NoError(t, importLedgerStateForTest(
				t.Context(),
				snapshotConfig(t, db, fixtureAnchorSlot, false, set),
			))
			cfg := snapshotConfig(t, db, fixtureCatchUpSlot, false, set)
			tc.strip(cfg.State)

			err := importLedgerStateForTest(t.Context(), cfg)
			require.ErrorContains(t, err, tc.want)

			_, pending, err := set.ImportPending()
			require.NoError(t, err)
			require.False(t, pending, "no import may be left pending")
			tip, ok, err := set.Tip()
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(
				t, fixtureAnchorSlot, tip.Slot,
				"the set must keep the state it held",
			)
		})
	}
}

// accountCertStateData builds CertStateData ([VState, PState, DState]) whose
// DState holds exactly one Conway-shaped account entry (reward, deposit) for
// stakingKey, with empty VState/PState/DRep maps.
func accountCertStateData(
	t *testing.T,
	stakingKey []byte,
	reward, deposit uint64,
) cbor.RawMessage {
	t.Helper()
	emptyMap, err := cbor.Encode(map[uint64]uint64{})
	require.NoError(t, err)
	vState, err := cbor.Encode([]any{cbor.RawMessage(emptyMap)})
	require.NoError(t, err)
	pState, err := cbor.Encode([]any{cbor.RawMessage(emptyMap)})
	require.NoError(t, err)

	credMap := encodeCredentialMapEntry(
		t,
		testCredentialKey{Type: 0, Hash: toFixed28(stakingKey)},
		[]any{reward, deposit},
	)
	dStateAccounts, err := cbor.Encode([]any{
		cbor.RawMessage(credMap), cbor.RawMessage(emptyMap),
	})
	require.NoError(t, err)
	dState, err := cbor.Encode([]any{cbor.RawMessage(dStateAccounts)})
	require.NoError(t, err)
	certState, err := cbor.Encode([]any{
		cbor.RawMessage(vState),
		cbor.RawMessage(pState),
		cbor.RawMessage(dState),
	})
	require.NoError(t, err)
	return certState
}
