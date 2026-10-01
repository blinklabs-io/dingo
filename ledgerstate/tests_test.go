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
	"encoding/hex"
	"io"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseCommitteeQuorumRange checks that an imported committee quorum must
// lie in the UnitInterval. A zero quorum stays valid.
func TestParseCommitteeQuorumRange(t *testing.T) {
	t.Parallel()
	encodeCommittee := func(t *testing.T, quorum any) []byte {
		t.Helper()
		// StrictMaybe SJust: [[members_map, quorum]]
		raw, err := cbor.Encode([]any{[]any{map[uint]uint{}, quorum}})
		require.NoError(t, err)
		return raw
	}
	rat := func(n, d int64) any {
		return cbor.Tag{
			Number:  30,
			Content: []any{n, d},
		}
	}
	for _, tc := range []struct {
		name    string
		quorum  any
		want    *big.Rat
		wantErr string
	}{
		{"zero", rat(0, 1), big.NewRat(0, 1), ""},
		{"one half", rat(1, 2), big.NewRat(1, 2), ""},
		{"one", rat(1, 1), big.NewRat(1, 1), ""},
		{"negative", rat(-1, 2), nil, "outside [0,1]"},
		{"above one", rat(3, 2), nil, "outside [0,1]"},
		{"integer above one", rat(2, 1), nil, "outside [0,1]"},
		{"zero denominator", rat(1, 0), nil, "denominator"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, quorum, err := parseCommittee(encodeCommittee(t, tc.quorum))
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
				require.Nil(
					t,
					quorum,
					"an out-of-range quorum must not be surfaced to callers",
				)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, quorum)
			require.Zero(t, tc.want.Cmp(quorum.Rat))
		})
	}
}

// Applying a reward round at the boundary into N reads this node's
// RewardAdaPots row for N-1. A node bootstrapped from a snapshot never saw
// that boundary, so without seeding, the first round after import finds no
// pots and is skipped — and a skipped round is never made up. Reward
// balances, and the leadership stake derived from them, stay short by an
// epoch's rewards for the life of the database, which is what makes such a
// node reject canonical blocks near the eligibility threshold (#3165).
func TestImportSeedsAdaPotsForTheImportedEpoch(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	require.NotNil(t, state.Tip)

	cfg := ImportConfig{
		Database: db,
		State:    state,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 500, nil
		},
	}
	noProgress := func(ImportProgress) {}
	ctx := context.Background()
	_, err = importCertState(ctx, cfg, state.Tip.Slot, noProgress)
	require.NoError(t, err)
	require.NoError(t, importSnapShots(
		ctx, cfg, state.Tip.Slot, noProgress, false,
	))

	pots, err := db.Metadata().GetRewardAdaPots(state.Epoch, nil)
	require.NoError(t, err)
	require.NotNil(t, pots,
		"the imported epoch needs an ADA pots row, or the first reward "+
			"round after bootstrap is skipped and never made up")
	require.Equal(t, state.Epoch, pots.Epoch)
	require.Equal(t, state.Treasury, uint64(pots.Treasury))
	require.Equal(t, state.Reserves, uint64(pots.Reserves))

	// The fee pot must be the one SnapShots captured at the boundary, not
	// UTxOState's running total for the current epoch: the reward pot is
	// incentives plus fees, and the row is read as the pots for a boundary.
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err)
	require.Equal(t, snapshots.Fee, uint64(pots.Fees),
		"the seeded fee pot must come from SnapShots' ssFee")
}

// seedImportedRewardBasis also seeds ImportedEpochFees: the epoch's own
// pre-anchor fee pot, UTxOState.utxosFees (RawLedgerState.Fees) minus
// SnapShots' ssFee. A later local boundary calculation adds it to the fees
// this node observes after the anchor instead of silently omitting
// everything before it (dingo #3975).
func TestImportSeedsPreAnchorFeesFromStateMinusSnapshotFee(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	const (
		epoch       = uint64(7)
		anchorSlot  = uint64(12345)
		stateFees   = uint64(750_000)
		snapshotFee = uint64(200_000)
	)
	cfg := ImportConfig{
		Database: db,
		State: &RawLedgerState{
			Epoch: epoch,
			Fees:  stateFees,
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	snapshots := &ParsedSnapShots{Fee: snapshotFee}

	require.NoError(
		t, seedImportedRewardBasis(cfg, snapshots, epoch, anchorSlot),
	)

	pots, err := db.Metadata().GetRewardAdaPots(epoch, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	require.NotNil(t, pots.ImportedEpochFees,
		"a reconciling basis must seed the pre-anchor fee pot")
	require.Equal(t, stateFees-snapshotFee, uint64(*pots.ImportedEpochFees))
	require.Equal(t, snapshotFee, uint64(pots.Fees),
		"the existing fee pot must remain SnapShots' ssFee")
}

// cardano-ledger's NEWEPOCH rule leaves UTxOState.utxosFees equal to the new
// ssFee after every boundary, and only transactions add to it within an
// epoch, so RawLedgerState.Fees < SnapShots.Fee means the snapshot was not
// decoded as a consistent ledger state. seedImportedRewardBasis must refuse
// it: leaving ImportedEpochFees unset would make the next boundary sum only
// the local post-anchor fees and credit that round short, the defect in
// dingo #3975.
func TestImportRejectsSnapshotWhoseFeesDoNotReconcile(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	const (
		epoch       = uint64(9)
		anchorSlot  = uint64(54321)
		stateFees   = uint64(100)
		snapshotFee = uint64(500)
	)
	cfg := ImportConfig{
		Database: db,
		State: &RawLedgerState{
			Epoch: epoch,
			Fees:  stateFees,
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	snapshots := &ParsedSnapShots{Fee: snapshotFee}

	err = seedImportedRewardBasis(cfg, snapshots, epoch, anchorSlot)
	require.ErrorContains(t, err, "less than the snapshot fee pot")

	pots, err := db.Metadata().GetRewardAdaPots(epoch, nil)
	require.NoError(t, err)
	require.Nil(t, pots,
		"a refused basis must not leave a pots row behind")
}

// Pool performance is beta/sigma_a, with beta the pool's share of the blocks
// minted in the performance epoch. A node bootstrapped from a snapshot has no
// local history for the epochs preceding its anchor and so can count none of
// those blocks -- but the snapshot itself carries them, in the two BlocksMade
// fields the ledger keeps for exactly this calculation. Discarding them at
// import is what leaves the first reward round with nothing to compute from.
func TestSnapshotCarriesBlocksMadeForBothEpochs(t *testing.T) {
	t.Parallel()

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	require.Equal(t, uint64(4), state.Epoch)

	require.Len(t, state.BlocksPrev, 2)
	assert.Equal(
		t,
		uint64(63),
		state.BlocksPrev[decodeSnapshotPoolKey(t, snapshotPoolA)],
	)
	assert.Equal(
		t,
		uint64(104),
		state.BlocksPrev[decodeSnapshotPoolKey(t, snapshotPoolB)],
	)
	assert.Equal(t, uint64(167), sumBlocksMade(state.BlocksPrev))

	require.Len(t, state.BlocksCur, 2)
	assert.Equal(
		t,
		uint64(63),
		state.BlocksCur[decodeSnapshotPoolKey(t, snapshotPoolA)],
	)
	assert.Equal(
		t,
		uint64(81),
		state.BlocksCur[decodeSnapshotPoolKey(t, snapshotPoolB)],
	)
	assert.Equal(t, uint64(144), sumBlocksMade(state.BlocksCur))
}

// The two pools that minted blocks in the devnet snapshot fixture.
const (
	snapshotPoolA = "2b00dcd8850e3baa26295ce80c9a36898566e26665e14fe10950a6f7"
	snapshotPoolB = "b0f3f3effa2365ab4937cfd7dea054cb3fb7a5b1fb65bb99c436527c"
)

func decodeSnapshotPoolKey(t *testing.T, hexKey string) string {
	t.Helper()
	raw, err := hex.DecodeString(hexKey)
	require.NoError(t, err)
	return string(raw)
}

// A dropped entry lowers one pool's beta and the epoch total every other
// pool's beta divides by, so a partially decoded map produces a
// complete-looking distribution at the wrong amount for every pool at once.
// The stake and delegation maps can afford to skip an entry; this one cannot.
func TestParseBlocksMadeRejectsMalformedEntries(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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

	// The expected values are spelled out rather than compared against the
	// parsed state, so a parse that returned nothing could not satisfy this by
	// agreeing with itself.
	prev, prevTotal, prevKnown, err := store.GetImportedPoolBlockCounts(
		state.Epoch-1, nil,
	)
	require.NoError(t, err)
	require.True(t, prevKnown)
	assert.Equal(t, map[string]uint64{
		decodeSnapshotPoolKey(t, snapshotPoolA): 63,
		decodeSnapshotPoolKey(t, snapshotPoolB): 104,
	}, prev)
	assert.Equal(t, uint64(167), prevTotal)

	cur, curTotal, curKnown, err := store.GetImportedPoolBlockCounts(
		state.Epoch, nil,
	)
	require.NoError(t, err)
	require.True(t, curKnown)
	assert.Equal(t, map[string]uint64{
		decodeSnapshotPoolKey(t, snapshotPoolA): 63,
		decodeSnapshotPoolKey(t, snapshotPoolB): 81,
	}, cur)
	assert.Equal(t, uint64(144), curTotal)

	// An epoch the snapshot says nothing about must read as unknown, so the
	// reward round can tell "not imported" from "imported as zero".
	_, _, olderKnown, err := store.GetImportedPoolBlockCounts(
		state.Epoch-2, nil,
	)
	require.NoError(t, err)
	assert.False(t, olderKnown)
}

// An epoch in which no pool minted a block is a state the certified snapshot
// asserts, not an epoch the import said nothing about. Only the epoch-total
// row can carry that, because an empty BlocksMade map writes no per-pool rows,
// and reading it as "not imported" would decline a reward round the reference
// runs.
func TestImportBlocksMadeRecordsAnEmptyMapAsACertifiedZero(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	store := db.Metadata()

	require.NoError(t, importBlocksMade(
		store,
		12,
		map[string]uint64{},
		map[string]uint64{},
		400,
		nil,
	))

	counts, total, known, err := store.GetImportedPoolBlockCounts(11, nil)
	require.NoError(t, err)
	assert.True(
		t,
		known,
		"an empty BlocksMade map is a zero-block epoch, not an absent one",
	)
	assert.Empty(t, counts)
	assert.Equal(t, uint64(0), total)
}

// The recorded total is checked against the rows rather than derived from
// them. A per-pool set truncated by a partial write would otherwise present as
// a smaller but self-consistent epoch, raising every surviving pool's share of
// the blocks and over-crediting its rewards.
func TestImportedBlockCountsRejectATotalThatDisagreesWithTheRows(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	store := db.Metadata()

	poolKey := make([]byte, credentialHashSize)
	require.NoError(t, store.SaveImportedPoolBlockCounts(
		[]models.ImportedPoolBlockCount{
			{
				Epoch:          20,
				PoolKeyHash:    poolKey,
				BlocksProduced: 4,
				CapturedSlot:   10,
			},
		},
		nil,
	))
	require.NoError(t, store.SaveImportedEpochBlockTotal(20, 9, 10, nil))

	_, _, _, err = store.GetImportedPoolBlockCounts(20, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sum to 4, recorded total is 9")
}

// A catch-up import carries a later anchor and a later nesBcur. Merging the
// new map into the old rows would leave one epoch holding counts taken at two
// different anchors, so the epoch is replaced rather than added to.
func TestImportBlocksMadeReplacesAnEpochRatherThanMerging(t *testing.T) {
	t.Parallel()

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

	cur, total, known, err := store.GetImportedPoolBlockCounts(9, nil)
	require.NoError(t, err)
	require.True(t, known)
	assert.Equal(t, map[string]uint64{poolA: 5}, cur)
	assert.Equal(t, uint64(5), total)
}

// TestPersistImportedCommitteeCertificatesWritesRows exercises the write path
// itself rather than the fee helper in isolation.
//
// persistImportedCommitteeCertificates carries the imported authorizations
// through SetTransactionMetadataOnly on a synthetic transaction that embeds
// TransactionBodyBase without overriding Fee, and TransactionBodyBase.Fee
// returns nil. Reverting either the nil guard or the setTransaction call site
// back to transaction.Fee().Uint64() panics here with a nil dereference, which
// is what took down a mainnet Mithril bootstrap immediately after the
// committee decoded for the first time. A test over the helper alone stays
// green through that revert, so it has to run this function.
func TestPersistImportedCommitteeCertificatesWritesRows(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	cold := Credential{
		Type: CredentialTypeKey,
		Hash: bytes.Repeat([]byte{0xc1}, 28),
	}
	hot := Credential{
		Type: CredentialTypeScript,
		Hash: bytes.Repeat([]byte{0x40}, 28),
	}
	resigned := Credential{
		Type: CredentialTypeScript,
		Hash: bytes.Repeat([]byte{0xc2}, 28),
	}

	certState := &ParsedCertState{
		CommitteeHotKeys: []ParsedCommitteeHotKey{
			{Cold: cold, Hot: hot},
		},
		CommitteeResignations: []Credential{resigned},
	}

	const slot = uint64(197789347)
	require.NotPanics(t, func() {
		require.NoError(t, persistImportedCommitteeCertificates(
			db, certState, slot, nil,
		))
	})

	// The authorization must be readable back by the same cold-credential
	// lookup the Conway unknown-voter rule uses.
	member, err := db.Metadata().GetCommitteeMember(
		uint8(cold.Type), cold.Hash, 0, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, member)
	require.Equal(t, hot.Hash, member.HotCredential)
	require.Equal(t, uint8(hot.Type), member.HotCredentialTag)
}

func TestMmapReadOnlyRejectsEmptyFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(path, nil, 0o640); err != nil {
		t.Fatalf("writing empty file: %v", err)
	}

	data, cleanup, err := mmapReadOnly(path)
	if err == nil {
		if cleanup != nil {
			cleanup()
		}
		t.Fatalf("expected error, got data len %d", len(data))
	}
	if !strings.Contains(err.Error(), "empty file") {
		t.Fatalf("expected empty file error, got %v", err)
	}
}

func TestMmapReadOnlyReturnsFileData(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "data")
	want := []byte("dingo mmap test")
	if err := os.WriteFile(path, want, 0o640); err != nil {
		t.Fatalf("writing data file: %v", err)
	}

	data, cleanup, err := mmapReadOnly(path)
	if err != nil {
		t.Fatalf("mmap read-only: %v", err)
	}
	defer cleanup()

	if !bytes.Equal(data, want) {
		t.Fatalf("mapped data = %q, want %q", data, want)
	}
}

func TestImportTipPersistsSnapshotNetworkState(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	const (
		treasury = uint64(87_920_693_660_807)
		reserves = uint64(14_914_270_613_432_674)
	)
	err = importTip(context.Background(), ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: &RawLedgerState{
			Epoch:    12,
			Treasury: treasury,
			Reserves: reserves,
			EraIndex: 6,
			Tip: &SnapshotTip{
				Slot:      123_456,
				BlockHash: make([]byte, 32),
			},
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	})
	require.NoError(t, err)

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(123_456), state.Slot)
	require.Equal(t, treasury, uint64(state.Treasury))
	require.Equal(t, reserves, uint64(state.Reserves))
}

// TestPersistableOpCertBoundMatchesStore ties eras.MaxPersistableOpCertCounter
// to the check that enforces it. The constant names the limit
// sqlstore.checkedInt64 imposes on pool_opcert_sequence.sequence and
// pool.latest_op_cert_sequence before a caller reaches it. The former
// ledger/eras test also checked the constant against the math.MaxInt64
// literal, but could not detect drift in the store's accepted range. This
// test drives the value through the real store write in both directions, so a
// change to the constant or to that range fails it.
//
// It is not next to checkedInt64 because checkedInt64 is unexported and the
// reviewed import direction in internal/architecture/tests_test.go
// forbids anything under database/ from importing ledger/, test files
// included. ledgerstate owns the write path the bound was found missing from
// (importOpCertCounters) and already imports ledger/eras, so it is the
// package that can see both sides.
func TestPersistableOpCertBoundMatchesStore(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	poolKeyHash := testPoolKeyHash(bytes.Repeat([]byte{0x7a}, 28))

	// The store records the highest counter the bound admits.
	require.NoError(t, db.Metadata().UpdatePoolOpCertSequence(
		poolKeyHash, eras.MaxPersistableOpCertCounter, 100, nil,
	))
	sequence, found, err := db.LatestPoolOpCertSequence(poolKeyHash, nil)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, eras.MaxPersistableOpCertCounter, sequence)

	// The store refuses the next one, which is what makes the bound the
	// bound rather than an arbitrary constant.
	require.Error(t, db.Metadata().UpdatePoolOpCertSequence(
		poolKeyHash, eras.MaxPersistableOpCertCounter+1, 101, nil,
	))
}

// encodeTestNonce builds a CBOR Nonce value: [0] for NeutralNonce or
// [1, hash] for Nonce(hash). The shape matches `decodeNonce`.
func encodeTestNonce(t *testing.T, hash []byte) []byte {
	t.Helper()
	if hash == nil {
		out, err := cbor.Encode([]any{uint64(0)})
		require.NoError(t, err)
		return out
	}
	out, err := cbor.Encode([]any{uint64(1), hash})
	require.NoError(t, err)
	return out
}

// TestExtractPraosNonces_LastEpochBlockNonceIs8FieldShape locks in the
// fix for the Mithril-bootstrap VRF wedge. The cardano-ledger PraosState
// has 8 fields with `previousEpochNonce` at index 5, shifting `labNonce`
// to 6 and `lastEpochBlockNonce` to 7. The epoch-rollover formula uses
// `lastEpochBlockNonce` (index 7), so picking up `labNonce` (index 6)
// yields the wrong eta0 and every header in the next epoch fails VRF.
func TestExtractPraosNonces_LastEpochBlockNonceIs8FieldShape(t *testing.T) {
	t.Parallel()

	evolving := bytes.Repeat([]byte{0xee}, 32)
	candidate := bytes.Repeat([]byte{0xcc}, 32)
	epoch := bytes.Repeat([]byte{0xee, 0x09}, 16)
	previous := bytes.Repeat([]byte{0xa0}, 32)
	lab := bytes.Repeat([]byte{0xab}, 32)
	lastEpochBlock := bytes.Repeat([]byte{0xfe}, 32)

	lastSlot, err := cbor.Encode(uint64(123456))
	require.NoError(t, err)
	poolKeyHash := bytes.Repeat([]byte{0x42}, 28)
	ocertCounters := encodeTestOpCertCounters(t, map[string]uint64{
		string(poolKeyHash): 490,
	})

	praosState := [][]byte{
		lastSlot,
		ocertCounters,
		encodeTestNonce(t, evolving),
		encodeTestNonce(t, candidate),
		encodeTestNonce(t, epoch),
		encodeTestNonce(t, previous),
		encodeTestNonce(t, lab),
		encodeTestNonce(t, lastEpochBlock),
	}

	got, err := extractPraosNonces(praosState)
	require.NoError(t, err)

	require.Equal(t, evolving, got.EvolvingNonce, "evolving nonce")
	require.Equal(t, candidate, got.CandidateNonce, "candidate nonce")
	require.Equal(t, epoch, got.EpochNonce, "epoch nonce")
	require.Equal(
		t, lastEpochBlock, got.LastEpochBlockNonce,
		"lastEpochBlockNonce must come from the LAST element "+
			"(index 7); reading index 6 picks up labNonce and "+
			"breaks VRF on every header in the next epoch",
	)
	require.Equal(t, uint64(490), got.OpCertCounters[string(poolKeyHash)])
	require.NotEqual(
		t, lab, got.LastEpochBlockNonce,
		"sanity: parser must not return labNonce as "+
			"LastEpochBlockNonce",
	)
}

// TestExtractPraosNonces_LastEpochBlockNonceIs7FieldShape covers the
// older PraosState shape (no `previousEpochNonce`), where
// `lastEpochBlockNonce` is at index 6.
func TestExtractPraosNonces_LastEpochBlockNonceIs7FieldShape(t *testing.T) {
	t.Parallel()

	evolving := bytes.Repeat([]byte{0xee}, 32)
	candidate := bytes.Repeat([]byte{0xcc}, 32)
	epoch := bytes.Repeat([]byte{0xee, 0x09}, 16)
	lab := bytes.Repeat([]byte{0xab}, 32)
	lastEpochBlock := bytes.Repeat([]byte{0xfe}, 32)

	lastSlot, err := cbor.Encode(uint64(123456))
	require.NoError(t, err)
	ocertCounters := encodeTestOpCertCounters(t, nil)

	praosState := [][]byte{
		lastSlot,
		ocertCounters,
		encodeTestNonce(t, evolving),
		encodeTestNonce(t, candidate),
		encodeTestNonce(t, epoch),
		encodeTestNonce(t, lab),
		encodeTestNonce(t, lastEpochBlock),
	}

	got, err := extractPraosNonces(praosState)
	require.NoError(t, err)
	require.Equal(t, evolving, got.EvolvingNonce)
	require.Equal(t, candidate, got.CandidateNonce)
	require.Equal(t, epoch, got.EpochNonce)
	require.Equal(t, lastEpochBlock, got.LastEpochBlockNonce)
}

func encodeTestOpCertCounters(
	t *testing.T,
	counters map[string]uint64,
) []byte {
	t.Helper()
	if len(counters) > 23 {
		t.Fatal("test helper supports at most 23 counters")
	}
	ret := []byte{0xa0 | byte(len(counters))}
	for poolKey, counter := range counters {
		key, err := cbor.Encode([]byte(poolKey))
		require.NoError(t, err)
		value, err := cbor.Encode(counter)
		require.NoError(t, err)
		ret = append(ret, key...)
		ret = append(ret, value...)
	}
	return ret
}

func TestDecodeOpCertCountersRejectsInvalidPoolKeyLength(t *testing.T) {
	t.Parallel()

	_, err := decodeOpCertCounters(encodeTestOpCertCounters(
		t, map[string]uint64{string(make([]byte, 27)): 1},
	))
	require.ErrorContains(t, err, "expected 28")
}

// encodeRoots encodes a 4-element GovRelation array of StrictMaybe
// (GovPurposeId p era), in the order [PParamUpdate, HardFork,
// Committee, Constitution]. ids[i] of nil encodes as SNothing ([]).
func encodeRoots(t *testing.T, ids [4]*ParsedGovActionId) []byte {
	t.Helper()
	encoded := make([]any, 4)
	for i, id := range ids {
		if id == nil {
			encoded[i] = []any{}
			continue
		}
		encoded[i] = []any{
			[]any{id.TxHash, uint64(id.ActionIndex)},
		}
	}
	data, err := cbor.Encode(encoded)
	require.NoError(t, err)
	return data
}

func TestParseProposalsRootsEmpty(t *testing.T) {
	t.Parallel()

	got, err := parseProposalsRoots(nil)
	require.NoError(t, err)
	require.Nil(t, got)

	emptyArr, err := cbor.Encode([]any{})
	require.NoError(t, err)
	got, err = parseProposalsRoots(emptyArr)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestParseProposalsRootsAllSNothing(t *testing.T) {
	t.Parallel()

	data := encodeRoots(t, [4]*ParsedGovActionId{nil, nil, nil, nil})
	got, err := parseProposalsRoots(data)
	require.NoError(t, err)
	require.Nil(t, got, "all SNothing should map to nil result")
}

func TestParseProposalsRootsAllSet(t *testing.T) {
	t.Parallel()

	pp := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x11}, 32),
		ActionIndex: 0,
	}
	hf := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x22}, 32),
		ActionIndex: 1,
	}
	cm := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x33}, 32),
		ActionIndex: 2,
	}
	cn := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x44}, 32),
		ActionIndex: 3,
	}

	data := encodeRoots(t, [4]*ParsedGovActionId{pp, hf, cm, cn})
	got, err := parseProposalsRoots(data)
	require.NoError(t, err)
	require.NotNil(t, got)

	require.NotNil(t, got.PParamUpdate)
	assert.Equal(t, pp.TxHash, got.PParamUpdate.TxHash)
	assert.Equal(t, pp.ActionIndex, got.PParamUpdate.ActionIndex)

	require.NotNil(t, got.HardFork)
	assert.Equal(t, hf.TxHash, got.HardFork.TxHash)
	assert.Equal(t, hf.ActionIndex, got.HardFork.ActionIndex)

	require.NotNil(t, got.Committee)
	assert.Equal(t, cm.TxHash, got.Committee.TxHash)
	assert.Equal(t, cm.ActionIndex, got.Committee.ActionIndex)

	require.NotNil(t, got.Constitution)
	assert.Equal(t, cn.TxHash, got.Constitution.TxHash)
	assert.Equal(t, cn.ActionIndex, got.Constitution.ActionIndex)
}

func TestParseProposalsRootsPartial(t *testing.T) {
	t.Parallel()

	hf := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0xAA}, 32),
		ActionIndex: 7,
	}
	data := encodeRoots(t, [4]*ParsedGovActionId{nil, hf, nil, nil})
	got, err := parseProposalsRoots(data)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Nil(t, got.PParamUpdate)
	require.NotNil(t, got.HardFork)
	assert.Equal(t, hf.TxHash, got.HardFork.TxHash)
	assert.Equal(t, hf.ActionIndex, got.HardFork.ActionIndex)
	assert.Nil(t, got.Committee)
	assert.Nil(t, got.Constitution)
}

func TestParseProposalsRootsBadShape(t *testing.T) {
	t.Parallel()

	// 3-element array is not a valid GovRelation.
	data, err := cbor.Encode([]any{[]any{}, []any{}, []any{}})
	require.NoError(t, err)
	got, err := parseProposalsRoots(data)
	require.Error(t, err)
	require.Nil(t, got)
	require.Contains(t, err.Error(), "GovRelation has 3 elements")
}

func TestParseStrictMaybeGovActionIdNullSentinel(t *testing.T) {
	t.Parallel()

	// CBOR null (0xf6) is treated as SNothing.
	got, err := parseStrictMaybeGovActionId([]byte{0xf6})
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestParseStrictMaybeGovActionIdDirectGovActionId(t *testing.T) {
	t.Parallel()

	// Tolerate a non-canonical encoder that emits the GovActionId
	// directly without the SJust 1-element wrapper.
	txHash := bytes.Repeat([]byte{0x55}, 32)
	data, err := cbor.Encode([]any{txHash, uint64(9)})
	require.NoError(t, err)
	got, err := parseStrictMaybeGovActionId(data)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, txHash, got.TxHash)
	assert.Equal(t, uint32(9), got.ActionIndex)
}

func TestParseStrictMaybeGovActionIdShortTxHash(t *testing.T) {
	t.Parallel()

	short := bytes.Repeat([]byte{0x77}, 16)
	data, err := cbor.Encode([]any{[]any{short, uint64(0)}})
	require.NoError(t, err)
	got, err := parseStrictMaybeGovActionId(data)
	require.Error(t, err)
	require.Nil(t, got)
	require.Contains(t, err.Error(), "txHash has 16 bytes")
}

func TestParseProposalsIncludesRoots(t *testing.T) {
	t.Parallel()

	// End-to-end: parseProposals returns both the OMap proposals
	// and the per-purpose roots.
	pp := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0xBC}, 32),
		ActionIndex: 4,
	}
	roots := encodeRootsAsAny(t, [4]*ParsedGovActionId{pp, nil, nil, nil})

	// One Info proposal so the OMap is non-empty.
	infoProp := []any{
		[]any{bytes.Repeat([]byte{0x01}, 32), uint64(0)},
		map[uint64]uint64{},
		map[uint64]uint64{},
		map[uint64]uint64{},
		[]any{
			uint64(0),
			bytes.Repeat([]byte{0x02}, 29),
			[]any{uint8(6)}, // GovActionType Info = 6
			[]any{
				"https://example.com/info",
				bytes.Repeat([]byte{0x03}, 32),
			},
		},
		uint64(10),
		uint64(20),
	}
	container, err := cbor.Encode([]any{roots, []any{infoProp}})
	require.NoError(t, err)

	props, prevIds, err := parseProposals(container)
	require.NoError(t, err)
	require.Len(t, props, 1)
	require.NotNil(t, prevIds)
	require.NotNil(t, prevIds.PParamUpdate)
	assert.Equal(t, pp.TxHash, prevIds.PParamUpdate.TxHash)
	assert.Equal(t, pp.ActionIndex, prevIds.PParamUpdate.ActionIndex)
}

// encodeRootsAsAny returns the GovRelation as []any so it can be
// embedded in a larger container before CBOR-encoding.
func encodeRootsAsAny(t *testing.T, ids [4]*ParsedGovActionId) []any {
	t.Helper()
	out := make([]any, 4)
	for i, id := range ids {
		if id == nil {
			out[i] = []any{}
			continue
		}
		out[i] = []any{
			[]any{id.TxHash, uint64(id.ActionIndex)},
		}
	}
	return out
}

// govStateWithRoots builds a Conway GovState CBOR payload whose
// proposals container has the given per-purpose roots set and an
// empty OMap. The committee field is encoded based on
// committeePresent: true for a 1-element StrictMaybe Committee
// wrapper with a 2/3 quorum and empty members; false for SNothing.
//
// The shape mirrors Conway's seven-field GovState encoding but exposes the
// roots so tests can assert seeding behavior end to end.
func govStateWithRoots(
	t *testing.T,
	roots [4]*ParsedGovActionId,
	committeePresent bool,
) []byte {
	return govStateWithRootsAndProposals(
		t, roots, committeePresent, nil, nil,
	)
}

func govStateWithRootsAndProposals(
	t *testing.T,
	roots [4]*ParsedGovActionId,
	committeePresent bool,
	proposals []any,
	drepPulsingState any,
) []byte {
	t.Helper()

	rootsAny := encodeRootsAsAny(t, roots)
	proposalsContainer := []any{rootsAny, proposals}

	var committee any
	if committeePresent {
		// committee field is StrictMaybe (Committee era), encoded
		// as [ committee_body ] for SJust where committee_body =
		// [members_map, quorum]. Empty members + 2/3 quorum is
		// enough for parseCommittee to set CommitteeQuorum non-nil
		// — the seeding heuristic checks for CommitteeQuorum != nil
		// to distinguish UpdateCommittee from NoConfidence roots.
		committee = []any{
			[]any{
				map[any]uint64{},
				cbor.Rat{Rat: big.NewRat(2, 3)},
			},
		}
	} else {
		committee = []any{}
	}

	govState := []any{
		proposalsContainer,
		committee,
		// Constitution: [anchor, scriptHash], anchor=[url,hash].
		[]any{
			[]any{
				"https://example.com/constitution",
				bytes.Repeat([]byte{0xAA}, 32),
			},
			nil,
		},
	}
	if drepPulsingState == nil {
		drepPulsingState = drepPulsingStateWithEnactCommittee(
			t, committee,
		)
	}
	govState = append(
		govState,
		map[uint64]uint64{},
		map[uint64]uint64{},
		map[uint64]uint64{},
		drepPulsingState,
	)
	data, err := cbor.Encode(govState)
	require.NoError(t, err)
	return data
}

func govActionStateForTest(
	txHash []byte,
	actionIdx uint64,
	actionType uint8,
	parent *ParsedGovActionId,
	proposedEpoch uint64,
) []any {
	var parentAny any = []any{}
	if parent != nil {
		parentAny = []any{
			[]any{parent.TxHash, uint64(parent.ActionIndex)},
		}
	}
	govAction := []any{
		actionType,
		parentAny,
		map[uint64]uint64{},
		nil,
	}
	return []any{
		[]any{txHash, actionIdx},
		map[uint64]uint64{},
		map[uint64]uint64{},
		map[uint64]uint64{},
		[]any{
			uint64(100_000_000),
			bytes.Repeat([]byte{0xa1}, 29),
			govAction,
			[]any{
				"https://example.com/proposal",
				bytes.Repeat([]byte{0xb2}, 32),
			},
		},
		proposedEpoch,
		proposedEpoch + 5,
	}
}

// snothingCommittee is the StrictMaybe SNothing encoding shared by
// cgsCommittee and EnactState.ensCommittee: a zero-element array.
func snothingCommittee() any {
	return []any{}
}

// drepPulsingStateWithRatified builds a pulsing state whose
// rsEnactState carries no committee, matching a cgsCommittee of
// SNothing.
func drepPulsingStateWithRatified(
	t *testing.T,
	proposals ...any,
) any {
	t.Helper()
	return drepPulsingStateWithEnactCommittee(
		t, snothingCommittee(), proposals...,
	)
}

func drepPulsingStateWithEnactCommittee(
	t *testing.T,
	committee any,
	proposals ...any,
) any {
	t.Helper()
	// RatifyState.rsEnactState is an EnactState whose first field is the
	// committee StrictMaybe. The remaining fields are irrelevant here, but
	// are retained to match Conway's seven-field encoding.
	enactState := []any{committee, nil, nil, nil, nil, nil, nil}
	return []any{
		[]any{
			[]any{},
			map[uint64]uint64{},
			map[uint64]uint64{},
			map[uint64]uint64{},
		},
		[]any{enactState, proposals, []any{}, false},
	}
}

// constitutionForTest is the constitution field shape shared by the
// governance fixtures in this file.
func constitutionForTest() any {
	return []any{
		[]any{
			"https://example.com/constitution",
			bytes.Repeat([]byte{0xAA}, 32),
		},
		nil,
	}
}

// conwayGovStateWithPulsing assembles the seven-field Conway
// ConwayGovState encoding around the given cgsCommittee and
// cgsDRepPulsingState.
func conwayGovStateWithPulsing(
	t *testing.T,
	committee any,
	pulsing any,
) []byte {
	t.Helper()
	rootsAny := encodeRootsAsAny(t, [4]*ParsedGovActionId{})
	data, err := cbor.Encode([]any{
		[]any{rootsAny, []any{}},
		committee,
		constitutionForTest(),
		map[uint64]uint64{},
		map[uint64]uint64{},
		map[uint64]uint64{},
		pulsing,
	})
	require.NoError(t, err)
	return data
}

func govImportConfigForTest(
	db *database.Database,
	govStateData []byte,
) ImportConfig {
	return ImportConfig{
		Database: db,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		State: &RawLedgerState{
			GovStateData:  govStateData,
			Epoch:         500,
			EraIndex:      EraConway,
			EraBoundEpoch: 100,
			EraBoundSlot:  10_000,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}
}

func committeeWithMember(t *testing.T, hash []byte, expiry uint64) any {
	return committeeWithCredentialTag(t, 0, hash, expiry)
}

func committeeWithCredentialTag(
	t *testing.T,
	tag uint64,
	hash []byte,
	expiry uint64,
) any {
	t.Helper()
	// Credential array keys must be kept as raw CBOR map keys; encoding a
	// Go map with []byte keys would produce a bytestring key instead.
	key, err := cbor.Encode([]any{tag, hash})
	require.NoError(t, err)
	value, err := cbor.Encode(expiry)
	require.NoError(t, err)
	memberMap := cbor.RawMessage(append(append([]byte{0xa1}, key...), value...))
	return []any{[]any{memberMap, cbor.Rat{Rat: big.NewRat(2, 3)}}}
}

func TestImportGovStatePreservesScriptCommitteeCredentialTag(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	hash := bytes.Repeat([]byte{0x44}, 28)
	committee := committeeWithCredentialTag(t, 1, hash, 700)
	govStateData := conwayGovStateWithPulsing(
		t,
		committee,
		drepPulsingStateWithEnactCommittee(t, committee),
	)

	require.NoError(t, importGovState(
		context.Background(),
		govImportConfigForTest(db, govStateData),
		func(ImportProgress) {},
	))

	members, err := db.GetCommitteeMembers(nil)
	require.NoError(t, err)
	require.Len(t, members, 1)
	assert.Equal(t, uint8(1), members[0].ColdCredentialTag)
	assert.Equal(t, hash, members[0].ColdCredHash)
}

func TestParseGovStateCommitteeMatchesEnactState(t *testing.T) {
	hash := bytes.Repeat([]byte{0x42}, 28)
	committee := committeeWithMember(t, hash, 700)
	rootsAny := encodeRootsAsAny(t, [4]*ParsedGovActionId{})
	data, err := cbor.Encode([]any{
		[]any{rootsAny, []any{}},
		committee,
		constitutionForTest(),
		map[uint64]uint64{}, map[uint64]uint64{}, map[uint64]uint64{},
		drepPulsingStateWithEnactCommittee(t, committee),
	})
	require.NoError(t, err)
	parsed, err := ParseGovState(data, EraConway)
	require.NoError(t, err)
	require.Len(t, parsed.Committee, 1)
	require.Len(t, parsed.EnactCommittee, 1)
	assert.Equal(t, parsed.Committee, parsed.EnactCommittee)
	assert.Equal(t, parsed.CommitteeQuorum.Rat, parsed.EnactCommitteeQuorum.Rat)
}

func TestParseGovStateEnactedWarningKeepsCommittee(t *testing.T) {
	committee := committeeWithMember(t, bytes.Repeat([]byte{0x42}, 28), 700)
	rootsAny := encodeRootsAsAny(t, [4]*ParsedGovActionId{})
	data, err := cbor.Encode([]any{
		[]any{rootsAny, []any{}}, committee,
		constitutionForTest(),
		map[uint64]uint64{}, map[uint64]uint64{}, map[uint64]uint64{},
		drepPulsingStateWithEnactCommittee(t, committee),
	})
	require.NoError(t, err)
	// Keep the active committee valid while making rsEnacted contain a
	// malformed action. The warning must not become a committee error.
	data, err = cbor.Encode([]any{
		[]any{rootsAny, []any{}}, committee,
		constitutionForTest(),
		map[uint64]uint64{}, map[uint64]uint64{}, map[uint64]uint64{},
		drepPulsingStateWithEnactCommittee(t, committee, cbor.RawMessage{0x01}),
	})
	require.NoError(t, err)
	parsed, err := ParseGovState(data, EraConway)
	require.Error(t, err)
	// A malformed rsEnacted entry is warning-grade; it must not be
	// reported as a failure to decode the enact-state committee.
	assert.Nil(t, parsed.PulsingStateParseError)
	require.Len(t, parsed.EnactCommittee, 1)
	assert.True(t, parsed.EnactedActionTypesUnknown)
}

func TestImportGovStateRejectsCommitteeMismatch(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	left := committeeWithMember(t, bytes.Repeat([]byte{0x42}, 28), 700)
	right := committeeWithMember(t, bytes.Repeat([]byte{0x43}, 28), 700)
	// rsEnacted is empty, so nothing can have rewritten ensCommittee and
	// the two views are required to agree.
	govStateData := conwayGovStateWithPulsing(
		t, left, drepPulsingStateWithEnactCommittee(t, right),
	)
	err = importGovState(
		context.Background(),
		govImportConfigForTest(db, govStateData),
		func(ImportProgress) {},
	)
	require.EqualError(t, err,
		"governance committee disagrees between cgsCommittee "+
			"and rsEnactState with no committee action in rsEnacted")
}

// TestImportGovStateAcceptsCommitteeChangeInRsEnacted covers the mainnet
// bootstrap shape: a snapshot from an epoch whose RATIFY pass accepted an
// UpdateCommittee. ENACT rewrote EnactState.ensCommittee, so
// RatifyState.rsEnactState carries the committee ConwayEPOCH will install
// at the next boundary while cgsCommittee still carries the one in force.
// The importer must persist cgsCommittee and must not reject the
// snapshot.
func TestImportGovStateAcceptsCommitteeChangeInRsEnacted(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	activeHash := bytes.Repeat([]byte{0x42}, 28)
	nextHash := bytes.Repeat([]byte{0x43}, 28)
	active := committeeWithMember(t, activeHash, 700)
	next := committeeWithMember(t, nextHash, 900)
	update := govActionStateForTest(
		bytes.Repeat([]byte{0x77}, 32),
		0,
		govActionTypeUpdateCommittee,
		nil,
		499,
	)
	govStateData := conwayGovStateWithPulsing(
		t,
		active,
		drepPulsingStateWithEnactCommittee(t, next, update),
	)
	require.NoError(t, importGovState(
		context.Background(),
		govImportConfigForTest(db, govStateData),
		func(ImportProgress) {},
	))

	members, err := db.GetCommitteeMembers(nil)
	require.NoError(t, err)
	require.Len(t, members, 1)
	assert.Equal(t, activeHash, members[0].ColdCredHash)
	assert.Equal(t, uint64(700), members[0].ExpiresEpoch)
}

// TestImportGovStateAcceptsNoConfidenceInRsEnacted covers the other
// committee-rewriting action: ENACT sets ensCommittee to SNothing for
// NoConfidence, so rsEnactState carries no committee while cgsCommittee
// still does.
func TestImportGovStateAcceptsNoConfidenceInRsEnacted(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	activeHash := bytes.Repeat([]byte{0x42}, 28)
	active := committeeWithMember(t, activeHash, 700)
	noConfidence := govActionStateForTest(
		bytes.Repeat([]byte{0x78}, 32),
		0,
		govActionTypeNoConfidence,
		nil,
		499,
	)
	govStateData := conwayGovStateWithPulsing(
		t,
		active,
		drepPulsingStateWithEnactCommittee(
			t, snothingCommittee(), noConfidence,
		),
	)
	require.NoError(t, importGovState(
		context.Background(),
		govImportConfigForTest(db, govStateData),
		func(ImportProgress) {},
	))

	members, err := db.GetCommitteeMembers(nil)
	require.NoError(t, err)
	require.Len(t, members, 1)
	assert.Equal(t, activeHash, members[0].ColdCredHash)
}

func TestImportedProposalDepositContributesToDRepVotingPower(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	drepCredential := bytes.Repeat([]byte{0x91}, 28)
	returnCredential := bytes.Repeat([]byte{0x92}, 28)
	require.NoError(t, db.CreateDrep(nil, &models.Drep{
		Credential: drepCredential,
		Active:     true,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: returnCredential,
		Drep:       drepCredential,
		DrepType:   models.DrepTypeAddrKeyHash,
		AddedSlot:  1,
		Active:     true,
	}))
	returnAddress, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		returnCredential,
	)
	require.NoError(t, err)
	returnAddressBytes, err := returnAddress.Bytes()
	require.NoError(t, err)

	proposalTxHash := bytes.Repeat([]byte{0x93}, 32)
	proposalAction := []any{
		uint64(govActionTypeParameterChange),
		[]any{},
		map[uint64]uint64{},
		nil,
	}
	proposal := []any{
		[]any{proposalTxHash, uint64(0)},
		map[uint64]uint64{},
		map[uint64]uint64{},
		map[uint64]uint64{},
		[]any{
			uint64(100),
			returnAddressBytes,
			proposalAction,
			[]any{
				"https://example.com/imported-proposal",
				bytes.Repeat([]byte{0x94}, 32),
			},
		},
		uint64(499),
		uint64(504),
	}
	govStateData := govStateWithRootsAndProposals(
		t, [4]*ParsedGovActionId{}, false, []any{proposal}, nil,
	)
	require.NoError(t, importGovState(
		context.Background(),
		govImportConfigForTest(db, govStateData),
		func(ImportProgress) {},
	))

	imported, err := db.GetGovernanceProposal(proposalTxHash, 0, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), imported.Deposit)
	state, err := governance.LoadDRepVotingState(db, nil, 500, false)
	require.NoError(t, err)
	ref := models.StakeCredentialRef{Tag: 0, Key: drepCredential}
	assert.Equal(t, uint64(100), state.Powers[ref.MapKey()])
}

// TestImportGovStateRejectsMismatchWithNonCommitteeRsEnacted keeps the
// check armed for the actions that cannot touch the committee: a
// TreasuryWithdrawals in rsEnacted leaves ensCommittee alone, so a
// disagreement there is still corruption.
func TestImportGovStateRejectsMismatchWithNonCommitteeRsEnacted(
	t *testing.T,
) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	left := committeeWithMember(t, bytes.Repeat([]byte{0x42}, 28), 700)
	right := committeeWithMember(t, bytes.Repeat([]byte{0x43}, 28), 700)
	withdrawal := govActionStateForTest(
		bytes.Repeat([]byte{0x79}, 32),
		0,
		govActionTypeTreasuryWithdrawals,
		nil,
		499,
	)
	govStateData := conwayGovStateWithPulsing(
		t,
		left,
		drepPulsingStateWithEnactCommittee(t, right, withdrawal),
	)
	err = importGovState(
		context.Background(),
		govImportConfigForTest(db, govStateData),
		func(ImportProgress) {},
	)
	require.EqualError(t, err,
		"governance committee disagrees between cgsCommittee "+
			"and rsEnactState with no committee action in rsEnacted")
}

// TestImportGovStateRejectsAbsentEnactState covers the absence case: an
// rsEnactState encoded as an empty array. EnactState always encodes seven
// fields in Conway, so this is malformed input and must not silently
// disarm the corroboration.
func TestImportGovStateRejectsAbsentEnactState(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	committee := committeeWithMember(t, bytes.Repeat([]byte{0x42}, 28), 700)
	pulsing := []any{
		[]any{
			[]any{},
			map[uint64]uint64{},
			map[uint64]uint64{},
			map[uint64]uint64{},
		},
		[]any{[]any{}, []any{}, []any{}, false},
	}
	govStateData := conwayGovStateWithPulsing(t, committee, pulsing)
	err = importGovState(
		context.Background(),
		govImportConfigForTest(db, govStateData),
		func(ImportProgress) {},
	)
	require.EqualError(t, err,
		"parsing imported governance pulsing state: RatifyState enact "+
			"state has 0 elements, expected 7")
}

// TestImportGovStateSkipsParityWhenEnactedTypesUnknown covers an
// undecidable rsEnacted: with an entry whose action type cannot be
// recovered, whether a committee action was accepted is unknown, so the
// corroboration is unavailable rather than failed.
func TestImportGovStateSkipsParityWhenEnactedTypesUnknown(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	left := committeeWithMember(t, bytes.Repeat([]byte{0x42}, 28), 700)
	right := committeeWithMember(t, bytes.Repeat([]byte{0x43}, 28), 700)
	govStateData := conwayGovStateWithPulsing(
		t,
		left,
		drepPulsingStateWithEnactCommittee(
			t, right, cbor.RawMessage{0x01},
		),
	)
	require.NoError(t, importGovState(
		context.Background(),
		govImportConfigForTest(db, govStateData),
		func(ImportProgress) {},
	))
}

func TestImportGovStateSeedsPrevGovActionIds(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	pp := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x11}, 32),
		ActionIndex: 0,
	}
	hf := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x22}, 32),
		ActionIndex: 0,
	}
	cm := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x33}, 32),
		ActionIndex: 0,
	}
	cn := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x44}, 32),
		ActionIndex: 0,
	}

	// committeePresent=false makes the committee root resolve to
	// NoConfidence (3); flipping it would make it UpdateCommittee
	// (4). We assert the action_type below.
	govStateData := govStateWithRoots(
		t,
		[4]*ParsedGovActionId{pp, hf, cm, cn},
		false,
	)

	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: &RawLedgerState{
			GovStateData:  govStateData,
			Epoch:         500,
			EraIndex:      EraConway,
			EraBoundEpoch: 100,
			EraBoundSlot:  10_000,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}

	require.NoError(t, importGovState(
		context.Background(),
		cfg,
		func(ImportProgress) {},
	))

	cases := []struct {
		name       string
		txHash     []byte
		actionIdx  uint32
		actionType uint8
		queryGroup []uint8
	}{
		{
			name:       "param-update",
			txHash:     pp.TxHash,
			actionIdx:  pp.ActionIndex,
			actionType: govActionTypeParameterChange,
			queryGroup: []uint8{govActionTypeParameterChange},
		},
		{
			name:       "hard-fork",
			txHash:     hf.TxHash,
			actionIdx:  hf.ActionIndex,
			actionType: govActionTypeHardForkInitiation,
			queryGroup: []uint8{govActionTypeHardForkInitiation},
		},
		{
			name:       "committee-no-confidence",
			txHash:     cm.TxHash,
			actionIdx:  cm.ActionIndex,
			actionType: govActionTypeNoConfidence,
			queryGroup: []uint8{
				govActionTypeNoConfidence,
				govActionTypeUpdateCommittee,
			},
		},
		{
			name:       "constitution",
			txHash:     cn.TxHash,
			actionIdx:  cn.ActionIndex,
			actionType: govActionTypeNewConstitution,
			queryGroup: []uint8{govActionTypeNewConstitution},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			row, err := db.Metadata().GetGovernanceProposal(
				c.txHash, c.actionIdx, nil,
			)
			require.NoError(t, err)
			require.NotNil(t, row, "synthetic row missing for %s", c.name)
			assert.Equal(
				t, c.actionType, row.ActionType,
				"unexpected action_type for %s", c.name,
			)
			require.NotNil(
				t,
				row.EnactedEpoch,
				"EnactedEpoch unset for %s",
				c.name,
			)
			require.NotNil(
				t,
				row.EnactedSlot,
				"EnactedSlot unset for %s",
				c.name,
			)
			assert.Equal(
				t, uint64(0), row.AddedSlot,
				"AddedSlot must be 0 to survive rollback for %s", c.name,
			)

			// GetLastEnactedGovernanceProposal must surface the
			// synthetic row for its purpose; that's the lookup
			// epoch.go performs at every boundary tick.
			root, err := db.GetLastEnactedGovernanceProposal(
				c.queryGroup, nil,
			)
			require.NoError(t, err)
			require.NotNil(
				t, root, "no enacted root visible for %s", c.name,
			)
			assert.Equal(t, c.txHash, root.TxHash)
			assert.Equal(t, c.actionIdx, root.ActionIndex)
		})
	}
}

func TestImportGovStateMarksRatifiedParameterChangeFromDRepPulsingState(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	ppRoot := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x11}, 32),
		ActionIndex: 0,
	}
	txHash := bytes.Repeat([]byte{0x22}, 32)
	proposal := govActionStateForTest(
		txHash,
		0,
		govActionTypeParameterChange,
		ppRoot,
		499,
	)
	govStateData := govStateWithRootsAndProposals(
		t,
		[4]*ParsedGovActionId{ppRoot, nil, nil, nil},
		false,
		[]any{proposal},
		drepPulsingStateWithRatified(t, proposal),
	)

	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: &RawLedgerState{
			GovStateData:  govStateData,
			Epoch:         500,
			EraIndex:      EraConway,
			EraBoundEpoch: 100,
			EraBoundSlot:  10_000,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}

	require.NoError(t, importGovState(
		context.Background(),
		cfg,
		func(ImportProgress) {},
	))

	row, err := db.Metadata().GetGovernanceProposal(txHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.NotNil(t, row.RatifiedEpoch)
	require.Equal(t, uint64(500), *row.RatifiedEpoch)
	require.NotNil(t, row.RatifiedSlot)
	require.Equal(t, uint64(50_000), *row.RatifiedSlot)
}

func TestImportGovStateSeedsCommitteeUpdateWhenCommitteePresent(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	cm := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x55}, 32),
		ActionIndex: 0,
	}

	govStateData := govStateWithRoots(
		t,
		[4]*ParsedGovActionId{nil, nil, cm, nil},
		true, // committee present → root action_type = UpdateCommittee
	)

	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: &RawLedgerState{
			GovStateData:  govStateData,
			Epoch:         500,
			EraIndex:      EraConway,
			EraBoundEpoch: 100,
			EraBoundSlot:  10_000,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}

	require.NoError(t, importGovState(
		context.Background(),
		cfg,
		func(ImportProgress) {},
	))

	row, err := db.Metadata().GetGovernanceProposal(
		cm.TxHash, cm.ActionIndex, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, govActionTypeUpdateCommittee, row.ActionType)
}

func TestImportGovStateNoSeedingWhenAllSNothing(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	govStateData := govStateWithRoots(
		t,
		[4]*ParsedGovActionId{nil, nil, nil, nil},
		false,
	)

	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: &RawLedgerState{
			GovStateData:  govStateData,
			Epoch:         500,
			EraIndex:      EraConway,
			EraBoundEpoch: 100,
			EraBoundSlot:  10_000,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}

	require.NoError(t, importGovState(
		context.Background(),
		cfg,
		func(ImportProgress) {},
	))

	// Without prev gov action IDs there is nothing to seed.
	for _, group := range [][]uint8{
		{govActionTypeParameterChange},
		{govActionTypeHardForkInitiation},
		{govActionTypeNoConfidence, govActionTypeUpdateCommittee},
		{govActionTypeNewConstitution},
	} {
		root, err := db.GetLastEnactedGovernanceProposal(group, nil)
		require.NoError(t, err)
		require.Nil(t, root, "unexpected synthetic root for %v", group)
	}
}
