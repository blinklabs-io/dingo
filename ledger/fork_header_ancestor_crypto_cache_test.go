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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestPeerForkAncestorCryptoCacheChecksGrowingPrefixOnce(t *testing.T) {
	t.Parallel()
	ls := &LedgerState{}
	conn := ouroboros.ConnectionId{}
	nonce := bytes.Repeat([]byte{0x42}, 32)
	var headers []gledger.BlockHeader
	for index := range 32 {
		var hash lcommon.Blake2b256
		binary.BigEndian.PutUint64(hash[:8], uint64(index+1))
		headers = append(
			headers,
			mockHeader{
				hash:        hash,
				slot:        uint64(index + 1),
				blockNumber: uint64(index + 1),
			},
		)
	}
	calls := 0
	for length := 2; length <= len(headers); length++ {
		path := headers[:length]
		ls.storePeerForkPathPrefix(connIdKey(conn), models.Block{}, path, 7, 7)
		for index := 0; index < len(path)-1; index++ {
			require.NoError(
				t,
				ls.verifyPeerForkAncestorCrypto(
					conn,
					path,
					index,
					1,
					nonce,
					7,
					func() error { calls++; return nil },
				),
			)
		}
	}
	require.Equal(
		t,
		31,
		calls,
		"each raw ancestor should pay for cryptography once, rather than 496 times across growing descendants",
	)
}

func TestPeerForkAncestorCryptoCacheDoesNotReuseWrongBasisOrFailure(
	t *testing.T,
) {
	t.Parallel()
	ls := &LedgerState{}
	conn := ouroboros.ConnectionId{}
	first := mockHeader{hash: lcommon.Blake2b256{1}, slot: 1, blockNumber: 1}
	last := mockHeader{hash: lcommon.Blake2b256{2}, slot: 2, blockNumber: 2}
	path := []gledger.BlockHeader{first, last}
	nonce := bytes.Repeat([]byte{0x42}, 32)
	ls.storePeerForkPathPrefix(connIdKey(conn), models.Block{}, path, 7, 7)
	calls := 0
	verify := func() error { calls++; return nil }
	require.NoError(
		t,
		ls.verifyPeerForkAncestorCrypto(conn, path, 0, 1, nonce, 7, verify),
	)
	require.NoError(
		t,
		ls.verifyPeerForkAncestorCrypto(conn, path, 0, 1, nonce, 7, verify),
	)
	require.Equal(t, 1, calls)
	badNonce := bytes.Repeat([]byte{0x43}, 32)
	rejected := errors.New("invalid signature or nonce")
	fail := func() error { calls++; return rejected }
	require.ErrorIs(
		t,
		ls.verifyPeerForkAncestorCrypto(conn, path, 0, 1, badNonce, 7, fail),
		rejected,
	)
	require.ErrorIs(
		t,
		ls.verifyPeerForkAncestorCrypto(conn, path, 0, 1, badNonce, 7, fail),
		rejected,
	)
	require.Equal(t, 3, calls)
	require.NoError(
		t,
		ls.verifyPeerForkAncestorCrypto(conn, path, 0, 1, nonce, 7, verify),
	)
	require.Equal(
		t,
		3,
		calls,
		"failed nonce verification must not replace a successful cache entry",
	)
	require.NoError(
		t,
		ls.verifyPeerForkAncestorCrypto(conn, path, 0, 2, nonce, 7, verify),
	)
	require.Equal(t, 4, calls)
	ls.storePeerForkPathPrefix(connIdKey(conn), models.Block{}, path, 8, 8)
	require.NoError(
		t,
		ls.verifyPeerForkAncestorCrypto(conn, path, 0, 2, nonce, 8, verify),
	)
	require.Equal(t, 5, calls)
	forkFirst := first
	forkFirst.hash = lcommon.Blake2b256{3}
	fork := []gledger.BlockHeader{forkFirst, last}
	ls.storePeerForkPathPrefix(connIdKey(conn), models.Block{}, fork, 8, 8)
	require.NoError(
		t,
		ls.verifyPeerForkAncestorCrypto(conn, fork, 0, 2, nonce, 8, verify),
	)
	require.Equal(t, 6, calls)
	require.NoError(
		t,
		ls.verifyPeerForkAncestorCrypto(conn, fork, 0, 2, nonce, 0, verify),
	)
	require.NoError(
		t,
		ls.verifyPeerForkAncestorCrypto(conn, fork, 0, 2, nonce, 0, verify),
	)
	require.Equal(
		t,
		8,
		calls,
		"an unbound verification generation must never produce a cache hit",
	)

	other := testConnectionId(11001, 12001)
	ls.storePeerForkPathPrefix(connIdKey(other), models.Block{}, fork, 8, 8)
	require.NoError(
		t,
		ls.verifyPeerForkAncestorCrypto(other, fork, 0, 2, nonce, 8, verify),
	)
	require.Equal(t, 9, calls, "another peer must verify its own retained path")
	ls.removePeerForkNoncePrefix(connIdKey(other))
	require.NotContains(t, ls.forkHeaderPathCache, connIdKey(other))
	originalNonce := bytes.Clone(nonce)
	nonce[0] ^= 0xff
	require.NoError(
		t,
		ls.verifyPeerForkAncestorCrypto(
			conn,
			fork,
			0,
			2,
			originalNonce,
			8,
			verify,
		),
	)
	require.Equal(t, 9, calls, "the cache must own its nonce bytes")
}
func TestPeerForkAncestorCryptoCacheStillChecksStateAfterCryptoSuccess(
	t *testing.T,
) {
	t.Parallel()

	cfg := newHighFreqShelleyGenesisCfg(t)
	epochNonce := bytes.Repeat([]byte{0x21}, lcommon.Blake2b256Size)
	anchor := createTestBlockWithNonceAtSlots(
		t,
		[32]byte{80},
		epochNonce,
		2_010,
		2_011,
		make([]byte, lcommon.Blake2b256Size),
		tamperNone,
	)
	anchorPoint := ocommon.NewPoint(
		anchor.block.SlotNumber(),
		anchor.block.Hash().Bytes(),
	)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.PrimaryChain().AddLocalBlock(context.Background(), anchor.block),
	)
	anchorNonce, err := eras.BabbageEraDesc.CalculateEtaVFunc(
		cfg,
		epochNonce,
		anchor.block,
	)
	require.NoError(t, err)
	require.NoError(t, db.SetBlockNonce(
		anchorPoint.Hash,
		anchorPoint.Slot,
		anchorNonce,
		false,
		nil,
	))

	epoch := models.Epoch{
		EpochId:        6,
		StartSlot:      2_000,
		SlotLength:     1_000,
		LengthInSlots:  2_000,
		EraId:          eras.BabbageEraDesc.Id,
		Nonce:          epochNonce,
		EvolvingNonce:  epochNonce,
		CandidateNonce: epochNonce,
	}
	ls := &LedgerState{
		db:           db,
		chain:        cm.PrimaryChain(),
		epochCache:   []models.Epoch{epoch},
		currentEpoch: epoch,
		currentEra:   eras.BabbageEraDesc,
		currentTip: ochainsync.Tip{
			Point:       anchorPoint,
			BlockNumber: anchor.block.BlockNumber(),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	ls.slotsPerKESPeriod.Store(anchor.slotsPerKesPeriod)
	ls.publishSnapshotsLocked()

	ancestorNumber := anchor.block.BlockNumber() + 1
	unverifiedAncestor := createTestBlockWithNonceAtSlotsAndBlockNumber(
		t,
		[32]byte{81},
		epochNonce,
		2_200,
		2_201,
		anchorPoint.Hash,
		&ancestorNumber,
		tamperNone,
	)
	targetNumber := ancestorNumber + 1
	target := createTestBlockWithNonceAtSlotsAndBlockNumber(
		t,
		[32]byte{82},
		epochNonce,
		2_300,
		2_301,
		unverifiedAncestor.block.Hash().Bytes(),
		&targetNumber,
		tamperNone,
	)
	seedBlockPoolRegistration(t, db, target.block)
	targetPool := target.block.IssuerVkey().Hash()
	seedPoolStakeSnapshot(t, db, 5, targetPool[:], 1_000_000_000)
	ls.config.PeerHeaderLookupFunc = func(
		_ ouroboros.ConnectionId,
		hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		if !bytes.Equal(hash, unverifiedAncestor.block.Hash().Bytes()) {
			return ChainsyncEvent{}, nil, false
		}
		return ChainsyncEvent{
			Point: ocommon.NewPoint(
				unverifiedAncestor.block.SlotNumber(),
				unverifiedAncestor.block.Hash().Bytes(),
			),
			BlockHeader: unverifiedAncestor.block.Header(),
		}, anchorPoint.Hash, true
	}

	contextGeneration := ls.loadConsensusSnapshot().contextGeneration
	for range 2 {
		ls.publishSnapshotsLocked()
		require.Greater(
			t,
			ls.loadConsensusSnapshot().generation,
			contextGeneration,
		)
		err = ls.ValidatePeerChainSelectionHeaderCrypto(
			ouroboros.ConnectionId{},
			target.block.Header(),
		)
		require.Error(t, err)
		require.True(
			t,
			IsHeaderVerificationDeferred(err),
			"successful cached cryptography must not authorize unavailable pool state",
		)
		require.Equal(
			t,
			contextGeneration,
			ls.forkHeaderPathCache[connIdKey(ouroboros.ConnectionId{})].ancestorCrypto.contextGeneration,
			"production verification must cache under the stable context",
		)
	}

	conn := ouroboros.ConnectionId{}
	cached := ls.forkHeaderPathCache[connIdKey(conn)]
	require.True(t, cached.ancestorCrypto.valid,
		"the production verifier must retain successful ancestor cryptography")
	observed, _, found := ls.config.PeerHeaderLookupFunc(
		conn,
		unverifiedAncestor.block.Hash().Bytes(),
	)
	require.True(t, found)
	require.Zero(
		t,
		observed.HeaderCryptoVerificationGeneration,
		"cached stateless cryptography must not mint a complete verification verdict",
	)
	badNonce := bytes.Clone(epochNonce)
	badNonce[0] ^= 0xff
	err = ls.verifyPeerForkTargetEpochAncestors(conn, cached.headers, epoch,
		badNonce, ls.loadConsensusSnapshot())
	require.Error(t, err)
	require.False(t, IsHeaderVerificationDeferred(err))
	require.ErrorContains(t, err, "VRF")

}

func TestPeerForkAncestorCryptoCacheSurvivesTipOnlyPublication(t *testing.T) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 0)
	conn := testConnectionId(11002, 12002)
	path := []gledger.BlockHeader{
		mockHeader{hash: lcommon.Blake2b256{1}, slot: 1, blockNumber: 1},
		mockHeader{hash: lcommon.Blake2b256{2}, slot: 2, blockNumber: 2},
	}
	generation, contextGeneration := f.ls.currentHeaderVerificationContext()
	nonce := bytes.Repeat([]byte{0x42}, 32)
	calls := 0
	verify := func() error { calls++; return nil }
	f.ls.storePeerForkPathPrefix(
		connIdKey(conn),
		models.Block{},
		path,
		generation,
		contextGeneration,
	)
	require.NoError(
		t,
		f.ls.verifyPeerForkAncestorCrypto(
			conn,
			path,
			0,
			1,
			nonce,
			contextGeneration,
			verify,
		),
	)
	require.Equal(t, 1, calls)
	f.ls.publishSnapshotsLocked()
	nextGeneration, nextContext := f.ls.currentHeaderVerificationContext()
	require.Greater(t, nextGeneration, generation)
	require.Equal(t, contextGeneration, nextContext)
	f.ls.storePeerForkPathPrefix(
		connIdKey(conn),
		models.Block{},
		path,
		nextGeneration,
		nextContext,
	)
	require.NoError(
		t,
		f.ls.verifyPeerForkAncestorCrypto(
			conn,
			path,
			0,
			1,
			nonce,
			nextContext,
			verify,
		),
	)
	require.Equal(
		t,
		1,
		calls,
		"tip-only publication must preserve successful stateless crypto",
	)
}
