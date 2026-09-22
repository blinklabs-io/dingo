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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFindPeerForkPathRejectsAncestorAheadOfTip is a regression test for a
// logical defect in findPeerForkPath: it resolved a "common ancestor" by a
// raw database.BlockByHash hash lookup with no check that the found block
// was actually at or before the local tip. A block row left behind in the
// persistent block index by an incomplete rollback cleanup (chain.Chain.
// rollbackLocked's per-index removeBlockByIndex loop has no atomicity across
// the whole range it deletes: a transient failure partway through leaves
// higher-index blocks stranded, still reachable via BlockByHash's hash
// index, even though the ledger's tip has already moved back below them)
// would then be trusted as a valid ancestor even though its slot was AFTER
// the local tip -- impossible for a genuine common ancestor of two chains,
// one of which sits at that tip.
//
// Trusting it would drive tryResolveFork to roll back toward a point ahead
// of where the node actually was. Every subsequent blockfetch batch would be
// built from headers past that bogus point and could never apply ("ignoring
// blockfetch block: ... does not fit on current chain tip"), so the ledger
// pipeline would make no progress, restart after restart, until it hit the
// no-progress halt -- reproducibly, since the stale row survives a process
// restart. (This defect was found and fixed while investigating a real
// mr-slave production halt on 2026-09-21/22; that incident's actual cause
// turned out to be a separate reward/withdrawal-balance validation mismatch,
// not this code path, but the defect fixed here is real and independently
// worth guarding against.)
//
// This test seeds exactly that stale row directly through
// database.Database.BlockCreate (bypassing chain.Chain's own tip-consistency
// checks, which is the point: it simulates the leftover artifact an
// incomplete rollback leaves, not a block chain.Chain would ever admit
// itself) and proves findPeerForkPath must not resolve it as an ancestor.
func TestFindPeerForkPathRejectsAncestorAheadOfTip(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	localTip := fixture.ls.chain.Tip()
	require.Equal(t, fixture.currentTip, localTip)

	// A block well AFTER the local tip, reachable only by hash -- exactly
	// what a rollback that removed everything above the tip except this one
	// stray index would leave behind. PrevHash is set to the real tip so
	// the row looks, superficially, like a legitimate direct child -- the
	// defect is not that this hash is unbelievable, it is that nothing
	// checks whether it is still reachable from where we actually are.
	orphanHash := testHashBytes("orphan-block-ahead-of-tip")
	orphanSlot := localTip.Point.Slot + 157
	require.NoError(t, fixture.ls.db.BlockCreate(models.Block{
		Slot:     orphanSlot,
		Hash:     orphanHash,
		Number:   localTip.BlockNumber + 1,
		PrevHash: localTip.Point.Hash,
		Cbor:     []byte{0x80},
	}, nil))

	// Confirm the seed actually reproduces the "reachable by hash, ahead of
	// tip" state the bug depends on, independent of findPeerForkPath: a
	// weakened seed would make every assertion below pass vacuously.
	seeded, err := fixture.ls.blockByHash(orphanHash)
	require.NoError(t, err, "the stale row must be reachable by hash")
	require.Greater(
		t,
		seeded.Slot,
		localTip.Point.Slot,
		"the seeded row must be ahead of the local tip for this test to "+
			"exercise the bug",
	)

	// A peer's incoming header claims orphanHash as its own parent -- this
	// is real, honestly-reported peer data; findPeerForkPath's job is to
	// decide whether OUR OWN local state can vouch for orphanHash as a
	// common ancestor, and it must not.
	evt := ChainsyncEvent{
		ConnectionId: testChainsyncConnId(6301, 3001),
		Point: ocommon.NewPoint(
			orphanSlot+1,
			testHashBytes("peer-child-of-orphan"),
		),
	}

	ancestorPoint, forkPath, err := fixture.ls.findPeerForkPath(
		evt,
		orphanHash,
		localTip.Point.Slot,
	)
	require.NoError(t, err)
	assert.Nil(
		t,
		ancestorPoint,
		"a block-index hit ahead of the local tip must never be accepted "+
			"as a common ancestor: found slot %d > local tip slot %d",
		seeded.Slot,
		localTip.Point.Slot,
	)
	assert.Nil(t, forkPath)
}

// TestFindPeerForkPathAcceptsAncestorAtOrBeforeTip is the positive control
// for the fix above: a block-index hit that genuinely IS at or before the
// local tip must still resolve normally. Without this, a broken fix that
// rejected every blockByHash hit unconditionally would also pass the
// negative test's "ancestorPoint is nil" assertion for the wrong reason.
func TestFindPeerForkPathAcceptsAncestorAtOrBeforeTip(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	localTip := fixture.ls.chain.Tip()

	evt := ChainsyncEvent{
		ConnectionId: testChainsyncConnId(6302, 3001),
		Point: ocommon.NewPoint(
			localTip.Point.Slot+1,
			testHashBytes("peer-child-of-tip"),
		),
	}

	// The fixture's own current tip is itself a valid ancestor of a fork
	// path that extends directly from it.
	ancestorPoint, forkPath, err := fixture.ls.findPeerForkPath(
		evt,
		localTip.Point.Hash,
		localTip.Point.Slot,
	)
	require.NoError(t, err)
	require.NotNil(t, ancestorPoint)
	assert.Equal(t, localTip.Point.Slot, ancestorPoint.Slot)
	assert.Equal(t, localTip.Point.Hash, ancestorPoint.Hash)
	assert.Len(t, forkPath, 1)
}
