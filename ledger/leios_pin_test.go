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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/leiosdb"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type staticImmutableTip struct {
	err   error
	point ocommon.Point
	found bool
}

func (s *staticImmutableTip) ImmutablePoint(
	context.Context,
) (ocommon.Point, bool, error) {
	return s.point, s.found, s.err
}

// leiosPinChain is the announcing ranking block and its certifying child on
// the primary chain, with the endorser block's content in the volatile
// partition.
type leiosPinChain struct {
	*leiosCertApplyFixture
	parent    *dijkstra.DijkstraBlock
	certifier *dijkstra.DijkstraBlock
	store     leiosdb.Store
	tip       *staticImmutableTip
	manifest  []byte
}

// withHeaderHash gives a fixture block its real header hash. The fixture
// leaves the header's CBOR unset, so every block would otherwise hash as the
// empty string and a chain of two could not tell them apart.
func withHeaderHash(t *testing.T, block *dijkstra.DijkstraBlock) {
	t.Helper()
	headerCbor, err := cbor.Encode(block.BlockHeader)
	require.NoError(t, err)
	block.BlockHeader.SetCbor(headerCbor)
}

func newLeiosPinChain(t *testing.T, withContent bool) *leiosPinChain {
	t.Helper()
	f := newLeiosCertApplyFixture(t)
	manifest := testLeiosManifest(f)
	f.ebHash = manifest.Hash()
	manifestCbor, err := manifest.MarshalCBOR()
	require.NoError(t, err)
	parent, _ := f.block(
		1, leiosCertApplyAnnouncingSlot, f.originHash, false, true,
	)
	withHeaderHash(t, parent)
	certifier, _, _ := f.certifyingChild(parent)
	withHeaderHash(t, certifier)
	cm, err := chain.NewManager(context.Background(), f.db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(securityParam(5)))
	c := cm.PrimaryChain()
	require.NoError(t, c.AddBlock(context.Background(), parent, nil))
	require.NoError(t, c.AddBlock(context.Background(), certifier, nil))
	f.ls.chain = c
	store, err := f.db.LeiosDB()
	require.NoError(t, err)
	if withContent {
		require.NoError(t, f.db.SetLeiosEB(
			parent.SlotNumber(), f.ebHash.Bytes(), manifestCbor, f.ebTxs,
		))
	}
	tip := &staticImmutableTip{found: true}
	f.ls.SetImmutableTipSource(tip)
	return &leiosPinChain{
		leiosCertApplyFixture: f,
		parent:                parent,
		certifier:             certifier,
		store:                 store,
		tip:                   tip,
		manifest:              manifestCbor,
	}
}

func (p *leiosPinChain) ebRef() leiosdb.Ref {
	return leiosdb.Ref{
		Slot: p.parent.SlotNumber(),
		Hash: p.ebHash.Bytes(),
	}
}

func (p *leiosPinChain) immutableAt(b *dijkstra.DijkstraBlock) {
	p.tip.point = ocommon.NewPoint(b.SlotNumber(), b.Hash().Bytes())
}

func TestAdvanceLeiosPinsPinsEndorserBlockOfImmutableCertifier(t *testing.T) {
	t.Parallel()
	p := newLeiosPinChain(t, true)
	p.immutableAt(p.certifier)

	require.NoError(t, p.ls.AdvanceLeiosPins(t.Context()))

	pinned, err := p.store.IsPinned(p.ebRef())
	require.NoError(t, err)
	require.True(t, pinned)
	wm, err := p.store.Watermarks()
	require.NoError(t, err)
	require.Equal(t, p.certifier.SlotNumber(), wm.PinnedThrough.Slot)
	require.Equal(t, p.certifier.Hash().Bytes(), wm.PinnedThrough.Hash)

	// Collecting everything the collector may reach leaves it readable: the
	// pin moved it out of the volatile partition.
	_, err = p.store.GarbageCollect(t.Context(), p.certifier.SlotNumber()+10)
	require.NoError(t, err)
	got, err := p.store.Get(p.ebRef())
	require.NoError(t, err)
	require.Equal(t, p.ebTxs, got.Txs)

	// A second pass has nothing left to do.
	require.NoError(t, p.ls.AdvanceLeiosPins(t.Context()))
}

// Until the certifying block is immutable it can still be rolled back, so the
// endorser block stays volatile and the collection bound keeps it.
func TestAdvanceLeiosPinsWaitsForImmutableCertifier(t *testing.T) {
	t.Parallel()
	p := newLeiosPinChain(t, true)
	p.immutableAt(p.parent)

	require.NoError(t, p.ls.AdvanceLeiosPins(t.Context()))

	pinned, err := p.store.IsPinned(p.ebRef())
	require.NoError(t, err)
	require.False(t, pinned)
	wm, err := p.store.Watermarks()
	require.NoError(t, err)
	require.Equal(t, p.parent.SlotNumber(), wm.PinnedThrough.Slot)
	// The collection bound is the pin watermark, strictly below: the
	// announcing block's own slot is protected, and its endorser block
	// survives a collection at that bound.
	_, err = p.store.GarbageCollect(t.Context(), wm.PinnedThrough.Slot)
	require.NoError(t, err)
	_, err = p.store.Get(p.ebRef())
	require.NoError(t, err)

	// The certifier becomes immutable later: the pin lands then.
	p.immutableAt(p.certifier)
	require.NoError(t, p.ls.AdvanceLeiosPins(t.Context()))
	pinned, err = p.store.IsPinned(p.ebRef())
	require.NoError(t, err)
	require.True(t, pinned)
}

// An endorser block certified only on a losing fork is never pinned, so the
// collector removes it once the immutable tip passes it, while the block the
// primary chain certifies is kept.
func TestLeiosEndorserBlockCertifiedOnLosingForkIsCollected(t *testing.T) {
	t.Parallel()
	p := newLeiosPinChain(t, true)
	p.immutableAt(p.certifier)
	forkRef := leiosdb.Ref{
		Slot: p.parent.SlotNumber(),
		Hash: bytes.Repeat([]byte{0xF0}, 32),
	}
	require.NoError(t, p.store.PutVolatile(forkRef, leiosdb.Content{
		Manifest: []byte("fork manifest"),
		Txs:      []cbor.RawMessage{},
	}))

	require.NoError(t, p.ls.AdvanceLeiosPins(t.Context()))
	wm, err := p.store.Watermarks()
	require.NoError(t, err)
	_, err = p.store.GarbageCollect(t.Context(), min(
		p.certifier.SlotNumber(), wm.PinnedThrough.Slot,
	)+1)
	require.NoError(t, err)

	_, err = p.store.Get(forkRef)
	require.ErrorIs(t, err, dbtypes.ErrBlobKeyNotFound)
	_, err = p.store.Get(p.ebRef())
	require.NoError(t, err)
}

// A certification pinned before its content arrived is recorded
// content-pending and promotion is not blocked; a later pass repairs it from
// the in-memory cache.
func TestAdvanceLeiosPinsRepairsContentPendingPin(t *testing.T) {
	t.Parallel()
	p := newLeiosPinChain(t, false)
	p.immutableAt(p.certifier)

	require.NoError(t, p.ls.AdvanceLeiosPins(t.Context()))
	pending, err := p.store.PendingPins()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, p.ebRef(), pending[0].EB)
	wm, err := p.store.Watermarks()
	require.NoError(t, err)
	require.Equal(t, p.certifier.SlotNumber(), wm.PinnedThrough.Slot,
		"a content-pending pin still advances promotion")

	p.ls.config.EndorserBlockDataProvider = func(
		hash []byte,
		slot uint64,
	) ([]byte, []cbor.RawMessage, bool) {
		if !bytes.Equal(hash, p.ebHash.Bytes()) ||
			slot != p.parent.SlotNumber() {
			return nil, nil, false
		}
		return p.manifest, p.ebTxs, true
	}
	require.NoError(t, p.ls.AdvanceLeiosPins(t.Context()))
	pending, err = p.store.PendingPins()
	require.NoError(t, err)
	require.Empty(t, pending)
	got, err := p.store.Get(p.ebRef())
	require.NoError(t, err)
	require.Equal(t, p.ebTxs, got.Txs)
}

func TestAdvanceLeiosPinsRepairsContentPendingPinFromBackfiller(t *testing.T) {
	t.Parallel()
	p := newLeiosPinChain(t, false)
	p.immutableAt(p.certifier)
	require.NoError(t, p.ls.AdvanceLeiosPins(t.Context()))

	var fetched bool
	p.ls.config.EndorserBlockDataProvider = func(
		hash []byte,
		slot uint64,
	) ([]byte, []cbor.RawMessage, bool) {
		if !fetched || !bytes.Equal(hash, p.ebHash.Bytes()) {
			return nil, nil, false
		}
		return p.manifest, p.ebTxs, true
	}
	p.ls.config.EndorserBlockFetcher = func(
		_ context.Context,
		slot uint64,
		hash []byte,
	) error {
		require.Equal(t, p.parent.SlotNumber(), slot)
		require.Equal(t, p.ebHash.Bytes(), hash)
		fetched = true
		return nil
	}
	require.NoError(t, p.ls.AdvanceLeiosPins(t.Context()))
	require.True(t, fetched)
	pending, err := p.store.PendingPins()
	require.NoError(t, err)
	require.Empty(t, pending)
}

// A truncation below the pin watermark whose pin rollback never ran leaves
// pins above the chain; the next pass reconciles them against the immutable
// point.
func TestAdvanceLeiosPinsReconcilesPinsAboveTruncatedChain(t *testing.T) {
	t.Parallel()
	p := newLeiosPinChain(t, true)
	p.immutableAt(p.certifier)
	require.NoError(t, p.ls.AdvanceLeiosPins(t.Context()))
	pinned, err := p.store.IsPinned(p.ebRef())
	require.NoError(t, err)
	require.True(t, pinned)

	// Truncate the chain below the certifier, then put the pin back as a
	// crash between the truncation's commit and the pin rollback would leave
	// it.
	parentPoint := ocommon.NewPoint(
		p.parent.SlotNumber(),
		p.parent.Hash().Bytes(),
	)
	require.NoError(t, p.ls.chain.Rollback(t.Context(), parentPoint))
	certRef := leiosdb.Ref{
		Slot: p.certifier.SlotNumber(), Hash: p.certifier.Hash().Bytes(),
	}
	_, err = p.store.Pin(p.ebRef(), certRef)
	require.NoError(t, err)
	require.NoError(t, p.store.AdvancePinnedThrough(certRef))

	p.immutableAt(p.parent)
	require.NoError(t, p.ls.AdvanceLeiosPins(t.Context()))

	pinned, err = p.store.IsPinned(p.ebRef())
	require.NoError(t, err)
	require.False(t, pinned, "the pin certified above the chain is removed")
	wm, err := p.store.Watermarks()
	require.NoError(t, err)
	require.Equal(t, p.parent.SlotNumber(), wm.PinnedThrough.Slot)
}

// An ordinary rollback of the applied tip lowers the immutable point while the
// certifying block is still on the chain. The pin, its content and its
// closure stay, because offsets into the closure still resolve through them.
func TestAdvanceLeiosPinsKeepsPinWhenImmutablePointRegresses(t *testing.T) {
	t.Parallel()
	p := newLeiosPinChain(t, true)
	closure := []byte("certified closure")
	require.NoError(t, p.db.PutLeiosClosure(
		p.parent.SlotNumber(), p.ebHash.Bytes(), closure,
	))
	p.immutableAt(p.certifier)
	require.NoError(t, p.ls.AdvanceLeiosPins(t.Context()))

	p.immutableAt(p.parent)
	require.NoError(t, p.ls.AdvanceLeiosPins(t.Context()))

	pinned, err := p.store.IsPinned(p.ebRef())
	require.NoError(t, err)
	require.True(t, pinned, "certifier is still on the chain")
	got, err := p.store.GetClosure(p.ebRef())
	require.NoError(t, err)
	require.Equal(t, closure, got)
	got2, err := p.store.Get(p.ebRef())
	require.NoError(t, err)
	require.Equal(t, p.ebTxs, got2.Txs)
	wm, err := p.store.Watermarks()
	require.NoError(t, err)
	require.Equal(t, p.certifier.SlotNumber(), wm.PinnedThrough.Slot)
}

func TestAdvanceLeiosPinsWithoutImmutablePointDoesNothing(t *testing.T) {
	t.Parallel()
	p := newLeiosPinChain(t, true)
	p.tip.found = false
	require.NoError(t, p.ls.AdvanceLeiosPins(t.Context()))
	wm, err := p.store.Watermarks()
	require.NoError(t, err)
	require.False(t, wm.PinnedThroughSet)
}

// The migration derives certification of a record with no marker from the
// primary chain.
func TestMigrateLeiosStoreDerivesCertificationFromChain(t *testing.T) {
	t.Parallel()
	p := newLeiosPinChain(t, false)
	p.immutableAt(p.certifier)

	// A legacy record under the old hash-first key, no marker.
	txn := p.db.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck
	slot := p.parent.SlotNumber()
	require.NoError(t, txn.BlobStore().Set(
		txn.Blob(),
		dbtypes.LeiosEBManifestKey(p.ebHash.Bytes(), slot), p.manifest,
	),
	)
	txsVal, err := cbor.Encode(p.ebTxs)
	require.NoError(t, err)
	require.NoError(t, txn.BlobStore().Set(
		txn.Blob(), dbtypes.LeiosEBTxsKey(p.ebHash.Bytes(), slot), txsVal,
	))
	// An uncertified record at the same slot, also below the immutable point.
	other := bytes.Repeat([]byte{0xAB}, 32)
	require.NoError(t, txn.BlobStore().Set(
		txn.Blob(), dbtypes.LeiosEBManifestKey(other, slot), []byte("other"),
	))
	require.NoError(t, txn.Commit())

	require.NoError(t, p.ls.MigrateLeiosStore(t.Context()))

	pinned, err := p.store.IsPinned(p.ebRef())
	require.NoError(t, err)
	require.True(t, pinned, "the chain certifies the record")
	pinnedOther, err := p.store.IsPinned(leiosdb.Ref{Slot: slot, Hash: other})
	require.NoError(t, err)
	require.False(t, pinnedOther)
	_, err = p.store.Get(leiosdb.Ref{Slot: slot, Hash: other})
	require.ErrorIs(t, err, dbtypes.ErrBlobKeyNotFound,
		"an uncertified record below the immutable point is dropped")
	_, err = database.BlockByPoint(
		t.Context(), p.db, ocommon.Point{Slot: slot, Hash: p.ebHash.Bytes()},
	)
	require.Error(t, err)
}

// A migration that has already finished does not consult the immutable point,
// so a chain that cannot report one does not stop startup.
func TestMigrateLeiosStoreDoneIgnoresImmutablePointError(t *testing.T) {
	t.Parallel()
	p := newLeiosPinChain(t, false)
	require.NoError(t, p.store.SetMigrationState(leiosdb.MigrationDone))
	p.tip.err = errors.New("chain unreadable")

	require.NoError(t, p.ls.MigrateLeiosStore(t.Context()))
}

// An unreadable immutable point is an unknown one: the legacy records are kept
// volatile for the pin walk to decide, and the migration still completes.
func TestMigrateLeiosStoreTreatsImmutablePointErrorAsUnknown(t *testing.T) {
	t.Parallel()
	p := newLeiosPinChain(t, false)
	p.tip.err = errors.New("chain unreadable")
	slot := p.parent.SlotNumber()
	txn := p.db.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck
	require.NoError(t, txn.BlobStore().Set(
		txn.Blob(),
		dbtypes.LeiosEBManifestKey(p.ebHash.Bytes(), slot), p.manifest,
	),
	)
	require.NoError(t, txn.Commit())

	require.NoError(t, p.ls.MigrateLeiosStore(t.Context()))

	state, err := p.store.MigrationState()
	require.NoError(t, err)
	require.Equal(t, leiosdb.MigrationDone, state)
	_, err = p.store.Get(p.ebRef())
	require.NoError(t, err, "the record is kept")
	pinned, err := p.store.IsPinned(p.ebRef())
	require.NoError(t, err)
	require.False(t, pinned)
}

// A pin watermark at the immutable slot but on a block the chain no longer
// holds must be reconciled, or the walk starts from a point the chain rejects
// and promotion stops.
func TestAdvanceLeiosPinsReconcilesWatermarkAtImmutableSlotWithOtherHash(
	t *testing.T,
) {
	t.Parallel()
	p := newLeiosPinChain(t, true)
	p.immutableAt(p.parent)
	require.NoError(t, p.store.AdvancePinnedThrough(leiosdb.Ref{
		Slot: p.parent.SlotNumber(),
		Hash: bytes.Repeat([]byte{0xAA}, 32),
	}))

	require.NoError(t, p.ls.AdvanceLeiosPins(t.Context()))

	wm, err := p.store.Watermarks()
	require.NoError(t, err)
	require.Equal(t, p.parent.SlotNumber(), wm.PinnedThrough.Slot)
	require.Equal(t, p.parent.Hash().Bytes(), wm.PinnedThrough.Hash)
}

// Pins that stay unavailable at the front of the list must not keep the pins
// behind them from being offered for repair.
func TestRepairPendingLeiosPinsRotatesPastUnavailablePins(t *testing.T) {
	t.Parallel()
	p := newLeiosPinChain(t, false)
	total := leiosPinRepairPerPass + 2
	refs := make([]leiosdb.Ref, total)
	manifests := map[string][]byte{}
	for i := range refs {
		manifest := []byte{byte(i), 0xEE}
		refs[i] = leiosdb.Ref{
			Slot: uint64(i + 1),
			Hash: lcommon.Blake2b256Hash(manifest).Bytes(),
		}
		manifests[string(refs[i].Hash)] = manifest
		_, err := p.store.Pin(refs[i], refs[i])
		require.NoError(t, err)
	}
	// Only the last pin's content is available.
	available := refs[total-1]
	p.ls.config.EndorserBlockDataProvider = func(
		hash []byte,
		_ uint64,
	) ([]byte, []cbor.RawMessage, bool) {
		if !bytes.Equal(hash, available.Hash) {
			return nil, nil, false
		}
		return manifests[string(hash)], nil, true
	}

	for range 2 {
		p.ls.leiosPinMu.Lock()
		p.ls.repairPendingLeiosPins(t.Context(), p.store)
		p.ls.leiosPinMu.Unlock()
	}

	pending, err := p.store.PendingPins()
	require.NoError(t, err)
	for _, pin := range pending {
		require.NotEqual(
			t, available, pin.EB,
			"the available pin behind the unavailable ones is repaired",
		)
	}
}
