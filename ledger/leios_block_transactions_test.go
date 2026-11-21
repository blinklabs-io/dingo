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
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestBlockTransactionsForCertifiedEndorserBlock(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	manifest := testLeiosManifest(f)
	f.ebHash = manifest.Hash()
	parent := f.announce()
	certifier, _, _ := f.certifyingChild(parent)
	manifestCbor, err := manifest.MarshalCBOR()
	require.NoError(t, err)
	require.NoError(t, f.db.SetLeiosEB(
		parent.SlotNumber(), f.ebHash.Bytes(), manifestCbor, f.ebTxs,
	))

	stored := models.Block{
		Slot: certifier.SlotNumber(), Hash: certifier.Hash().Bytes(),
		PrevHash: certifier.PrevHash().Bytes(), Number: certifier.BlockNumber(),
		Type: gledger.BlockTypeDijkstra, Cbor: certifier.Cbor(),
	}
	txs, err := f.ls.BlockTransactions(context.Background(), stored)
	require.NoError(t, err)
	require.Len(t, txs, 1)
	require.Equal(t, f.ebTx.Hash(), txs[0].Hash())
	require.Equal(t, f.ebTx.Cbor(), txs[0].Cbor())
}

// securityParam gives a chain manager the security parameter it needs to roll
// back.
type securityParam int

func (p securityParam) SecurityParam() int { return int(p) }

// A certifying block's parent can be rolled back along with it, which deletes
// the parent from the database and leaves it only in the chain manager's
// cache. Reading the certified transactions must still find the parent.
func TestBlockTransactionsFindsRolledBackParent(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	manifest := testLeiosManifest(f)
	f.ebHash = manifest.Hash()
	parent, _ := f.block(
		1,
		leiosCertApplyAnnouncingSlot,
		f.originHash,
		false,
		true,
	)
	certifier, _, _ := f.certifyingChild(parent)
	manifestCbor, err := manifest.MarshalCBOR()
	require.NoError(t, err)
	require.NoError(t, f.db.SetLeiosEB(
		parent.SlotNumber(), f.ebHash.Bytes(), manifestCbor, f.ebTxs,
	))

	// Persist the parent through the chain, then roll it back.
	cm, err := chain.NewManager(context.Background(), f.db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(securityParam(5)))
	c := cm.PrimaryChain()
	require.NoError(t, c.AddBlock(context.Background(), parent, nil))
	f.ls.chain = c
	require.NoError(
		t,
		c.Rollback(context.Background(), ocommon.NewPointOrigin()),
	)
	_, err = f.ls.BlockByHash(context.Background(), parent.Hash().Bytes())
	require.ErrorIs(t, err, models.ErrBlockNotFound)

	stored := models.Block{
		Slot: certifier.SlotNumber(), Hash: certifier.Hash().Bytes(),
		PrevHash: certifier.PrevHash().Bytes(), Number: certifier.BlockNumber(),
		Type: gledger.BlockTypeDijkstra, Cbor: certifier.Cbor(),
	}
	txs, err := f.ls.BlockTransactions(context.Background(), stored)
	require.NoError(t, err)
	require.Len(t, txs, 1)
	require.Equal(t, f.ebTx.Hash(), txs[0].Hash())
}

func TestBlockTransactionsUsesLiveEndorserBlockBeforePersistence(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	manifest := testLeiosManifest(f)
	f.ebHash = manifest.Hash()
	parent := f.announce()
	certifier, _, _ := f.certifyingChild(parent)
	manifestCbor, err := manifest.MarshalCBOR()
	require.NoError(t, err)
	f.ls.config.EndorserBlockDataProvider = func(hash []byte, slot uint64) ([]byte, []cbor.RawMessage, bool) {
		if !bytes.Equal(hash, f.ebHash.Bytes()) || slot != parent.SlotNumber() {
			return nil, nil, false
		}
		return manifestCbor, f.ebTxs, true
	}
	stored := models.Block{
		Slot: certifier.SlotNumber(), Hash: certifier.Hash().Bytes(),
		PrevHash: certifier.PrevHash().Bytes(), Number: certifier.BlockNumber(),
		Type: gledger.BlockTypeDijkstra, Cbor: certifier.Cbor(),
	}

	// Leios block persistence is asynchronous, but a certified ranking block
	// can be applied while its verified endorser block is still in memory.
	txs, err := f.ls.BlockTransactions(context.Background(), stored)
	require.NoError(t, err)
	require.Len(t, txs, 1)
	require.Equal(t, f.ebTx.Hash(), txs[0].Hash())
	require.Equal(t, f.ebTx.Cbor(), txs[0].Cbor())
}

func TestBlockTransactionsForCertifiedEndorserBlockRejectsInvalidStoredBody(
	t *testing.T,
) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	manifest := testLeiosManifest(f)
	f.ebHash = manifest.Hash()
	parent := f.announce()
	certifier, _, _ := f.certifyingChild(parent)
	manifestCbor, err := manifest.MarshalCBOR()
	require.NoError(t, err)
	badBody, err := cbor.Encode([]any{cbor.RawMessage{0x80}})
	require.NoError(t, err)
	require.NoError(t, f.db.SetLeiosEB(
		parent.SlotNumber(), f.ebHash.Bytes(), manifestCbor,
		[]cbor.RawMessage{badBody},
	))
	stored := models.Block{
		Slot: certifier.SlotNumber(), Hash: certifier.Hash().Bytes(),
		PrevHash: certifier.PrevHash().Bytes(), Number: certifier.BlockNumber(),
		Type: gledger.BlockTypeDijkstra, Cbor: certifier.Cbor(),
	}
	_, err = f.ls.BlockTransactions(context.Background(), stored)
	require.Error(t, err)
	require.NotErrorIs(
		t, err, ErrCertifiedClosureUnavailable,
		"a corrupt closure must not be retried as if it were absent",
	)
}

// A certifying block whose closure the ledger has not persisted yet, with no
// provider data either, is reported as unavailable so a caller can wait for
// the ledger to apply it rather than treat it as a failure.
func TestBlockTransactionsReportsUnavailableClosure(t *testing.T) {
	t.Parallel()
	for name, provider := range map[string]EndorserBlockDataProviderFunc{
		"no provider": nil,
		"provider misses": func(
			[]byte, uint64,
		) ([]byte, []cbor.RawMessage, bool) {
			return nil, nil, false
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newLeiosCertApplyFixture(t)
			manifest := testLeiosManifest(f)
			f.ebHash = manifest.Hash()
			parent := f.announce()
			certifier, _, _ := f.certifyingChild(parent)
			f.ls.config.EndorserBlockDataProvider = provider
			stored := models.Block{
				Slot: certifier.SlotNumber(), Hash: certifier.Hash().Bytes(),
				PrevHash: certifier.PrevHash().Bytes(),
				Number:   certifier.BlockNumber(),
				Type:     gledger.BlockTypeDijkstra, Cbor: certifier.Cbor(),
			}
			_, err := f.ls.BlockTransactions(context.Background(), stored)
			require.ErrorIs(t, err, ErrCertifiedClosureUnavailable)
		})
	}
}

func testLeiosManifest(f *leiosCertApplyFixture) *lcommon.LeiosEndorserBlock {
	return &lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{{
			TransactionHash: lcommon.Blake2b256Hash(f.ebTxs[0]),
			TransactionSize: uint32(len(f.ebTxs[0])),
		}},
	}
}

func TestBlockTransactionCountReceiptRechecksStoredContent(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"body", "count", "manifest", "pruned"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			f := newLeiosCertApplyFixture(t)
			manifest := testLeiosManifest(f)
			f.ebHash = manifest.Hash()
			parent := f.announce()
			certifier, _, _ := f.certifyingChild(parent)
			raw, err := manifest.MarshalCBOR()
			require.NoError(t, err)
			require.NoError(
				t,
				f.db.SetLeiosEB(
					parent.SlotNumber(),
					f.ebHash.Bytes(),
					raw,
					f.ebTxs,
				),
			)
			stored := models.Block{
				Slot:     certifier.SlotNumber(),
				Hash:     certifier.Hash().Bytes(),
				PrevHash: certifier.PrevHash().Bytes(),
				Number:   certifier.BlockNumber(),
				Type:     gledger.BlockTypeDijkstra,
				Cbor:     certifier.Cbor(),
			}
			count, err := f.ls.BlockTransactionCount(t.Context(), stored)
			require.NoError(t, err)
			require.Equal(t, 1, count)
			receipt := f.ls.certifiedClosureReceipt.Load()
			require.NotNil(t, receipt)
			count, err = f.ls.BlockTransactionCount(t.Context(), stored)
			require.NoError(t, err)
			require.Equal(t, 1, count)
			require.Same(
				t,
				receipt,
				f.ls.certifiedClosureReceipt.Load(),
				"verified count must reuse the completed decode",
			)
			switch change {
			case "body":
				require.NoError(
					t,
					f.db.SetLeiosEB(
						parent.SlotNumber(),
						f.ebHash.Bytes(),
						raw,
						[]cbor.RawMessage{{0x80}},
					),
				)
			case "count":
				require.NoError(
					t,
					f.db.SetLeiosEB(
						parent.SlotNumber(),
						f.ebHash.Bytes(),
						raw,
						[]cbor.RawMessage{},
					),
				)
			case "manifest":
				require.NoError(
					t,
					f.db.SetLeiosEB(
						parent.SlotNumber(),
						f.ebHash.Bytes(),
						[]byte{0x80},
						f.ebTxs,
					),
				)
			case "pruned":
				store, err := f.db.LeiosDB()
				require.NoError(t, err)
				_, err = store.GarbageCollect(
					t.Context(),
					parent.SlotNumber()+1,
				)
				require.NoError(t, err)
			}
			_, fullErr := f.ls.BlockTransactions(t.Context(), stored)
			require.Error(t, fullErr)
			_, countErr := f.ls.BlockTransactionCount(t.Context(), stored)
			require.EqualError(t, countErr, fullErr.Error())
			require.Same(
				t,
				receipt,
				f.ls.certifiedClosureReceipt.Load(),
				"failed reads must not publish a receipt",
			)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			_, err = f.ls.BlockTransactionCount(ctx, stored)
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

func TestBlockTransactionCountKeepsTopLevelSemantics(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixtureWith(
		t,
		pathTx{top: pathLevel{funds: 1}, children: []pathLevel{{funds: 1}}},
	)
	manifest := testLeiosManifest(f)
	f.ebHash = manifest.Hash()
	parent := f.announce()
	certifier, _, _ := f.certifyingChild(parent)
	raw, err := manifest.MarshalCBOR()
	require.NoError(t, err)
	require.NoError(
		t,
		f.db.SetLeiosEB(parent.SlotNumber(), f.ebHash.Bytes(), raw, f.ebTxs),
	)
	stored := models.Block{
		Slot:     certifier.SlotNumber(),
		Hash:     certifier.Hash().Bytes(),
		PrevHash: certifier.PrevHash().Bytes(),
		Number:   certifier.BlockNumber(),
		Type:     gledger.BlockTypeDijkstra,
		Cbor:     certifier.Cbor(),
	}
	txs, err := f.ls.BlockTransactions(t.Context(), stored)
	require.NoError(t, err)
	require.Len(t, txs, 1)
	receipt := f.ls.certifiedClosureReceipt.Load()
	require.NotNil(t, receipt)
	count, err := f.ls.BlockTransactionCount(t.Context(), stored)
	require.NoError(t, err)
	require.Equal(t, len(txs), count)
	require.Same(t, receipt, f.ls.certifiedClosureReceipt.Load())
}

// Applying a certifying ranking block persists the certified endorser block
// itself. The fixture installs no endorser-block persistence, so nothing but
// the apply wrote em/et, and the in-memory data provider has since expired.
func TestCertifiedApplyPersistsEndorserBlockWithoutPersistWriter(
	t *testing.T,
) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	manifest := testLeiosManifest(f)
	f.ebHash = manifest.Hash()
	manifestCbor, err := manifest.MarshalCBOR()
	require.NoError(t, err)
	var providerExpired atomic.Bool
	f.ls.config.EndorserBlockDataProvider = func(
		hash []byte,
		_ uint64,
	) ([]byte, []cbor.RawMessage, bool) {
		if providerExpired.Load() || !bytes.Equal(hash, f.ebHash.Bytes()) {
			return nil, nil, false
		}
		return manifestCbor, f.ebTxs, true
	}
	parent := f.announce()
	certifier, offsets, envParent := f.certifyingChild(parent)
	require.NoError(t, f.ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{certifier},
	))
	require.NoError(t, f.process(certifier, offsets, envParent))
	providerExpired.Store(true)

	gotManifest, err := f.db.GetLeiosEBManifest(
		f.ebHash.Bytes(),
		parent.SlotNumber(),
	)
	require.NoError(t, err)
	require.Equal(t, manifestCbor, gotManifest)
	gotTxs, err := f.db.GetLeiosEBTxs(f.ebHash.Bytes(), parent.SlotNumber())
	require.NoError(t, err)
	require.Equal(t, f.ebTxs, gotTxs)

	stored := models.Block{
		Slot: certifier.SlotNumber(), Hash: certifier.Hash().Bytes(),
		PrevHash: certifier.PrevHash().Bytes(), Number: certifier.BlockNumber(),
		Type: gledger.BlockTypeDijkstra, Cbor: certifier.Cbor(),
	}
	txs, err := f.ls.BlockTransactions(t.Context(), stored)
	require.NoError(t, err)
	require.Len(t, txs, 1)
	require.Equal(t, f.ebTx.Hash(), txs[0].Hash())
}

// A certifying block rejected during closure validation leaves no endorser
// block record behind.
func TestRejectedCertifyingBlockPersistsNoEndorserBlock(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	f.removeEndorserWitnesses()
	manifest := testLeiosManifest(f)
	f.ebHash = manifest.Hash()
	manifestCbor, err := manifest.MarshalCBOR()
	require.NoError(t, err)
	f.ls.config.EndorserBlockDataProvider = func(
		hash []byte,
		_ uint64,
	) ([]byte, []cbor.RawMessage, bool) {
		return manifestCbor, f.ebTxs, bytes.Equal(hash, f.ebHash.Bytes())
	}
	requireRejectedEndorserClosure(t, f)

	_, err = f.db.GetLeiosEBManifest(
		f.ebHash.Bytes(),
		leiosCertApplyAnnouncingSlot,
	)
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
	pinned, err := f.db.IsLeiosEBPinned(
		leiosCertApplyAnnouncingSlot,
		f.ebHash.Bytes(),
	)
	require.NoError(t, err)
	require.False(t, pinned)
}

// The manifest persisted for a certified endorser block comes from the same
// cache snapshot as the transactions apply used. An eviction right after that
// lookup (TTL or the aggregate byte budget) must neither fail the block nor
// leave the closure unpersisted.
func TestCertifiedApplyPersistsEndorserBlockEvictedAfterLookup(
	t *testing.T,
) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	manifest := testLeiosManifest(f)
	f.ebHash = manifest.Hash()
	manifestCbor, err := manifest.MarshalCBOR()
	require.NoError(t, err)
	var evicted atomic.Bool
	txsProvider := f.ls.config.EndorserBlockProvider
	f.ls.config.EndorserBlockProvider = func(
		hash []byte,
		slot uint64,
	) ([]cbor.RawMessage, bool) {
		if evicted.Swap(true) {
			return nil, false
		}
		return txsProvider(hash, slot)
	}
	f.ls.config.EndorserBlockDataProvider = func(
		hash []byte,
		slot uint64,
	) ([]byte, []cbor.RawMessage, bool) {
		if evicted.Swap(true) || !bytes.Equal(hash, f.ebHash.Bytes()) ||
			slot != leiosCertApplyAnnouncingSlot {
			return nil, nil, false
		}
		return manifestCbor, f.ebTxs, true
	}
	parent := f.announce()
	certifier, offsets, envParent := f.certifyingChild(parent)
	require.NoError(t, f.process(certifier, offsets, envParent))

	gotManifest, err := f.db.GetLeiosEBManifest(
		f.ebHash.Bytes(),
		parent.SlotNumber(),
	)
	require.NoError(t, err)
	require.Equal(t, manifestCbor, gotManifest)
	gotTxs, err := f.db.GetLeiosEBTxs(f.ebHash.Bytes(), parent.SlotNumber())
	require.NoError(t, err)
	require.Equal(t, f.ebTxs, gotTxs)
}

// Equal transaction counts do not make stored bytes the verified closure: a
// same-length stale list is rewritten.
func TestPersistCertifiedEndorserBlockRepairsSameCountStoredTxs(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	manifest := testLeiosManifest(f)
	f.ebHash = manifest.Hash()
	manifestCbor, err := manifest.MarshalCBOR()
	require.NoError(t, err)
	hash, slot := f.ebHash.Bytes(), uint64(7)
	stale := make([]cbor.RawMessage, len(f.ebTxs))
	for i := range stale {
		stale[i] = cbor.RawMessage{0x40}
	}
	require.NoError(t, f.db.SetLeiosEB(slot, hash, manifestCbor, stale))

	require.NoError(t, f.ls.persistCertifiedEndorserBlock(
		hash, slot, manifestCbor, f.ebTxs,
	))
	got, err := f.db.GetLeiosEBTxs(hash, slot)
	require.NoError(t, err)
	require.Equal(t, f.ebTxs, got)
}
