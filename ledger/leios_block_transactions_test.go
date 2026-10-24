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
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
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

func TestBlockTransactionsForCertifiedEndorserBlockRejectsInvalidStoredBody(t *testing.T) {
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
}

func testLeiosManifest(f *leiosCertApplyFixture) *lcommon.LeiosEndorserBlock {
	return &lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{{
			TransactionHash: lcommon.Blake2b256Hash(f.ebTxs[0]),
			TransactionSize: uint32(len(f.ebTxs[0])),
		}},
	}
}
