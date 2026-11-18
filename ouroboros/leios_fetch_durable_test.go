// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ouroboros

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database/leiosdb"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	"github.com/stretchr/testify/require"
)

// oneTxLeiosOfferRequester serves a one-transaction endorser block.
type oneTxLeiosOfferRequester struct {
	blockRaw []byte
	tx       cbor.RawMessage
}

func (r *oneTxLeiosOfferRequester) BlockRequest(
	context.Context,
	ocommon.Point,
) (protocol.Message, error) {
	return &leiosfetch.MsgBlock{BlockRaw: r.blockRaw}, nil
}

func (r *oneTxLeiosOfferRequester) BlockTxsRequest(
	context.Context,
	ocommon.Point,
	map[uint16]uint64,
) (protocol.Message, error) {
	return leiosfetch.NewMsgBlockTxs([]cbor.RawMessage{r.tx}), nil
}

// A completed peer fetch must have its manifest and transactions in the
// Leios store when it reports success, even when the asynchronous persistence
// queue cannot accept the write.
func TestFetchLeiosTxOfferReturnsOnlyAfterDurableWrite(t *testing.T) {
	t.Parallel()

	for name, disable := range map[string]func(*Ouroboros){
		"queue full": func(o *Ouroboros) {
			o.leiosPersistMu.Lock()
			defer o.leiosPersistMu.Unlock()
			for i := range leiosPersistMaxPending {
				o.leiosPersistPending[fmt.Sprintf("filler-%d", i)] =
					&leiosPersistJob{}
			}
		},
		"writer stopped": func(o *Ouroboros) {
			close(o.leiosPersistStop)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			o := newTestOuroborosWithPausedLeiosPersistWriter(t)
			disable(o)
			// Without a ledger the announcement binding never ages out, so the
			// low test slot stays a live election.
			o.leiosAnnouncementLedger = nil

			tx, ref := testLeiosManifestTx(t, 7)
			manifestRaw, err := lcommon.LeiosEndorserBlock{
				TransactionReferences: []lcommon.LeiosTransactionReference{
					ref,
				},
			}.MarshalCBOR()
			require.NoError(t, err)
			point := ocommon.NewPoint(
				300,
				lcommon.Blake2b256Hash(manifestRaw).Bytes(),
			)
			announceLeiosElectionForTest(
				t, o, point, uint64(len(manifestRaw)),
			)
			client := &oneTxLeiosOfferRequester{
				blockRaw: manifestRaw,
				tx:       tx,
			}

			fetched := o.fetchLeiosTxOffer(
				context.Background(), client, "peer", point,
			)

			db := o.leiosDatabase()
			require.NotNil(t, db)
			manifest, _ := db.GetLeiosEBManifest(point.Hash, point.Slot)
			txs, _ := db.GetLeiosEBTxs(point.Hash, point.Slot)
			durable := string(manifest) == string(manifestRaw) &&
				len(txs) == 1
			require.Equal(
				t,
				durable,
				fetched,
				"fetch success must coincide with the endorser block being in the Leios store",
			)
			require.True(t, fetched, "the fetch itself must succeed")
		})
	}
}

// failingPutLeiosStore fails every volatile write that carries transactions
// while failing is set, so a manifest write succeeds and the write completing
// the endorser block fails.
type failingPutLeiosStore struct {
	leiosdb.Store
	failing atomic.Bool
}

func (s *failingPutLeiosStore) PutVolatile(
	eb leiosdb.Ref,
	content leiosdb.Content,
) error {
	if s.failing.Load() && content.Txs != nil {
		return errors.New("injected leios store write failure")
	}
	return s.Store.PutVolatile(eb, content)
}

// A fetch whose store write fails must report failure and leave no complete
// cache entry behind, so the next offer fetches and writes the block again
// instead of finding it cached and reporting success without a durable copy.
func TestFetchLeiosTxOfferFailsWhenDurableWriteFails(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithPausedLeiosPersistWriter(t)
	o.leiosAnnouncementLedger = nil
	db := o.leiosDatabase()
	require.NotNil(t, db)
	base, err := db.LeiosDB()
	require.NoError(t, err)
	store := &failingPutLeiosStore{Store: base}
	store.failing.Store(true)
	db.SetLeiosDB(store)
	t.Cleanup(func() { db.SetLeiosDB(base) })

	tx, ref := testLeiosManifestTx(t, 9)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(
		301,
		lcommon.Blake2b256Hash(manifestRaw).Bytes(),
	)
	announceLeiosElectionForTest(t, o, point, uint64(len(manifestRaw)))
	client := &oneTxLeiosOfferRequester{blockRaw: manifestRaw, tx: tx}

	require.False(
		t,
		o.fetchLeiosTxOffer(context.Background(), client, "peer", point),
		"a fetch whose store write failed must not report success",
	)
	cached, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.False(
		t,
		ok && cached.completeTxCache(),
		"a failed write must not leave a complete cache entry",
	)

	store.failing.Store(false)
	require.True(
		t,
		o.fetchLeiosTxOffer(context.Background(), client, "peer", point),
		"the next offer must fetch again once the store accepts writes",
	)
	txs, err := db.GetLeiosEBTxs(point.Hash, point.Slot)
	require.NoError(t, err)
	require.Len(t, txs, 1)
}
