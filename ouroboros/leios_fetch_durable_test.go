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
	"fmt"
	"testing"

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
