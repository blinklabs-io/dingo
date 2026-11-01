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

package ouroboros

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// A retry requested while an endorser block's validation is still running
// skips that block, since it is pending rather than retryable. If the running
// attempt then finds its basis block not yet the ledger tip, the request must
// still be honoured, or the block waits for the next ranking block and its
// vote window can close first.
func TestLeiosValidationRetryDuringPendingAttemptIsNotLost(t *testing.T) {
	t.Parallel()

	txRaw := testDijkstraTx(t, 0)
	blockRaw, err := cbor.Encode(&lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{{
			TransactionHash: lcommon.Blake2b256Hash(txRaw),
			TransactionSize: uint32(len(txRaw)),
		}},
	})
	require.NoError(t, err)
	point := ocommon.NewPoint(81, lcommon.Blake2b256Hash(blockRaw).Bytes())
	headerRaw := testDijkstraAnnouncementHeaderRawFor(
		t,
		point.Slot,
		testEbHash(point),
		uint64(len(blockRaw)),
	)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFirst) }) }
	retried := make(chan struct{}, 1)
	var calls atomic.Int32
	validationLedger := &fakeLeiosAnnouncementLedger{
		txValidationFunc: func(context.Context, gledger.BlockHeader) error {
			if calls.Add(1) == 1 {
				close(firstStarted)
				<-releaseFirst
				return ledger.ErrLeiosValidationParentUnavailable
			}
			select {
			case retried <- struct{}{}:
			default:
			}
			return nil
		},
	}
	o := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LeiosAnnouncementLedger: validationLedger,
	})
	defer func() { require.NoError(t, o.Close()) }()
	defer release()
	key := leiosBlockKey(point.Slot, point.Hash)
	data := &leiosEndorserBlockData{
		point:                    point,
		blockRaw:                 blockRaw,
		txCount:                  1,
		txsRaw:                   []cbor.RawMessage{txRaw},
		announcementHeaderRaw:    headerRaw,
		semanticValidationStatus: leiosEBValidationUnknown,
		slotVerified:             true,
		cacheKeys:                []string{key},
	}
	o.leiosMu.Lock()
	o.leiosEndorserBlocks[key] = data
	o.leiosMu.Unlock()

	o.scheduleLeiosEndorserBlockValidation(point, data)
	testutil.RequireReceive(
		t,
		firstStarted,
		time.Second,
		"first validation attempt start",
	)
	// The basis block becomes the ledger tip while the first attempt runs.
	o.RetryLeiosEndorserBlockValidations()
	release()

	testutil.RequireReceive(
		t,
		retried,
		5*time.Second,
		"validation retried after the pending attempt finished",
	)
	require.Eventually(t, func() bool {
		o.leiosMu.RLock()
		defer o.leiosMu.RUnlock()
		return o.leiosEndorserBlocks[key].semanticValidationStatus ==
			leiosEBValidationValid
	}, 5*time.Second, 10*time.Millisecond)
}
