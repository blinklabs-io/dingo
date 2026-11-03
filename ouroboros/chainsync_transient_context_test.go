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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func transientContextErr(t *testing.T) error {
	t.Helper()
	_, err := (&ledger.LedgerState{}).ValidateChainSelectionHeaderCryptoWithGeneration(nil)
	require.True(t, ledger.IsHeaderVerificationContextTransient(err))
	return err
}

func TestChainsyncClientRollForwardRetriesTransientLocalExtension(t *testing.T) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.46:3001")
	transientErr := transientContextErr(t)
	settle := newSettleSignal()
	f.o.config.ChainsyncHeaderVerificationSettled = settle.current
	header := newTestBlockHeader(400, 1, 0xb7)
	header.(*testBlockHeader).prevHash[0] = 0xb0
	parent := header.PrevHash().Bytes()
	f.o.chainsyncLocalChainTips = func() ([]byte, []byte) { return []byte{0xa0}, parent }
	var verified atomic.Bool
	f.o.config.ChainsyncApplyEligible = func(ouroboros.ConnectionId) bool {
		return verified.Load()
	}
	var attempts atomic.Int32
	attempted := make(chan struct{}, 1)
	f.o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		if attempts.Add(1) == 1 {
			attempted <- struct{}{}
			return 0, transientErr
		}
		verified.Store(true)
		return 42, nil
	}
	done := make(chan struct{})
	connDone := make(chan any)
	var callbackErr error
	go func() {
		defer close(done)
		callbackErr = f.o.chainsyncClientRollForward(
			ochainsync.CallbackContext{ConnectionId: f.conn, ConnectionDoneChan: connDone},
			0, header,
			ochainsync.Tip{
				Point:       ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes()),
				BlockNumber: header.BlockNumber(),
			},
		)
	}()
	t.Cleanup(func() {
		close(connDone)
		testutil.RequireReceive(t, done, testutil.AsyncWait, "callback cleanup")
	})
	testutil.RequireReceive(t, attempted, testutil.AsyncWait, "initial context race")
	testutil.RequireNoReceive(t, done, 100*time.Millisecond,
		"the peer cursor must retain an extension while its context settles")
	settle.publish()
	testutil.RequireReceive(t, done, testutil.AsyncWait, "verified extension")
	require.NoError(t, callbackErr)
	require.EqualValues(t, 2, attempts.Load())
	delivered := testutil.RequireReceive(t, f.ledgerCh, testutil.AsyncWait, "parent admission")
	parentEvent := delivered.Data.(ledger.ChainsyncEvent)
	require.EqualValues(t, 42, parentEvent.HeaderCryptoVerificationGeneration)
	require.Equal(t, header.Hash().Bytes(), parentEvent.Point.Hash)

	child := newTestBlockHeader(401, 2, 0xb8)
	copy(child.(*testBlockHeader).prevHash[:], header.Hash().Bytes())
	f.rollForward(t, child)
	delivered = testutil.RequireReceive(t, f.ledgerCh, testutil.AsyncWait, "linked child admission")
	require.Equal(t, child.Hash().Bytes(), delivered.Data.(ledger.ChainsyncEvent).Point.Hash)
	testutil.RequireNoReceive(t, f.recycle, 100*time.Millisecond, "no peer fault")
}

func TestChainsyncClientRollForwardParksTransientLocalExtensionAtBound(t *testing.T) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.47:3001")
	f.o.ledgerState = newTestLedgerState(t)
	transientErr := transientContextErr(t)
	f.o.catchUpBandWaitTimeout = 25 * time.Millisecond
	f.o.config.ChainsyncHeaderVerificationSettled = newSettleSignal().current
	header := newTestBlockHeader(400, 1, 0xb9)
	parent := header.PrevHash().Bytes()
	f.o.chainsyncLocalChainTips = func() ([]byte, []byte) { return []byte{0xa0}, parent }
	f.o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		return 0, transientErr
	}

	f.rollForward(t, header)
	f.o.deferredHeaderRecoveryMu.Lock()
	_, parked := f.o.deferredHeaderRecovery[f.conn]
	f.o.deferredHeaderRecoveryMu.Unlock()
	require.True(t, parked, "an expired transient-context retry must park the extension")
	testutil.RequireNoReceive(t, f.ledgerCh, 100*time.Millisecond,
		"an exhausted context retry must not admit an unstamped extension")
	testutil.RequireNoReceive(t, f.recycle, 100*time.Millisecond, "no peer fault")
}

func TestChainsyncClientRollForwardRejectsInvalidExtensionAfterContextRetry(t *testing.T) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.48:3001")
	transientErr := transientContextErr(t)
	settle := newSettleSignal()
	f.o.config.ChainsyncHeaderVerificationSettled = settle.current
	header := newTestBlockHeader(400, 1, 0xba)
	parent := header.PrevHash().Bytes()
	f.o.chainsyncLocalChainTips = func() ([]byte, []byte) { return []byte{0xa0}, parent }
	var attempts atomic.Int32
	f.o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		if attempts.Add(1) == 1 {
			settle.publish()
			return 0, transientErr
		}
		return 0, errors.New("invalid VRF proof")
	}

	f.rollForward(t, header)
	require.EqualValues(t, 2, attempts.Load())
	testutil.RequireReceive(t, f.recycle, testutil.AsyncWait, "invalid proof rejection")
	testutil.RequireNoReceive(t, f.ledgerCh, 100*time.Millisecond,
		"retrying a context race must not admit an invalid header")
}
