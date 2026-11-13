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
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestChainsyncEagernessRecoveryAuthenticatesAndBindsSession(t *testing.T) {
	t.Parallel()
	f := newPatienceRollForwardFixture(t, false)
	f.o.chainSelectionShouldVerifyHeaderCrypto = func(ocommon.Point) bool { return true }
	var verified atomic.Int64
	f.o.chainSelectionVerifyHeaderCrypto = func(ouroboros.ConnectionId, gledger.BlockHeader) (uint64, error) {
		verified.Add(1)
		return 42, nil
	}
	header := newTestBlockHeader(100, 1, 0xaa)
	var current func() bool
	var currentMutex sync.Mutex
	var recovered atomic.Int64
	f.o.config.ChainsyncRecoverDeliveredPeerTip = func(
		_ context.Context,
		cb ochainsync.CallbackContext,
		update chainselection.PeerTipUpdateEvent,
		h gledger.BlockHeader,
		valid func() bool,
	) {
		recovered.Add(1)
		require.EqualValues(
			t,
			1,
			verified.Load(),
			"recovery must follow successful authentication",
		)
		require.Equal(t, f.conn, cb.ConnectionId)
		require.Equal(t, header.Hash(), h.Hash())
		require.Equal(t, header.Hash().Bytes(), update.ObservedTip.Point.Hash)
		require.EqualValues(t, 42, update.HeaderCryptoVerificationGeneration)
		require.Zero(
			t,
			update.AdmissionID,
			"observer recovery cannot manufacture admission",
		)
		currentMutex.Lock()
		current = valid
		currentMutex.Unlock()
	}
	f.rollForward(t, header)
	require.GreaterOrEqual(
		t,
		recovered.Load(),
		int64(1),
		"the observer path must invoke the configured recovery hook",
	)
	require.NotNil(t, current)
	require.True(t, current())
	f.state.RemoveClientConnId(f.conn)
	require.True(t, f.state.AddClientConnId(f.conn))
	require.False(
		t,
		current(),
		"the captured callback must not authorize a replaced session",
	)
}

func TestChainsyncEagernessRecoveryRefreshesProofWhileHeld(t *testing.T) {
	t.Parallel()
	f := newPatienceRollForwardFixture(t, true)
	f.o.eagernessWaitTimeout = 5 * time.Second
	var generation atomic.Uint64
	generation.Store(217)
	selector := chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{
			GenesisMode: true, SecurityParam: 432, MinCorroboratingPeers: 1,
			CurrentHeaderCryptoVerificationGeneration: generation.Load,
			HeaderCryptoVerificationGenerationCurrent: func(value uint64) bool {
				return value == generation.Load()
			},
			PinHeaderCryptoVerificationGeneration: func(value uint64) (func(), bool) {
				return func() {}, value == generation.Load()
			},
		},
	)
	header := newTestBlockHeader(7656, 7656, 0xaa)
	var path []ochainsync.Tip
	for number := uint64(7210); number <= header.BlockNumber(); number++ {
		hash := sha256.Sum256([]byte(fmt.Sprint(number)))
		path = append(path, ochainsync.Tip{
			Point: ocommon.NewPoint(number, hash[:]), BlockNumber: number,
		})
	}
	path[len(path)-1].Point = ocommon.NewPoint(
		header.SlotNumber(),
		header.Hash().Bytes(),
	)
	admitted := chainselection.PeerTipUpdateEvent{
		ConnectionId: f.conn, AdmissionID: 1, ObservedTip: path[0],
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(1_000_000, make([]byte, 32)),
			BlockNumber: 1_000_000,
		},
		HeaderCryptoVerificationGeneration: generation.Load(),
	}
	require.True(t, selector.PreparePeerTipAdmission(admitted))
	require.True(t, selector.CommitPeerTipAdmission(admitted))
	selector.SetLocalTip(
		ochainsync.Tip{Point: path[432].Point, BlockNumber: 7642},
	)
	require.EqualValues(t, 7642, selector.EagernessLimit().BlockNumber)
	f.o.chainSelectionShouldVerifyHeaderCrypto = func(ocommon.Point) bool { return true }
	var verifications atomic.Int64
	f.o.chainSelectionVerifyHeaderCrypto = func(ouroboros.ConnectionId, gledger.BlockHeader) (uint64, error) {
		verifications.Add(1)
		return generation.Load(), nil
	}
	f.o.config.ChainsyncObservePeerTip = func(chainselection.PeerTipUpdateEvent) chainselection.PeerTipUpdateDisposition {
		return chainselection.PeerTipUpdateAccepted
	}
	var attempts atomic.Int64
	f.o.config.ChainsyncRecoverDeliveredPeerTip = func(
		_ context.Context, _ ochainsync.CallbackContext, update chainselection.PeerTipUpdateEvent,
		h gledger.BlockHeader, sessionCurrent func() bool,
	) {
		attempts.Add(1)
		anchor, _, needed := selector.EagernessDeliveryRepair(update)
		if needed {
			verifiedGeneration, err := f.o.chainSelectionVerifyHeaderCrypto(update.ConnectionId, h)
			require.NoError(t, err)
			selector.RecordVerifiedEagernessPath(
				update,
				anchor,
				path,
				verifiedGeneration,
				sessionCurrent,
				sessionCurrent,
			)
		}
	}
	f.o.config.ChainsyncAwaitEagerness = func(
		ctx context.Context, conn ouroboros.ConnectionId, block uint64, applied func() ochainsync.Tip,
	) error {
		require.EqualValues(
			t,
			8088,
			selector.EagernessLimit().BlockNumber,
			"the initial authenticated delivery must repair the path before waiting",
		)
		generation.Store(237)
		require.EqualValues(
			t,
			7642,
			selector.EagernessLimit().BlockNumber,
			"a ledger publication must expire the old proof even while its header is held",
		)
		return selector.AwaitEagernessLimit(ctx, conn, block, applied)
	}
	f.rollForward(t, header)
	require.GreaterOrEqual(t, verifications.Load(), int64(3),
		"both initial and renewed path recovery must invoke authentication")
	require.GreaterOrEqual(
		t,
		attempts.Load(),
		int64(2),
		"a held header must reauthenticate its expired proof without another delivery",
	)
	require.EqualValues(t, 8088, selector.EagernessLimit().BlockNumber,
		"the renewed proof must release the actual selector wait")
}

func TestChainsyncEagernessRecoveryStopCancelsAndJoins(t *testing.T) {
	t.Parallel()
	f := newPatienceRollForwardFixture(t, true)
	entered, canceled := make(chan struct{}), make(chan struct{})
	f.o.config.ChainsyncRecoverDeliveredPeerTip = func(
		ctx context.Context, _ ochainsync.CallbackContext, _ chainselection.PeerTipUpdateEvent,
		_ gledger.BlockHeader, _ func() bool,
	) {
		close(entered)
		<-ctx.Done()
		close(canceled)
	}
	header := newTestBlockHeader(100, 1, 0xaa)
	session := f.state.ClientSession(f.conn)
	f.state.RecordAuthenticatedDeliveryForSession(
		session,
		chainsync.ObservedHeader{
			ConnectionId: f.conn, BlockHeader: header,
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockNumber: header.BlockNumber(), HeaderCryptoVerificationGeneration: 42,
		},
	)
	stop := f.o.startChainsyncEagernessRecovery(
		t.Context(),
		ochainsync.CallbackContext{ConnectionId: f.conn},
		session,
	)
	t.Cleanup(stop)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the held-header retry did not start")
	}
	stop()
	select {
	case <-canceled:
	default:
		t.Fatal("ending the wait must cancel and join the in-flight repair")
	}
}

func TestChainsyncEagernessRecoveryRefreshesObserverBeforeAuthentication(
	t *testing.T,
) {
	t.Parallel()
	f := newPatienceRollForwardFixture(t, false)
	f.o.chainSelectionShouldVerifyHeaderCrypto = func(ocommon.Point) bool { return true }
	var generation atomic.Uint64
	generation.Store(217)
	selector := chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{
			GenesisMode: true, SecurityParam: 432, MinCorroboratingPeers: 1,
			CurrentHeaderCryptoVerificationGeneration: generation.Load,
			HeaderCryptoVerificationGenerationCurrent: func(value uint64) bool {
				return value == generation.Load()
			},
			PinHeaderCryptoVerificationGeneration: func(value uint64) (func(), bool) {
				return func() {}, value == generation.Load()
			},
		},
	)
	previous := newTestBlockHeader(6441, 6441, 0xaa)
	var path []ochainsync.Tip
	for number := uint64(6021); number <= previous.BlockNumber(); number++ {
		hash := sha256.Sum256([]byte(fmt.Sprint(number)))
		path = append(
			path,
			ochainsync.Tip{
				Point:       ocommon.NewPoint(number, hash[:]),
				BlockNumber: number,
			},
		)
	}
	path[len(path)-1].Point = ocommon.NewPoint(
		previous.SlotNumber(),
		previous.Hash().Bytes(),
	)
	admitted := chainselection.PeerTipUpdateEvent{
		ConnectionId: f.conn, AdmissionID: 1, ObservedTip: path[0],
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(1_000_000, make([]byte, 32)),
			BlockNumber: 1_000_000,
		},
		HeaderCryptoVerificationGeneration: generation.Load(),
	}
	require.True(t, selector.PreparePeerTipAdmission(admitted))
	require.True(t, selector.CommitPeerTipAdmission(admitted))
	selector.SetLocalTip(
		ochainsync.Tip{
			Point:       ocommon.NewPoint(6453, make([]byte, 32)),
			BlockNumber: 6453,
		},
	)
	var pendingChecks atomic.Int64
	withheld := transientContextErr(t)
	f.o.chainSelectionVerifyHeaderCrypto = func(_ ouroboros.ConnectionId, h gledger.BlockHeader) (uint64, error) {
		if h.BlockNumber() != previous.BlockNumber() {
			pendingChecks.Add(1)
			return 0, withheld
		}
		return generation.Load(), nil
	}
	var wrongHeader atomic.Bool
	f.o.config.ChainsyncRecoverDeliveredPeerTip = func(
		_ context.Context, _ ochainsync.CallbackContext, update chainselection.PeerTipUpdateEvent,
		h gledger.BlockHeader, current func() bool,
	) {
		if h.BlockNumber() != previous.BlockNumber() {
			wrongHeader.Store(true)
			return
		}
		anchor, _, needed := selector.EagernessDeliveryRepair(update)
		if needed {
			selector.RecordVerifiedEagernessPath(
				update,
				anchor,
				path,
				generation.Load(),
				current,
				current,
			)
		}
	}
	f.rollForward(t, previous)
	retained, retainedOK := f.state.LatestAuthenticatedDeliveryForSession(
		f.state.ClientSession(f.conn),
		f.conn,
	)
	require.True(
		t,
		retainedOK,
		"successful observer authentication must remain available to its next callback",
	)
	require.Equal(t, previous.BlockNumber(), retained.BlockNumber)
	require.EqualValues(t, 217, retained.HeaderCryptoVerificationGeneration)
	require.EqualValues(t, 6873, selector.EagernessLimit().BlockNumber)
	generation.Store(237)
	require.EqualValues(
		t,
		6453,
		selector.EagernessLimit().BlockNumber,
		"the observer's old proof must expire before its next unverified header arrives",
	)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	f.o.chainsyncObserverHorizonWait = func(ctx context.Context, _ ledger.ChainsyncEvent) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() { done <- f.rollForwardError(newTestBlockHeader(6500, 6500, 0xbb)) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the observer did not reach its pre-verification horizon wait")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	waitErr := selector.AwaitEagernessLimit(ctx, f.conn, 6454, nil)
	require.Zero(
		t,
		pendingChecks.Load(),
		"the pending raw header must remain unauthenticated while it is held",
	)
	unblock()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal(
			"the observer callback did not finish after its horizon was released",
		)
	}
	require.NoError(
		t,
		waitErr,
		"a parked observer must refresh its prior authenticated delivery and release another header's eagerness wait",
	)
	require.False(
		t,
		wrongHeader.Load(),
		"recovery must never substitute the observer's pending raw header",
	)
	require.EqualValues(t, 6873, selector.EagernessLimit().BlockNumber)
}

func TestChainsyncEagernessRecoveryExcludesUnverifiedHeader(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"invalid crypto", "transient publication"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newPatienceRollForwardFixture(t, false)
			f.o.chainSelectionShouldVerifyHeaderCrypto = func(ocommon.Point) bool { return true }
			verifyErr := errors.New("bad vrf proof")
			if name == "transient publication" {
				verifyErr = transientContextErr(t)
			}
			f.o.chainSelectionVerifyHeaderCrypto = func(ouroboros.ConnectionId, gledger.BlockHeader) (uint64, error) {
				return 0, verifyErr
			}
			recovered := false
			f.o.config.ChainsyncRecoverDeliveredPeerTip = func(
				context.Context, ochainsync.CallbackContext, chainselection.PeerTipUpdateEvent, gledger.BlockHeader, func() bool,
			) {
				recovered = true
			}
			f.rollForward(t, newTestBlockHeader(100, 1, 0xab))
			require.False(
				t,
				recovered,
				"unverified headers must not trigger a trusted-path repair",
			)
		})
	}
}
