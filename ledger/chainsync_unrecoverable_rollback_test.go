// Copyright 2025 Blink Labs Software
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
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// freshUnrecoverableConnId returns a distinct connection ID for each call,
// mimicking the new bearer that peer governance opens after every forced
// reconnect during the unrecoverable rollback loop.
func freshUnrecoverableConnId(port int) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port},
	}
}

// TestUnrecoverableRollbackTrackerSurvivesResyncReset reproduces the core
// gap behind the rollback loop: a diverged node is asked to roll back to the
// same canonical point it cannot cross to, and on every attempt it wipes
// rollbackHistory (the resync reset) and reconnects with a fresh connection.
// The per-connection rollback loop detector therefore never accumulates. The
// point-keyed unrecoverableRollbacks tracker must accumulate instead, so the
// stuck condition is eventually recognised.
func TestUnrecoverableRollbackTrackerSurvivesResyncReset(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	point := ocommon.NewPoint(
		116385054,
		[]byte{0x46, 0xc5, 0xd0, 0x72, 0x6c, 0xcf, 0xc5, 0xde},
	)

	var lastCount int
	var everStuck bool
	for cycle := range unrecoverableRollbackThreshold {
		// A fresh connection each cycle, exactly as after a forced
		// reconnect. Record it the way the per-connection loop detector
		// does so we can show that detector never reaches its threshold.
		connKey := connIdKey(freshUnrecoverableConnId(3001 + cycle))
		ls.rollbackHistory = append(ls.rollbackHistory, rollbackRecord{
			point:     point,
			connKey:   connKey,
			timestamp: time.Now(),
		})
		var perConnCount int
		for _, r := range ls.rollbackHistory {
			if r.connKey == connKey && pointMatches(r.point, point) {
				perConnCount++
			}
		}
		require.Equal(
			t,
			1,
			perConnCount,
			"per-connection loop detector must see only one hit per fresh connection",
		)

		// resetChainsyncResyncState / requestChainsyncResync wipe
		// rollbackHistory on every un-crossable rollback.
		ls.rollbackHistory = nil

		count, stuck := ls.noteUnrecoverableRollback(point)
		lastCount = count
		if stuck {
			everStuck = true
		}
	}

	require.Equal(
		t,
		unrecoverableRollbackThreshold,
		lastCount,
		"point-keyed tracker must accumulate across reset+reconnect cycles",
	)
	require.True(
		t,
		everStuck,
		"tracker must report stuck once the threshold is reached",
	)
	require.Nil(
		t,
		ls.rollbackHistory,
		"per-connection loop detector state is wiped each cycle, so it never trips",
	)
}

// TestReportUnrecoverableRollbackSurfacesOperatorSignal verifies that once
// the same un-crossable rollback point recurs past the threshold, the stuck
// condition is surfaced via the dedicated metric instead of only the
// per-attempt WARN that otherwise loops forever.
func TestReportUnrecoverableRollbackSurfacesOperatorSignal(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	point := ocommon.NewPoint(
		fixture.ancestorTip.Point.Slot,
		testHashBytes("missing-canonical-rollback-point"),
	)

	// Below threshold: no metric increment.
	for cycle := range unrecoverableRollbackThreshold - 1 {
		ls.reportUnrecoverableRollbackIfStuck(
			point,
			event.ChainsyncResyncReasonRollbackNotFound,
			freshUnrecoverableConnId(3001+cycle),
		)
	}
	require.Equal(
		t,
		float64(0),
		promtestutil.ToFloat64(ls.metrics.unrecoverableRollbacks),
		"metric must stay zero until the threshold is crossed",
	)

	// Crossing the threshold trips the metric.
	ls.reportUnrecoverableRollbackIfStuck(
		point,
		event.ChainsyncResyncReasonRollbackNotFound,
		freshUnrecoverableConnId(4000),
	)
	require.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(ls.metrics.unrecoverableRollbacks),
		"metric must increment once the node is recognised as stuck",
	)

	// Every further stuck attempt keeps incrementing the counter.
	ls.reportUnrecoverableRollbackIfStuck(
		point,
		event.ChainsyncResyncReasonRollbackNotFound,
		freshUnrecoverableConnId(4001),
	)
	require.Equal(
		t,
		float64(2),
		promtestutil.ToFloat64(ls.metrics.unrecoverableRollbacks),
	)

	// Forward progress clears the tracker, so an unrelated later
	// divergence starts counting from zero.
	ls.clearUnrecoverableRollbacks()
	count, stuck := ls.noteUnrecoverableRollback(point)
	require.Equal(t, 1, count)
	require.False(t, stuck)
}

func TestReportUnrecoverableRollbackClearsEvidenceAfterLocalProgress(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{
		event.ChainsyncResyncReasonRollbackNotFound,
		event.ChainsyncResyncReasonRollbackLoop,
		event.ChainsyncResyncReasonRollbackExceedsK,
		event.ChainsyncResyncReasonRollbackExceedsMithril,
		event.ChainsyncResyncReasonRollbackBelowUtxoPruneFloor,
	} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			key := unrecoverableRollbackKey(fixture.ancestorTip.Point)
			fixture.ls.unrecoverableRollbacks = map[string]unrecoverableRollbackRecord{
				key: {
					count:    unrecoverableRollbackThreshold - 1,
					lastSeen: time.Now(),
					localTip: fixture.currentTip.Point,
				},
			}
			nextBlock := chain.RawBlock{
				Slot:        fixture.currentTip.Point.Slot + 1,
				Hash:        testHashBytes("next-canonical-block"),
				BlockNumber: fixture.currentTip.BlockNumber + 1,
				Type:        1,
				PrevHash:    fixture.currentTip.Point.Hash,
				Cbor:        []byte{0x80},
			}
			require.NoError(t, fixture.ls.chain.AddRawBlocks(
				t.Context(),
				[]chain.RawBlock{nextBlock},
			))

			fixture.ls.reportUnrecoverableRollbackIfStuck(
				fixture.ancestorTip.Point,
				reason,
				fixture.connId,
			)
			require.Zero(
				t,
				promtestutil.ToFloat64(fixture.ls.metrics.unrecoverableRollbacks),
			)
			_, tracked := fixture.ls.unrecoverableRollbacks[key]
			require.False(t, tracked)
		})
	}
}

func TestProtocolConstrainedRollbackProgressDoesNotAccumulate(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{
		event.ChainsyncResyncReasonRollbackExceedsK,
		event.ChainsyncResyncReasonRollbackExceedsMithril,
		event.ChainsyncResyncReasonRollbackBelowUtxoPruneFloor,
	} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			key := unrecoverableRollbackKey(fixture.ancestorTip.Point)
			for attempt := range unrecoverableRollbackThreshold + 2 {
				if attempt > 0 {
					localTip := fixture.ls.chain.HeaderTip()
					nextBlock := chain.RawBlock{
						Slot:        localTip.Point.Slot + 1,
						Hash:        testHashBytes(fmt.Sprintf("progress-%s-%d", reason, attempt)),
						BlockNumber: localTip.BlockNumber + 1,
						Type:        1,
						PrevHash:    localTip.Point.Hash,
						Cbor:        []byte{0x80},
					}
					require.NoError(t, fixture.ls.chain.AddRawBlocks(
						t.Context(),
						[]chain.RawBlock{nextBlock},
					))
				}

				fixture.ls.reportUnrecoverableRollbackIfStuck(
					fixture.ancestorTip.Point,
					reason,
					fixture.connId,
				)
				require.Zero(
					t,
					promtestutil.ToFloat64(
						fixture.ls.metrics.unrecoverableRollbacks,
					),
					"canonical progress between failures must reset escalation evidence",
				)
				if record, tracked := fixture.ls.unrecoverableRollbacks[key]; tracked {
					require.Equal(t, 1, record.count)
				}
			}
		})
	}
}

func TestReportUnrecoverableRollbackClearsProgressBeforeFuturePointReturn(t *testing.T) {
	t.Parallel()
	fixture := newChainsyncRollbackFixture(t)
	point := ocommon.NewPoint(
		fixture.currentTip.Point.Slot+5,
		testHashBytes("future-rollback-point"),
	)
	key := unrecoverableRollbackKey(point)
	fixture.ls.unrecoverableRollbacks = map[string]unrecoverableRollbackRecord{
		key: {
			count:     unrecoverableRollbackThreshold - 1,
			firstSeen: time.Now(),
			lastSeen:  time.Now(),
			localTip:  fixture.currentTip.Point,
		},
	}
	nextBlock := chain.RawBlock{
		Slot:        fixture.currentTip.Point.Slot + 1,
		Hash:        testHashBytes("next-canonical-block"),
		BlockNumber: fixture.currentTip.BlockNumber + 1,
		Type:        1,
		PrevHash:    fixture.currentTip.Point.Hash,
		Cbor:        []byte{0x80},
	}
	require.NoError(t, fixture.ls.chain.AddRawBlocks(
		t.Context(),
		[]chain.RawBlock{nextBlock},
	))

	fixture.ls.reportUnrecoverableRollbackIfStuck(
		point,
		event.ChainsyncResyncReasonRollbackBelowUtxoPruneFloor,
		fixture.connId,
	)
	_, tracked := fixture.ls.unrecoverableRollbacks[key]
	require.False(t, tracked)
}

func TestReportUnrecoverableRollbackWaitsForStalledPrimaryChain(t *testing.T) {
	t.Parallel()
	fixture := newChainsyncRollbackFixture(t)
	key := unrecoverableRollbackKey(fixture.ancestorTip.Point)
	fixture.ls.unrecoverableRollbacks = map[string]unrecoverableRollbackRecord{
		key: {
			count:     unrecoverableRollbackThreshold - 1,
			firstSeen: time.Now(),
			lastSeen:  time.Now(),
			localTip:  fixture.currentTip.Point,
		},
	}
	store := &remoteCountingBlobStore{BlobStore: fixture.ls.db.Blob()}
	fixture.ls.db.SetBlobStore(store)
	onPrimary, err := fixture.ls.historicalPointOnPrimaryChain(
		fixture.ancestorTip.Point,
		fixture.ls.chain.HeaderTip(),
	)
	require.NoError(t, err)
	require.True(t, onPrimary)
	store.getBlockLocalCalls.Store(0)

	for range unrecoverableRollbackThreshold + 2 {
		fixture.ls.reportUnrecoverableRollbackIfStuck(
			fixture.ancestorTip.Point,
			event.ChainsyncResyncReasonRollbackLoop,
			fixture.connId,
		)
	}
	require.Zero(t, promtestutil.ToFloat64(fixture.ls.metrics.unrecoverableRollbacks))
	require.Equal(t, int64(1), store.getBlockLocalCalls.Load(), "primary-chain membership is cached for this tip")
	require.True(t, fixture.ls.unrecoverableRollbacks[key].primaryChainChecked)
	require.True(t, fixture.ls.unrecoverableRollbacks[key].onPrimaryChain)

	fixture.ls.unrecoverableRollbacks[key] = unrecoverableRollbackRecord{
		count:               unrecoverableRollbackThreshold - 1,
		firstSeen:           time.Now().Add(-unrecoverableRollbackWindow),
		lastSeen:            time.Now(),
		localTip:            fixture.currentTip.Point,
		primaryChainChecked: true,
		onPrimaryChain:      true,
	}
	fixture.ls.reportUnrecoverableRollbackIfStuck(
		fixture.ancestorTip.Point,
		event.ChainsyncResyncReasonRollbackLoop,
		fixture.connId,
	)
	require.Equal(t, float64(1), promtestutil.ToFloat64(fixture.ls.metrics.unrecoverableRollbacks))
}

func TestReportUnrecoverableRollbackRequiresAuthoritativeChain(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	ls.metrics.init(prometheus.NewRegistry())
	point := ocommon.NewPoint(110374142, []byte("unverified-point"))

	for range unrecoverableRollbackThreshold + 1 {
		ls.reportUnrecoverableRollbackIfStuck(
			point,
			event.ChainsyncResyncReasonRollbackNotFound,
			freshUnrecoverableConnId(3001),
		)
	}
	require.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.unrecoverableRollbacks),
	)
	require.Empty(t, ls.unrecoverableRollbacks)
}

func TestReportUnrecoverableRollbackDoesNotGuessOnLookupError(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.db.SetBlobStore(
		&localReadFailingBlobStore{BlobStore: fixture.ls.db.Blob()},
	)
	point := ocommon.NewPoint(
		fixture.ancestorTip.Point.Slot,
		testHashBytes("unreadable-rollback-point"),
	)

	for range unrecoverableRollbackThreshold + 1 {
		fixture.ls.reportUnrecoverableRollbackIfStuck(
			point,
			event.ChainsyncResyncReasonRollbackNotFound,
			fixture.connId,
		)
	}
	require.Zero(
		t,
		promtestutil.ToFloat64(fixture.ls.metrics.unrecoverableRollbacks),
	)
}

var protocolConstrainedRollbackReasons = []string{
	event.ChainsyncResyncReasonRollbackExceedsK,
	event.ChainsyncResyncReasonRollbackExceedsMithril,
	event.ChainsyncResyncReasonRollbackBelowUtxoPruneFloor,
}

// A protocol-constrained refusal of a point that is off our primary chain is
// the genuine divergence signal and escalates at the recurrence threshold.
func TestReportProtocolConstrainedRollbackOffChainEscalates(t *testing.T) {
	t.Parallel()
	for _, reason := range protocolConstrainedRollbackReasons {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			point := ocommon.NewPoint(
				fixture.ancestorTip.Point.Slot,
				testHashBytes("off-chain-rollback-point"),
			)
			for range unrecoverableRollbackThreshold {
				fixture.ls.reportUnrecoverableRollbackIfStuck(
					point, reason, fixture.connId,
				)
			}
			require.Equal(t, float64(1), promtestutil.ToFloat64(fixture.ls.metrics.unrecoverableRollbacks))
		})
	}
}

// A refused rollback to a point on our own primary chain says the peer is
// behind or forked, not that the local chain diverged.
func TestReportProtocolConstrainedRollbackOnPrimaryChainDoesNotAlarm(t *testing.T) {
	t.Parallel()
	for _, reason := range protocolConstrainedRollbackReasons {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			for cycle := range unrecoverableRollbackThreshold * 3 {
				fixture.ls.reportUnrecoverableRollbackIfStuck(
					fixture.ancestorTip.Point,
					reason,
					freshUnrecoverableConnId(5000+cycle),
				)
			}
			require.Zero(t, promtestutil.ToFloat64(fixture.ls.metrics.unrecoverableRollbacks))
		})
	}
}

// Origin is on every chain, so a peer whose only intersection with us is
// origin must never produce the divergence alarm.
func TestReportProtocolConstrainedRollbackAtOriginDoesNotAlarm(t *testing.T) {
	t.Parallel()
	for _, reason := range protocolConstrainedRollbackReasons {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			for cycle := range unrecoverableRollbackThreshold * 3 {
				fixture.ls.reportUnrecoverableRollbackIfStuck(
					ocommon.Point{},
					reason,
					freshUnrecoverableConnId(5000+cycle),
				)
			}
			require.Zero(t, promtestutil.ToFloat64(fixture.ls.metrics.unrecoverableRollbacks))
		})
	}
}

// expirePrimaryChainBlock expires point's local CBOR the way history expiry
// does, keeping the metadata and block index that record chain membership.
func expirePrimaryChainBlock(t *testing.T, ls *LedgerState, point ocommon.Point) {
	t.Helper()
	_, err := ls.db.PruneBlock(context.Background(), point.Slot, point.Hash)
	require.NoError(t, err)
	_, err = database.BlockByPoint(context.Background(), ls.db, point)
	require.ErrorIs(t, err, dbtypes.ErrHistoryExpired)
}

// Expiring a primary-chain block's CBOR does not take it off the primary
// chain, so a refused rollback to it must keep the on-chain classification.
func TestReportProtocolConstrainedRollbackExpiredOnPrimaryChainDoesNotAlarm(t *testing.T) {
	t.Parallel()
	for _, reason := range protocolConstrainedRollbackReasons {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			expirePrimaryChainBlock(t, fixture.ls, fixture.ancestorTip.Point)
			for cycle := range unrecoverableRollbackThreshold * 3 {
				fixture.ls.reportUnrecoverableRollbackIfStuck(
					fixture.ancestorTip.Point,
					reason,
					freshUnrecoverableConnId(6000+cycle),
				)
			}
			require.Zero(t, promtestutil.ToFloat64(fixture.ls.metrics.unrecoverableRollbacks))
			rec := fixture.ls.unrecoverableRollbacks[unrecoverableRollbackKey(fixture.ancestorTip.Point)]
			require.True(t, rec.primaryChainChecked, "expired CBOR must not make membership unknown")
			require.True(t, rec.onPrimaryChain)
		})
	}
}

// A prune-floor rejection of an expired primary-chain target is logged at
// WARN, not ERROR, because the target is still on the local chain.
func TestRollbackTargetOnLocalChainExpiredCbor(t *testing.T) {
	t.Parallel()
	fixture := newChainsyncRollbackFixture(t)
	expirePrimaryChainBlock(t, fixture.ls, fixture.ancestorTip.Point)
	require.True(t, fixture.ls.rollbackTargetOnLocalChain(fixture.ancestorTip.Point))
	require.False(t, fixture.ls.rollbackTargetOnLocalChain(
		ocommon.NewPoint(
			fixture.ancestorTip.Point.Slot,
			testHashBytes("expired-off-chain-point"),
		),
	))
}

// The prune-floor rejection classifies its target once: the WARN/ERROR choice
// reuses the membership result the stuck-rollback tracker cached for the
// unchanged local tip instead of repeating the storage lookup.
func TestRollbackTargetOnLocalChainReusesTrackerClassification(t *testing.T) {
	t.Parallel()
	fixture := newChainsyncRollbackFixture(t)
	store := &remoteCountingBlobStore{BlobStore: fixture.ls.db.Blob()}
	fixture.ls.db.SetBlobStore(store)
	for range 3 {
		fixture.ls.reportUnrecoverableRollbackIfStuck(
			fixture.ancestorTip.Point,
			event.ChainsyncResyncReasonRollbackBelowUtxoPruneFloor,
			fixture.connId,
		)
		require.True(t, fixture.ls.rollbackTargetOnLocalChain(fixture.ancestorTip.Point))
	}
	require.Equal(t, int64(1), store.getBlockLocalCalls.Load(), "membership is classified once for this tip")
}

var errArchiveUnavailable = errors.New("archive unavailable")

// archiveDownBlobStore models a Bark store whose archive cannot be reached:
// GetBlock turns locally expired CBOR into an archive fetch, which fails, and
// GetBlockLocal never leaves the local store.
type archiveDownBlobStore struct {
	blob.BlobStore
	archiveFetches atomic.Int64
}

func (s *archiveDownBlobStore) GetBlock(
	txn dbtypes.Txn,
	slot uint64,
	hash []byte,
) ([]byte, dbtypes.BlockMetadata, error) {
	cbor, metadata, err := s.BlobStore.GetBlock(txn, slot, hash)
	if errors.Is(err, dbtypes.ErrHistoryExpired) {
		s.archiveFetches.Add(1)
		return nil, dbtypes.BlockMetadata{}, errArchiveUnavailable
	}
	return cbor, metadata, err
}

func (s *archiveDownBlobStore) GetBlockLocal(
	txn dbtypes.Txn,
	slot uint64,
	hash []byte,
) ([]byte, dbtypes.BlockMetadata, error) {
	return s.BlobStore.GetBlock(txn, slot, hash)
}

// localReadFailingBlobStore fails the bounded local probe, so a membership
// check reports a lookup error instead of an answer.
type localReadFailingBlobStore struct {
	blob.BlobStore
}

func (s *localReadFailingBlobStore) GetBlockLocal(
	dbtypes.Txn,
	uint64,
	[]byte,
) ([]byte, dbtypes.BlockMetadata, error) {
	return nil, dbtypes.BlockMetadata{}, errors.New("lookup failed")
}

// Membership is answered by retained local metadata, so an expired point stays
// on the primary chain without an archive read, and with the archive down.
func TestHistoricalPointOnPrimaryChainExpiredSkipsArchive(t *testing.T) {
	t.Parallel()
	fixture := newChainsyncRollbackFixture(t)
	expirePrimaryChainBlock(t, fixture.ls, fixture.ancestorTip.Point)
	store := &archiveDownBlobStore{BlobStore: fixture.ls.db.Blob()}
	fixture.ls.db.SetBlobStore(store)

	onPrimary, err := fixture.ls.historicalPointOnPrimaryChain(
		fixture.ancestorTip.Point,
		fixture.ls.chain.HeaderTip(),
	)
	require.NoError(t, err)
	require.True(t, onPrimary)
	require.Zero(t, store.archiveFetches.Load())
}

// The rejection below the prune floor is logged at WARN when the target is on
// the local chain, origin and expired CBOR with an unreachable archive
// included, and at ERROR when the target is off it.
func TestHandleEventChainsyncRollbackPruneFloorLogLevel(t *testing.T) {
	t.Parallel()
	const rejection = "chainsync rollback is below the consumed UTxO prune floor, rejecting peer chain"
	testCases := []struct {
		name        string
		expire      bool
		offChain    bool
		origin      bool
		expectLevel string
	}{
		{name: "on chain", expectLevel: "WARN"},
		{name: "origin", origin: true, expectLevel: "WARN"},
		{name: "expired on chain with archive down", expire: true, expectLevel: "WARN"},
		{name: "off chain", offChain: true, expectLevel: "ERROR"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			bus := newTestEventBus(t, nil)
			t.Cleanup(func() { bus.Stop() })
			fixture.ls.config.EventBus = bus
			var logBuf bytes.Buffer
			fixture.ls.config.Logger = slog.New(slog.NewJSONHandler(&logBuf, nil))
			require.NoError(t, fixture.ls.db.SetSyncState(
				database.ConsumedUtxoPruneFloorSyncKey,
				strconv.FormatUint(fixture.currentTip.Point.Slot, 10),
				nil,
			))
			target := fixture.ancestorTip.Point
			if tc.offChain {
				target = ocommon.NewPoint(target.Slot, testHashBytes("prune-floor-off-chain"))
			}
			if tc.origin {
				target = ocommon.NewPointOrigin()
			}
			if tc.expire {
				expirePrimaryChainBlock(t, fixture.ls, target)
				fixture.ls.db.SetBlobStore(
					&archiveDownBlobStore{BlobStore: fixture.ls.db.Blob()},
				)
			}

			require.NoError(t, fixture.ls.handleEventChainsyncRollback(
				ChainsyncEvent{ConnectionId: fixture.connId, Point: target},
				nil,
			))

			record := findLogRecord(t, logBuf.String(), rejection)
			require.Equal(t, tc.expectLevel, record["level"])
		})
	}
}
