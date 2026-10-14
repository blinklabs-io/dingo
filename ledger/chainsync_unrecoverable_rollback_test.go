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
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
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
	lookups := 0
	fixture.ls.lookupBlockByHash = func(hash []byte) (models.Block, error) {
		lookups++
		return database.BlockByHash(t.Context(), fixture.ls.db, hash)
	}
	onPrimary, err := fixture.ls.historicalPointOnPrimaryChain(
		fixture.ancestorTip.Point,
		fixture.ls.chain.HeaderTip(),
	)
	require.NoError(t, err)
	require.True(t, onPrimary)
	lookups = 0

	for range unrecoverableRollbackThreshold + 2 {
		fixture.ls.reportUnrecoverableRollbackIfStuck(
			fixture.ancestorTip.Point,
			event.ChainsyncResyncReasonRollbackLoop,
			fixture.connId,
		)
	}
	require.Zero(t, promtestutil.ToFloat64(fixture.ls.metrics.unrecoverableRollbacks))
	require.Equal(t, 1, lookups, "primary-chain membership is cached for this tip")
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
	fixture.ls.lookupBlockByHash = func([]byte) (models.Block, error) {
		return models.Block{}, errors.New("lookup failed")
	}
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

func TestReportProtocolConstrainedRollbackEscalatesImmediately(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{
		event.ChainsyncResyncReasonRollbackExceedsK,
		event.ChainsyncResyncReasonRollbackExceedsMithril,
		event.ChainsyncResyncReasonRollbackBelowUtxoPruneFloor,
	} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			for range unrecoverableRollbackThreshold {
				fixture.ls.reportUnrecoverableRollbackIfStuck(fixture.ancestorTip.Point, reason, fixture.connId)
			}
			require.Equal(t, float64(1), promtestutil.ToFloat64(fixture.ls.metrics.unrecoverableRollbacks))
		})
	}
}

func TestReportProtocolConstrainedRollbackAtOrigin(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{
		event.ChainsyncResyncReasonRollbackExceedsK,
		event.ChainsyncResyncReasonRollbackExceedsMithril,
		event.ChainsyncResyncReasonRollbackBelowUtxoPruneFloor,
	} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			for range unrecoverableRollbackThreshold {
				fixture.ls.reportUnrecoverableRollbackIfStuck(
					ocommon.Point{}, reason, fixture.connId,
				)
			}
			require.Equal(t, float64(1), promtestutil.ToFloat64(fixture.ls.metrics.unrecoverableRollbacks))
		})
	}
}
