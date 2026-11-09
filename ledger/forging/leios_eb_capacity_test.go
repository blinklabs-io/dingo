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

package forging

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// leiosCapacityCandidates returns count mempool transactions of roughly
// paddedSize bytes each, every one spending a distinct input.
func leiosCapacityCandidates(
	t *testing.T,
	count int,
	paddedSize int,
) []MempoolTransaction {
	t.Helper()
	txs := make([]MempoolTransaction, 0, count)
	for i := range count {
		txs = append(txs, MempoolTransaction{
			Hash: fmt.Sprintf("%064x", i+1),
			Cbor: makeMinimalTxCbor(t, byte(i+1), paddedSize),
			Type: conway.TxTypeConway,
		})
	}
	return txs
}

// leiosCapacityForger is a forger whose operator caps admit the whole
// mempool, so only the protocol parameters can bound the endorser block.
func leiosCapacityForger(
	txs []MempoolTransaction,
	pparams lcommon.ProtocolParameters,
	caster *forgerTestLeiosCaster,
) *BlockForger {
	return &BlockForger{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:    time.Now,
		// No slot clock: the selection budget is this reserve from now.
		forgeEBSelectionReserve: time.Hour,
		forgeEBMaxTxRefs:        defaultForgeEBMaxTxRefs,
		forgeEBMaxBytes:         defaultForgeEBMaxBytes,
		leiosChecker:            &forgerTestLeiosChecker{allowed: true},
		leiosMempool:            forgerTestMempoolProvider{txs: txs},
		leiosEBCaster:           caster,
		eraParams: funcParamsProvider(
			func(uint64) lcommon.ProtocolParameters { return pparams },
		),
	}
}

func leiosClosureBytes(bodies [][]byte) uint64 {
	var total uint64
	for _, body := range bodies {
		total += uint64(len(body))
	}
	return total
}

// The referenced transactions must fit the slot's protocol capacity, even
// when the operator's byte cap would allow more.
func TestCheckAndForgeLeiosEBRespectsProtocolTxsSize(t *testing.T) {
	t.Parallel()
	txs := leiosCapacityCandidates(t, 10, 0)
	txSize := len(blockTxFromMempool(t, txs[0].Cbor))
	// Room for three transactions and part of a fourth.
	limit := uint32(3*txSize + txSize/2) // #nosec G115 -- small fixture size
	caster := &forgerTestLeiosCaster{}
	forger := leiosCapacityForger(txs,
		&dijkstra.DijkstraProtocolParameters{
			MaxEndorserBlockTxsSize:        limit,
			MaxEndorserBlockReferencesSize: 40000,
		},
		caster,
	)

	announcement, err := forger.checkAndForgeLeiosEB(
		context.Background(),
		10,
		leiosSelectionBasis{},
	)
	require.NoError(t, err)
	require.NotNil(t, announcement)
	require.Len(t, caster.txBodies, 3)
	require.LessOrEqual(t, leiosClosureBytes(caster.txBodies), uint64(limit))
	eb, err := lcommon.NewLeiosEndorserBlockFromCbor(caster.cbor)
	require.NoError(t, err)
	var referenced uint64
	for _, ref := range eb.TransactionReferences {
		referenced += uint64(ref.TransactionSize)
	}
	require.LessOrEqual(t, referenced, uint64(limit))
}

// The announced endorser-block size is the manifest's encoded size, and a
// header announcing more than maxEndorserBlockReferencesSize is invalid
// (LeiosEbTooBig; dijkstra.ValidateLeiosAnnouncementSize). The forger must
// never announce such a manifest.
func TestCheckAndForgeLeiosEBRespectsProtocolReferencesSize(t *testing.T) {
	t.Parallel()
	txs := leiosCapacityCandidates(t, 10, 0)
	// A map header of one byte plus two references of a 34-byte hash and a
	// two-byte size each, with a byte to spare.
	const limit = 1 + 2*(34+2) + 1
	caster := &forgerTestLeiosCaster{}
	forger := leiosCapacityForger(txs,
		&dijkstra.DijkstraProtocolParameters{
			MaxEndorserBlockTxsSize:        260000,
			MaxEndorserBlockReferencesSize: limit,
		},
		caster,
	)

	announcement, err := forger.checkAndForgeLeiosEB(
		context.Background(),
		10,
		leiosSelectionBasis{},
	)
	require.NoError(t, err)
	require.NotNil(t, announcement)
	require.Len(t, caster.txBodies, 2)
	require.LessOrEqual(t, len(caster.cbor), limit)
	require.LessOrEqual(t, announcement.Size, uint64(limit))
}

// The protocol limits bound the endorser block whatever the operator caps
// say, and an operator cap below them still applies.
func TestEffectiveLeiosEBCaps(t *testing.T) {
	t.Parallel()
	pparams := &dijkstra.DijkstraProtocolParameters{
		MaxEndorserBlockTxsSize:        260000,
		MaxEndorserBlockReferencesSize: 40000,
	}
	caps, ok := effectiveLeiosEBCaps(
		leiosEBCaps{maxRefs: 20000, maxBytes: 24 << 20},
		pparams,
	)
	require.True(t, ok)
	require.Equal(t, leiosEBCaps{
		maxRefs:          20000,
		maxBytes:         260000,
		maxManifestBytes: 40000,
	}, caps)

	caps, ok = effectiveLeiosEBCaps(leiosEBCaps{maxBytes: 1000}, pparams)
	require.True(t, ok)
	require.Equal(t, uint64(1000), caps.maxBytes)

	// Zero is the unlimited operator cap, never a smaller one.
	caps, ok = effectiveLeiosEBCaps(leiosEBCaps{}, pparams)
	require.True(t, ok)
	require.Equal(t, uint64(260000), caps.maxBytes)

	// The Conway-header prototype has no Dijkstra capacity fields.
	operator := leiosEBCaps{maxRefs: 5, maxBytes: 6}
	caps, ok = effectiveLeiosEBCaps(
		operator,
		&conway.ConwayProtocolParameters{},
	)
	require.True(t, ok)
	require.Equal(t, operator, caps)

}

// Selection stops once the endorser block is full, rather than validating
// the rest of a deep mempool that manifest construction would drop. Those
// validations would consume the ranking block's remaining slot budget.
func TestLeiosEBSelectionStopsWhenFull(t *testing.T) {
	t.Parallel()
	txs := leiosCandidateTxs(t, 10)
	txSize := uint64(len(blockTxFromMempool(t, txs[0].Cbor)))
	validator := &sessionMockTxValidator{}
	selected, truncated, err := selectValidLeiosTransactions(
		context.Background(),
		txs,
		validator,
		leiosSelectionLimits{caps: leiosEBCaps{maxBytes: 2 * txSize}},
		leiosSelectionBasis{},
	)
	require.NoError(t, err)
	require.False(t, truncated, "a full endorser block is not a deadline")
	require.Equal(t, txs[:2], selected)
	require.Equal(t, 3, validator.validateCalls)
}

// The capacity accounting must agree byte for byte with the manifest
// encoding, across every CBOR head width the reference count and the
// transaction sizes reach, or a manifest at the limit would be announced
// one byte over it.
func TestLeiosEBCapacityMatchesManifestEncoding(t *testing.T) {
	t.Parallel()
	for _, count := range []int{1, 23, 24, 255, 256} {
		for _, size := range []uint32{1, 23, 24, 255, 256, 65535, 65536} {
			capacity := leiosEBCapacity{}
			refs := make([]lcommon.LeiosTransactionReference, 0, count)
			for i := range count {
				require.True(t, capacity.admit(uint64(size)))
				hash := make([]byte, 32)
				hash[0], hash[1] = byte(i), byte(i>>8)
				refs = append(refs, lcommon.LeiosTransactionReference{
					TransactionHash: lcommon.NewBlake2b256(hash),
					TransactionSize: size,
				})
			}
			raw, err := (lcommon.LeiosEndorserBlock{
				TransactionReferences: refs,
			}).MarshalCBOR()
			require.NoError(t, err)
			require.Equal(t,
				uint64(len(raw)),
				cborHeadSize(capacity.refs)+capacity.entryBytes,
				"count %d size %d", count, size,
			)
			// A manifest cap of exactly that size admits it, and one
			// byte less refuses the last reference.
			exact := leiosEBCapacity{
				caps: leiosEBCaps{maxManifestBytes: uint64(len(raw))},
			}
			short := leiosEBCapacity{
				caps: leiosEBCaps{maxManifestBytes: uint64(len(raw)) - 1},
			}
			for i := range count {
				require.True(t, exact.admit(uint64(size)))
				require.Equal(t, i < count-1, short.admit(uint64(size)))
			}
		}
	}
}

// Parameter updates must take effect in the next slot's selection, rather
// than retaining a capacity cached at genesis or a previous forge.
func TestCheckAndForgeLeiosEBFollowsProtocolParameterUpdate(t *testing.T) {
	t.Parallel()
	txs := leiosCandidateTxs(t, 10)
	txSize := uint32(
		len(blockTxFromMempool(t, txs[0].Cbor)),
	) // #nosec G115 -- small fixture
	current := &dijkstra.DijkstraProtocolParameters{
		MaxEndorserBlockTxsSize:        2 * txSize,
		MaxEndorserBlockReferencesSize: 40000,
	}
	caster := &forgerTestLeiosCaster{}
	forger := leiosCapacityForger(txs, nil, caster)
	forger.eraParams = funcParamsProvider(
		func(uint64) lcommon.ProtocolParameters { return current },
	)

	_, err := forger.checkAndForgeLeiosEB(context.Background(), 10,
		leiosSelectionBasis{})
	require.NoError(t, err)
	require.Len(t, caster.txBodies, 2)

	// A parameter update raising both limits takes effect.
	current = &dijkstra.DijkstraProtocolParameters{
		MaxEndorserBlockTxsSize:        2000000,
		MaxEndorserBlockReferencesSize: 308000,
	}
	_, err = forger.checkAndForgeLeiosEB(context.Background(), 11,
		leiosSelectionBasis{})
	require.NoError(t, err)
	require.Len(t, caster.txBodies, 10)
}

func TestCheckAndForgeLeiosEBRequiresProtocolCapacity(t *testing.T) {
	t.Parallel()
	var nilProvider *mockPParamsProvider
	var nilFunc funcParamsProvider
	var nilDijkstra *dijkstra.DijkstraProtocolParameters
	var nilConway *conway.ConwayProtocolParameters
	for _, tc := range []struct {
		name     string
		provider ProtocolParamsProvider
	}{
		{"missing provider", nil},
		{"typed nil provider", nilProvider},
		{"nil function provider", nilFunc},
		{"missing slot parameters", &mockPParamsProvider{}},
		{"unrecognized parameters", &mockPParamsProvider{pparams: unrecognizedProtocolParameters{}}},
		{"typed nil Dijkstra parameters", &mockPParamsProvider{pparams: nilDijkstra}},
		{"typed nil Conway parameters", &mockPParamsProvider{pparams: nilConway}},
		{"zero transaction capacity", &mockPParamsProvider{pparams: &dijkstra.DijkstraProtocolParameters{MaxEndorserBlockReferencesSize: 40000}}},
		{"zero manifest capacity", &mockPParamsProvider{pparams: &dijkstra.DijkstraProtocolParameters{MaxEndorserBlockTxsSize: 260000}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			caster := &forgerTestLeiosCaster{}
			forger := leiosCapacityForger(
				leiosCapacityCandidates(t, 1, 0),
				nil,
				caster,
			)
			forger.eraParams = tc.provider
			validator := &sessionMockTxValidator{}
			forger.leiosValidator = validator
			var logs bytes.Buffer
			forger.logger = slog.New(
				slog.NewTextHandler(
					&logs,
					&slog.HandlerOptions{Level: slog.LevelDebug},
				),
			)
			forger.metrics = initForgingMetrics(prometheus.NewRegistry())
			announcement, err := forger.checkAndForgeLeiosEB(
				context.Background(),
				10,
				leiosSelectionBasis{},
			)
			require.NoError(t, err)
			require.Nil(
				t,
				announcement,
				"unresolved or zero capacity must not announce an EB",
			)
			require.Empty(
				t,
				caster.hash,
				"unresolved or zero capacity must not broadcast an EB",
			)
			require.Zero(
				t,
				validator.validateCalls,
				"capacity must be checked before transaction validation",
			)
			require.Contains(t, logs.String(), "protocol_capacity_unavailable")
			require.Equal(
				t,
				float64(1),
				testutil.ToFloat64(
					forger.metrics.leiosEbSkipped.WithLabelValues(
						"protocol_capacity_unavailable",
					),
				),
			)
		})
	}
}

func TestCheckAndForgeLeiosEBPreservesConwayPrototype(t *testing.T) {
	t.Parallel()
	caster := &forgerTestLeiosCaster{}
	forger := leiosCapacityForger(
		leiosCapacityCandidates(t, 1, 0),
		&conway.ConwayProtocolParameters{},
		caster,
	)
	announcement, err := forger.checkAndForgeLeiosEB(
		context.Background(),
		10,
		leiosSelectionBasis{},
	)
	require.NoError(t, err)
	require.NotNil(t, announcement)
	require.Len(t, caster.txBodies, 1)
}

func TestMissingEBCapacityStillForgesRankingBlock(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	validator := &sessionMockTxValidator{}
	caster := &forgerTestLeiosCaster{}
	broadcaster := &forgerTestBroadcaster{}
	forger := newEBTimingForger(t, &logs, &ebTestSlotClock{
		currentSlot: 10, chainTipSlot: 9, slotsPerKESPeriod: 100,
		slotEnd: time.Now().Add(time.Hour),
	}, validator, caster, broadcaster, leiosCandidateTxs(t, 1))
	forger.eraParams = nil
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Empty(t, caster.hash, "missing capacity must suppress EB diffusion")
	require.Equal(
		t,
		1,
		broadcaster.calls,
		"missing EB capacity must not lose the ranking block",
	)
	require.Zero(t, validator.validateCalls)
}
