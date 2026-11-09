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
	"context"
	"database/sql"
	"io"
	"log/slog"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// rollbackCycle applies one epoch boundary of a govDiffScenario, rolls it back
// to a point before the boundary, and applies it again. It is the whole-database
// comparison of TestRollbackAcrossGovernanceBoundariesRestoresEveryTable made
// reusable, with a choice of rollback entry point and rollback target.
//
// With ledger set, the rollback goes through the ledger's own
// rollbackWithBlocksAndIntent over a real chain, tip and block nonces, so the
// in-memory reload and the ledger-owned rollback steps run as well; otherwise
// it is the database sweep alone.
type rollbackCycle struct {
	s   *govDiffScenario
	raw *sql.DB
	// ledger selects the ledger-level rollback.
	ledger bool
	// deferred runs RATIFY after the boundary commit.
	deferred bool
	// gap is how many slots before the boundary the rollback target sits.
	// Rows written between the target and the boundary are rolled back too.
	gap uint64
	// prepare writes the reward basis, or anything else a boundary reads, once
	// per boundary after the default preparation.
	prepare func(t *testing.T, epoch models.Epoch)
	// postTarget writes the rows of the blocks between the rollback target and
	// the boundary. replay is false for the first application, true for the
	// re-application.
	postTarget func(t *testing.T, next models.Epoch, replay bool)
	// afterRollback runs after the rollback and before the post-rollback
	// comparison, for in-memory assertions.
	afterRollback func(t *testing.T, epoch models.Epoch, pparams lcommon.ProtocolParameters)

	cm        *chain.ChainManager
	baseStart uint64
	nextBlock uint64
}

func (c *rollbackCycle) settle() func(*LedgerState) {
	if !c.deferred {
		return func(*LedgerState) {}
	}
	return func(ls *LedgerState) {
		require.NoError(c.s.tb, ls.WaitEpochBoundaryJob(context.Background()))
	}
}

func rollbackPoint(slot uint64) ocommon.Point {
	return ocommon.NewPoint(slot, testHashBytes("rollback-cycle-"+slotLabel(slot)))
}

func slotLabel(slot uint64) string { return strconv.FormatUint(slot, 10) }

// newRollbackCycle wires the scenario for the requested rollback entry point.
// boundaries is the number of boundaries the caller will apply.
func newRollbackCycle(
	t *testing.T,
	s *govDiffScenario,
	ledger bool,
	deferred bool,
	gap uint64,
	boundaries int,
) *rollbackCycle {
	t.Helper()
	s.tb = t
	raw, err := dbtest.RawSQLiteMetadata(t, s.db)
	require.NoError(t, err)
	c := &rollbackCycle{
		baseStart: s.epoch.StartSlot,
		s:         s,
		raw:       raw,
		ledger:    ledger,
		deferred:  deferred,
		gap:       gap,
	}
	seedAccountCertificates(t, s, raw)
	if deferred {
		wireDeferredBoundarySnapshot(s.ls, s.snapshotMgr)
	} else {
		s.ls.ratifyAtBoundary = true
	}
	if ledger {
		c.attachLedger(t, boundaries)
	}
	return c
}

// attachLedger replaces the scenario's hand-built ledger with a real one over
// a chain that holds a block at every rollback target and every boundary's
// first slot, copying the state the scenario seeded.
func (c *rollbackCycle) attachLedger(t *testing.T, boundaries int) {
	t.Helper()
	s := c.s
	cm, err := chain.NewManager(context.Background(), s.db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(
		testSecurityParamLedger{securityParam: 100},
	))
	var blocks []chain.RawBlock
	var prev []byte
	number := uint64(0)
	for i := 1; i <= boundaries; i++ {
		start := c.boundaryStart(i)
		for _, slot := range []uint64{start - c.gap, start} {
			number++
			point := rollbackPoint(slot)
			blocks = append(blocks, chain.RawBlock{
				Slot:        slot,
				Hash:        point.Hash,
				BlockNumber: number,
				Type:        1,
				PrevHash:    prev,
				Cbor:        []byte{0x80},
			})
			prev = point.Hash
		}
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(
		context.Background(), blocks,
	))
	c.cm = cm

	old := s.ls
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          s.db,
		ChainManager:      cm,
		CardanoNodeConfig: old.config.CardanoNodeConfig,
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentEra = old.currentEra
	ls.currentEpoch = old.currentEpoch
	ls.currentPParams = old.currentPParams
	ls.ledgerEpoch = old.currentEpoch.EpochId
	ls.ratifyAtBoundary = old.ratifyAtBoundary
	epochs, err := s.db.GetEpochs(nil)
	require.NoError(t, err)
	ls.epochCache = epochs
	s.ls = ls
	s.wireSnapshotHooks()
	if c.deferred {
		wireDeferredBoundarySnapshot(ls, s.snapshotMgr)
	}
}

// setTip records that the ledger has applied every block up to slot.
func (c *rollbackCycle) setTip(t *testing.T, slot uint64) {
	t.Helper()
	if !c.ledger {
		return
	}
	var number uint64
	for i := range c.chainSlots() {
		if c.chainSlots()[i] == slot {
			number = uint64(i) + 1
		}
	}
	require.NotZero(t, number, "slot %d is not a chain block", slot)
	point := rollbackPoint(slot)
	nonce := testHashBytes("nonce-" + slotLabel(slot))
	require.NoError(t, c.s.db.SetBlockNonce(
		point.Hash, point.Slot, nonce, false, nil,
	))
	tip := ochainsync.Tip{Point: point, BlockNumber: number}
	require.NoError(t, c.s.db.SetTip(tip, nil))
	c.s.ls.Lock()
	c.s.ls.currentTip = tip
	c.s.ls.currentTipBlockNonce = nonce
	c.s.ls.publishSnapshotsLocked()
	c.s.ls.Unlock()
}

// boundaryStart is the first slot of the i-th epoch after the scenario's
// first: the seeded epoch is govDiffEpochLength slots long, and every epoch the
// rollover creates takes the genesis epoch length.
func (c *rollbackCycle) boundaryStart(i int) uint64 {
	return c.baseStart + govDiffEpochLength +
		uint64(i-1)*epochBoundaryBenchEpochLength
}

func (c *rollbackCycle) chainSlots() []uint64 {
	var slots []uint64
	for i := 1; ; i++ {
		if i > 16 {
			return slots
		}
		start := c.boundaryStart(i)
		slots = append(slots, start-c.gap, start)
	}
}

// cycleResult is what one boundary left behind.
type cycleResult struct {
	epoch   models.Epoch
	pparams lcommon.ProtocolParameters
	// before is every table at the rollback target; first is every table after
	// the boundary; changed names the tables the boundary changed.
	before  map[string][]string
	first   map[string][]string
	changed map[string]bool
	preDump govDiffEpochDump
	dump    govDiffEpochDump
}

// run applies the boundary after epoch, rolls it back to the target, checks
// that every table is as it was, and applies it again.
func (c *rollbackCycle) run(
	t *testing.T,
	epoch models.Epoch,
	pparams lcommon.ProtocolParameters,
) cycleResult {
	t.Helper()
	s := c.s
	s.prepareRollover(t, epoch, pparams)
	if c.prepare != nil {
		c.prepare(t, epoch)
	}
	next := models.Epoch{EpochId: epoch.EpochId + 1}
	next.StartSlot = epoch.StartSlot + uint64(epoch.LengthInSlots)
	target := next.StartSlot - c.gap
	if c.postTarget == nil && c.gap > 1 {
		t.Fatalf("a rollback target %d slots early needs post-target rows", c.gap)
	}
	if c.ledger {
		c.setTip(t, target)
	}
	before := dumpComparableTables(t, c.raw)
	preDump := s.dump(t, epoch.EpochId, pparams)

	apply := func(replay bool) (models.Epoch, lcommon.ProtocolParameters) {
		if c.postTarget != nil {
			c.postTarget(t, next, replay)
		}
		newEpoch, newPParams := s.commitRollover(
			t, epoch, pparams, c.settle(),
		)
		c.setTip(t, next.StartSlot)
		return newEpoch, newPParams
	}
	firstEpoch, firstPParams := apply(false)
	first := s.dump(t, firstEpoch.EpochId, firstPParams)
	firstTables := dumpComparableTables(t, c.raw)
	changed := changedTables(before, firstTables)

	if c.ledger {
		require.NoError(t, s.ls.rollbackWithBlocksAndIntent(
			context.Background(), rollbackPoint(target), nil,
			false, false, false,
		))
		// Let any work the rollback queued finish before reading the tables.
		s.ls.ratificationWG.Wait()
		s.ls.rewardPrecomputeWG.Wait()
	} else {
		require.NoError(t, s.db.RollbackMetadataAfterSlot(
			context.Background(), ocommon.Point{Slot: target}, 0, nil,
		))
	}
	after := dumpComparableTables(t, c.raw)
	require.Equal(t, preDump, s.dump(t, epoch.EpochId, pparams),
		"epoch %d: the rollback must restore the pre-boundary ledger state",
		firstEpoch.EpochId,
	)
	diffs := diffMetadataTables(before, after)
	for _, d := range diffs {
		t.Logf("epoch %d: %s", firstEpoch.EpochId, d)
	}
	require.Empty(t, diffs, "epoch %d", firstEpoch.EpochId)
	if c.ledger {
		s.ls.RLock()
		gotPParams, err := cbor.Encode(s.ls.currentPParams)
		gotEpoch := s.ls.currentEpoch
		gotLedgerEpoch := s.ls.ledgerEpoch
		gotEra := s.ls.currentEra.Id
		s.ls.RUnlock()
		require.NoError(t, err)
		wantPParams, err := cbor.Encode(pparams)
		require.NoError(t, err)
		require.Equal(t, wantPParams, gotPParams,
			"the in-memory protocol parameters must be reloaded to the "+
				"pre-boundary set")
		require.Equal(t, epoch.EpochId, gotEpoch.EpochId)
		require.Equal(t, epoch.EpochId, gotLedgerEpoch)
		require.Equal(t, eras.ConwayEraDesc.Id, gotEra)
	} else {
		s.ls.currentEpoch = epoch
		s.ls.currentPParams = pparams
	}
	if c.afterRollback != nil {
		c.afterRollback(t, epoch, pparams)
	}

	secondEpoch, secondPParams := apply(true)
	second := s.dump(t, secondEpoch.EpochId, secondPParams)
	require.Equal(t, first, second,
		"epoch %d: replay must reproduce the first application",
		firstEpoch.EpochId,
	)
	require.Equal(t,
		normalizeReplayEpoch(firstEpoch), normalizeReplayEpoch(secondEpoch),
	)
	secondTables := dumpComparableTables(t, c.raw)
	require.Empty(t,
		diffMetadataTables(
			withoutRowIDs(firstTables), withoutRowIDs(secondTables),
		),
		"epoch %d: replay must reproduce the first application's tables",
		firstEpoch.EpochId,
	)
	s.ls.currentEpoch = secondEpoch
	s.ls.currentPParams = secondPParams
	s.epoch, s.pparams = secondEpoch, secondPParams
	return cycleResult{
		epoch:   secondEpoch,
		pparams: secondPParams,
		before:  before,
		first:   firstTables,
		changed: changed,
		preDump: preDump,
		dump:    second,
	}
}
