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

package dingo

import (
	"crypto/ed25519"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// poolFixture is a block producer whose blocks carry a real cold key.
type poolFixture struct {
	seed [32]byte
	// poolID is derived from the seed the way the block builder derives the
	// cold key, independent of decoding any block.
	poolID string
}

func newPoolFixture(b byte) poolFixture {
	seed := [32]byte{b}
	coldSeed := seed
	coldSeed[0] ^= 0xBB
	cold := ed25519.NewKeyFromSeed(coldSeed[:]).
		Public().(ed25519.PublicKey)
	return poolFixture{
		seed:   seed,
		poolID: lcommon.IssuerVkey(cold).PoolId(),
	}
}

// block returns a stored block issued by the pool. The detector reads slot,
// number and hash from the stored fields, so they are set here directly while
// the CBOR supplies the real issuer key.
func (p poolFixture) block(
	t *testing.T,
	slot, number uint64,
	hash string,
) models.Block {
	t.Helper()
	built := testutil.BuildValidatedConwayBlockBytes(t, p.seed, 1, slot, number)
	return models.Block{
		Hash:   []byte(hash),
		Cbor:   built.Cbor,
		Slot:   slot,
		Number: number,
		Type:   ledger.BlockTypeConway,
	}
}

func newTestEquivocationDetector(
	t *testing.T,
) (*equivocationDetector, *prometheus.Registry) {
	t.Helper()
	registry := prometheus.NewRegistry()
	return newEquivocationDetector(
		registry,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	), registry
}

func rollbackEvt(blocks ...models.Block) event.Event {
	return event.NewEvent(
		chain.ChainUpdateEventType,
		chain.ChainRollbackEvent{RolledBackBlocks: blocks},
	)
}

func addEvt(block models.Block) event.Event {
	return event.NewEvent(
		chain.ChainUpdateEventType,
		chain.ChainBlockEvent{Block: block},
	)
}

func equivocations(
	t *testing.T,
	registry *prometheus.Registry,
) map[string]float64 {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	values := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "dingo_equivocation_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			key := ""
			for _, pair := range metric.GetLabel() {
				key += pair.GetName() + "=" + pair.GetValue() + ","
			}
			values[key] = metric.GetCounter().GetValue()
		}
	}
	return values
}

func TestEquivocationSameSlotSamePool(t *testing.T) {
	t.Parallel()
	d, registry := newTestEquivocationDetector(t)
	pool := newPoolFixture(1)

	d.handleChainUpdate(rollbackEvt(pool.block(t, 100, 10, "loser")))
	d.handleChainUpdate(addEvt(pool.block(t, 100, 10, "winner")))

	assert.Equal(t, map[string]float64{
		"pool_id=" + pool.poolID + ",self_key=false,": 1,
	}, equivocations(t, registry))
}

func TestEquivocationSameHeightDifferentSlotSamePool(t *testing.T) {
	t.Parallel()
	d, registry := newTestEquivocationDetector(t)
	pool := newPoolFixture(1)

	d.handleChainUpdate(rollbackEvt(pool.block(t, 100, 10, "loser")))
	d.handleChainUpdate(addEvt(pool.block(t, 103, 10, "winner")))

	assert.Equal(t, map[string]float64{
		"pool_id=" + pool.poolID + ",self_key=false,": 1,
	}, equivocations(t, registry))
}

func TestEquivocationSameSlotDifferentHeightSamePool(t *testing.T) {
	t.Parallel()
	d, registry := newTestEquivocationDetector(t)
	pool := newPoolFixture(1)

	d.handleChainUpdate(rollbackEvt(pool.block(t, 100, 10, "loser")))
	d.handleChainUpdate(addEvt(pool.block(t, 100, 12, "winner")))

	assert.Equal(t, map[string]float64{
		"pool_id=" + pool.poolID + ",self_key=false,": 1,
	}, equivocations(t, registry))
}

// Two pools forging the same slot is an ordinary slot battle.
func TestEquivocationIgnoresDifferentPools(t *testing.T) {
	t.Parallel()
	d, registry := newTestEquivocationDetector(t)

	d.handleChainUpdate(rollbackEvt(newPoolFixture(1).block(t, 100, 10, "a")))
	d.handleChainUpdate(addEvt(newPoolFixture(2).block(t, 100, 10, "b")))

	assert.Empty(t, equivocations(t, registry))
}

// The same block leaving and re-entering the chain is a rollback, not a
// second block.
func TestEquivocationIgnoresSameBlockReapplied(t *testing.T) {
	t.Parallel()
	d, registry := newTestEquivocationDetector(t)
	pool := newPoolFixture(1)
	block := pool.block(t, 100, 10, "same")

	d.handleChainUpdate(rollbackEvt(block))
	d.handleChainUpdate(addEvt(block))

	assert.Empty(t, equivocations(t, registry))
}

func TestEquivocationIgnoresDistinctSlotAndHeight(t *testing.T) {
	t.Parallel()
	d, registry := newTestEquivocationDetector(t)
	pool := newPoolFixture(1)

	d.handleChainUpdate(rollbackEvt(pool.block(t, 100, 10, "a")))
	d.handleChainUpdate(addEvt(pool.block(t, 101, 11, "b")))

	assert.Empty(t, equivocations(t, registry))
}

func TestEquivocationLabelsOwnKey(t *testing.T) {
	t.Parallel()
	d, registry := newTestEquivocationDetector(t)
	own := newPoolFixture(1)
	d.setSelfPoolID(own.poolID)
	other := newPoolFixture(2)

	d.handleChainUpdate(rollbackEvt(
		own.block(t, 100, 10, "own-loser"),
		other.block(t, 90, 9, "other-loser"),
	))
	d.handleChainUpdate(addEvt(own.block(t, 100, 10, "own-winner")))
	d.handleChainUpdate(addEvt(other.block(t, 90, 9, "other-winner")))

	assert.Equal(t, map[string]float64{
		"pool_id=" + own.poolID + ",self_key=true,":    1,
		"pool_id=" + other.poolID + ",self_key=false,": 1,
	}, equivocations(t, registry))
}

// Three competing blocks from one pool are three pairs.
func TestEquivocationCountsEveryCompetingPair(t *testing.T) {
	t.Parallel()
	d, registry := newTestEquivocationDetector(t)
	pool := newPoolFixture(1)
	second := pool.block(t, 100, 10, "second")

	d.handleChainUpdate(rollbackEvt(pool.block(t, 100, 10, "first")))
	d.handleChainUpdate(addEvt(second))
	d.handleChainUpdate(rollbackEvt(second))
	d.handleChainUpdate(addEvt(pool.block(t, 100, 10, "third")))

	assert.Equal(t, map[string]float64{
		"pool_id=" + pool.poolID + ",self_key=false,": 3,
	}, equivocations(t, registry))
}

// The node must subscribe the detector to chain.update; a detector nothing
// feeds never counts.
func TestNodeSubscribesEquivocationDetectorToChainUpdates(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Close)
	n := &Node{
		eventBus:     bus,
		equivocation: newEquivocationDetector(registry, logger),
	}
	n.subscribeEquivocationDetector()
	pool := newPoolFixture(1)

	bus.Publish(
		chain.ChainUpdateEventType,
		rollbackEvt(pool.block(t, 100, 10, "loser")),
	)
	bus.Publish(
		chain.ChainUpdateEventType,
		addEvt(pool.block(t, 100, 10, "winner")),
	)

	testutil.WaitForCondition(
		t,
		func() bool {
			return equivocations(t, registry)["pool_id="+pool.poolID+",self_key=false,"] == 1
		},
		5*time.Second,
		"equivocation counter to reach 1",
	)
}
