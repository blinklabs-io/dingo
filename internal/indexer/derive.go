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

package indexer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"slices"
	"strconv"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/epochengine"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// DeriveConfig is the configuration the epoch-boundary computation reads. It
// carries the node's reward and governance settings, so the IndexerDB derives
// the facts the ledger does.
type DeriveConfig struct {
	Logger     *slog.Logger
	NodeConfig *cardano.CardanoNodeConfig
	// Eras is the era table the node runs; nil selects the default.
	Eras []eras.EraDesc

	MinPoolMargin              uint
	PledgeLeverageEnabled      bool
	PledgeLeverage             uint
	FullPotRewardsEnabled      bool
	DelegatorInactivityEnabled bool
	DelegatorInactivity        uint64
	WarmGenesisStake           bool
}

// Deriver derives the ledger facts of the IndexerDB. It is the second host
// of ledger/epochengine: as the writer replays blocks in chain order, it
// crosses each epoch boundary by running the engine's steps over the
// IndexerDB's own rows, synchronously and inside the batch's transaction.
// Nothing is read from the ledger.
//
// A Deriver is used by one goroutine, the writer's.
type Deriver struct {
	cfg     DeriveConfig
	ldb     ledgerdb.LedgerDB
	adapter dbadapter.Adapter
	engine  *epochengine.Engine
	snap    *snapshot.Manager
	eraList []eras.EraDesc

	// txn and ctx are the transaction and context the host's reads run in
	// while a boundary is derived.
	txn ledgerdb.Txn
	ctx context.Context
	// cur is the newest epoch row, loaded when a batch begins and advanced
	// as boundaries are crossed.
	db   *database.Database
	pots map[uint64]types.Uint64
	// affected are the accounts a rollback must recompute the expiration of.
	affected []models.StakeCredentialRef
	cur      *models.Epoch
	loaded   bool
}

// NewDeriver builds the deriver over the IndexerDB.
func NewDeriver(db *database.Database, cfg DeriveConfig) (*Deriver, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	// A deriver with no node config can still remove facts, which is all a
	// writer that only rolls the IndexerDB back does.
	d := &Deriver{cfg: cfg, db: db, adapter: dbadapter.New(db)}
	d.ldb = d.adapter
	d.eraList = cfg.Eras
	if d.eraList == nil {
		d.eraList = eras.ErasWithDijkstra
	}
	d.engine = epochengine.New(epochengine.Config{
		Logger:                     cfg.Logger,
		CardanoNodeConfig:          cfg.NodeConfig,
		MinPoolMargin:              cfg.MinPoolMargin,
		PledgeLeverageEnabled:      cfg.PledgeLeverageEnabled,
		PledgeLeverage:             cfg.PledgeLeverage,
		FullPotRewardsEnabled:      cfg.FullPotRewardsEnabled,
		DelegatorInactivityEnabled: cfg.DelegatorInactivityEnabled,
		DelegatorInactivity:        cfg.DelegatorInactivity,
		WarmGenesisStake:           cfg.WarmGenesisStake,
		FatalErrorFunc:             func(error) {},
		HaltError:                  errDeriveHalt,
	}, deriveHost{d: d})
	d.snap = snapshot.NewManager(d.ldb, nil, cfg.Logger)
	if err := d.snap.SetDelegatorInactivity(
		cfg.DelegatorInactivityEnabled, cfg.DelegatorInactivity,
	); err != nil {
		return nil, fmt.Errorf("configure delegator inactivity: %w", err)
	}
	return d, nil
}

var errDeriveHalt = errors.New("indexer: epoch boundary cannot be derived")

// Reset drops the epoch the deriver holds, so the next batch reads it again.
// The writer calls it whenever a transaction fails or rows are rolled back.
func (d *Deriver) Reset() {
	d.cur, d.loaded = nil, false
}

func (d *Deriver) load(txn *database.Txn) error {
	if d.loaded {
		return nil
	}
	epochs, err := d.ldb.GetEpochs(txn)
	if err != nil {
		return fmt.Errorf("read epochs: %w", err)
	}
	d.cur = nil
	if len(epochs) > 0 {
		last := epochs[len(epochs)-1]
		d.cur = &last
	}
	d.loaded = true
	return nil
}

// Split returns how many of blocks, which follow the epoch the IndexerDB
// holds, belong to it. The block at that index opens a later epoch, so the
// caller indexes the leading blocks, calls Cross with the next one, and
// splits again. An IndexerDB with no epoch yet splits at zero.
func (d *Deriver) Split(txn *database.Txn, blocks []models.Block) (int, error) {
	if err := d.load(txn); err != nil {
		return 0, err
	}
	if d.cur == nil {
		return 0, nil
	}
	end := d.cur.StartSlot + uint64(d.cur.LengthInSlots)
	for i, b := range blocks {
		if b.Slot >= end {
			return i, nil
		}
	}
	return len(blocks), nil
}

// Cross derives the facts of the boundary block opens: the first epoch when
// the IndexerDB holds none, otherwise the epoch containing block's slot. It
// runs before block is indexed, as the ledger's rollover runs before the
// block that crosses it.
func (d *Deriver) Cross(
	ctx context.Context,
	txn *database.Txn,
	block models.Block,
) error {
	if d.cfg.NodeConfig == nil {
		return errors.New("indexer: deriving facts needs the node config")
	}
	if err := d.load(txn); err != nil {
		return err
	}
	d.txn, d.ctx = txn, ctx
	defer func() { d.txn, d.ctx = nil, nil }()
	blockEra, err := d.blockEra(block)
	if err != nil {
		return err
	}
	if d.cur == nil {
		if err := d.initialize(ctx, txn, block, blockEra); err != nil {
			return err
		}
		return d.reload(txn)
	}
	if err := d.rollover(ctx, txn, block, blockEra); err != nil {
		return err
	}
	return d.reload(txn)
}

func (d *Deriver) reload(txn *database.Txn) error {
	d.loaded = false
	return d.load(txn)
}

func (d *Deriver) blockEra(block models.Block) (uint, error) {
	decoded, err := block.Decode()
	if err != nil {
		return 0, fmt.Errorf("decode block at slot %d: %w", block.Slot, err)
	}
	return uint(decoded.Era().Id), nil
}

func (d *Deriver) era(id uint) (*eras.EraDesc, error) {
	e := eras.GetEraByIdIn(d.eraList, id)
	if e == nil {
		return nil, fmt.Errorf("unknown era ID %d", id)
	}
	return e, nil
}

func (d *Deriver) updateQuorum() int {
	if g := d.cfg.NodeConfig.ShelleyGenesis(); g != nil {
		return g.UpdateQuorum
	}
	return 0
}

func (d *Deriver) setPParams(
	txn *database.Txn,
	pp lcommon.ProtocolParameters,
	slot, epoch uint64,
	eraID uint,
) error {
	raw, err := cbor.Encode(&pp)
	if err != nil {
		return fmt.Errorf("encode pparams: %w", err)
	}
	return d.ldb.SetPParams(raw, slot, epoch, eraID, txn)
}

// initialize writes what the ledger writes before it applies the chain's
// first block: the genesis network state and ADA pots, the parameters of
// every era up to the first block's, epoch 0, and the epoch 0 mark snapshot.
func (d *Deriver) initialize(
	ctx context.Context,
	txn *database.Txn,
	first models.Block,
	firstEra uint,
) error {
	reserves, err := genesisReserves(d.cfg.NodeConfig)
	if err != nil {
		return err
	}
	if err := d.ldb.SetNetworkState(0, reserves, 0, txn); err != nil {
		return fmt.Errorf("set genesis network state: %w", err)
	}
	if err := d.ldb.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:    0,
		Reserves: types.Uint64(reserves),
	}, txn); err != nil {
		return fmt.Errorf("save genesis ADA pots: %w", err)
	}
	var pp lcommon.ProtocolParameters
	for _, e := range d.eraList {
		if e.Id == 0 || e.Id > firstEra || e.HardForkFunc == nil {
			continue
		}
		if pp, err = e.HardForkFunc(d.cfg.NodeConfig, pp); err != nil {
			return fmt.Errorf("hard fork to era %d: %w", e.Id, err)
		}
		if err := d.setPParams(txn, pp, 0, 0, e.Id); err != nil {
			return err
		}
	}
	era, err := d.era(firstEra)
	if err != nil {
		return err
	}
	slotLength, length, err := era.EpochLengthFunc(d.cfg.NodeConfig)
	if err != nil {
		return fmt.Errorf("epoch length: %w", err)
	}
	nonce, evolving, candidate, lab, err := d.engine.CalculateEpochNonce(
		ctx, txn, 0, *era, models.Epoch{}, pp,
	)
	if err != nil {
		return fmt.Errorf("calculate epoch 0 nonce: %w", err)
	}
	if err := d.ldb.SetEpoch(
		0, 0, nonce, evolving, candidate, lab, era.Id, slotLength, length, txn,
	); err != nil {
		return fmt.Errorf("set epoch 0: %w", err)
	}
	evt := event.EpochTransitionEvent{NewEpoch: 0}
	if err := d.snap.CaptureEpochBoundarySnapshot(ctx, txn, evt); err != nil {
		return fmt.Errorf("capture epoch 0 snapshot: %w", err)
	}
	_ = first
	return nil
}

// genesisReserves is the maximum supply less the genesis outputs.
func genesisReserves(cfg *cardano.CardanoNodeConfig) (uint64, error) {
	byron, err := cfg.ByronGenesis().GenesisUtxos()
	if err != nil {
		return 0, fmt.Errorf("byron genesis outputs: %w", err)
	}
	g := cfg.ShelleyGenesis()
	if g == nil {
		return 0, errors.New("missing Shelley genesis")
	}
	shelley, err := g.GenesisUtxos()
	if err != nil {
		return 0, fmt.Errorf("shelley genesis outputs: %w", err)
	}
	circulating := new(big.Int)
	for _, u := range slices.Concat(byron, shelley) {
		if u.Output == nil {
			return 0, errors.New("genesis output without a value")
		}
		circulating.Add(circulating, u.Output.Amount())
	}
	supply := new(big.Int).SetUint64(g.MaxLovelaceSupply)
	if circulating.Cmp(supply) > 0 {
		return 0, errors.New("genesis outputs exceed the maximum supply")
	}
	return supply.Sub(supply, circulating).Uint64(), nil
}

func genesisBlockHash(cfg *cardano.CardanoNodeConfig) ([]byte, error) {
	if cfg == nil || cfg.ByronGenesisHash == "" {
		return nil, errors.New("byron genesis hash is not in the config")
	}
	return hex.DecodeString(cfg.ByronGenesisHash)
}

// rollover crosses from the newest epoch to the one block is in.
func (d *Deriver) rollover(
	ctx context.Context,
	txn *database.Txn,
	block models.Block,
	blockEra uint,
) error {
	prev := *d.cur
	prevEnd := prev.StartSlot + uint64(prev.LengthInSlots)
	if block.Slot < prevEnd {
		return nil
	}
	currentEra, err := d.era(prev.EraId)
	if err != nil {
		return err
	}
	pparams, err := d.currentPParams(txn, prev)
	if err != nil {
		return err
	}
	// The epoch containing the block, assuming the epoch length does not
	// change across epochs that hold no block. The first epoch after Byron has
	// the length of the era that opens it, which the Byron length does not
	// say, so a block beyond it is reached by a later crossing.
	target := prev.EpochId + 1
	if prev.EraId != eras.ByronEraDesc.Id || blockEra == eras.ByronEraDesc.Id {
		target += (block.Slot - prevEnd) / uint64(prev.LengthInSlots)
	}
	start := prev.StartSlot + (target-prev.EpochId)*uint64(prev.LengthInSlots)
	if target != prev.EpochId+1 {
		return d.jumpEpoch(ctx, txn, prev, *currentEra, pparams, target, start)
	}
	return d.successorEpoch(ctx, txn, prev, *currentEra, pparams, blockEra, start)
}

func (d *Deriver) currentPParams(
	txn *database.Txn,
	prev models.Epoch,
) (lcommon.ProtocolParameters, error) {
	era, err := d.era(prev.EraId)
	if err != nil {
		return nil, err
	}
	if era.DecodePParamsFunc == nil {
		return nil, nil
	}
	rows, err := d.adapter.GetPParamsRows(prev.EpochId, prev.EraId, txn)
	if err != nil {
		return nil, fmt.Errorf("read pparams: %w", err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf(
			"no protocol parameters for epoch %d era %d",
			prev.EpochId, prev.EraId,
		)
	}
	return era.DecodePParamsFunc(rows[0].Cbor)
}

// jumpEpoch writes the sparse row of an epoch reached after epochs that held
// no block: the reference rules run an epoch's boundary effects only for the
// successor of the ledger's epoch.
func (d *Deriver) jumpEpoch(
	ctx context.Context,
	txn *database.Txn,
	prev models.Epoch,
	era eras.EraDesc,
	pparams lcommon.ProtocolParameters,
	target, start uint64,
) error {
	slotLength, length, err := era.EpochLengthFunc(d.cfg.NodeConfig)
	if err != nil {
		return err
	}
	nonce, evolving, candidate, lab, err := d.engine.CalculateEpochNonce(
		ctx, txn, start, era, prev, pparams,
	)
	if err != nil {
		return fmt.Errorf("calculate jumped epoch nonce: %w", err)
	}
	return d.ldb.SetEpoch(
		start, target, nonce, evolving, candidate, lab,
		era.Id, slotLength, length, txn,
	)
}

// successorEpoch is the ledger's epoch rollover for a boundary into the next
// epoch, with ratification applied at the boundary.
func (d *Deriver) successorEpoch(
	ctx context.Context,
	txn *database.Txn,
	prev models.Epoch,
	currentEra eras.EraDesc,
	pparams lcommon.ProtocolParameters,
	blockEra uint,
	boundary uint64,
) error {
	newEpoch := prev.EpochId + 1
	byronTranslated := currentEra.Id == eras.ByronEraDesc.Id &&
		blockEra != eras.ByronEraDesc.Id
	if currentEra.Id == eras.ByronEraDesc.Id && !byronTranslated {
		// A Byron epoch has no parameters, rewards or governance: the next
		// one only opens.
		return d.jumpEpoch(ctx, txn, prev, currentEra, nil, newEpoch, boundary)
	}
	if pparams == nil && !byronTranslated {
		return fmt.Errorf("no protocol parameters for era %d", currentEra.Id)
	}
	classicApplied := false
	var transitionMajors []uint
	if blockEra > currentEra.Id {
		var err error
		pparams, currentEra, transitionMajors, err = d.transition(
			ctx, txn, currentEra, pparams, blockEra, newEpoch, boundary,
		)
		if err != nil {
			return err
		}
		classicApplied = true
	}
	// Byron has no parameters, and the era hop above supplies the first
	// Shelley ones, so only a boundary that cannot be derived reaches here
	// with none.
	if pparams == nil {
		return fmt.Errorf("no protocol parameters for era %d", currentEra.Id)
	}
	eng := d.engine
	// The hard-fork combinator translates the Byron ledger before the first
	// Shelley tick, so the translated reserves precede every Shelley rule and
	// a later rule that moves reserves at this boundary builds on them.
	if byronTranslated {
		if err := eng.ApplyByronTranslationReserves(
			txn, newEpoch, boundary,
		); err != nil {
			return fmt.Errorf("apply Byron-to-Shelley reserves: %w", err)
		}
	}

	apply, err := eng.ShouldApplyStakeRewardUpdate(txn, prev)
	if err != nil {
		return err
	}
	if apply {
		if err := eng.ApplyStakeRewards(ctx, txn, newEpoch, boundary, false); err != nil {
			return fmt.Errorf("apply stake rewards: %w", err)
		}
	}
	if err := eng.ApplyMIRCerts(
		ctx, txn, prev.StartSlot, boundary, prev.EraId,
	); err != nil {
		return fmt.Errorf("apply MIR certs: %w", err)
	}

	evt := event.EpochTransitionEvent{
		PreviousEpoch: prev.EpochId,
		NewEpoch:      newEpoch,
		BoundarySlot:  boundary,
		SnapshotSlot:  boundary - 1,
	}
	dijkstra := currentEra.Id >= eras.DijkstraEraDesc.Id
	var spo *governance.SPOVotingState
	if !dijkstra {
		if spo, err = d.spoState(ctx, txn, evt, currentEra.Id); err != nil {
			return err
		}
	}

	newPParams := pparams
	if !classicApplied {
		newPParams, _, err = d.ldb.ComputeAndApplyPParamUpdates(
			ctx, boundary, newEpoch, currentEra.Id, d.updateQuorum(),
			pparams,
			currentEra.DecodePParamsUpdateFunc,
			currentEra.PParamsUpdateFunc,
			currentEra.ParamUpdateHasPlutusV2CostModelFunc,
			txn,
		)
		if err != nil {
			return fmt.Errorf("apply pparam updates: %w", err)
		}
	}
	if err := eng.ApplyPoolRetirements(ctx, txn, newEpoch, boundary); err != nil {
		return fmt.Errorf("apply pool retirements: %w", err)
	}
	if err := d.activateDelegatorInactivity(ctx, txn, newEpoch); err != nil {
		return fmt.Errorf("activate delegator inactivity: %w", err)
	}

	donations, err := d.ldb.SumNetworkDonationsForEpoch(prev.EpochId, txn)
	if err != nil {
		return fmt.Errorf("sum donations: %w", err)
	}
	conwayGenesis := d.cfg.NodeConfig.ConwayGenesis()
	out, err := governance.ProcessEpoch(ctx, &governance.EpochInput{
		DB:                       d.ldb,
		Txn:                      txn,
		Logger:                   d.cfg.Logger,
		PrevEpoch:                prev.EpochId,
		NewEpoch:                 newEpoch,
		BoundarySlot:             boundary,
		PrevEpochStartSlot:       prev.StartSlot,
		PParams:                  newPParams,
		UpdateFn:                 currentEra.PParamsUpdateFunc,
		ConwayGenesis:            conwayGenesis,
		DelegatorInactivityOn:    d.cfg.DelegatorInactivityEnabled,
		CurrentBoundarySPOState:  spo,
		DeferRatification:        true,
		BoundarySPOStateDeferred: dijkstra,
		PendingTreasuryDonations: donations,
	})
	if err != nil {
		return fmt.Errorf("process governance epoch: %w", err)
	}
	hardFork := majorChanged(pparams, out.UpdatedPParams)
	ratify := func() error {
		if out.Ratification == nil {
			return nil
		}
		decision, err := out.Ratification.Decide(ctx, txn)
		if err != nil {
			return err
		}
		_, err = out.Ratification.Apply(ctx, decision, txn)
		return err
	}
	afterEnactment := dijkstra
	if v, err := epochengine.GetProtocolVersion(out.UpdatedPParams); err == nil &&
		v.Major >= lcommon.ProtocolVersionDijkstra {
		afterEnactment = true
	}
	if !afterEnactment {
		if err := ratify(); err != nil {
			return fmt.Errorf("ratify governance: %w", err)
		}
	}
	if err := eng.ApplyEpochDonations(txn, prev.EpochId, boundary); err != nil {
		return fmt.Errorf("apply epoch donations: %w", err)
	}
	if out.PParamsChanged {
		newPParams = out.UpdatedPParams
		if err := d.setPParams(txn, newPParams, boundary, newEpoch, currentEra.Id); err != nil {
			return fmt.Errorf("persist enacted pparams: %w", err)
		}
	}
	for _, major := range transitionMajors {
		if err := d.hardForkRule(ctx, txn, major, newEpoch, boundary); err != nil {
			return err
		}
	}
	if hardFork {
		if v, err := epochengine.GetProtocolVersion(newPParams); err == nil {
			if err := d.hardForkRule(ctx, txn, v.Major, newEpoch, boundary); err != nil {
				return err
			}
		}
	}
	if err := eng.SaveRewardAdaPotsForEpoch(txn, newEpoch, prev, boundary); err != nil {
		return fmt.Errorf("save reward ADA pots: %w", err)
	}

	slotLength, length, err := currentEra.EpochLengthFunc(d.cfg.NodeConfig)
	if err != nil {
		return fmt.Errorf("epoch length: %w", err)
	}
	nonce, evolving, candidate, lab, err := eng.CalculateEpochNonce(
		ctx, txn, boundary, currentEra, prev, newPParams,
	)
	if err != nil {
		return fmt.Errorf("calculate epoch nonce: %w", err)
	}
	if err := d.ldb.SetEpoch(
		boundary, newEpoch, nonce, evolving, candidate, lab,
		currentEra.Id, slotLength, length, txn,
	); err != nil {
		return fmt.Errorf("set epoch: %w", err)
	}

	evt.EpochNonce = nonce
	evt.ProtocolVersion = protocolMajor(newPParams)
	if afterEnactment {
		if spo, err = d.spoState(ctx, txn, evt, currentEra.Id); err != nil {
			return err
		}
		if out.Ratification != nil {
			out.Ratification.SetBoundarySPOState(spo)
		}
		if err := ratify(); err != nil {
			return fmt.Errorf("ratify governance: %w", err)
		}
	}
	if err := d.snap.CaptureEpochBoundarySnapshot(ctx, txn, evt); err != nil {
		return fmt.Errorf("capture epoch %d snapshot: %w", newEpoch, err)
	}
	return nil
}

// spoState is the pool stake the boundary's RATIFY tallies. It reads the
// distribution from the IndexerDB's history as of the boundary rather than
// from a live aggregate, which a replay that has not reached the end of the
// chain does not keep.
func (d *Deriver) spoState(
	ctx context.Context,
	txn *database.Txn,
	evt event.EpochTransitionEvent,
	eraID uint,
) (*governance.SPOVotingState, error) {
	rows, err := d.snap.CurrentBoundarySPOStakeRows(ctx, txn, evt, eraID)
	if err != nil {
		return nil, fmt.Errorf("resolve boundary SPO stake: %w", err)
	}
	var total uint64
	for _, r := range rows {
		total += uint64(r.TotalStake)
	}
	return &governance.SPOVotingState{Dist: rows, TotalStake: total}, nil
}

func (d *Deriver) transition(
	ctx context.Context,
	txn *database.Txn,
	from eras.EraDesc,
	pparams lcommon.ProtocolParameters,
	toEra uint,
	newEpoch, boundary uint64,
) (lcommon.ProtocolParameters, eras.EraDesc, []uint, error) {
	working, workingEra := pparams, from
	if from.Id != eras.ByronEraDesc.Id {
		if pparams == nil {
			return nil, from, nil, fmt.Errorf(
				"no protocol parameters for era %d", from.Id,
			)
		}
		var err error
		working, _, err = d.ldb.ComputeAndApplyPParamUpdates(
			ctx, boundary, newEpoch, from.Id, d.updateQuorum(), pparams,
			from.DecodePParamsUpdateFunc, from.PParamsUpdateFunc,
			from.ParamUpdateHasPlutusV2CostModelFunc, txn,
		)
		if err != nil {
			return nil, from, nil, fmt.Errorf("source-era pparam updates: %w", err)
		}
	}
	var majors []uint
	for id := from.Id + 1; id <= toEra; id++ {
		next, err := d.era(id)
		if err != nil {
			return nil, from, nil, err
		}
		if next.HardForkFunc != nil {
			translated, err := next.HardForkFunc(d.cfg.NodeConfig, working)
			if err != nil {
				return nil, from, nil, fmt.Errorf("hard fork to era %d: %w", id, err)
			}
			if err := d.setPParams(txn, translated, boundary, newEpoch, id); err != nil {
				return nil, from, nil, err
			}
			if majorChanged(working, translated) {
				if v, err := epochengine.GetProtocolVersion(translated); err == nil {
					majors = append(majors, v.Major)
				}
			}
			working = translated
		}
		if err := d.translateGovernance(ctx, txn, workingEra.Id, id); err != nil {
			return nil, from, nil, err
		}
		workingEra = *next
	}
	return working, workingEra, majors, nil
}

func (d *Deriver) translateGovernance(
	ctx context.Context,
	txn *database.Txn,
	from, to uint,
) error {
	if err := governance.TranslateRatifiedGovActions(ctx, d.ldb, txn, from, to); err != nil {
		return fmt.Errorf("translate governance actions: %w", err)
	}
	return nil
}

// hardForkRule applies the per-major-version HARDFORK state rule: the Allegra
// return of AVVM outputs to the reserves and the Plomin clearing of dangling
// DRep delegations.
func (d *Deriver) hardForkRule(
	ctx context.Context,
	txn *database.Txn,
	major uint,
	newEpoch, boundary uint64,
) error {
	switch major {
	case 3:
		return d.engine.ApplyAllegraAvvmReturn(ctx, txn, newEpoch, boundary)
	case 10:
		if _, err := d.ldb.ClearDanglingDRepDelegations(ctx, boundary, txn); err != nil {
			return fmt.Errorf("clear dangling DRep delegations: %w", err)
		}
	}
	return nil
}

// activateDelegatorInactivity is the one-time CIP-0163 activation: the first
// boundary derived with the gate on gives every active account a full window
// from the epoch it enters, and records that epoch, which the reward round
// clamps to. The marker commits with the boundary's other rows, so a failed
// batch neither stamps nor records.
func (d *Deriver) activateDelegatorInactivity(
	ctx context.Context,
	txn *database.Txn,
	newEpoch uint64,
) error {
	if !d.cfg.DelegatorInactivityEnabled {
		return nil
	}
	done, err := d.ldb.GetSyncState(database.DelegatorInactivityActivatedSyncKey, txn)
	if err != nil {
		return err
	}
	if done != "" {
		return nil
	}
	expiration := newEpoch + d.cfg.DelegatorInactivity
	stamped, err := d.db.StampAllActiveAccountExpirations(ctx, expiration, txn)
	if err != nil {
		return err
	}
	if err := d.ldb.SetSyncState(
		database.DelegatorInactivityActivatedSyncKey,
		strconv.FormatUint(newEpoch, 10),
		txn,
	); err != nil {
		return err
	}
	d.cfg.Logger.Info(
		"CIP-0163 delegator inactivity activated",
		"component", "indexer",
		"activation_epoch", newEpoch,
		"expiration_epoch", expiration,
		"accounts_stamped", stamped,
	)
	return nil
}

func majorChanged(before, after lcommon.ProtocolParameters) bool {
	a, errA := epochengine.GetProtocolVersion(before)
	b, errB := epochengine.GetProtocolVersion(after)
	return errA == nil && errB == nil && a.Major != b.Major
}

func protocolMajor(pp lcommon.ProtocolParameters) uint {
	v, err := epochengine.GetProtocolVersion(pp)
	if err != nil {
		return 0
	}
	return v.Major
}

// BeforeRollback records the reward pot of every epoch, which the block
// indexer's rollback clears for each round that has no pool outputs even when
// the round was applied below the rollback point, and the accounts witnessed
// after slot, whose expiration the rollback must recompute. RollbackTo puts
// them back. It runs before the block indexer's rollback deletes the
// witnessing rows.
func (d *Deriver) BeforeRollback(
	ctx context.Context,
	txn *database.Txn,
	slot uint64,
) error {
	d.affected = nil
	if d.cfg.DelegatorInactivityEnabled {
		refs, err := d.db.AccountsWitnessedAfterSlot(ctx, slot, txn)
		if err != nil {
			return fmt.Errorf("collect rolled-back witnessed accounts: %w", err)
		}
		d.affected = refs
	}
	epochs, err := d.ldb.GetEpochs(txn)
	if err != nil {
		return fmt.Errorf("read epochs: %w", err)
	}
	d.pots = map[uint64]types.Uint64{}
	for _, e := range epochs {
		pots, err := d.ldb.GetRewardAdaPots(e.EpochId, txn)
		if err != nil {
			return fmt.Errorf("read ADA pots of epoch %d: %w", e.EpochId, err)
		}
		if pots != nil && pots.Rewards != 0 {
			d.pots[e.EpochId] = pots.Rewards
		}
	}
	return nil
}

// RollbackTo removes the facts dated after slot and restores the reward pots
// of the rounds that remain applied.
func (d *Deriver) RollbackTo(
	ctx context.Context,
	txn *database.Txn,
	slot uint64,
) error {
	store, ok := d.adapterMetadata().(metadata.FactRowStore)
	if !ok {
		return errors.New("indexer metadata store cannot remove facts")
	}
	for _, table := range metadata.FactTables {
		if err := store.DeleteFactRowsAfter(table, slot, txn.Metadata()); err != nil {
			return err
		}
	}
	pots := d.pots
	d.pots = nil
	for epoch, rewards := range pots {
		if epoch < 2 {
			continue
		}
		applied, err := d.ldb.HasAppliedRewardCreditRound(epoch-2, txn)
		if err != nil {
			return err
		}
		row, err := d.ldb.GetRewardAdaPots(epoch, txn)
		if err != nil {
			return err
		}
		if !applied || row == nil || row.Rewards != 0 {
			continue
		}
		row.Rewards = rewards
		if err := d.ldb.SaveRewardAdaPots(row, txn); err != nil {
			return err
		}
	}
	affected := d.affected
	d.affected = nil
	if err := database.RecomputeAccountExpirationsAfterTruncate(
		ctx, d.db, txn, d.cfg.DelegatorInactivityEnabled,
		d.cfg.DelegatorInactivity, slot, affected,
	); err != nil {
		return fmt.Errorf("recompute account expirations: %w", err)
	}
	d.Reset()
	return nil
}

func (d *Deriver) adapterMetadata() metadata.MetadataStore { return d.db.Metadata() }
