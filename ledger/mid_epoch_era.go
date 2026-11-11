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
	"errors"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// midEpochEraGate decides the era of each block inside an epoch that follows
// an empty epoch, where the ledger epoch label is behind the block's epoch.
//
// The hard-fork combinator evaluates the transition on the ledger state
// before the block (HardFork/Combinator/State.hs extendToSlot): the pending
// update names the next era's major, shelleyAfterVoting >= k, and the
// transition epoch, the successor of the label, is at or before the block's
// slot. A frozen label keeps that epoch behind the block, so the block is in
// the next era as soon as k blocks of the block's own epoch lie at or past
// its voting deadline, which Dingo otherwise decides only at an epoch
// boundary. Eras whose major is enacted by governance have no such pending
// update and are not covered.
type midEpochEraGate struct {
	era      eras.EraDesc
	epoch    models.Epoch
	label    uint64
	pparams  lcommon.ProtocolParameters
	enacted  lcommon.ProtocolParameters
	resolved bool
}

// ledgerEpochLabel returns the epoch label of the ledger state. Call it
// without ls being written concurrently.
func (ls *LedgerState) ledgerEpochLabel() uint64 {
	ls.RLock()
	defer ls.RUnlock()
	label := ls.ledgerEpoch
	if label == 0 && ls.currentEpoch.EpochId != 0 {
		if len(ls.epochCache) > 0 {
			return ledgerEpochFromCache(ls.epochCache)
		}
		return ls.currentEpoch.EpochId
	}
	return label
}

// newMidEpochEraGate returns the gate for blocks in epoch, or nil when era's
// successor is not decided from a pending classic update or the epoch is not
// ahead of the label.
func (ls *LedgerState) newMidEpochEraGate(
	era eras.EraDesc,
	epoch models.Epoch,
	pparams lcommon.ProtocolParameters,
) *midEpochEraGate {
	return ls.newMidEpochEraGateAt(
		era, epoch, pparams, ls.ledgerEpochLabel(),
	)
}

// newMidEpochEraGateAt is newMidEpochEraGate for a caller that already holds
// a consistent view of the ledger and the epoch label it implies, such as a
// published snapshot, and must not take the ledger lock.
func (ls *LedgerState) newMidEpochEraGateAt(
	era eras.EraDesc,
	epoch models.Epoch,
	pparams lcommon.ProtocolParameters,
	label uint64,
) *midEpochEraGate {
	if era.Id == eras.ByronEraDesc.Id || era.Id >= eras.ConwayEraDesc.Id ||
		pparams == nil {
		return nil
	}
	entry, ok := ls.eraShape().EraForID(era.Id)
	if !ok || entry.NextEraTrigger.Kind != hardfork.TriggerAtVersion {
		return nil
	}
	if epoch.EpochId <= label {
		return nil
	}
	return &midEpochEraGate{
		era: era, epoch: epoch, label: label, pparams: pparams,
	}
}

// requiredEra returns the era the block after prev must be encoded in.
func (g *midEpochEraGate) requiredEra(
	ctx context.Context,
	ls *LedgerState,
	txn *database.Txn,
	prev ochainsync.Tip,
) (uint, error) {
	countFrom, ok := ls.votingCountStartSlot(g.era.Id, g.epoch)
	if !ok || prev.Point.Slot < countFrom {
		return g.era.Id, nil
	}
	if !g.resolved {
		forecast, err := ls.forecastPendingPParamUpdateIn(
			g.era, g.label+1, g.pparams, txn,
		)
		if err != nil {
			return 0, fmt.Errorf("forecast pparam updates: %w", err)
		}
		g.enacted = g.pparams
		if forecast != nil {
			g.enacted = forecast
		}
		g.resolved = true
	}
	eraID, decided, err := ls.ledgerBoundaryEra(ctx, boundaryEraInputs{
		sourceEra:  g.era,
		epoch:      g.epoch,
		tip:        prev,
		pparams:    g.enacted,
		newEpochID: g.epoch.EpochId,
		txn:        txn,
	})
	if err != nil {
		return 0, fmt.Errorf("decide mid-epoch era: %w", err)
	}
	if !decided {
		return g.era.Id, nil
	}
	return eraID, nil
}

// midEpochEraDecision is a block the batch stopped at because its era is not
// the ledger's, or the ledger's era is about to change.
type midEpochEraDecision struct {
	required uint
	body     uint
}

// applyMidEpochEraSwitch translates the ledger into targetEra before block,
// the first block of that era. The translation is keyed at the block's slot
// so a rollback to any earlier block removes it, and it is recorded so the
// epoch row's era is restored with it (database.MidEpochEraSwitch). It runs
// the translation only: the pending update is not enacted, because the
// hard-fork combinator translates the frozen state without ticking it.
func (ls *LedgerState) applyMidEpochEraSwitch(
	ctx context.Context,
	block ledger.Block,
	targetEra uint,
) error {
	ls.RLock()
	era := ls.currentEra
	epoch := ls.currentEpoch
	pparams := ls.currentPParams
	ls.RUnlock()
	label := ls.ledgerEpochLabel()
	path, ok := ls.eraTransitionPath(era.Id, targetEra, false)
	if !ok || len(path) == 0 {
		return fmt.Errorf(
			"cannot translate from era %d to era %d inside epoch %d",
			era.Id, targetEra, epoch.EpochId,
		)
	}
	if epoch.EpochId <= label+1 {
		return fmt.Errorf(
			"epoch %d follows no empty epoch of label %d",
			epoch.EpochId, label,
		)
	}
	transitionEpoch := models.Epoch{
		EpochId:   label + 1,
		StartSlot: block.SlotNumber(),
		EraId:     targetEra,
	}
	hardForkSlot := epoch.StartSlot - (epoch.EpochId-transitionEpoch.EpochId)*
		uint64(epoch.LengthInSlots)

	ls.fenceRewardPrecompute()
	finishValidationTransition := ls.beginTxValidationTransition()
	var (
		results []*EraTransitionResult
		newEra  uint
	)
	err := ls.SubmitAsyncDBTxn(ctx, func(txn *database.Txn) error {
		var err error
		_, newEra, results, _, _, err = ls.prepareEraTransitionsForEpoch(
			ctx, txn, transitionEpoch, era, pparams, path, false,
		)
		if err != nil {
			return err
		}
		if len(results) == 0 {
			return errors.New("no era transition produced")
		}
		if err := ls.db.SetEpoch(
			epoch.StartSlot, epoch.EpochId,
			epoch.Nonce, epoch.EvolvingNonce, epoch.CandidateNonce,
			epoch.LastEpochBlockNonce,
			newEra, epoch.SlotLength, epoch.LengthInSlots, txn,
		); err != nil {
			return fmt.Errorf("set switched epoch: %w", err)
		}
		return ls.db.SetMidEpochEraSwitch(database.MidEpochEraSwitch{
			Epoch:   epoch.EpochId,
			Slot:    block.SlotNumber(),
			FromEra: era.Id,
			ToEra:   newEra,
		}, txn)
	}, true)
	if err != nil {
		finishValidationTransition()
		return fmt.Errorf("apply mid-epoch era switch: %w", err)
	}
	if len(results) == 0 {
		finishValidationTransition()
		return errors.New("apply mid-epoch era switch: no era transition")
	}
	ls.Lock()
	for _, result := range results {
		ls.applyEraTransition(result)
	}
	ls.currentEpoch.EraId = newEra
	if n := len(ls.epochCache); n > 0 && ls.epochCache[n-1].EpochId == epoch.EpochId {
		cache := slices.Clone(ls.epochCache)
		cache[n-1].EraId = newEra
		ls.epochCache = cache
	}
	ls.evaluateTriggerAtEpoch()
	ls.queueProtocolVersionBumpEvaluation(ctx)
	ls.publishSnapshotsLocked()
	ls.Unlock()
	finishValidationTransition()

	if ls.config.EventBus != nil {
		oldVer, _ := GetProtocolVersion(pparams)
		newVer, _ := GetProtocolVersion(results[len(results)-1].NewPParams)
		ls.config.EventBus.Publish(
			event.HardForkEventType,
			event.NewEvent(event.HardForkEventType, event.HardForkEvent{
				Slot:            hardForkSlot,
				EpochNo:         transitionEpoch.EpochId,
				FromEra:         era.Id,
				ToEra:           newEra,
				OldMajorVersion: oldVer.Major,
				OldMinorVersion: oldVer.Minor,
				NewMajorVersion: newVer.Major,
				NewMinorVersion: newVer.Minor,
			}),
		)
	}
	return nil
}

var errMidEpochEraBlock = fmt.Errorf(
	"%w: block era is not the era of the ledger state before it",
	errBoundaryEraNotAuthorized,
)

func midEpochEraRefusal(
	block ledger.Block,
	dec *midEpochEraDecision,
) error {
	return fmt.Errorf(
		"%w: block at slot %d is in era %d, the ledger requires era %d",
		errMidEpochEraBlock, block.SlotNumber(), dec.body, dec.required,
	)
}

// midEpochEraAfterTip returns the era of the block that follows the applied
// tip when the ledger is inside an epoch the mid-epoch rule covers and the
// transition is already known at the tip, and false otherwise. The answer is
// a function of the published ledger and tip alone, so the forger, the leader
// schedule and header verification see the era the apply path will require
// for that block. A slot beyond the next block is not decided by a tip that
// does not yet know the transition, as in the reference node, whose forecast
// from the tip cannot cross an era end the tip's ledger state does not know.
func (ls *LedgerState) midEpochEraAfterTip(
	ctx context.Context,
	snapshot *consensusSnapshot,
	tip *tipSnapshot,
) (eras.EraDesc, bool) {
	if snapshot == nil || tip == nil {
		return eras.EraDesc{}, false
	}
	gate := ls.newMidEpochEraGateAt(
		snapshot.currentEra,
		snapshot.currentEpoch,
		snapshot.currentPParams,
		ledgerEpochLabelInCache(
			snapshot.currentEpoch.EpochId, snapshot.epochCache,
		),
	)
	if gate == nil {
		return eras.EraDesc{}, false
	}
	required, err := gate.requiredEra(ctx, ls, nil, tip.currentTip)
	if err != nil || required == snapshot.currentEra.Id {
		return eras.EraDesc{}, false
	}
	era, ok := ls.eraById(required)
	if !ok || era == nil {
		return eras.EraDesc{}, false
	}
	return *era, true
}
