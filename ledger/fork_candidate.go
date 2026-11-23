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
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// forkCandidate is a preferred peer suffix whose bodies are fetched while the
// selected primary chain remains untouched. It is guarded by
// chainsyncBlockfetchMutex.
type forkCandidate struct {
	connection ouroboros.ConnectionId
	ancestor   ocommon.Point
	localTip   ochainsync.Tip
	generation uint64
	path       []ChainsyncEvent
	blocks     []BlockfetchEvent
	request    chan struct{}
	requestID  uint64
	timer      *time.Timer
	err        error
}

type forkCandidateValidationContextKey struct{}

func forkCandidateValidationFrom(ctx context.Context) bool {
	validation, _ := ctx.Value(forkCandidateValidationContextKey{}).(bool)
	return validation
}

func (c *forkCandidate) matchesEvent(e BlockfetchEvent) bool {
	if c == nil || !sameConnectionId(c.connection, e.ConnectionId) {
		return false
	}
	return c.requestID == 0 || e.RequestId == 0 || c.requestID == e.RequestId
}

func (c *forkCandidate) acceptBlock(e BlockfetchEvent) {
	if c.err != nil {
		return
	}
	index := len(c.blocks)
	if index >= len(c.path) || !pointMatches(c.path[index].Point, e.Point) {
		c.err = fmt.Errorf(
			"fork candidate returned unexpected block at slot %d",
			e.Point.Slot,
		)
		return
	}
	if e.Block == nil || !pointMatches(e.Point, ocommon.NewPoint(
		e.Block.SlotNumber(), e.Block.Hash().Bytes(),
	)) {
		c.err = fmt.Errorf(
			"fork candidate block does not match requested point at slot %d",
			e.Point.Slot,
		)
		return
	}
	c.blocks = append(c.blocks, e)
}

// startForkCandidateValidation fetches a preferred suffix before permitting
// its rollback point to affect the primary chain. The caller owns
// chainsyncMutex; this method temporarily owns chainsyncBlockfetchMutex around
// the request bookkeeping but never across RequestRange.
func (ls *LedgerState) startForkCandidateValidation(
	connection ouroboros.ConnectionId,
	ancestor ocommon.Point,
	localTip ochainsync.Tip,
	path []ChainsyncEvent,
) error {
	if len(path) == 0 || ls.config.BlockfetchRequestRangeFunc == nil {
		return errors.New("fork candidate blockfetch is unavailable")
	}
	candidate := &forkCandidate{
		connection: connection,
		ancestor: ocommon.Point{
			Slot: ancestor.Slot,
			Hash: bytes.Clone(ancestor.Hash),
		},
		localTip:   cloneTip(localTip),
		generation: ls.CurrentHeaderCryptoVerificationGeneration(),
		path:       append([]ChainsyncEvent(nil), path...),
		blocks:     make([]BlockfetchEvent, 0, len(path)),
	}

	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	if ls.forkCandidate != nil {
		if pointMatches(ls.forkCandidate.path[len(ls.forkCandidate.path)-1].Point,
			path[len(path)-1].Point) {
			return nil
		}
		ls.discardForkCandidateLocked()
	}
	// Requests on the candidate connection cannot overlap because callbacks
	// may arrive before RequestRange returns its ID. A batch on another peer
	// keeps running; request IDs and connection identity keep its events apart.
	usesCandidateConnection := sameConnectionId(
		ls.activeBlockfetchConnId, connection,
	) || sameConnectionId(ls.shadowBlockfetchConnId, connection)
	if next := ls.nextBlockfetchRequest; next != nil {
		usesCandidateConnection = usesCandidateConnection ||
			sameConnectionId(next.connId, connection)
	}
	if usesCandidateConnection {
		ls.blockfetchRequestRangeCleanup()
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	}
	ls.forkCandidate = candidate
	if err := ls.waitForBlockfetchRequestLockedWithSignal(connection, nil); err != nil {
		ls.forkCandidate = nil
		return err
	}
	candidate.request = ls.beginBlockfetchRequestLocked(connection)
	ls.chainsyncBlockfetchMutex.Unlock()
	requestID, err := ls.blockfetchRequestRangeStart(
		connection,
		path[0].Point,
		path[len(path)-1].Point,
	)
	ls.chainsyncBlockfetchMutex.Lock()
	if err != nil {
		ls.endBlockfetchRequestLocked(connection, candidate.request)
		if ls.forkCandidate == candidate {
			ls.forkCandidate = nil
		}
		return err
	}
	candidate.requestID = requestID
	ls.bindBlockfetchRequestIdLocked(connection, candidate.request, requestID)
	if ls.forkCandidate != candidate {
		return nil
	}
	candidate.timer = time.AfterFunc(blockfetchBusyTimeout, func() {
		ls.chainsyncBlockfetchMutex.Lock()
		defer ls.chainsyncBlockfetchMutex.Unlock()
		if ls.forkCandidate != candidate {
			return
		}
		ls.config.Logger.Warn(
			"fork candidate blockfetch timed out without changing the selected chain",
			"component", "ledger",
			"connection_id", connection.String(),
			"candidate_tip_slot", path[len(path)-1].Point.Slot,
		)
		ls.discardForkCandidateLocked()
	})
	return nil
}

func (ls *LedgerState) discardForkCandidateLocked() {
	candidate := ls.forkCandidate
	if candidate == nil {
		return
	}
	if candidate.timer != nil {
		candidate.timer.Stop()
	}
	ls.releaseBlockfetchRequestLocked(
		candidate.connection,
		candidate.request,
		false,
	)
	ls.forkCandidate = nil
}

// handleForkCandidateBlockfetchEventLocked consumes events for the isolated
// candidate request. A completed suffix is handed to a worker because full
// ledger validation owns a database writer and must not block the blockfetch
// subscriber.
func (ls *LedgerState) handleForkCandidateBlockfetchEventLocked(
	e BlockfetchEvent,
) bool {
	candidate := ls.forkCandidate
	if candidate == nil || !candidate.matchesEvent(e) {
		return false
	}
	if released := ls.blockfetchReleasedRequestIds[connIdKey(e.ConnectionId)]; e.RequestId != 0 && released != nil {
		if _, stale := released[e.RequestId]; stale {
			return false
		}
	}
	if !e.BatchDone {
		candidate.acceptBlock(e)
		return true
	}
	ls.completeBlockfetchRequestForEventLocked(e.ConnectionId, e.RequestId)
	if candidate.timer != nil {
		candidate.timer.Stop()
	}
	ls.forkCandidate = nil
	if e.RangeErr != nil && candidate.err == nil {
		candidate.err = fmt.Errorf("fetch fork candidate: %w", e.RangeErr)
	}
	if len(candidate.blocks) != len(candidate.path) && candidate.err == nil {
		candidate.err = fmt.Errorf(
			"fork candidate returned %d of %d requested blocks",
			len(candidate.blocks), len(candidate.path),
		)
	}
	if candidate.err != nil {
		ls.config.Logger.Warn(
			"discarding incomplete fork candidate without changing the selected chain",
			"component", "ledger",
			"connection_id", candidate.connection.String(),
			"error", candidate.err,
		)
		return true
	}
	ls.startForkCandidateWorkerLocked(candidate)
	return true
}

func (ls *LedgerState) startForkCandidateWorkerLocked(candidate *forkCandidate) {
	if ls.closed.Load() {
		return
	}
	ls.blockfetchContinuationMu.Lock()
	if ls.closed.Load() {
		ls.blockfetchContinuationMu.Unlock()
		return
	}
	ls.blockfetchContinuationWG.Add(1)
	ls.blockfetchContinuationMu.Unlock()
	go func() {
		defer ls.blockfetchContinuationWG.Done()
		ls.finishForkCandidate(candidate)
	}()
}

func (ls *LedgerState) finishForkCandidate(candidate *forkCandidate) {
	if !ls.headerCryptoVerificationGenerationCurrent(candidate.generation) {
		return
	}
	blocks := make([]gledger.Block, len(candidate.blocks))
	for i := range candidate.blocks {
		blocks[i] = candidate.blocks[i].Block
	}
	if err := ls.validateForkCandidate(
		ls.lifecycleContext(), candidate.ancestor, candidate.localTip, blocks,
	); err != nil {
		ls.config.Logger.Warn(
			"rejecting fork candidate before rollback",
			"component", "ledger",
			"connection_id", candidate.connection.String(),
			"candidate_tip_slot", candidate.path[len(candidate.path)-1].Point.Slot,
			"error", err,
		)
		return
	}
	if !ls.headerCryptoVerificationGenerationCurrent(candidate.generation) {
		return
	}

	var pending pendingPublishes
	pending.drainChain(ls.chain)
	defer pending.flush()
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()
	if !pointMatches(ls.chain.Tip().Point, candidate.localTip.Point) {
		return
	}
	if err := ls.rollbackChainAndStateDeferredIf(
		ls.lifecycleContext(),
		candidate.ancestor,
		&pending,
		func() error {
			if !pointMatches(ls.chain.Tip().Point, candidate.localTip.Point) ||
				!ls.headerCryptoVerificationGenerationCurrent(candidate.generation) {
				return errors.New("validated fork candidate is stale")
			}
			return nil
		},
	); err != nil {
		ls.config.Logger.Warn(
			"validated fork candidate became unavailable before switch",
			"component", "ledger",
			"error", err,
		)
		return
	}
	for i, forkEvent := range candidate.path {
		if err := ls.addForkPathHeader(
			forkEvent,
			candidate.path[len(candidate.path)-1].Point,
			forkEvent.HeaderCryptoVerificationGeneration,
		); err != nil {
			ls.config.Logger.Warn(
				"failed to queue validated fork header",
				"component", "ledger",
				"slot", forkEvent.Point.Slot,
				"error", err,
			)
			return
		}
		candidate.blocks[i].ConnectionId = candidate.connection
	}
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	ls.blockfetchBatchRollbackGeneration = ls.blockfetchRollbackGeneration.Load()
	ls.blockfetchBatchChainGeneration = ls.chainRollbackGeneration.Load()
	ls.pendingBlockfetchEvents = append(
		ls.pendingBlockfetchEvents,
		candidate.blocks...,
	)
	if err := ls.flushPendingBlockfetchBlocksDeferred(&pending); err != nil {
		ls.config.Logger.Error(
			"failed to install validated fork candidate",
			"component", "ledger",
			"error", err,
		)
		return
	}
	ls.clearHeaderMismatches()
	ls.clearRollbackHistory()
	ls.clearUnrecoverableRollbacks()
	ls.setChainsyncState(RollbackChainsyncState)
}

var errForkCandidateValidationComplete = errors.New(
	"fork candidate validation complete",
)

// validateForkCandidate validates a suffix against a rollback-only database
// view rooted at ancestor. Returning the sentinel from the transaction keeps
// both the honest suffix and the candidate effects out of durable state.
func (ls *LedgerState) validateForkCandidate(
	ctx context.Context,
	ancestor ocommon.Point,
	localTip ochainsync.Tip,
	blocks []gledger.Block,
) error {
	if len(blocks) == 0 {
		return errors.New("fork candidate has no blocks")
	}
	if !pointMatches(ls.chain.Tip().Point, localTip.Point) {
		return errors.New("selected chain changed during fork candidate fetch")
	}
	ctx = context.WithValue(ctx, forkCandidateValidationContextKey{}, true)
	txn := ls.ledgerDB().Begin(ctx, true)
	defer txn.Release()
	err := ls.ledgerDB().Run(txn, func(txn ledgerdb.Txn) error {
		ls.RLock()
		mithrilFloor := ls.mithrilLedgerSlot
		ls.RUnlock()
		_, runningNonce, err := ls.ledgerDB().TruncateAfterSlot(
			ctx,
			ancestor,
			mithrilFloor,
			dbadapter.Unwrap(txn),
		)
		if err != nil {
			return fmt.Errorf("prepare fork validation state: %w", err)
		}
		if err := ls.stageUtxoSetRollbackForValidation(txn, ancestor); err != nil { //nolint:contextcheck // rollback-only transaction never runs its after-commit callback
			return fmt.Errorf("prepare fork validation UTxO set: %w", err)
		}
		epochs, err := ls.ledgerDB().GetEpochs(txn)
		if err != nil {
			return fmt.Errorf("load fork validation epochs: %w", err)
		}
		if len(epochs) == 0 {
			return errors.New("fork validation ancestor has no epoch state")
		}
		currentEpoch := epochs[len(epochs)-1]
		currentEraPtr, ok := ls.eraById(currentEpoch.EraId)
		if !ok || currentEraPtr == nil {
			return fmt.Errorf("unknown fork validation era %d", currentEpoch.EraId)
		}
		currentEra := *currentEraPtr
		pparams, prevPParams, err := ls.computePParamsIn(
			currentEpoch, currentEra, epochs, txn,
		)
		if err != nil {
			return err
		}
		syntheticValue, err := ls.ledgerDB().GetSyncState(
			database.SyntheticV2CostModelSyncKey, txn,
		)
		if err != nil {
			return err
		}
		synthetic := resolveSyntheticV2CostModel(syntheticValue, pparams)
		parentBlock, err := database.BlockByPointTxn(
			dbadapter.Unwrap(txn), ancestor,
		)
		if err != nil {
			return fmt.Errorf("load fork validation ancestor: %w", err)
		}
		parent := envelopeParentFromTip(
			parentBlock.Slot,
			parentBlock.Number,
			parentBlock.Hash,
			parentBlock.Type,
			true,
		)
		parent, err = parent.withStoredByronPosition(parentBlock)
		if err != nil {
			return fmt.Errorf("position fork validation ancestor: %w", err)
		}
		expectedPrevHash := bytes.Clone(ancestor.Hash)
		runningTip := ochainsync.Tip{
			Point:       ancestor,
			BlockNumber: parentBlock.Number,
		}
		live := ls.loadConsensusSnapshot()
		if live == nil {
			return errors.New("fork validation consensus snapshot unavailable")
		}
		epochCache := slices.Clone(epochs)
		for _, block := range blocks {
			if block.SlotNumber() >= currentEpoch.StartSlot+
				uint64(currentEpoch.LengthInSlots) {
				var transitionErr error
				currentEpoch, currentEra, pparams, prevPParams, synthetic,
					transitionErr = ls.tickForkCandidateEpoch(
					ctx, txn, runningTip, block, currentEpoch, currentEra,
					pparams, prevPParams, synthetic,
				)
				if transitionErr != nil {
					return transitionErr
				}
				epochCache = append(epochCache, currentEpoch)
			}
			point := ocommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
			var offsets *database.BlockIngestionResult
			if len(block.Transactions()) > 0 {
				indexer := database.NewBlockIndexer(point.Slot, point.Hash)
				offsets, err = indexer.ComputeOffsets(block.Cbor(), block)
				if err != nil {
					return fmt.Errorf("index candidate block at slot %d: %w", point.Slot, err)
				}
			}
			delta, err := ls.ledgerProcessBlock(
				ctx, txn, point, block, true, true, false,
				expectedPrevHash, parent, offsets, currentEra, pparams,
				prevPParams, currentEpoch.EpochId, currentEpoch.StartSlot,
				synthetic,
			)
			if err != nil {
				return fmt.Errorf("validate candidate block at slot %d: %w", point.Slot, err)
			}
			if delta != nil {
				candidate := *live
				candidate.currentEpoch = currentEpoch
				candidate.currentEra = currentEra
				candidate.currentPParams = pparams
				candidate.prevEraPParams = prevPParams
				candidate.epochCache = epochCache
				delta.consensus = &candidate
				if err := delta.apply(ctx, ls, txn); err != nil {
					delta.Release()
					return fmt.Errorf("stage candidate block at slot %d: %w", point.Slot, err)
				}
				delta.Release()
			}
			if err := ls.utxoSetValidationError(txn); err != nil {
				return fmt.Errorf("stage candidate block at slot %d: %w", point.Slot, err)
			}
			runningTip = ochainsync.Tip{Point: point, BlockNumber: block.BlockNumber()}
			if err := ls.ledgerDB().SetTip(runningTip, txn); err != nil {
				return err
			}
			expectedPrevHash = bytes.Clone(point.Hash)
			parent = envelopeParentFromBlock(block)
			if currentEra.CalculateEtaVFunc != nil {
				runningNonce, err = currentEra.CalculateEtaVFunc(
					ls.config.CardanoNodeConfig, runningNonce, block,
				)
				if err != nil {
					return fmt.Errorf("calculate candidate nonce at slot %d: %w", point.Slot, err)
				}
			}
		}
		return errForkCandidateValidationComplete
	})
	if errors.Is(err, errForkCandidateValidationComplete) {
		return nil
	}
	return err
}

func (ls *LedgerState) tickForkCandidateEpoch(
	ctx context.Context,
	txn ledgerdb.Txn,
	tip ochainsync.Tip,
	block gledger.Block,
	currentEpoch models.Epoch,
	currentEra eras.EraDesc,
	currentPParams lcommon.ProtocolParameters,
	prevPParams lcommon.ProtocolParameters,
	synthetic bool,
) (
	models.Epoch,
	eras.EraDesc,
	lcommon.ProtocolParameters,
	lcommon.ProtocolParameters,
	bool,
	error,
) {
	targetEpoch, err := epochAtSlot(currentEpoch, block.SlotNumber())
	if err != nil {
		return currentEpoch, currentEra, currentPParams, prevPParams,
			synthetic, err
	}
	if targetEpoch.EpochId == currentEpoch.EpochId {
		return currentEpoch, currentEra, currentPParams, prevPParams,
			synthetic, errors.New("candidate epoch tick made no progress")
	}
	enacted := currentPParams
	forecast, err := ls.forecastPendingPParamUpdateIn(
		currentEra,
		currentEpoch.EpochId+1,
		currentPParams,
		txn,
	)
	if err != nil {
		return currentEpoch, currentEra, currentPParams, prevPParams,
			synthetic, fmt.Errorf("forecast candidate pparams: %w", err)
	}
	if forecast != nil {
		enacted = forecast
	}
	bodyEraID := uint(block.Era().Id)
	headerMajor, headerMajorKnown := HeaderProtocolMajor(block.Header())
	targetEraID, allowTwo := ls.boundaryEraForBlock(
		currentEra.Id,
		bodyEraID,
		headerMajor,
		headerMajorKnown,
		targetEpoch.EpochId,
	)
	targetEraID, err = ls.authorizeBoundaryEra(
		ctx,
		boundaryEraInputs{
			sourceEra:  currentEra,
			epoch:      currentEpoch,
			tip:        tip,
			pparams:    enacted,
			newEpochID: targetEpoch.EpochId,
			txn:        txn,
		},
		bodyEraID,
		targetEraID,
	)
	if err != nil {
		return currentEpoch, currentEra, currentPParams, prevPParams,
			synthetic, err
	}
	transitionPath, ok := ls.eraTransitionPath(
		currentEra.Id,
		targetEraID,
		allowTwo,
	)
	if !ok {
		return currentEpoch, currentEra, currentPParams, prevPParams,
			synthetic, fmt.Errorf(
				"invalid candidate era transition from %d to %d",
				currentEra.Id, targetEraID,
			)
	}
	workingPParams, workingEraID, transitionResults,
		transitionMajors, realV2Written, err := ls.prepareEraTransitionsForEpochWithOptions(
		ctx,
		txn,
		targetEpoch,
		currentEra,
		currentPParams,
		transitionPath,
		true,
		true,
	)
	if err != nil {
		return currentEpoch, currentEra, currentPParams, prevPParams,
			synthetic, err
	}
	workingEra, ok := ls.eraById(workingEraID)
	if !ok || workingEra == nil {
		return currentEpoch, currentEra, currentPParams, prevPParams,
			synthetic, fmt.Errorf("unknown candidate era %d", workingEraID)
	}
	result, err := ls.processEpochRolloverForTargetWithOptions(
		ctx,
		txn,
		currentEpoch,
		targetEpoch,
		targetEpoch,
		currentEpoch.EpochId,
		*workingEra,
		workingPParams,
		len(transitionPath) > 0,
		realV2Written,
		transitionMajors,
		true,
	)
	if err != nil {
		return currentEpoch, currentEra, currentPParams, prevPParams,
			synthetic, err
	}
	if len(transitionResults) > 0 {
		prevPParams = currentPParams
	}
	for _, transition := range transitionResults {
		if transition.InjectedSyntheticV2CostModel {
			synthetic = true
		}
	}
	if result.RealV2CostModelObserved {
		synthetic = false
	}
	return result.NewCurrentEpoch, result.NewCurrentEra,
		result.NewCurrentPParams, prevPParams, synthetic, nil
}
