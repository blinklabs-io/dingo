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

	"github.com/blinklabs-io/dingo/database/models"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// peerForkAncestorPrefix records that every path index up to lastIndex passed
// one kind of ancestor verification under the stated consensus context, epoch
// and nonce.
type peerForkAncestorPrefix struct {
	contextGeneration uint64
	epoch             uint64
	nonce             []byte
	lastIndex         int
	valid             bool
}

type peerForkPathPrefix struct {
	ancestorCrypto peerForkAncestorPrefix
	ancestorState  peerForkAncestorPrefix
	ancestor       models.Block
	headers        []gledger.BlockHeader
	generation     uint64
	opCertLastHash []byte
	opCertCounters map[lcommon.PoolKeyHash]opCertCounterSnapshot
	// overflowTail is the hash of the latest header deferred because its
	// path exceeds the history limit, and overflowDepth is how many headers
	// it lies past the last cached header. Replacing the entry clears both.
	overflowTail  []byte
	overflowDepth uint64
}

type peerForkNoncePrefix struct {
	sourceEpoch           uint64
	cutoff                uint64
	sourceNonce           []byte
	initialEvolvingNonce  []byte
	initialCandidateNonce []byte
	lastHash              []byte
	evolving              []byte
	candidate             []byte
}

// peerChainSelectionEpochNonce returns a candidate-chain nonce when the
// header's retained peer ancestry reaches the primary chain.
func (ls *LedgerState) peerChainSelectionEpochNonce(
	connId ouroboros.ConnectionId,
	header gledger.BlockHeader,
	snapshot *consensusSnapshot,
) ([]byte, models.Epoch, bool, error) {
	if header.Era().Id == byron.EraIdByron {
		return nil, models.Epoch{}, false, nil
	}
	ancestor, forkHeaders, found, err := ls.peerHeaderForkPath(connId, header)
	if err != nil {
		return nil, models.Epoch{}, false, err
	}
	if !found {
		if ls.chain == nil || ls.config.PeerHeaderLookupFunc == nil {
			return nil, models.Epoch{}, false, nil
		}
		if len(ls.chain.Tip().Point.Hash) == 0 {
			return nil, models.Epoch{}, false, nil
		}
		// A deferred header remains in per-connection history with generation
		// zero. Its child extends a local queued parent, so the local verifier
		// checks it instead of withholding it as a competing candidate.
		if ls.chain.HoldsQueuedHeaderHash(header.PrevHash().Bytes()) {
			return nil, models.Epoch{}, false, nil
		}
		return nil, models.Epoch{}, false, fmt.Errorf(
			"%w: candidate ancestry is unavailable for header at slot %d",
			ErrHeaderVerificationWithheld,
			header.SlotNumber(),
		)
	}
	consensus, appliedTip := ls.loadConsensusAndTipSnapshots()
	if consensus == nil || appliedTip == nil || consensus != snapshot ||
		consensus.generation != appliedTip.generation {
		return nil, models.Epoch{}, false, fmt.Errorf(
			"%w during ancestry lookup",
			errHeaderVerificationContextChanged,
		)
	}
	localTip := appliedTip.currentTip.Point
	// The applied tip can advance while peerHeaderForkPath reconstructs the
	// ancestry. If the new tip is the candidate's parent, verify this as a
	// local extension against the newly published ledger state.
	if peerHeaderPathHasParentPoint(
		forkHeaders,
		localTip,
	) {
		return nil, models.Epoch{}, false, nil
	}
	if _, err := ls.headerVerificationEpoch(
		context.Background(),
		header.SlotNumber(),
		false,
	); err != nil {
		// A direct child of the applied tip can use the local verifier's
		// immutable one-epoch forecast when the published cache has not reached
		// its slot yet. The local verifier still enforces the forecast horizon
		// and parent/block-number constraints.
		if IsHeaderVerificationDeferred(err) && len(forkHeaders) == 1 &&
			localTip.Slot == ancestor.Slot && bytes.Equal(localTip.Hash, ancestor.Hash) {
			return nil, models.Epoch{}, false, nil
		}
		return nil, models.Epoch{}, false, err
	}
	if snapshot == nil {
		return nil, models.Epoch{}, false, fmt.Errorf(
			"%w: consensus snapshot is unavailable",
			errHeaderVerificationDeferred,
		)
	}
	targetEpoch, err := epochForSlotInCache(
		snapshot.epochCache,
		header.SlotNumber(),
	)
	if err != nil {
		return nil, models.Epoch{}, false, fmt.Errorf(
			"%w: resolve candidate header epoch: %w",
			errHeaderVerificationDeferred,
			err,
		)
	}
	sourceEpoch, err := epochForSlotInCache(
		snapshot.epochCache,
		ancestor.Slot,
	)
	if err != nil {
		return nil, models.Epoch{}, false, fmt.Errorf(
			"%w: resolve candidate intersection epoch: %w",
			errHeaderVerificationDeferred,
			err,
		)
	}
	if targetEpoch.EpochId == sourceEpoch.EpochId {
		if err := ls.verifyPeerForkTargetEpochAncestors(
			connId,
			forkHeaders,
			targetEpoch,
			targetEpoch.Nonce,
			snapshot,
		); err != nil {
			return nil, models.Epoch{}, false, err
		}
		return cloneNonce(targetEpoch.Nonce), targetEpoch, true, nil
	}
	if sourceEpoch.EraId == uint(byron.EraIdByron) {
		if err := ls.verifyByronTransitionAncestry(
			forkHeaders,
			targetEpoch,
			snapshot.epochCache,
		); err != nil {
			return nil, models.Epoch{}, false, err
		}
		return cloneNonce(targetEpoch.Nonce), targetEpoch, true, nil
	}

	return ls.peerChainSelectionEpochNonceAcrossEpochs(
		connId,
		ancestor,
		forkHeaders,
		sourceEpoch,
		targetEpoch,
		snapshot,
	)
}

func (ls *LedgerState) verifyPeerForkTargetEpochAncestors(
	connID ouroboros.ConnectionId,
	headers []gledger.BlockHeader,
	epoch models.Epoch,
	epochNonce []byte,
	snapshot *consensusSnapshot,
) error {
	if len(headers) < 2 {
		return nil
	}
	epochEnd := epoch.StartSlot + uint64(epoch.LengthInSlots)
	for index, header := range headers[:len(headers)-1] {
		if header.SlotNumber() < epoch.StartSlot ||
			header.SlotNumber() >= epochEnd || ls.headerApplied(header) {
			continue
		}
		hash := header.Hash().Bytes()
		event, _, ok := ls.config.PeerHeaderLookupFunc(connID, hash)
		if !ok || event.BlockHeader == nil ||
			!bytes.Equal(event.Point.Hash, hash) ||
			!bytes.Equal(event.BlockHeader.Hash().Bytes(), hash) {
			return fmt.Errorf(
				"%w: candidate ancestor at slot %d is no longer available",
				errHeaderVerificationDeferred,
				header.SlotNumber(),
			)
		}
		if headerVerdictInContext(
			event.HeaderCryptoVerificationGeneration,
			snapshot.contextGeneration,
			snapshot.generation,
		) {
			continue
		}
		if event.HeaderCryptoVerificationGeneration != 0 {
			return fmt.Errorf(
				"%w: candidate ancestor at slot %d was verified in a stale context",
				errHeaderVerificationDeferred,
				header.SlotNumber(),
			)
		}
		block := headerOnlyBlock{header: header, peerRelative: true}
		if err := ls.verifyPeerForkAncestorCrypto(connID, headers, index,
			epoch.EpochId, epochNonce, snapshot.contextGeneration, func() error {
				return ls.verifyBlockHeaderStatelessCryptoWithEpochNonce(
					block,
					epoch,
					epochNonce,
				)
			}); err != nil {
			return err
		}
		if err := ls.verifyPeerForkAncestorMemo(
			connID, headers, index, epoch.EpochId, epochNonce,
			snapshot.contextGeneration, selectAncestorState,
			func() error {
				if ls.onPeerForkAncestorStateCheck != nil {
					ls.onPeerForkAncestorStateCheck(header.SlotNumber())
				}
				return chainSelectionHeaderStateError(
					ls.verifySelectedBlockHeaderStateWithCache(
						context.Background(),
						block,
						epoch.EpochId,
						snapshot.epochCache,
						epochNonce,
					),
				)
			}); err != nil {
			return err
		}
	}
	return nil
}

func (ls *LedgerState) peerChainSelectionEpochNonceAcrossEpochs(
	connId ouroboros.ConnectionId,
	ancestor ocommon.Point,
	forkHeaders []gledger.BlockHeader,
	sourceEpoch models.Epoch,
	targetEpoch models.Epoch,
	snapshot *consensusSnapshot,
) ([]byte, models.Epoch, bool, error) {
	retainedEpochs := map[uint64]struct{}{sourceEpoch.EpochId: {}}
	for _, header := range forkHeaders {
		if epoch, err := epochForSlotInCache(
			snapshot.epochCache,
			header.SlotNumber(),
		); err == nil {
			retainedEpochs[epoch.EpochId] = struct{}{}
		}
	}
	ls.retainPeerForkNoncePrefixes(connId, retainedEpochs)
	originAncestor := ancestor.Slot == 0 && len(ancestor.Hash) == 0
	ancestorBlock := models.Block{}
	var err error
	if !originAncestor {
		ancestorBlock, err = ls.blockByHash(context.Background(), ancestor.Hash)
		if err != nil {
			return nil, models.Epoch{}, false, fmt.Errorf(
				"%w: lookup candidate intersection block: %w",
				errHeaderVerificationDeferred,
				err,
			)
		}
	}
	currentEpoch := sourceEpoch
	epochNonce := cloneNonce(sourceEpoch.Nonce)
	labNonce := cloneNonce(sourceEpoch.LastEpochBlockNonce)
	var initialEvolvingNonce, initialCandidateNonce []byte
	if originAncestor {
		_, initialEvolvingNonce, initialCandidateNonce, _, err = ls.computeEpochNonceForSlot(
			context.Background(),
			0,
			models.Epoch{},
		)
	} else {
		initialEvolvingNonce, err = ls.db.GetBlockNonce(ancestor, nil)
		initialCandidateNonce = cloneNonce(initialEvolvingNonce)
	}
	if err != nil {
		return nil, models.Epoch{}, false, fmt.Errorf(
			"%w: %w: read candidate intersection nonce: %w",
			errHeaderVerificationDeferred,
			errHeaderStateLookupFailed,
			err,
		)
	}
	cutoff, ready := ls.nextEpochNonceReadyCutoffSlot(sourceEpoch)
	if !ready {
		return nil, models.Epoch{}, false, fmt.Errorf(
			"%w: candidate nonce cutoff is unavailable for epoch %d",
			errHeaderVerificationDeferred,
			sourceEpoch.EpochId,
		)
	}
	if ancestor.Slot >= cutoff {
		nextEpoch, ok := epochByIDInCache(
			snapshot.epochCache,
			sourceEpoch.EpochId+1,
		)
		if !ok || len(nextEpoch.CandidateNonce) != lcommon.Blake2b256Size {
			return nil, models.Epoch{}, false, fmt.Errorf(
				"%w: frozen candidate nonce for epoch %d is unavailable",
				errHeaderVerificationDeferred,
				sourceEpoch.EpochId,
			)
		}
		initialCandidateNonce = cloneNonce(nextEpoch.CandidateNonce)
		// The evolving nonce after the cutoff cannot change the next
		// epoch's nonce, which uses the frozen candidate. Blocks
		// without a stored nonce (e.g. Mithril-imported) are only
		// usable when the target is that next epoch.
		if len(initialEvolvingNonce) != lcommon.Blake2b256Size &&
			targetEpoch.EpochId == sourceEpoch.EpochId+1 {
			initialEvolvingNonce = cloneNonce(initialCandidateNonce)
		}
	}
	if len(initialEvolvingNonce) != lcommon.Blake2b256Size {
		return nil, models.Epoch{}, false, fmt.Errorf(
			"%w: candidate intersection at slot %d has no 32-byte nonce",
			errHeaderVerificationDeferred,
			ancestor.Slot,
		)
	}
	evolvingNonce := cloneNonce(initialEvolvingNonce)
	candidateNonce := cloneNonce(initialCandidateNonce)

	for epochID := sourceEpoch.EpochId; epochID < targetEpoch.EpochId; epochID++ {
		if epochID != sourceEpoch.EpochId {
			var ok bool
			currentEpoch, ok = epochByIDInCache(snapshot.epochCache, epochID)
			if !ok {
				return nil, models.Epoch{}, false, fmt.Errorf(
					"%w: candidate epoch %d is unavailable",
					errHeaderVerificationDeferred,
					epochID,
				)
			}
			initialEvolvingNonce = cloneNonce(evolvingNonce)
			initialCandidateNonce = cloneNonce(candidateNonce)
			currentEpoch.Nonce = cloneNonce(epochNonce)
		}
		cutoff, ready := ls.nextEpochNonceReadyCutoffSlot(currentEpoch)
		if !ready {
			return nil, models.Epoch{}, false, fmt.Errorf(
				"%w: candidate nonce cutoff is unavailable for epoch %d",
				errHeaderVerificationDeferred,
				currentEpoch.EpochId,
			)
		}
		evolving := cloneNonce(initialEvolvingNonce)
		candidate := cloneNonce(initialCandidateNonce)
		startIndex := 0
		if cachedEvolving, cachedCandidate, next, ok := ls.peerForkNoncePrefix(
			connId,
			currentEpoch,
			cutoff,
			initialEvolvingNonce,
			initialCandidateNonce,
			forkHeaders,
		); ok {
			evolving = cachedEvolving
			candidate = cachedCandidate
			startIndex = next
		}
		var lastFolded gledger.BlockHeader
		epochEnd := currentEpoch.StartSlot + uint64(currentEpoch.LengthInSlots)
		for index, forkHeader := range forkHeaders {
			if index < startIndex {
				continue
			}
			if forkHeader.SlotNumber() >= epochEnd {
				break
			}
			if forkHeader.SlotNumber() < currentEpoch.StartSlot {
				continue
			}
			block := headerOnlyBlock{header: forkHeader, peerRelative: true}
			if err := ls.verifyBlockHeaderStatelessCryptoWithEpochNonce(
				block,
				currentEpoch,
				epochNonce,
			); err != nil {
				return nil, models.Epoch{}, false, err
			}
			if err := chainSelectionHeaderStateError(
				ls.verifySelectedBlockHeaderStateWithCache(
					context.Background(),
					block,
					currentEpoch.EpochId,
					snapshot.epochCache,
					epochNonce,
				),
			); err != nil {
				return nil, models.Epoch{}, false, err
			}
			var foldErr error
			evolving, foldErr = ls.foldHeaderEtaV(evolving, forkHeader)
			if foldErr != nil {
				return nil, models.Epoch{}, false, fmt.Errorf(
					"%w: fold candidate header at slot %d: %w",
					errHeaderVerificationDeferred,
					forkHeader.SlotNumber(),
					foldErr,
				)
			}
			if forkHeader.SlotNumber() < cutoff {
				candidate = cloneNonce(evolving)
			}
			lastFolded = forkHeader
		}
		if lastFolded != nil {
			ls.storePeerForkNoncePrefix(
				connId,
				currentEpoch,
				cutoff,
				initialEvolvingNonce,
				initialCandidateNonce,
				lastFolded,
				evolving,
				candidate,
			)
		}
		if len(evolving) != lcommon.Blake2b256Size ||
			len(candidate) != lcommon.Blake2b256Size {
			return nil, models.Epoch{}, false, fmt.Errorf(
				"%w: candidate epoch %d rolling nonce state is incomplete",
				errHeaderVerificationDeferred,
				currentEpoch.EpochId,
			)
		}
		var nextEpoch models.Epoch
		var ok bool
		nextEpoch, ok = epochByIDInCache(snapshot.epochCache, epochID+1)
		if !ok {
			return nil, models.Epoch{}, false, fmt.Errorf(
				"%w: candidate epoch %d is unavailable",
				errHeaderVerificationDeferred,
				epochID+1,
			)
		}
		extraEntropy, err := ls.extraEntropyForCandidateEpoch(
			nextEpoch,
			snapshot,
		)
		if err != nil {
			return nil, models.Epoch{}, false, fmt.Errorf(
				"%w: %w: resolve candidate epoch extra entropy: %w",
				errHeaderVerificationDeferred,
				errHeaderStateLookupFailed,
				err,
			)
		}
		epochNonce, err = assembleEpochNonce(candidate, labNonce, extraEntropy)
		if err != nil {
			return nil, models.Epoch{}, false, fmt.Errorf(
				"%w: assemble candidate epoch nonce: %w",
				errHeaderVerificationDeferred,
				err,
			)
		}
		if nextEpoch.EpochId == targetEpoch.EpochId {
			if err := ls.verifyPeerForkTargetEpochAncestors(
				connId,
				forkHeaders,
				targetEpoch,
				epochNonce,
				snapshot,
			); err != nil {
				return nil, models.Epoch{}, false, err
			}
			return epochNonce, targetEpoch, true, nil
		}
		evolvingNonce = evolving
		candidateNonce = candidate
		labNonce, err = ls.peerForkEpochLabNonce(
			currentEpoch,
			ancestorBlock,
			forkHeaders,
			labNonce,
		)
		if err != nil {
			return nil, models.Epoch{}, false, err
		}
	}
	return nil, models.Epoch{}, false, fmt.Errorf(
		"%w: candidate epoch path did not reach epoch %d",
		errHeaderVerificationDeferred,
		targetEpoch.EpochId,
	)
}

func (ls *LedgerState) retainPeerForkNoncePrefixes(
	connId ouroboros.ConnectionId,
	retainedEpochs map[uint64]struct{},
) {
	key := connIdKey(connId)
	ls.forkHeaderNonceCacheMutex.Lock()
	defer ls.forkHeaderNonceCacheMutex.Unlock()
	entries := ls.forkHeaderNonceCache[key]
	for epochID := range entries {
		if _, retained := retainedEpochs[epochID]; !retained {
			delete(entries, epochID)
		}
	}
	if len(entries) == 0 {
		delete(ls.forkHeaderNonceCache, key)
	}
}

func epochByIDInCache(
	cache []models.Epoch,
	epochID uint64,
) (models.Epoch, bool) {
	for _, epoch := range cache {
		if epoch.EpochId == epochID && epoch.LengthInSlots > 0 {
			return epoch, true
		}
	}
	return models.Epoch{}, false
}

func (ls *LedgerState) extraEntropyForCandidateEpoch(
	epoch models.Epoch,
	snapshot *consensusSnapshot,
) ([]byte, error) {
	if epoch.EpochId > snapshot.currentEpoch.EpochId {
		// The forecast sees the update that will enact at the requested
		// boundary. It cannot include updates from intervening, not-yet-enacted
		// epochs, so only the immediately following epoch is safe to forecast.
		if eraHasExtraEntropy(epoch.EraId) &&
			epoch.EpochId-snapshot.currentEpoch.EpochId > 1 {
			return nil, fmt.Errorf(
				"%w: extra entropy for epoch %d is beyond the one-epoch forecast horizon",
				errHeaderVerificationDeferred,
				epoch.EpochId,
			)
		}
		return ls.forecastExtraEntropyForEpoch(epoch.EpochId, epoch.EraId)
	}
	return ls.recordedExtraEntropyForEpoch(epoch.EpochId, epoch.EraId)
}

func (ls *LedgerState) peerForkEpochLabNonce(
	epoch models.Epoch,
	ancestor models.Block,
	headers []gledger.BlockHeader,
	carried []byte,
) ([]byte, error) {
	var lastParent []byte
	var hasLast bool
	if len(ancestor.Hash) > 0 && ancestor.Slot >= epoch.StartSlot &&
		ancestor.Slot < epoch.StartSlot+uint64(epoch.LengthInSlots) {
		parent, err := blockPrevHash(ancestor)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: read candidate epoch %d intersection parent: %w",
				errHeaderVerificationDeferred,
				epoch.EpochId,
				err,
			)
		}
		lastParent, hasLast = parent, true
	}
	for _, header := range headers {
		if header.SlotNumber() >= epoch.StartSlot &&
			header.SlotNumber() < epoch.StartSlot+uint64(epoch.LengthInSlots) {
			lastParent = header.PrevHash().Bytes()
			hasLast = true
		}
	}
	if !hasLast || len(lastParent) == 0 {
		return cloneNonce(carried), nil
	}
	genesisHash, err := GenesisBlockHash(ls.config.CardanoNodeConfig)
	if err == nil && bytes.Equal(lastParent, genesisHash[:]) {
		return cloneNonce(carried), nil
	}
	if len(lastParent) != lcommon.Blake2b256Size {
		return nil, fmt.Errorf(
			"%w: candidate epoch %d last block parent has length %d",
			errHeaderVerificationDeferred,
			epoch.EpochId,
			len(lastParent),
		)
	}
	return cloneNonce(lastParent), nil
}

func peerHeaderPathHasParentPoint(
	headers []gledger.BlockHeader,
	point ocommon.Point,
) bool {
	if len(headers) < 2 || len(point.Hash) == 0 {
		return false
	}
	parent := headers[len(headers)-2]
	return parent.SlotNumber() == point.Slot &&
		bytes.Equal(parent.Hash().Bytes(), point.Hash)
}

func (ls *LedgerState) peerForkNoncePrefix(
	connId ouroboros.ConnectionId,
	sourceEpoch models.Epoch,
	cutoff uint64,
	initialEvolvingNonce []byte,
	initialCandidateNonce []byte,
	headers []gledger.BlockHeader,
) ([]byte, []byte, int, bool) {
	key := connIdKey(connId)
	ls.forkHeaderNonceCacheMutex.Lock()
	entry, ok := ls.forkHeaderNonceCache[key][sourceEpoch.EpochId]
	ls.forkHeaderNonceCacheMutex.Unlock()
	if !ok || entry.sourceEpoch != sourceEpoch.EpochId ||
		entry.cutoff != cutoff ||
		!bytes.Equal(entry.sourceNonce, sourceEpoch.Nonce) ||
		!bytes.Equal(entry.initialEvolvingNonce, initialEvolvingNonce) ||
		!bytes.Equal(entry.initialCandidateNonce, initialCandidateNonce) {
		return nil, nil, 0, false
	}
	epochEnd := sourceEpoch.StartSlot + uint64(sourceEpoch.LengthInSlots)
	for index, header := range headers {
		if header.SlotNumber() >= epochEnd {
			break
		}
		if bytes.Equal(header.Hash().Bytes(), entry.lastHash) {
			return cloneNonce(entry.evolving), cloneNonce(entry.candidate),
				index + 1, true
		}
	}
	return nil, nil, 0, false
}

func (ls *LedgerState) storePeerForkNoncePrefix(
	connId ouroboros.ConnectionId,
	sourceEpoch models.Epoch,
	cutoff uint64,
	initialEvolvingNonce []byte,
	initialCandidateNonce []byte,
	header gledger.BlockHeader,
	evolving []byte,
	candidate []byte,
) {
	key := connIdKey(connId)
	ls.forkHeaderNonceCacheMutex.Lock()
	defer ls.forkHeaderNonceCacheMutex.Unlock()
	if ls.forkHeaderNonceCache == nil {
		ls.forkHeaderNonceCache = make(
			map[string]map[uint64]peerForkNoncePrefix,
		)
	}
	if ls.forkHeaderNonceCache[key] == nil {
		ls.forkHeaderNonceCache[key] = make(map[uint64]peerForkNoncePrefix)
	}
	ls.forkHeaderNonceCache[key][sourceEpoch.EpochId] = peerForkNoncePrefix{
		sourceEpoch:           sourceEpoch.EpochId,
		cutoff:                cutoff,
		sourceNonce:           cloneNonce(sourceEpoch.Nonce),
		initialEvolvingNonce:  cloneNonce(initialEvolvingNonce),
		initialCandidateNonce: cloneNonce(initialCandidateNonce),
		lastHash:              append([]byte(nil), header.Hash().Bytes()...),
		evolving:              cloneNonce(evolving),
		candidate:             cloneNonce(candidate),
	}
}

func (ls *LedgerState) removePeerForkNoncePrefix(key string) {
	ls.forkHeaderNonceCacheMutex.Lock()
	defer ls.forkHeaderNonceCacheMutex.Unlock()
	delete(ls.forkHeaderNonceCache, key)
	delete(ls.forkHeaderPathCache, key)
}

func (ls *LedgerState) verifyByronTransitionAncestry(
	headers []gledger.BlockHeader,
	targetEpoch models.Epoch,
	epochCache []models.Epoch,
) error {
	if len(headers) < 2 {
		return nil
	}
	for _, header := range headers[:len(headers)-1] {
		block := headerOnlyBlock{header: header, peerRelative: true}
		if header.Era().Id == byron.EraIdByron {
			if err := ls.verifyBlockHeaderStatelessCryptoWithEpochNonce(
				block,
				models.Epoch{},
				nil,
			); err != nil {
				return err
			}
			continue
		}
		if err := ls.verifyBlockHeaderStatelessCryptoWithEpochNonce(
			block,
			targetEpoch,
			targetEpoch.Nonce,
		); err != nil {
			return err
		}
		if err := chainSelectionHeaderStateError(
			ls.verifySelectedBlockHeaderStateWithCache(
				context.Background(),
				block,
				targetEpoch.EpochId,
				epochCache,
				targetEpoch.Nonce,
			),
		); err != nil {
			return err
		}
	}
	return nil
}

func (ls *LedgerState) peerHeaderForkPath(
	connId ouroboros.ConnectionId,
	header gledger.BlockHeader,
) (ocommon.Point, []gledger.BlockHeader, bool, error) {
	return ls.peerHeaderForkPathContext(context.Background(), connId, header)
}

func (ls *LedgerState) peerHeaderForkPathContext(
	ctx context.Context,
	connId ouroboros.ConnectionId,
	header gledger.BlockHeader,
) (ocommon.Point, []gledger.BlockHeader, bool, error) {
	if ls.chain == nil || ls.config.PeerHeaderLookupFunc == nil {
		return ocommon.Point{}, nil, false, nil
	}
	key := connIdKey(connId)
	// Ancestor verdicts and the cached path stay usable while the consensus
	// context is unchanged. Applying a block moves only the tip, and queued
	// ancestors do not need re-verification for that, as in the reference
	// ChainSync client, which only trims the candidate when the selection
	// advances.
	verificationGeneration, contextGeneration := ls.currentHeaderVerificationContext()
	contextChanged := func() bool {
		current, since := ls.currentHeaderVerificationContext()
		return current == 0 || since != contextGeneration
	}
	ls.forkHeaderNonceCacheMutex.Lock()
	cached, cachedOK := ls.forkHeaderPathCache[key]
	ls.forkHeaderNonceCacheMutex.Unlock()
	cachedAncestor := ocommon.NewPoint(
		cached.ancestor.Slot,
		cached.ancestor.Hash,
	)
	validCached := cachedOK && headerVerdictInContext(
		cached.generation, contextGeneration, verificationGeneration,
	) && ls.peerForkAncestorOnChain(ctx, cachedAncestor)
	if validCached {
		var err error
		cached, err = ls.trimPeerForkPathAtAppliedTip(
			ctx,
			key,
			cached,
			verificationGeneration,
		)
		if err != nil {
			return ocommon.Point{}, nil, false, err
		}
		cachedAncestor = ocommon.NewPoint(
			cached.ancestor.Slot,
			cached.ancestor.Hash,
		)
	}
	if validCached && len(cached.headers) > 0 {
		lastHeader := cached.headers[len(cached.headers)-1]
		if bytes.Equal(lastHeader.Hash().Bytes(), header.Hash().Bytes()) {
			tipOnCachedPath := false
			if tip := ls.loadTipSnapshot(); tip != nil {
				point := tip.currentTip.Point
				for _, cachedHeader := range cached.headers {
					if cachedHeader.SlotNumber() == point.Slot &&
						bytes.Equal(cachedHeader.Hash().Bytes(), point.Hash) {
						tipOnCachedPath = true
						break
					}
				}
			}
			if !tipOnCachedPath {
				if contextChanged() {
					return ocommon.Point{}, nil, false,
						errHeaderVerificationContextChanged
				}
				return cachedAncestor, slices.Clone(cached.headers), true, nil
			}
		}
		// The ancestry walk reads the limit+1 ancestors closest to the header
		// and succeeds only if the primary chain holds one of them. A header
		// extending a saturated cached path, or the latest header deferred for
		// that reason, has ancestors whose block numbers follow from the last
		// cached header. While the chain tip is below all of them the walk
		// would read every ancestor and defer, so defer without the walk.
		if limit := ls.peerHeaderHistoryLimit(); len(cached.headers) > limit {
			prevHash := header.PrevHash().Bytes()
			parentDepth, extends := uint64(0), false
			switch {
			case bytes.Equal(lastHeader.Hash().Bytes(), prevHash):
				extends = true
			case len(cached.overflowTail) > 0 &&
				bytes.Equal(cached.overflowTail, prevHash):
				parentDepth, extends = cached.overflowDepth, true
			}
			// The lowest ancestor the walk reads is at parentNumber-limit.
			parentNumber := lastHeader.BlockNumber() + parentDepth
			//nolint:gosec // G115: the history limit is never negative
			walkSpan := uint64(limit)
			if extends && ls.chain.Tip().BlockNumber+walkSpan < parentNumber {
				ls.markPeerForkPathOverflow(
					key,
					lastHeader.Hash().Bytes(),
					cached.overflowTail,
					parentDepth,
					header,
				)
				return ocommon.Point{}, nil, false,
					errHeaderVerificationDeferred
			}
		}
		if len(cached.headers) < ls.peerHeaderHistoryLimit() &&
			bytes.Equal(
				lastHeader.Hash().Bytes(),
				header.PrevHash().Bytes(),
			) {
			parent := headerOnlyBlock{
				header:       lastHeader,
				peerRelative: true,
			}
			if err := validateBlockOrder(
				headerOnlyBlock{header: header, peerRelative: true},
				envelopeParentFromBlock(parent),
				byronEpochSlots(ls.config.CardanoNodeConfig),
			); err != nil {
				return ocommon.Point{}, nil, false, err
			}
			parentPoint := ocommon.NewPoint(
				parent.SlotNumber(),
				parent.Hash().Bytes(),
			)
			event, _, ok := ls.config.PeerHeaderLookupFunc(
				connId,
				parentPoint.Hash,
			)
			parentVerdictCurrent := ok &&
				headerVerdictInContext(
					event.HeaderCryptoVerificationGeneration,
					contextGeneration,
					verificationGeneration,
				)
			if !parentVerdictCurrent {
				if ls.chain.HoldsQueuedHeaderHash(parentPoint.Hash) {
					return ocommon.Point{}, nil, false, nil
				}
				if (!ok || event.HeaderCryptoVerificationGeneration != 0) &&
					!ls.chain.HoldsPoint(ctx, parentPoint) {
					return ocommon.Point{}, nil, false, fmt.Errorf(
						"%w: candidate parent at slot %d has no current crypto verdict",
						errHeaderVerificationDeferred,
						parentPoint.Slot,
					)
				}
			}
			path := append(slices.Clone(cached.headers), header)
			if contextChanged() {
				return ocommon.Point{}, nil, false,
					errHeaderVerificationContextChanged
			}
			ls.storePeerForkPathPrefix(
				key,
				cached.ancestor,
				path,
				verificationGeneration,
				contextGeneration,
			)
			return cachedAncestor, path, true, nil
		}
	}
	pathReversed := []gledger.BlockHeader{header}
	prevHash := append([]byte(nil), header.PrevHash().Bytes()...)
	visited := map[string]struct{}{string(header.Hash().Bytes()): {}}
	for remaining := ls.peerHeaderHistoryLimit(); ; remaining-- {
		var nullPrevHash lcommon.Blake2b256
		// Shelley header decoders represent CBOR null prev_hash as a zero hash.
		isOrigin := len(prevHash) == 0 || bytes.Equal(prevHash, nullPrevHash[:])
		if !isOrigin {
			genesisHash, err := GenesisBlockHash(ls.config.CardanoNodeConfig)
			isOrigin = err == nil && bytes.Equal(prevHash, genesisHash[:])
		}
		if isOrigin {
			origin := ocommon.Point{}
			if !ls.peerForkAncestorOnChain(ctx, origin) {
				return ocommon.Point{}, nil, false, nil
			}
			slices.Reverse(pathReversed)
			if contextChanged() {
				return ocommon.Point{}, nil, false,
					errHeaderVerificationContextChanged
			}
			if err := ls.validatePeerHeaderPath(
				models.Block{},
				pathReversed,
			); err != nil {
				return ocommon.Point{}, nil, false, err
			}
			ls.storePeerForkPathPrefix(
				key,
				models.Block{},
				pathReversed,
				verificationGeneration,
				contextGeneration,
			)
			return origin, pathReversed, true, nil
		}
		block, err := ls.blockByHash(ctx, prevHash)
		if err == nil {
			point := ocommon.NewPoint(block.Slot, block.Hash)
			if ls.chain.HoldsPoint(ctx, point) {
				slices.Reverse(pathReversed)
				if contextChanged() {
					return ocommon.Point{}, nil, false,
						errHeaderVerificationContextChanged
				}
				if err := ls.validatePeerHeaderPath(
					block,
					pathReversed,
				); err != nil {
					return ocommon.Point{}, nil, false, err
				}
				ls.storePeerForkPathPrefix(
					key,
					block,
					pathReversed,
					verificationGeneration,
					contextGeneration,
				)
				return point, pathReversed, true, nil
			}
		} else if !errors.Is(err, models.ErrBlockNotFound) {
			return ocommon.Point{}, nil, false, fmt.Errorf(
				"%w: lookup candidate ancestor %x: %w",
				errHeaderVerificationDeferred,
				prevHash,
				err,
			)
		}
		key := string(prevHash)
		if _, ok := visited[key]; ok {
			return ocommon.Point{}, nil, false, nil
		}
		visited[key] = struct{}{}
		if remaining == 0 {
			return ocommon.Point{}, nil, false, errHeaderVerificationDeferred
		}
		event, nextPrevHash, ok := ls.config.PeerHeaderLookupFunc(
			connId,
			prevHash,
		)
		if !ok || event.BlockHeader == nil ||
			!bytes.Equal(event.Point.Hash, prevHash) ||
			!bytes.Equal(event.BlockHeader.Hash().Bytes(), prevHash) ||
			!bytes.Equal(
				event.BlockHeader.PrevHash().Bytes(),
				nextPrevHash,
			) {
			return ocommon.Point{}, nil, false, nil
		}
		// Do not use an unverified queued header as fork ancestry; its child
		// must go through local admission or wait for a current verdict.
		if !headerVerdictInContext(
			event.HeaderCryptoVerificationGeneration,
			contextGeneration,
			verificationGeneration,
		) {
			if ls.chain.HoldsQueuedHeaderHash(prevHash) {
				return ocommon.Point{}, nil, false, nil
			}
			if event.HeaderCryptoVerificationGeneration != 0 &&
				!ls.chain.HoldsPoint(ctx, event.Point) {
				return ocommon.Point{}, nil, false, fmt.Errorf(
					"%w: candidate ancestor at slot %d has no current crypto verdict",
					errHeaderVerificationDeferred,
					event.Point.Slot,
				)
			}
		}
		pathReversed = append(pathReversed, event.BlockHeader)
		prevHash = append(prevHash[:0], nextPrevHash...)
	}
}

// markPeerForkPathOverflow records header as the deferred tail of the cached
// path ending at lastHash, one past a parent parentDepth headers beyond it.
// An entry whose last header or deferred tail changed since the caller read
// them is left alone.
func (ls *LedgerState) markPeerForkPathOverflow(
	key string,
	lastHash []byte,
	tail []byte,
	parentDepth uint64,
	header gledger.BlockHeader,
) {
	ls.forkHeaderNonceCacheMutex.Lock()
	defer ls.forkHeaderNonceCacheMutex.Unlock()
	entry, ok := ls.forkHeaderPathCache[key]
	if !ok || len(entry.headers) == 0 ||
		!bytes.Equal(
			entry.headers[len(entry.headers)-1].Hash().Bytes(),
			lastHash,
		) ||
		!bytes.Equal(entry.overflowTail, tail) {
		return
	}
	entry.overflowTail = append([]byte(nil), header.Hash().Bytes()...)
	entry.overflowDepth = parentDepth + 1
	ls.forkHeaderPathCache[key] = entry
}

func (ls *LedgerState) peerForkAncestorOnChain(
	ctx context.Context,
	point ocommon.Point,
) bool {
	if ls.chain == nil {
		return false
	}
	if point.Slot == 0 && len(point.Hash) == 0 {
		_, _, ancestor, err := ls.chain.TipRelationContext(ctx, point)
		return err == nil && ancestor
	}
	return ls.chain.HoldsPoint(ctx, point)
}

func (ls *LedgerState) storePeerForkPathPrefix(
	key string,
	ancestor models.Block,
	headers []gledger.BlockHeader,
	generation, contextGeneration uint64,
) {
	ls.forkHeaderNonceCacheMutex.Lock()
	defer ls.forkHeaderNonceCacheMutex.Unlock()
	if ls.forkHeaderPathCache == nil {
		ls.forkHeaderPathCache = make(map[string]peerForkPathPrefix)
	}
	var ancestorCrypto, ancestorState peerForkAncestorPrefix
	if cached, ok := ls.forkHeaderPathCache[key]; ok &&
		headerVerdictInContext(
			cached.generation,
			contextGeneration,
			generation,
		) &&
		cached.ancestor.Slot == ancestor.Slot &&
		bytes.Equal(cached.ancestor.Hash, ancestor.Hash) {
		retain := func(
			prefix peerForkAncestorPrefix,
		) peerForkAncestorPrefix {
			if !prefix.valid ||
				prefix.contextGeneration != contextGeneration ||
				prefix.lastIndex >= len(cached.headers) ||
				prefix.lastIndex >= len(headers) {
				return peerForkAncestorPrefix{}
			}
			for index := 0; index <= prefix.lastIndex; index++ {
				if !bytes.Equal(
					cached.headers[index].Hash().Bytes(),
					headers[index].Hash().Bytes(),
				) {
					return peerForkAncestorPrefix{}
				}
			}
			return prefix
		}
		ancestorCrypto = retain(cached.ancestorCrypto)
		ancestorState = retain(cached.ancestorState)
	}
	var opCertLastHash []byte
	var opCertCounters map[lcommon.PoolKeyHash]opCertCounterSnapshot
	if cached, ok := ls.forkHeaderPathCache[key]; ok &&
		cached.generation == generation &&
		cached.ancestor.Slot == ancestor.Slot &&
		bytes.Equal(cached.ancestor.Hash, ancestor.Hash) &&
		len(cached.opCertLastHash) > 0 {
		cachedIndex := -1
		for index, header := range cached.headers {
			if bytes.Equal(header.Hash().Bytes(), cached.opCertLastHash) {
				cachedIndex = index
				break
			}
		}
		if cachedIndex >= 0 && len(headers) > cachedIndex {
			prefixMatches := true
			for index := 0; index <= cachedIndex; index++ {
				if !bytes.Equal(
					cached.headers[index].Hash().Bytes(),
					headers[index].Hash().Bytes(),
				) {
					prefixMatches = false
					break
				}
			}
			if prefixMatches {
				opCertLastHash = slices.Clone(cached.opCertLastHash)
				opCertCounters = cached.opCertCounters
			}
		}
	}
	ls.forkHeaderPathCache[key] = peerForkPathPrefix{
		ancestorCrypto: ancestorCrypto,
		ancestorState:  ancestorState,
		ancestor:       ancestor,
		headers:        slices.Clone(headers),
		generation:     generation,
		opCertLastHash: opCertLastHash,
		opCertCounters: opCertCounters,
	}
}

func (ls *LedgerState) validatePeerHeaderPath(
	ancestor models.Block,
	headers []gledger.BlockHeader,
) error {
	parent := envelopeParentFromTip(
		ancestor.Slot,
		ancestor.Number,
		ancestor.Hash,
		ancestor.Type,
		true,
	)
	var err error
	parent, err = parent.withStoredByronPosition(ancestor)
	if err != nil {
		return fmt.Errorf("resolve candidate ancestor envelope: %w", err)
	}
	for _, header := range headers {
		block := headerOnlyBlock{header: header, peerRelative: true}
		if err := validateBlockOrder(
			block,
			parent,
			byronEpochSlots(ls.config.CardanoNodeConfig),
		); err != nil {
			return fmt.Errorf(
				"candidate header at slot %d does not follow its parent: %w",
				header.SlotNumber(),
				err,
			)
		}
		parent = envelopeParentFromBlock(block)
	}
	return nil
}

func (ls *LedgerState) foldHeaderEtaV(
	evolvingNonce []byte,
	header gledger.BlockHeader,
) ([]byte, error) {
	era, ok := ls.eraById(uint(header.Era().Id))
	if !ok || era == nil || era.CalculateEtaVFunc == nil {
		return nil, fmt.Errorf(
			"era %d has no nonce contribution calculator",
			header.Era().Id,
		)
	}
	return era.CalculateEtaVFunc(
		ls.config.CardanoNodeConfig,
		evolvingNonce,
		headerOnlyBlock{header: header, peerRelative: true},
	)
}

func selectAncestorCrypto(p *peerForkPathPrefix) *peerForkAncestorPrefix {
	return &p.ancestorCrypto
}

func selectAncestorState(p *peerForkPathPrefix) *peerForkAncestorPrefix {
	return &p.ancestorState
}

// verifyPeerForkAncestorCrypto memoizes only stateless cryptography. The caller
// still checks peer-scoped ancestry, lookup freshness and the stateful rules,
// traversing ancestors in order so the marker covers only successful crypto.
func (ls *LedgerState) verifyPeerForkAncestorCrypto(
	connID ouroboros.ConnectionId,
	headers []gledger.BlockHeader,
	index int,
	epoch uint64,
	nonce []byte,
	contextGeneration uint64,
	verify func() error,
) error {
	return ls.verifyPeerForkAncestorMemo(
		connID, headers, index, epoch, nonce, contextGeneration,
		selectAncestorCrypto, verify,
	)
}

// verifyPeerForkAncestorMemo runs verify unless the marker chosen by selector
// already covers index. Only successful verdicts are recorded, in path order,
// and a marker holds only for the consensus context, epoch and nonce it was
// recorded under, so a rollback or context change re-verifies.
func (ls *LedgerState) verifyPeerForkAncestorMemo(
	connID ouroboros.ConnectionId,
	headers []gledger.BlockHeader,
	index int,
	epoch uint64,
	nonce []byte,
	contextGeneration uint64,
	selector func(*peerForkPathPrefix) *peerForkAncestorPrefix,
	verify func() error,
) error {
	key := connIdKey(connID)
	hash := headers[index].Hash().Bytes()
	ls.forkHeaderNonceCacheMutex.Lock()
	entry, exists := ls.forkHeaderPathCache[key]
	prefix := *selector(&entry)
	cached := exists && contextGeneration != 0 &&
		entry.generation >= contextGeneration && prefix.valid &&
		prefix.contextGeneration == contextGeneration && prefix.epoch == epoch &&
		bytes.Equal(prefix.nonce, nonce) && index <= prefix.lastIndex &&
		index < len(
			entry.headers,
		) && bytes.Equal(entry.headers[index].Hash().Bytes(), hash)
	ls.forkHeaderNonceCacheMutex.Unlock()
	if cached {
		return nil
	}
	if err := verify(); err != nil {
		return err
	}
	ls.forkHeaderNonceCacheMutex.Lock()
	defer ls.forkHeaderNonceCacheMutex.Unlock()
	entry, exists = ls.forkHeaderPathCache[key]
	if !exists || contextGeneration == 0 ||
		entry.generation < contextGeneration ||
		index >= len(entry.headers) ||
		!bytes.Equal(entry.headers[index].Hash().Bytes(), hash) {
		return nil
	}
	prefix = *selector(&entry)
	if !prefix.valid || prefix.contextGeneration != contextGeneration ||
		prefix.epoch != epoch ||
		!bytes.Equal(prefix.nonce, nonce) ||
		index > prefix.lastIndex {
		*selector(&entry) = peerForkAncestorPrefix{
			contextGeneration: contextGeneration,
			epoch:             epoch,
			nonce:             cloneNonce(nonce),
			lastIndex:         index,
			valid:             true,
		}
		ls.forkHeaderPathCache[key] = entry
	}
	return nil
}

// trimPeerForkPathAtAppliedTip advances only to an exact applied hash already
// linked into this peer's validated path. Older peer records may be evicted
// once that prefix is applied; keeping the old anchor would require them again.
func (ls *LedgerState) trimPeerForkPathAtAppliedTip(
	ctx context.Context,
	key string,
	cached peerForkPathPrefix,
	generation uint64,
) (peerForkPathPrefix, error) {
	if err := ctx.Err(); err != nil {
		return cached, err
	}
	tip := ls.loadTipSnapshot()
	if tip == nil || generation == 0 || tip.generation != generation ||
		len(tip.currentTip.Point.Hash) == 0 {
		return cached, nil
	}
	point := tip.currentTip.Point
	for index, header := range cached.headers {
		if header.SlotNumber() != point.Slot ||
			!bytes.Equal(header.Hash().Bytes(), point.Hash) {
			continue
		}
		block, err := ls.blockByHash(ctx, point.Hash)
		if err != nil {
			return cached, fmt.Errorf(
				"%w: read applied candidate anchor: %w",
				errHeaderVerificationDeferred,
				err,
			)
		}
		if block.Slot != point.Slot || !bytes.Equal(block.Hash, point.Hash) ||
			!ls.peerForkAncestorOnChain(ctx, point) {
			return cached, fmt.Errorf(
				"%w: applied candidate anchor changed",
				errHeaderVerificationDeferred,
			)
		}
		trimmed := peerForkPathPrefix{
			ancestor:   block,
			headers:    slices.Clone(cached.headers[index+1:]),
			generation: generation,
		}
		// Nonce folding reloads this anchor's stored nonce. The old path's
		// operational-certificate and crypto prefixes use a different anchor.
		ls.storePeerForkPathPrefix(key, block, trimmed.headers, generation, 0)
		return trimmed, nil
	}
	return cached, nil
}
