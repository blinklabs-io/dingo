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

type peerForkNoncePrefix struct {
	sourceEpoch uint64
	cutoff      uint64
	sourceNonce []byte
	lastHash    []byte
	candidate   []byte
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
		return nil, models.Epoch{}, false, fmt.Errorf(
			"%w: candidate ancestry is unavailable for header at slot %d",
			ErrHeaderVerificationWithheld,
			header.SlotNumber(),
		)
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
		return cloneNonce(targetEpoch.Nonce), targetEpoch, true, nil
	}
	if targetEpoch.EpochId != sourceEpoch.EpochId+1 {
		return nil, models.Epoch{}, false, fmt.Errorf(
			"%w: candidate fork spans epoch %d to %d",
			ErrHeaderVerificationWithheld,
			sourceEpoch.EpochId,
			targetEpoch.EpochId,
		)
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

	cutoff, ready := ls.nextEpochNonceReadyCutoffSlot(sourceEpoch)
	if !ready {
		return nil, models.Epoch{}, false, fmt.Errorf(
			"%w: candidate nonce cutoff is unavailable for epoch %d",
			errHeaderVerificationDeferred,
			sourceEpoch.EpochId,
		)
	}
	var evolving []byte
	var candidate []byte
	startIndex := 0
	if ancestor.Slot < cutoff {
		evolving, err = ls.db.GetBlockNonce(ancestor, nil)
		if err != nil {
			return nil, models.Epoch{}, false, fmt.Errorf(
				"%w: %w: read candidate intersection nonce: %w",
				errHeaderVerificationDeferred,
				errHeaderStateLookupFailed,
				err,
			)
		}
		if len(evolving) != lcommon.Blake2b256Size {
			return nil, models.Epoch{}, false, fmt.Errorf(
				"%w: candidate intersection at slot %d has no 32-byte nonce",
				errHeaderVerificationDeferred,
				ancestor.Slot,
			)
		}
		candidate = cloneNonce(evolving)
		if cached, next, ok := ls.peerForkNoncePrefix(
			connId,
			sourceEpoch,
			cutoff,
			forkHeaders,
		); ok {
			candidate = cached
			evolving = cloneNonce(cached)
			startIndex = next
		}
	} else {
		candidate = cloneNonce(targetEpoch.CandidateNonce)
	}
	var lastFolded gledger.BlockHeader
	for index, forkHeader := range forkHeaders {
		if index < startIndex {
			continue
		}
		if forkHeader.SlotNumber() >= cutoff {
			break
		}
		if forkHeader.SlotNumber() < sourceEpoch.StartSlot {
			return nil, models.Epoch{}, false, fmt.Errorf(
				"%w: candidate header at slot %d precedes intersection epoch %d",
				errHeaderVerificationDeferred,
				forkHeader.SlotNumber(),
				sourceEpoch.EpochId,
			)
		}
		block := headerOnlyBlock{header: forkHeader, peerRelative: true}
		if err := ls.verifyBlockHeaderStatelessCryptoWithEpochNonce(
			block,
			sourceEpoch,
			sourceEpoch.Nonce,
		); err != nil {
			return nil, models.Epoch{}, false, err
		}
		if err := chainSelectionHeaderStateError(
			ls.verifyBlockHeaderStateWithCache(
				context.Background(),
				block,
				sourceEpoch.EpochId,
				snapshot.epochCache,
				true,
			),
		); err != nil {
			return nil, models.Epoch{}, false, err
		}
		evolving, err = ls.foldHeaderEtaV(evolving, forkHeader)
		if err != nil {
			return nil, models.Epoch{}, false, fmt.Errorf(
				"%w: fold candidate header at slot %d: %w",
				errHeaderVerificationDeferred,
				forkHeader.SlotNumber(),
				err,
			)
		}
		candidate = cloneNonce(evolving)
		lastFolded = forkHeader
	}
	if lastFolded != nil {
		ls.storePeerForkNoncePrefix(
			connId,
			sourceEpoch,
			cutoff,
			lastFolded,
			candidate,
		)
	}
	if len(candidate) != lcommon.Blake2b256Size {
		return nil, models.Epoch{}, false, fmt.Errorf(
			"%w: candidate epoch %d has no frozen nonce",
			errHeaderVerificationDeferred,
			sourceEpoch.EpochId,
		)
	}

	var extraEntropy []byte
	if targetEpoch.EpochId > snapshot.currentEpoch.EpochId {
		extraEntropy, err = ls.forecastExtraEntropyForEpoch(
			targetEpoch.EpochId,
			targetEpoch.EraId,
		)
	} else {
		extraEntropy, err = ls.recordedExtraEntropyForEpoch(
			targetEpoch.EpochId,
			targetEpoch.EraId,
		)
	}
	if err != nil {
		return nil, models.Epoch{}, false, fmt.Errorf(
			"%w: %w: resolve candidate epoch extra entropy: %w",
			errHeaderVerificationDeferred,
			errHeaderStateLookupFailed,
			err,
		)
	}
	epochNonce, err := assembleEpochNonce(
		candidate,
		sourceEpoch.LastEpochBlockNonce,
		extraEntropy,
	)
	if err != nil {
		return nil, models.Epoch{}, false, fmt.Errorf(
			"%w: assemble candidate epoch nonce: %w",
			errHeaderVerificationDeferred,
			err,
		)
	}
	return epochNonce, targetEpoch, true, nil
}

func (ls *LedgerState) peerForkNoncePrefix(
	connId ouroboros.ConnectionId,
	sourceEpoch models.Epoch,
	cutoff uint64,
	headers []gledger.BlockHeader,
) ([]byte, int, bool) {
	key := connIdKey(connId)
	ls.forkHeaderNonceCacheMutex.Lock()
	entry, ok := ls.forkHeaderNonceCache[key]
	ls.forkHeaderNonceCacheMutex.Unlock()
	if !ok || entry.sourceEpoch != sourceEpoch.EpochId ||
		entry.cutoff != cutoff ||
		!bytes.Equal(entry.sourceNonce, sourceEpoch.Nonce) {
		return nil, 0, false
	}
	for index, header := range headers {
		if header.SlotNumber() >= cutoff {
			break
		}
		if bytes.Equal(header.Hash().Bytes(), entry.lastHash) {
			return cloneNonce(entry.candidate), index + 1, true
		}
	}
	return nil, 0, false
}

func (ls *LedgerState) storePeerForkNoncePrefix(
	connId ouroboros.ConnectionId,
	sourceEpoch models.Epoch,
	cutoff uint64,
	header gledger.BlockHeader,
	candidate []byte,
) {
	key := connIdKey(connId)
	ls.forkHeaderNonceCacheMutex.Lock()
	defer ls.forkHeaderNonceCacheMutex.Unlock()
	if ls.forkHeaderNonceCache == nil {
		ls.forkHeaderNonceCache = make(map[string]peerForkNoncePrefix)
	}
	ls.forkHeaderNonceCache[key] = peerForkNoncePrefix{
		sourceEpoch: sourceEpoch.EpochId,
		cutoff:      cutoff,
		sourceNonce: cloneNonce(sourceEpoch.Nonce),
		lastHash:    append([]byte(nil), header.Hash().Bytes()...),
		candidate:   cloneNonce(candidate),
	}
}

func (ls *LedgerState) removePeerForkNoncePrefix(key string) {
	ls.forkHeaderNonceCacheMutex.Lock()
	defer ls.forkHeaderNonceCacheMutex.Unlock()
	delete(ls.forkHeaderNonceCache, key)
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
			ls.verifyBlockHeaderStateWithCache(
				context.Background(),
				block,
				targetEpoch.EpochId,
				epochCache,
				true,
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
	if ls.chain == nil || ls.config.PeerHeaderLookupFunc == nil {
		return ocommon.Point{}, nil, false, nil
	}
	pathReversed := []gledger.BlockHeader{header}
	prevHash := append([]byte(nil), header.PrevHash().Bytes()...)
	visited := map[string]struct{}{string(header.Hash().Bytes()): {}}
	for remaining := ls.peerHeaderHistoryLimit(); ; remaining-- {
		if len(prevHash) == 0 {
			return ocommon.Point{}, nil, false, nil
		}
		block, err := ls.blockByHash(context.Background(), prevHash)
		if err == nil {
			point := ocommon.NewPoint(block.Slot, block.Hash)
			if ls.chain.HoldsPoint(context.Background(), point) {
				slices.Reverse(pathReversed)
				if err := ls.validatePeerHeaderPath(
					block,
					pathReversed,
				); err != nil {
					return ocommon.Point{}, nil, false, err
				}
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
			return ocommon.Point{}, nil, false, nil
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
		pathReversed = append(pathReversed, event.BlockHeader)
		prevHash = append(prevHash[:0], nextPrevHash...)
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
