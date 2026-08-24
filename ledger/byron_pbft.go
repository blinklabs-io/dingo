// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	byronconsensus "github.com/blinklabs-io/gouroboros/consensus/byron"
	"github.com/blinklabs-io/gouroboros/ledger"
	ledgerbyron "github.com/blinklabs-io/gouroboros/ledger/byron"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type byronPBFTCache struct {
	config      *byronconsensus.ByronConfig
	state       byronPBFTState
	tip         ocommon.Point
	initialized bool
}

type byronPBFTState struct {
	issuerState     byronconsensus.PBFTState
	delegationState byronconsensus.PBFTDelegationState
}

var errByronPBFTCurrentSlotUnavailable = errors.New(
	"byron PBFT current slot unavailable",
)

func newByronPBFTCache(lsConfig LedgerStateConfig) (byronPBFTCache, error) {
	if lsConfig.CardanoNodeConfig == nil ||
		lsConfig.CardanoNodeConfig.ByronGenesis() == nil {
		return byronPBFTCache{}, nil
	}
	config, err := byronconsensus.NewByronConfigFromGenesis(
		lsConfig.CardanoNodeConfig.ByronGenesis(),
	)
	if err != nil {
		return byronPBFTCache{}, fmt.Errorf(
			"build Byron PBFT config from genesis: %w",
			err,
		)
	}
	return byronPBFTCache{config: &config}, nil
}

func (ls *LedgerState) byronPBFTConfig() (byronconsensus.ByronConfig, error) {
	if ls.byronPBFT.config != nil {
		return *ls.byronPBFT.config, nil
	}
	cache, err := newByronPBFTCache(ls.config)
	if err != nil {
		return byronconsensus.ByronConfig{}, err
	}
	if cache.config == nil {
		return byronconsensus.ByronConfig{}, errors.New(
			"byron PBFT validation requires Byron genesis configuration",
		)
	}
	return *cache.config, nil
}

func (ls *LedgerState) validateByronPBFTHeader(
	block ledger.Block,
	delegationState byronconsensus.PBFTDelegationState,
) (byronconsensus.PBFTIssuer, error) {
	if err := ls.validateByronPBFTHeaderCrypto(block); err != nil {
		return byronconsensus.PBFTIssuer{}, err
	}
	if block.Type() == ledgerbyron.BlockTypeByronEbb {
		return byronconsensus.PBFTIssuer{}, nil
	}
	header, ok := block.Header().(*ledgerbyron.ByronMainBlockHeader)
	if !ok || header == nil {
		return byronconsensus.PBFTIssuer{}, fmt.Errorf(
			"byron main block at slot %d has unexpected header type %T",
			block.SlotNumber(),
			block.Header(),
		)
	}
	config, err := ls.byronPBFTConfig()
	if err != nil {
		return byronconsensus.PBFTIssuer{}, err
	}
	config.GenesisDelegations = delegationState.ActiveDelegations()
	issuer, err := byronconsensus.ValidatePBFTHeader(header, config)
	if err != nil {
		return byronconsensus.PBFTIssuer{}, fmt.Errorf(
			"byron PBFT header verification failed at slot %d: %w",
			block.SlotNumber(),
			err,
		)
	}
	return issuer, nil
}

func (ls *LedgerState) validateByronPBFTHeaderCrypto(
	block ledger.Block,
) error {
	if block == nil {
		return errors.New(
			"cannot validate nil Byron PBFT block",
		)
	}
	if block.Type() == ledgerbyron.BlockTypeByronEbb {
		return ls.validateByronPBFTCurrentSlot(block)
	}
	header, ok := block.Header().(*ledgerbyron.ByronMainBlockHeader)
	if !ok || header == nil {
		return fmt.Errorf(
			"byron main block at slot %d has unexpected header type %T",
			block.SlotNumber(),
			block.Header(),
		)
	}
	config, err := ls.byronPBFTConfig()
	if err != nil {
		return err
	}
	_, err = byronconsensus.ValidatePBFTHeaderCrypto(header, config)
	if err != nil {
		return fmt.Errorf(
			"byron PBFT header verification failed at slot %d: %w",
			block.SlotNumber(),
			err,
		)
	}
	if err := ls.validateByronPBFTCurrentSlot(block); err != nil {
		return err
	}
	return nil
}

func (ls *LedgerState) validateByronPBFTCurrentSlot(block ledger.Block) error {
	currentSlot, err := ls.CurrentSlot()
	if err != nil {
		return fmt.Errorf(
			"%w for header at slot %d: %w",
			errByronPBFTCurrentSlotUnavailable,
			block.SlotNumber(),
			err,
		)
	}
	if err := validateByronPBFTSlot(block.SlotNumber(), currentSlot); err != nil {
		return err
	}
	return nil
}

func classifyByronPBFTApplyError(
	point ocommon.Point,
	err error,
	shouldValidate bool,
) error {
	if shouldValidate && !errors.Is(err, errByronPBFTCurrentSlotUnavailable) {
		return &headerValidationError{
			BlockPoint: point,
			Cause:      err,
		}
	}
	return err
}

func validateByronPBFTSlot(blockSlot, currentSlot uint64) error {
	if blockSlot > currentSlot {
		return fmt.Errorf(
			"byron PBFT block slot %d is after current slot %d",
			blockSlot,
			currentSlot,
		)
	}
	return nil
}

func batchContainsByronBlocks(blocks []ledger.Block) bool {
	for _, block := range blocks {
		if block != nil && block.Era().Id == ledgerbyron.EraIdByron {
			return true
		}
	}
	return false
}

func (ls *LedgerState) byronPBFTStateAtTip(
	ctx context.Context,
	tip ocommon.Tip,
) (byronPBFTState, error) {
	config, err := ls.byronPBFTConfig()
	if err != nil {
		return byronPBFTState{}, err
	}
	if config.SecurityParam == 0 {
		return byronPBFTState{}, errors.New(
			"byron PBFT security parameter must be greater than zero",
		)
	}

	ls.RLock()
	cachedState := ls.byronPBFT.state
	cachedTip := ls.byronPBFT.tip
	cached := ls.byronPBFT.initialized
	ls.RUnlock()
	if cached && cachedTip.Slot == tip.Point.Slot &&
		bytes.Equal(cachedTip.Hash, tip.Point.Hash) {
		return cachedState, nil
	}
	state, err := newByronPBFTState(config)
	if err != nil {
		return byronPBFTState{}, err
	}
	if tip.Point.Slot == 0 && len(tip.Point.Hash) == 0 {
		return state, nil
	}
	if ls.chain == nil {
		return byronPBFTState{}, errors.New(
			"rebuild Byron PBFT state: primary chain is unavailable",
		)
	}

	rebuildStarted := time.Now()
	rebuildStart := ocommon.Point{}
	reusedCache := false
	var iter *chain.ChainIterator
	if cached && cachedTip.Slot < tip.Point.Slot {
		iter, err = ls.chain.FromPointContext(ctx, cachedTip, false)
		if err == nil {
			state = cachedState
			rebuildStart = cachedTip
			reusedCache = true
		} else if !errors.Is(err, models.ErrBlockNotFound) {
			return byronPBFTState{}, fmt.Errorf(
				"rebuild Byron PBFT state from cached tip %d/%x: %w",
				cachedTip.Slot,
				cachedTip.Hash,
				err,
			)
		}
	}
	if iter == nil {
		iter, err = ls.chain.FromPointContext(ctx, ocommon.Point{}, false)
		if err != nil {
			return byronPBFTState{}, fmt.Errorf(
				"rebuild Byron PBFT state through tip %d/%x: %w",
				tip.Point.Slot,
				tip.Point.Hash,
				err,
			)
		}
	}
	defer iter.Cancel()
	for {
		result, err := iter.Next(false)
		if errors.Is(err, chain.ErrIteratorChainTip) {
			return byronPBFTState{}, fmt.Errorf(
				"rebuild Byron PBFT state: canonical chain ended before tip %d/%x",
				tip.Point.Slot,
				tip.Point.Hash,
			)
		}
		if err != nil {
			return byronPBFTState{}, fmt.Errorf(
				"rebuild Byron PBFT state: walk canonical chain: %w",
				err,
			)
		}
		if result == nil {
			return byronPBFTState{}, errors.New(
				"rebuild Byron PBFT state: iterator returned nil result",
			)
		}
		if result.Block.Type != ledgerbyron.BlockTypeByronMain &&
			result.Block.Type != ledgerbyron.BlockTypeByronEbb {
			return byronPBFTState{}, fmt.Errorf(
				"rebuild Byron PBFT state: encountered non-Byron block at slot %d before Byron tip %d",
				result.Block.Slot,
				tip.Point.Slot,
			)
		}
		block, err := result.Block.Decode()
		if err != nil {
			return byronPBFTState{}, fmt.Errorf(
				"rebuild Byron PBFT state: decode block at slot %d: %w",
				result.Block.Slot,
				err,
			)
		}
		state, err = ls.advanceByronPBFTState(state, block, false)
		if err != nil {
			return byronPBFTState{}, fmt.Errorf(
				"rebuild Byron PBFT state: apply block at slot %d: %w",
				result.Block.Slot,
				err,
			)
		}
		if result.Block.Slot == tip.Point.Slot &&
			bytes.Equal(result.Block.Hash, tip.Point.Hash) {
			if ls.config.Logger != nil {
				ls.config.Logger.Debug(
					"rebuilt Byron PBFT state",
					"start_slot", rebuildStart.Slot,
					"tip_slot", tip.Point.Slot,
					"cached", reusedCache,
					"duration", time.Since(rebuildStarted),
				)
			}
			return state, nil
		}
	}
}

func newByronPBFTState(
	config byronconsensus.ByronConfig,
) (byronPBFTState, error) {
	issuerState, err := byronconsensus.NewPBFTState(
		nil,
		config.SecurityParam,
	)
	if err != nil {
		return byronPBFTState{}, err
	}
	delegationState, err := byronconsensus.NewPBFTDelegationState(config)
	if err != nil {
		return byronPBFTState{}, err
	}
	return byronPBFTState{
		issuerState:     issuerState,
		delegationState: delegationState,
	}, nil
}

func (ls *LedgerState) advanceByronPBFTState(
	state byronPBFTState,
	block ledger.Block,
	shouldValidate bool,
) (byronPBFTState, error) {
	epoch, err := byronBlockEpoch(block)
	if err != nil {
		return byronPBFTState{}, err
	}
	state.delegationState = state.delegationState.Tick(
		epoch,
		block.SlotNumber(),
	)
	if shouldValidate {
		issuer, err := ls.validateByronPBFTHeader(
			block,
			state.delegationState,
		)
		if err != nil {
			return byronPBFTState{}, err
		}
		if block.Type() != ledgerbyron.BlockTypeByronEbb {
			state.issuerState, err = state.issuerState.Transition(
				issuer.GenesisKeyHash,
			)
			if err != nil {
				return byronPBFTState{}, err
			}
		}
	}
	if block.Type() == ledgerbyron.BlockTypeByronEbb {
		return state, nil
	}
	header, ok := block.Header().(*ledgerbyron.ByronMainBlockHeader)
	if !ok || header == nil {
		return byronPBFTState{}, fmt.Errorf(
			"advance Byron PBFT state: block at slot %d has header type %T",
			block.SlotNumber(),
			block.Header(),
		)
	}
	if !shouldValidate {
		issuer, err := byronconsensus.PBFTIssuerFromHeader(header)
		if err != nil {
			return byronPBFTState{}, fmt.Errorf(
				"observe trusted Byron PBFT header at slot %d: %w",
				block.SlotNumber(),
				err,
			)
		}
		state.issuerState, err = state.issuerState.Observe(
			issuer.GenesisKeyHash,
		)
		if err != nil {
			return byronPBFTState{}, err
		}
	}
	mainBlock, ok := block.(*ledgerbyron.ByronMainBlock)
	if !ok || mainBlock == nil {
		return byronPBFTState{}, fmt.Errorf(
			"advance Byron PBFT delegation state: block at slot %d has type %T",
			block.SlotNumber(),
			block,
		)
	}
	state.delegationState, err = state.delegationState.ApplyPayload(
		epoch,
		block.SlotNumber(),
		mainBlock.Body.DlgPayload,
	)
	if err != nil {
		return byronPBFTState{}, fmt.Errorf(
			"apply Byron PBFT delegation payload at slot %d: %w",
			block.SlotNumber(),
			err,
		)
	}
	return state, nil
}

func byronBlockEpoch(block ledger.Block) (uint64, error) {
	switch header := block.Header().(type) {
	case *ledgerbyron.ByronMainBlockHeader:
		if header == nil {
			return 0, errors.New("nil Byron main-block header")
		}
		return header.ConsensusData.SlotId.Epoch, nil
	case *ledgerbyron.ByronEpochBoundaryBlockHeader:
		if header == nil {
			return 0, errors.New("nil Byron epoch-boundary header")
		}
		return header.ConsensusData.Epoch, nil
	default:
		return 0, fmt.Errorf(
			"byron block at slot %d has unexpected header type %T",
			block.SlotNumber(),
			block.Header(),
		)
	}
}
