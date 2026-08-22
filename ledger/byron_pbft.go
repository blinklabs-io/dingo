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
	"slices"

	"github.com/blinklabs-io/dingo/chain"
	byronconsensus "github.com/blinklabs-io/gouroboros/consensus/byron"
	"github.com/blinklabs-io/gouroboros/ledger"
	ledgerbyron "github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type byronPBFTCache struct {
	config      *byronconsensus.ByronConfig
	state       byronconsensus.PBFTState
	tip         ocommon.Point
	initialized bool
}

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
) (byronconsensus.PBFTIssuer, error) {
	if block == nil {
		return byronconsensus.PBFTIssuer{}, errors.New(
			"cannot validate nil Byron PBFT block",
		)
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
	issuer, err := byronconsensus.ValidatePBFTHeader(header, config)
	if err != nil {
		return byronconsensus.PBFTIssuer{}, fmt.Errorf(
			"byron PBFT header verification failed at slot %d: %w",
			block.SlotNumber(),
			err,
		)
	}
	currentSlot, err := ls.CurrentSlot()
	if err != nil {
		return byronconsensus.PBFTIssuer{}, fmt.Errorf(
			"resolve current slot for Byron PBFT header at slot %d: %w",
			block.SlotNumber(),
			err,
		)
	}
	if err := validateByronPBFTSlot(block.SlotNumber(), currentSlot); err != nil {
		return byronconsensus.PBFTIssuer{}, err
	}
	return issuer, nil
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
) (byronconsensus.PBFTState, error) {
	config, err := ls.byronPBFTConfig()
	if err != nil {
		return byronconsensus.PBFTState{}, err
	}
	if config.SecurityParam == 0 {
		return byronconsensus.PBFTState{}, errors.New(
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
		return byronconsensus.NewPBFTState(
			cachedState.SignatureHistory(),
			config.SecurityParam,
		)
	}
	if tip.Point.Slot == 0 && len(tip.Point.Hash) == 0 {
		return byronconsensus.NewPBFTState(nil, config.SecurityParam)
	}
	if ls.chain == nil {
		return byronconsensus.PBFTState{}, errors.New(
			"rebuild Byron PBFT state: primary chain is unavailable",
		)
	}

	iter, err := ls.chain.FromPointReverseContext(ctx, tip.Point, true)
	if err != nil {
		return byronconsensus.PBFTState{}, fmt.Errorf(
			"rebuild Byron PBFT state from tip %d/%x: %w",
			tip.Point.Slot,
			tip.Point.Hash,
			err,
		)
	}
	defer iter.Cancel()
	issuersNewestFirst := make(
		[]lcommon.Blake2b224,
		0,
		min(config.SecurityParam, uint64(128)),
	)
	for uint64(len(issuersNewestFirst)) < config.SecurityParam {
		result, err := iter.Next(false)
		if errors.Is(err, chain.ErrIteratorChainOrigin) {
			break
		}
		if err != nil {
			return byronconsensus.PBFTState{}, fmt.Errorf(
				"rebuild Byron PBFT state: walk canonical chain: %w",
				err,
			)
		}
		if result == nil {
			return byronconsensus.PBFTState{}, errors.New(
				"rebuild Byron PBFT state: reverse iterator returned nil result",
			)
		}
		if result.Block.Type != ledgerbyron.BlockTypeByronMain &&
			result.Block.Type != ledgerbyron.BlockTypeByronEbb {
			break
		}
		if result.Block.Type == ledgerbyron.BlockTypeByronEbb {
			continue
		}
		block, err := result.Block.Decode()
		if err != nil {
			return byronconsensus.PBFTState{}, fmt.Errorf(
				"rebuild Byron PBFT state: decode block at slot %d: %w",
				result.Block.Slot,
				err,
			)
		}
		header, ok := block.Header().(*ledgerbyron.ByronMainBlockHeader)
		if !ok || header == nil {
			return byronconsensus.PBFTState{}, fmt.Errorf(
				"rebuild Byron PBFT state: block at slot %d has header type %T",
				result.Block.Slot,
				block.Header(),
			)
		}
		genesisHash, err := byronconsensus.PBFTVerificationKeyHash(
			header.ConsensusData.PubKey,
		)
		if err != nil {
			return byronconsensus.PBFTState{}, fmt.Errorf(
				"rebuild Byron PBFT state: derive issuer at slot %d: %w",
				result.Block.Slot,
				err,
			)
		}
		issuersNewestFirst = append(
			issuersNewestFirst,
			genesisHash,
		)
	}
	slices.Reverse(issuersNewestFirst)
	return byronconsensus.NewPBFTState(
		issuersNewestFirst,
		config.SecurityParam,
	)
}

func (ls *LedgerState) advanceByronPBFTState(
	state byronconsensus.PBFTState,
	block ledger.Block,
	shouldValidate bool,
) (byronconsensus.PBFTState, error) {
	if block.Type() == ledgerbyron.BlockTypeByronEbb {
		return state, nil
	}
	header, ok := block.Header().(*ledgerbyron.ByronMainBlockHeader)
	if !ok || header == nil {
		return byronconsensus.PBFTState{}, fmt.Errorf(
			"advance Byron PBFT state: block at slot %d has header type %T",
			block.SlotNumber(),
			block.Header(),
		)
	}
	var issuerHash lcommon.Blake2b224
	if shouldValidate {
		issuer, err := ls.validateByronPBFTHeader(block)
		if err != nil {
			return byronconsensus.PBFTState{}, err
		}
		issuerHash = issuer.GenesisKeyHash
		return state.Transition(issuerHash)
	}
	issuerHash, err := byronconsensus.PBFTVerificationKeyHash(
		header.ConsensusData.PubKey,
	)
	if err != nil {
		return byronconsensus.PBFTState{}, fmt.Errorf(
			"observe trusted Byron PBFT header at slot %d: %w",
			block.SlotNumber(),
			err,
		)
	}
	return state.Observe(issuerHash)
}
