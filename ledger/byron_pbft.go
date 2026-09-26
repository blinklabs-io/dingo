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
	"github.com/blinklabs-io/dingo/ledger/byronupdate"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/cbor"
	byronconsensus "github.com/blinklabs-io/gouroboros/consensus/byron"
	"github.com/blinklabs-io/gouroboros/ledger"
	ledgerbyron "github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
	// update is the Byron update-system state. It is left uninitialized
	// when no genesis protocol parameters were supplied, and then no update
	// payload is processed.
	update byronupdate.State
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
	// Header-only blocks cannot preserve the enclosing block discriminator, so
	// distinguish EBBs from main blocks by their concrete header type.
	header := block.Header()
	if ebbHeader, ok := header.(*ledgerbyron.ByronEpochBoundaryBlockHeader); ok {
		if ls.atByronChainOrigin() {
			// An EBB's block number (Difficulty.Value) and slot (derived from
			// ConsensusData.Epoch) are independent fields: chain.firstBlockNumberValid
			// only constrains the former, and validateByronPBFTCurrentSlot only
			// rejects a future slot, not a past one. Without this, an EBB with
			// Difficulty 0, PrevBlock equal to the configured genesis hash, and
			// any past nonzero epoch would pass every other check here despite
			// skipping every epoch before it -- the first EBB of any Byron chain
			// is always epoch 0, unconditionally.
			if ebbHeader.ConsensusData.Epoch != 0 {
				return fmt.Errorf(
					"byron epoch-boundary block at slot %d: the first block "+
						"of a from-genesis chain must be epoch 0, got epoch %d",
					block.SlotNumber(),
					ebbHeader.ConsensusData.Epoch,
				)
			}
			if err := ls.validateByronGenesisAnchor(ebbHeader); err != nil {
				return err
			}
		} else if ebbHeader.ConsensusData.Epoch == 0 ||
			ebbHeader.HasGenesisTag() {
			// The reference reads an EBB's previous hash as a genesis hash
			// in epoch 0, or later when the deprecated 255 => "Genesis"
			// attribute is present, and a genesis hash cannot continue a
			// chain that already has a block (ChainValidationExpectedHeaderHash),
			// whatever its bytes.
			return fmt.Errorf(
				"byron epoch-boundary block at slot %d: previous hash is a "+
					"genesis hash but the chain already has a block",
				block.SlotNumber(),
			)
		}
		return ls.validateByronPBFTCurrentSlot(block)
	}
	// Only an epoch-boundary block may be the first block of a from-genesis
	// (or post-rollback-to-origin) chain. The chain package's own anchor
	// (chain.firstBlockNumberValid) only checks that the candidate's block
	// number is 0; it cannot also require the candidate to be an EBB, because
	// it has no notion of Byron block kinds at all. A PBFT-signed regular
	// block claiming block number 0 would otherwise pass that check and reach
	// the crypto verification below, which validates the signature but not
	// that this is the right kind of block to open the chain
	// (blinklabs-io/dingo#4399).
	if ls.atByronChainOrigin() {
		return fmt.Errorf(
			"byron block at slot %d: only an epoch-boundary block may be "+
				"the first block of a from-genesis chain",
			block.SlotNumber(),
		)
	}
	mainHeader, ok := header.(*ledgerbyron.ByronMainBlockHeader)
	if !ok || header == nil {
		return fmt.Errorf(
			"byron main block at slot %d has unexpected header type %T",
			block.SlotNumber(),
			header,
		)
	}
	config, err := ls.byronPBFTConfig()
	if err != nil {
		return err
	}
	_, err = byronconsensus.ValidatePBFTHeaderCrypto(mainHeader, config)
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

// atByronChainOrigin reports whether the primary chain currently sits at
// origin -- no blocks yet -- either because this is a genuine from-genesis
// start or because a rollback emptied the chain back to origin
// (chain.Chain.atOriginAfterMutation covers the same two cases at the chain
// layer, for the block-number half of this same anchor).
//
// A nil chain reports false rather than true. Production wiring always sets
// a chain before any header reaches this validation; a nil chain only occurs
// in a bare LedgerState built directly in a test, and treating that as
// "at origin" would force every such test to configure a Byron genesis hash
// it has no reason to care about.
func (ls *LedgerState) atByronChainOrigin() bool {
	ls.RLock()
	c := ls.chain
	ls.RUnlock()
	if c == nil {
		return false
	}
	tip := c.Tip()
	return tip.Point.Slot == 0 && len(tip.Point.Hash) == 0
}

// validateByronGenesisAnchor requires the first epoch-boundary block of a
// from-genesis (or post-rollback-to-origin) Byron chain to chain onto the
// configured Byron genesis hash. The reference tracks the previous hash as
// either the configured genesis hash or a prior header hash, and rejects a
// mismatch at the first block with ChainValidationGenesisHashMismatch. The
// chain package cannot enforce this itself -- it has no knowledge of the
// network's genesis hash (see chain.firstBlockNumberValid) -- so binding the
// anchor belongs here, in the ledger/config-aware layer
// (blinklabs-io/dingo#4399).
//
// A ledger started from a snapshot or bulk import at a trusted non-origin
// point never reaches this function with an unanchored EBB: its primary
// chain tip is that trusted point, not origin, so atByronChainOrigin already
// reports false and this check does not run.
func (ls *LedgerState) validateByronGenesisAnchor(
	header *ledgerbyron.ByronEpochBoundaryBlockHeader,
) error {
	if ls.config.CardanoNodeConfig == nil {
		return errors.New(
			"byron genesis hash is not configured; cannot anchor the first epoch-boundary block",
		)
	}
	genesisHash := ls.config.CardanoNodeConfig.ByronGenesisHash
	if genesisHash == "" {
		return errors.New(
			"byron genesis hash is not configured; cannot anchor the first epoch-boundary block",
		)
	}
	if header == nil {
		return errors.New(
			"cannot anchor a nil Byron epoch-boundary block header",
		)
	}
	prevHash := header.PrevBlock.String()
	if prevHash != genesisHash {
		return fmt.Errorf(
			"byron epoch-boundary block at slot %d: previous hash %s does not match configured Byron genesis hash %s",
			header.SlotNumber(),
			prevHash,
			genesisHash,
		)
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
	genesisParams, err := ls.byronGenesisProtocolParameters()
	if err != nil {
		return byronPBFTState{}, err
	}
	state, err := newByronPBFTState(config, genesisParams)
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
	genesisParams *eras.ByronProtocolParameters,
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
	ret := byronPBFTState{
		issuerState:     issuerState,
		delegationState: delegationState,
	}
	if genesisParams != nil {
		ret.update = byronupdate.NewState(genesisParams)
	}
	return ret, nil
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
	var updateConfig byronupdate.Config
	var updateSlot uint64
	if state.update.Initialized() {
		config, err := ls.byronPBFTConfig()
		if err != nil {
			return byronPBFTState{}, err
		}
		updateConfig = byronupdate.Config{
			ProtocolMagic:  config.ProtocolMagic,
			K:              config.SecurityParam,
			NumGenesisKeys: config.NumGenesisKeys,
		}
		updateSlot, err = byronFlatSlot(block, config.SecurityParam)
		if err != nil {
			return byronPBFTState{}, err
		}
		state.update = state.update.Tick(updateConfig, updateSlot)
	}
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
		if state.update.Initialized() {
			state.update = state.update.Advance(updateSlot, block.BlockNumber())
		}
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
	// The update rules read the delegation map as ticked to this block's
	// slot, before its own delegation certificates are applied.
	delegateToGenesis := make(map[byronupdate.KeyHash]byronupdate.KeyHash)
	for genesis, delegate := range state.delegationState.ActiveDelegations() {
		delegateToGenesis[delegate] = genesis
	}
	dlgPayload, err := byronDelegationPayload(mainBlock)
	if err != nil {
		return byronPBFTState{}, fmt.Errorf(
			"apply Byron PBFT delegation payload at slot %d: %w",
			block.SlotNumber(),
			err,
		)
	}
	state.delegationState, err = state.delegationState.ApplyPayloadCbor(
		epoch,
		block.SlotNumber(),
		dlgPayload,
	)
	if err != nil {
		return byronPBFTState{}, fmt.Errorf(
			"apply Byron PBFT delegation payload at slot %d: %w",
			block.SlotNumber(),
			err,
		)
	}
	if !state.update.Initialized() {
		return state, nil
	}
	nextUpdate, err := applyByronUpdatePayload(
		state.update,
		byronupdate.Environment{
			Config:            updateConfig,
			DelegateToGenesis: delegateToGenesis,
		},
		mainBlock,
		header,
		updateSlot,
	)
	if err != nil {
		// Only a state built from the first block knows every registered
		// proposal; a partial one would reject valid votes for proposals
		// registered before its trusted start.
		if shouldValidate && state.update.Complete() {
			return byronPBFTState{}, fmt.Errorf(
				"apply Byron update payload at slot %d: %w",
				block.SlotNumber(),
				err,
			)
		}
		// A trusted block was accepted by the network, and a partial state
		// cannot judge one, so keep following the chain and record the
		// disagreement rather than refusing it.
		if ls.config.Logger != nil {
			ls.config.Logger.Warn(
				"skipping Byron update payload of trusted block",
				"slot", block.SlotNumber(),
				"error", err,
			)
		}
		nextUpdate = state.update.Advance(updateSlot, block.BlockNumber())
	}
	state.update = nextUpdate
	return state, nil
}

// applyByronUpdatePayload registers a main block's update proposal, votes
// and endorsement.
func applyByronUpdatePayload(
	update byronupdate.State,
	env byronupdate.Environment,
	block *ledgerbyron.ByronMainBlock,
	header *ledgerbyron.ByronMainBlockHeader,
	slot uint64,
) (byronupdate.State, error) {
	input := byronupdate.Block{
		Slot:    slot,
		BlockNo: block.BlockNumber(),
		Version: byronupdate.ProtocolVersion{
			Major: header.ExtraData.BlockVersion.Major,
			Minor: header.ExtraData.BlockVersion.Minor,
			Alt:   header.ExtraData.BlockVersion.Unknown,
		},
	}
	issuer, err := byronconsensus.PBFTIssuerFromHeader(header)
	if err != nil {
		return byronupdate.State{}, fmt.Errorf("block issuer: %w", err)
	}
	input.IssuerKeyHash = issuer.DelegateKeyHash
	proposals := block.Body.UpdPayload.Proposals
	switch len(proposals) {
	case 0:
	case 1:
		input.Proposal = &proposals[0]
	default:
		return byronupdate.State{}, fmt.Errorf(
			"update payload carries %d proposals, expected at most one",
			len(proposals),
		)
	}
	votes, err := byronUpdateVotes(block)
	if err != nil {
		return byronupdate.State{}, err
	}
	input.Votes = votes
	return update.Apply(env, input)
}

// byronUpdateVotes returns a main block's update votes parsed from the CBOR
// they arrived in; a vote's signature covers its proposal id's original
// encoding.
func byronUpdateVotes(
	block *ledgerbyron.ByronMainBlock,
) ([]*ledgerbyron.UpdateVote, error) {
	raw := block.Body.UpdPayloadCbor()
	if len(raw) == 0 {
		if len(block.Body.UpdPayload.Votes) > 0 {
			return nil, fmt.Errorf(
				"block body has %d update vote(s) but no preserved CBOR",
				len(block.Body.UpdPayload.Votes),
			)
		}
		return nil, nil
	}
	var payload []cbor.RawMessage
	if _, err := cbor.Decode(raw, &payload); err != nil {
		return nil, fmt.Errorf("decode update payload: %w", err)
	}
	if len(payload) != 2 {
		return nil, fmt.Errorf(
			"update payload has %d fields, expected 2",
			len(payload),
		)
	}
	var rawVotes []cbor.RawMessage
	if _, err := cbor.Decode(payload[1], &rawVotes); err != nil {
		return nil, fmt.Errorf("decode update votes: %w", err)
	}
	votes := make([]*ledgerbyron.UpdateVote, 0, len(rawVotes))
	for i, rawVote := range rawVotes {
		vote, err := ledgerbyron.ParseUpdateVote(rawVote)
		if err != nil {
			return nil, fmt.Errorf("update vote %d: %w", i, err)
		}
		votes = append(votes, vote)
	}
	return votes, nil
}

// byronFlatSlot returns a Byron block's slot as the reference numbers it:
// epoch * 10k plus the slot within the epoch.
func byronFlatSlot(block ledger.Block, securityParam uint64) (uint64, error) {
	epochSlots := 10 * securityParam
	switch header := block.Header().(type) {
	case *ledgerbyron.ByronMainBlockHeader:
		return header.ConsensusData.SlotId.Epoch*epochSlots +
			header.ConsensusData.SlotId.Slot, nil
	case *ledgerbyron.ByronEpochBoundaryBlockHeader:
		return header.ConsensusData.Epoch * epochSlots, nil
	default:
		return 0, fmt.Errorf(
			"byron block at slot %d has header type %T",
			block.SlotNumber(),
			block.Header(),
		)
	}
}

// byronGenesisProtocolParameters returns the Byron protocol parameters Byron
// genesis initializes the update system with, or nil without a Byron genesis.
func (ls *LedgerState) byronGenesisProtocolParameters() (
	*eras.ByronProtocolParameters,
	error,
) {
	if ls.config.CardanoNodeConfig == nil ||
		ls.config.CardanoNodeConfig.ByronGenesis() == nil {
		return nil, nil
	}
	params, err := eras.NewByronProtocolParametersFromGenesis(
		ls.config.CardanoNodeConfig.ByronGenesis(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"load Byron genesis protocol parameters: %w",
			err,
		)
	}
	return params, nil
}

// byronDelegationPayload returns the block's delegation certificates as the
// CBOR they arrived in. A certificate's signature covers the wire encoding of
// its epoch field, so re-encoding a decoded certificate can produce bytes the
// issuer never signed; the preserved payload is what PBFTDelegationState needs
// to verify them.
func byronDelegationPayload(
	block *ledgerbyron.ByronMainBlock,
) ([]cbor.RawMessage, error) {
	raw := block.Body.DlgPayloadCbor()
	if len(raw) == 0 {
		// A body that never round-tripped through the CBOR decoder has no
		// preserved payload. That is only consistent with an empty payload;
		// anything else would mean silently dropping delegation certificates.
		if len(block.Body.DlgPayload) > 0 {
			return nil, fmt.Errorf(
				"block body has %d delegation certificate(s) but no preserved CBOR",
				len(block.Body.DlgPayload),
			)
		}
		return nil, nil
	}
	var payload []cbor.RawMessage
	if _, err := cbor.Decode(raw, &payload); err != nil {
		return nil, fmt.Errorf("decode delegation payload: %w", err)
	}
	return payload, nil
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

// validateByronShelleyTransition gates the move from Byron to the next era
// under a version trigger (TriggerAtVersion). ouroboros-consensus decides that
// transition from the Byron update state alone: it happens in the epoch a
// stable candidate for the trigger's major version is adopted in, and in no
// other epoch, whatever the next block's shape claims. A boundary block that
// leaves Byron without such a candidate is rejected, and so is a Byron block
// in or after the epoch the candidate takes effect. An epoch trigger
// (TestShelleyHardForkAtEpoch) is left to the era schedule.
func (ls *LedgerState) validateByronShelleyTransition(
	ctx context.Context,
	newEpoch uint64,
	blockEraID uint,
) error {
	entry, ok := ls.eraShape().EraForID(ledgerbyron.EraIdByron)
	if !ok || entry.NextEraTrigger.Kind != hardfork.TriggerAtVersion {
		return nil
	}
	state, err := ls.byronPBFTStateAtTip(ctx, ls.Tip())
	if err != nil {
		return fmt.Errorf("check Byron transition: %w", err)
	}
	// A state rebuilt from a trusted start after genesis cannot see the
	// candidate that start inherited, so it has no basis to refuse.
	if !state.update.Initialized() || !state.update.Complete() {
		return nil
	}
	config, err := ls.byronPBFTConfig()
	if err != nil {
		return err
	}
	//nolint:gosec // a Byron protocol major version is a Word16
	major := uint16(entry.NextEraTrigger.Version)
	return state.update.CheckTransition(
		byronupdate.Config{
			ProtocolMagic:  config.ProtocolMagic,
			K:              config.SecurityParam,
			NumGenesisKeys: config.NumGenesisKeys,
		},
		major,
		newEpoch,
		blockEraID != ledgerbyron.EraIdByron,
	)
}

// byronBlockPParams returns the protocol parameters a block validates
// against: for a Byron block, those its update state adopted for the block's
// epoch, which state has been ticked to; otherwise pparams.
func byronBlockPParams(
	block ledger.Block,
	state byronPBFTState,
	pparams lcommon.ProtocolParameters,
) lcommon.ProtocolParameters {
	if block.Era().Id != ledgerbyron.EraIdByron ||
		!state.update.Initialized() {
		return pparams
	}
	return state.update.AdoptedParams()
}
