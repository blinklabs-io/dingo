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

// Package midnightindexer scans applied blocks for Midnight-specific
// on-chain events: Technical Committee / Council governance datums,
// Ariadne permissioned-candidate parameters, and the in-memory
// committee-candidate UTxO set that is snapshotted at every epoch
// boundary.
package midnightindexer

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/blinklabs-io/dingo/database/models"
	dingoEvent "github.com/blinklabs-io/dingo/event"
	dingoLedger "github.com/blinklabs-io/dingo/ledger"
	gouroboros "github.com/blinklabs-io/gouroboros/ledger"
	fxcbor "github.com/fxamacker/cbor/v2"
)

// Store is the narrow persistence interface required by the indexer.
// *database.Database satisfies it via database/midnight.go.
type Store interface {
	GetMidnightCandidates(string) ([]models.Utxo, error)
	InsertMidnightGovernanceDatum(*models.MidnightGovernanceDatum) error
	GetLatestMidnightAriadneParams() (*models.MidnightAriadneParams, error)
	UpsertMidnightAriadneParams(*models.MidnightAriadneParams) error
	UpsertMidnightEpochCandidates(*models.MidnightEpochCandidates) error
}

// Config holds all parameters needed to run the Midnight indexer.
type Config struct {
	Logger   *slog.Logger
	Store    Store
	EventBus *dingoEvent.EventBus

	// Addresses (bech32) and policy IDs (hex) sourced from MidnightConfig.
	TechnicalCommitteeAddress   string
	TechnicalCommitteePolicyID  string
	CouncilAddress              string
	CouncilPolicyID             string
	PermissionedCandidatePolicy string
	CommitteeCandidateAddress   string

	// InitialEpoch seeds currentEpoch before the first EpochTransitionEvent.
	InitialEpoch uint64
}

// candidateKey uniquely identifies a UTxO at the committee-candidate address.
type candidateKey struct {
	TxHash      [32]byte
	OutputIndex uint32
}

// candidateEntry is one element of the epoch-candidate snapshot.
type candidateEntry struct {
	TxHash      []byte `cbor:"1,keyasint"`
	OutputIndex uint32 `cbor:"2,keyasint"`
	Datum       []byte `cbor:"3,keyasint"`
}

// Indexer subscribes to block and epoch-transition events and writes
// Midnight-specific rows to the metadata store.
type Indexer struct {
	config Config
	logger *slog.Logger

	// Parsed, byte-comparable forms of the configured addresses/policies.
	techCommitteeAddrBytes   []byte
	councilAddrBytes         []byte
	techCommitteePolicyBytes []byte
	councilPolicyBytes       []byte
	permCandidatePolicyBytes []byte
	candidateAddrBytes       []byte

	mu               sync.Mutex
	candidates       map[candidateKey][]byte // key → inline datum CBOR
	lastAriadneDatum []byte
	currentEpoch     uint64

	running    bool
	stopping   bool
	cancel     context.CancelFunc
	blockSubId dingoEvent.EventSubscriberId
	epochSubId dingoEvent.EventSubscriberId
	loopWg     sync.WaitGroup
}

// New creates an Indexer from cfg. Parses configured addresses and policy IDs
// eagerly so that Start() never fails for config reasons.
func New(cfg Config) (*Indexer, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	idx := &Indexer{
		config:       cfg,
		logger:       cfg.Logger,
		candidates:   make(map[candidateKey][]byte),
		currentEpoch: cfg.InitialEpoch,
	}

	if err := idx.parseConfig(); err != nil {
		return nil, fmt.Errorf("midnight indexer: %w", err)
	}

	return idx, nil
}

// parseConfig decodes bech32 addresses and hex policy IDs into raw bytes so
// block-event handlers can compare with a single bytes.Equal call.
func (idx *Indexer) parseConfig() error {
	type addrField struct {
		name string
		val  string
		dst  *[]byte
	}
	addrFields := []addrField{
		{"technical_committee_address", idx.config.TechnicalCommitteeAddress, &idx.techCommitteeAddrBytes},
		{"council_address", idx.config.CouncilAddress, &idx.councilAddrBytes},
		{"committee_candidate_address", idx.config.CommitteeCandidateAddress, &idx.candidateAddrBytes},
	}
	for _, f := range addrFields {
		if f.val == "" {
			continue
		}
		addr, err := gouroboros.NewAddress(f.val)
		if err != nil {
			return fmt.Errorf("parse %s %q: %w", f.name, f.val, err)
		}
		b, err := addr.Bytes()
		if err != nil {
			return fmt.Errorf("encode %s %q: %w", f.name, f.val, err)
		}
		*f.dst = b
	}

	type policyField struct {
		name string
		val  string
		dst  *[]byte
	}
	policyFields := []policyField{
		{"technical_committee_policy_id", idx.config.TechnicalCommitteePolicyID, &idx.techCommitteePolicyBytes},
		{"council_policy_id", idx.config.CouncilPolicyID, &idx.councilPolicyBytes},
		{"permissioned_candidate_policy", idx.config.PermissionedCandidatePolicy, &idx.permCandidatePolicyBytes},
	}
	for _, f := range policyFields {
		if f.val == "" {
			continue
		}
		b, err := hex.DecodeString(f.val)
		if err != nil {
			return fmt.Errorf("decode %s %q: %w", f.name, f.val, err)
		}
		*f.dst = b
	}
	return nil
}

// Start subscribes to block and epoch-transition events and begins indexing.
func (idx *Indexer) Start(ctx context.Context) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.running {
		return nil
	}
	if idx.stopping {
		return errors.New("midnight indexer: Stop in progress, cannot Start")
	}
	if ctx == nil {
		return errors.New("midnight indexer: nil context")
	}
	if idx.config.Store == nil {
		return errors.New("midnight indexer: nil store")
	}
	if idx.config.EventBus == nil {
		return errors.New("midnight indexer: nil event bus")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("midnight indexer: parent context already done: %w", err)
	}

	// Load the last Ariadne datum from DB so the first real datum in a
	// running session is correctly deduplicated against historical data.
	if latest, err := idx.config.Store.GetLatestMidnightAriadneParams(); err != nil {
		return fmt.Errorf("midnight indexer: load latest ariadne params: %w", err)
	} else if latest != nil {
		idx.lastAriadneDatum = latest.Datum
	}
	if idx.config.CommitteeCandidateAddress != "" {
		utxos, err := idx.config.Store.GetMidnightCandidates(
			idx.config.CommitteeCandidateAddress,
		)
		if err != nil {
			return fmt.Errorf("midnight indexer: load candidates: %w", err)
		}
		for _, utxo := range utxos {
			var key candidateKey
			copy(key.TxHash[:], utxo.TxId)
			key.OutputIndex = utxo.OutputIdx
			idx.candidates[key] = bytes.Clone(utxo.Datum)
		}
	}

	childCtx, cancel := context.WithCancel(ctx)
	idx.cancel = cancel
	idx.running = true

	var blockCh, epochCh <-chan dingoEvent.Event
	idx.blockSubId, blockCh = idx.config.EventBus.Subscribe(dingoLedger.BlockEventType)
	idx.epochSubId, epochCh = idx.config.EventBus.Subscribe(dingoEvent.EpochTransitionEventType)

	idx.loopWg.Go(func() { idx.blockLoop(childCtx, blockCh) })
	idx.loopWg.Go(func() { idx.epochLoop(childCtx, epochCh) })

	idx.logger.Info("midnight indexer started")
	return nil
}

// Stop drains in-flight events and shuts down the indexer.
func (idx *Indexer) Stop() {
	idx.mu.Lock()
	if !idx.running {
		idx.mu.Unlock()
		return
	}
	idx.stopping = true
	if idx.cancel != nil {
		idx.cancel()
	}
	if idx.blockSubId != 0 {
		idx.config.EventBus.Unsubscribe(dingoLedger.BlockEventType, idx.blockSubId)
		idx.blockSubId = 0
	}
	if idx.epochSubId != 0 {
		idx.config.EventBus.Unsubscribe(dingoEvent.EpochTransitionEventType, idx.epochSubId)
		idx.epochSubId = 0
	}
	idx.mu.Unlock()

	idx.loopWg.Wait()

	idx.mu.Lock()
	idx.running = false
	idx.stopping = false
	idx.mu.Unlock()

	idx.logger.Info("midnight indexer stopped")
}

// blockLoop drains the block-event channel until ctx is cancelled.
func (idx *Indexer) blockLoop(ctx context.Context, ch <-chan dingoEvent.Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			idx.handleBlockEvent(evt)
		}
	}
}

// epochLoop drains the epoch-transition channel until ctx is cancelled.
func (idx *Indexer) epochLoop(ctx context.Context, ch <-chan dingoEvent.Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			idx.handleEpochTransition(evt)
		}
	}
}

// handleBlockEvent processes a single BlockEvent. Only BlockActionApply is
// handled here; rollbacks are handled by #2116.
func (idx *Indexer) handleBlockEvent(evt dingoEvent.Event) {
	blockEvt, ok := evt.Data.(dingoLedger.BlockEvent)
	if !ok {
		return
	}
	if blockEvt.Action != dingoLedger.BlockActionApply {
		return
	}

	blk, err := blockEvt.Block.Decode()
	if err != nil {
		idx.logger.Error(
			"midnight indexer: failed to decode block",
			"slot", blockEvt.Block.Slot,
			"error", err,
		)
		return
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	for txIdx, tx := range blk.Transactions() {
		_ = txIdx
		txHash := tx.Hash()
		for _, utxo := range tx.Produced() {
			idx.processOutput(blockEvt.Block.Number, txHash.Bytes(), utxo.Id.Index(), utxo.Output)
		}
		for _, input := range tx.Consumed() {
			idx.removeCandidate(input.Id().Bytes(), input.Index())
		}
	}
}

// processOutput applies all three scan flows (governance, Ariadne, candidate)
// to a single transaction output.
func (idx *Indexer) processOutput(
	blockNumber uint64,
	txHash []byte,
	outputIndex uint32,
	output gouroboros.TransactionOutput,
) {
	addrBytes, err := output.Address().Bytes()
	if err != nil {
		// Non-parseable address — skip silently; this can happen for
		// exotic address types that are not relevant to midnight.
		return
	}

	datum := output.Datum()
	var datumCbor []byte
	if datum != nil {
		datumCbor = datum.Cbor()
		if len(datumCbor) == 0 {
			// Datum present but empty — still proceed; downstream writes
			// will store an empty slice which is valid.
			datumCbor = nil
		}
	}

	// Governance scan: Technical Committee
	if len(idx.techCommitteeAddrBytes) > 0 &&
		bytes.Equal(addrBytes, idx.techCommitteeAddrBytes) &&
		idx.outputHasPolicy(output, idx.techCommitteePolicyBytes) &&
		datumCbor != nil {
		if err := idx.config.Store.InsertMidnightGovernanceDatum(
			&models.MidnightGovernanceDatum{
				DatumType:   models.MidnightGovernanceDatumTypeTechnicalCommittee,
				Datum:       datumCbor,
				BlockNumber: blockNumber,
			},
		); err != nil {
			idx.logger.Error(
				"midnight indexer: insert technical committee datum",
				"block", blockNumber,
				"error", err,
			)
		}
	}

	// Governance scan: Council
	if len(idx.councilAddrBytes) > 0 &&
		bytes.Equal(addrBytes, idx.councilAddrBytes) &&
		idx.outputHasPolicy(output, idx.councilPolicyBytes) &&
		datumCbor != nil {
		if err := idx.config.Store.InsertMidnightGovernanceDatum(
			&models.MidnightGovernanceDatum{
				DatumType:   models.MidnightGovernanceDatumTypeCouncil,
				Datum:       datumCbor,
				BlockNumber: blockNumber,
			},
		); err != nil {
			idx.logger.Error(
				"midnight indexer: insert council datum",
				"block", blockNumber,
				"error", err,
			)
		}
	}

	// Ariadne scan: permissioned-candidate token outputs
	if len(idx.permCandidatePolicyBytes) > 0 &&
		idx.outputHasPolicy(output, idx.permCandidatePolicyBytes) &&
		datumCbor != nil {
		if !bytes.Equal(datumCbor, idx.lastAriadneDatum) {
			epoch := idx.currentEpoch
			if err := idx.config.Store.UpsertMidnightAriadneParams(
				&models.MidnightAriadneParams{
					Epoch: epoch,
					Datum: datumCbor,
				},
			); err != nil {
				idx.logger.Error(
					"midnight indexer: upsert ariadne params",
					"epoch", epoch,
					"error", err,
				)
			} else {
				idx.lastAriadneDatum = datumCbor
			}
		}
	}

	// Committee-candidate tracking: add UTxO to the in-memory set
	if len(idx.candidateAddrBytes) > 0 &&
		bytes.Equal(addrBytes, idx.candidateAddrBytes) {
		var key candidateKey
		copy(key.TxHash[:], txHash)
		key.OutputIndex = outputIndex
		idx.candidates[key] = datumCbor
	}
}

// removeCandidate removes a UTxO from the in-memory candidate set when it is
// consumed by a transaction.
func (idx *Indexer) removeCandidate(txHashBytes []byte, outputIndex uint32) {
	var key candidateKey
	copy(key.TxHash[:], txHashBytes)
	key.OutputIndex = outputIndex
	delete(idx.candidates, key)
}

// outputHasPolicy reports whether output carries any asset under policyID.
func (idx *Indexer) outputHasPolicy(
	output gouroboros.TransactionOutput,
	policyID []byte,
) bool {
	if len(policyID) == 0 {
		return false
	}
	assets := output.Assets()
	if assets == nil {
		return false
	}
	for _, p := range assets.Policies() {
		if bytes.Equal(p.Bytes(), policyID) {
			return true
		}
	}
	return false
}

// handleEpochTransition snapshots the in-memory candidate set to the database
// and advances the tracked epoch.
func (idx *Indexer) handleEpochTransition(evt dingoEvent.Event) {
	epochEvt, ok := evt.Data.(dingoEvent.EpochTransitionEvent)
	if !ok {
		return
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Snapshot the current candidate set for the epoch that just ended.
	entries := make([]candidateEntry, 0, len(idx.candidates))
	for k, datum := range idx.candidates {
		hashCopy := make([]byte, 32)
		copy(hashCopy, k.TxHash[:])
		entries = append(entries, candidateEntry{
			TxHash:      hashCopy,
			OutputIndex: k.OutputIndex,
			Datum:       datum,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		if cmp := bytes.Compare(entries[i].TxHash, entries[j].TxHash); cmp != 0 {
			return cmp < 0
		}
		return entries[i].OutputIndex < entries[j].OutputIndex
	})
	snapshotCbor, err := fxcbor.Marshal(entries)
	if err != nil {
		idx.logger.Error(
			"midnight indexer: encode candidate snapshot",
			"epoch", epochEvt.PreviousEpoch,
			"error", err,
		)
	} else {
		if err := idx.config.Store.UpsertMidnightEpochCandidates(
			&models.MidnightEpochCandidates{
				Epoch:          epochEvt.PreviousEpoch,
				CandidatesCbor: snapshotCbor,
			},
		); err != nil {
			idx.logger.Error(
				"midnight indexer: upsert epoch candidates",
				"epoch", epochEvt.PreviousEpoch,
				"error", err,
			)
		}
	}

	idx.currentEpoch = epochEvt.NewEpoch
}
