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

// Package indexer subscribes to ledger block events and indexes
// Midnight-relevant transactions (cNIGHT creates/spends and mapping-validator
// registrations/deregistrations) into the database.
package indexer

import (
	"encoding/hex"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// SlotTimer converts a slot number to a wall-clock time.
type SlotTimer interface {
	SlotToTime(slot uint64) (time.Time, error)
}

// Config holds the Indexer configuration.
type Config struct {
	EventBus  *event.EventBus
	Metadata  metadata.MetadataStore
	SlotTimer SlotTimer
	Logger    *slog.Logger
	// Midnight chain parameters
	CNightPolicyID          string
	CNightAssetName         string
	MappingValidatorAddress string
	// AuthTokenPolicyID scopes auth-token matching to a specific policy.
	// When empty, any policy carrying AuthTokenAssetName is accepted.
	AuthTokenPolicyID  string
	AuthTokenAssetName string
}

// utxoKey identifies a UTxO by tx-hash (hex) and output index.
type utxoKey struct {
	TxHash string
	Index  uint32
}

// cNightUTxO holds the on-chain data needed to write a spend row.
type cNightUTxO struct {
	Address  []byte
	Quantity uint64
}

// registrationUTxO holds the inline datum of a registration output.
type registrationUTxO struct {
	FullDatum []byte
}

// Indexer scans blocks for Midnight-relevant transactions and writes rows
// to the midnight_* tables.
type Indexer struct {
	config Config
	mu     sync.RWMutex
	// In-memory tracked UTxO sets; keyed by (tx_hash_hex, output_index).
	cNightUTxOs map[utxoKey]cNightUTxO
	regUTxOs    map[utxoKey]registrationUTxO
	// Parsed cNIGHT asset parameters; valid only when cnightEnabled is true.
	cnightPolicyID  lcommon.Blake2b224
	cnightAssetName []byte
	cnightEnabled   bool
	// Parsed auth token parameters; valid only when authEnabled is true.
	// authPolicyID is the zero value (all-zeros) when not configured, in
	// which case hasAuthToken matches any policy.
	authPolicyID  lcommon.Blake2b224
	authPolicySet bool
	authAssetName []byte
	authEnabled   bool
	// Event bus subscription identifier returned by Start.
	subID event.EventSubscriberId
}

// New creates and initialises a new Indexer. It parses the configured policy
// IDs and asset names, then loads the existing unspent UTxO sets from the
// database so that spends arriving in the first block after restart are
// matched correctly.
func New(cfg Config) (*Indexer, error) {
	idx := &Indexer{
		config:      cfg,
		cNightUTxOs: make(map[utxoKey]cNightUTxO),
		regUTxOs:    make(map[utxoKey]registrationUTxO),
	}

	if cfg.CNightPolicyID != "" && cfg.CNightAssetName != "" {
		policyBytes, err := hex.DecodeString(cfg.CNightPolicyID)
		if err != nil {
			return nil, fmt.Errorf("invalid cnight_policy_id: %w", err)
		}
		if len(policyBytes) != 28 {
			return nil, fmt.Errorf(
				"invalid cnight_policy_id: must be 56 hex characters (28 bytes), got %d bytes",
				len(policyBytes),
			)
		}
		assetNameBytes, err := hex.DecodeString(cfg.CNightAssetName)
		if err != nil {
			return nil, fmt.Errorf("invalid cnight_asset_name: %w", err)
		}
		idx.cnightPolicyID = lcommon.NewBlake2b224(policyBytes)
		idx.cnightAssetName = assetNameBytes
		idx.cnightEnabled = true
	}

	if cfg.AuthTokenAssetName != "" {
		assetNameBytes, err := hex.DecodeString(cfg.AuthTokenAssetName)
		if err != nil {
			return nil, fmt.Errorf("invalid auth_token_asset_name: %w", err)
		}
		idx.authAssetName = assetNameBytes
		idx.authEnabled = true
		if cfg.AuthTokenPolicyID != "" {
			policyBytes, err := hex.DecodeString(cfg.AuthTokenPolicyID)
			if err != nil {
				return nil, fmt.Errorf("invalid auth_token_policy_id: %w", err)
			}
			if len(policyBytes) != 28 {
				return nil, fmt.Errorf(
					"invalid auth_token_policy_id: must be 56 hex characters (28 bytes), got %d bytes",
					len(policyBytes),
				)
			}
			idx.authPolicyID = lcommon.NewBlake2b224(policyBytes)
			idx.authPolicySet = true
		}
	}

	if err := idx.loadTrackedUTxOs(); err != nil {
		return nil, fmt.Errorf("midnight indexer: loading tracked utxos: %w", err)
	}
	return idx, nil
}

// loadTrackedUTxOs populates the in-memory UTxO sets from the database.
func (idx *Indexer) loadTrackedUTxOs() error {
	if idx.cnightEnabled {
		creates, err := idx.config.Metadata.FindUnspentMidnightAssetCreates()
		if err != nil {
			return fmt.Errorf("querying unspent cnight utxos: %w", err)
		}
		for _, c := range creates {
			key := utxoKey{
				TxHash: hex.EncodeToString(c.TxHash),
				Index:  c.OutputIndex,
			}
			idx.cNightUTxOs[key] = cNightUTxO{
				Address:  c.Address,
				Quantity: c.Quantity,
			}
		}
	}

	if idx.config.MappingValidatorAddress != "" {
		regs, err := idx.config.Metadata.FindUnspentMidnightRegistrations()
		if err != nil {
			return fmt.Errorf("querying unspent registrations: %w", err)
		}
		for _, r := range regs {
			key := utxoKey{
				TxHash: hex.EncodeToString(r.TxHash),
				Index:  r.OutputIndex,
			}
			idx.regUTxOs[key] = registrationUTxO{FullDatum: r.FullDatum}
		}
	}
	return nil
}

// Start subscribes to ledger block events. The indexer begins processing
// blocks immediately after Start returns.
func (idx *Indexer) Start() {
	idx.subID = idx.config.EventBus.SubscribeFuncWithBuffer(
		ledger.BlockEventType,
		event.EventQueueSize,
		idx.handleBlockEvent,
	)
}

// Stop unsubscribes from block events.
func (idx *Indexer) Stop() {
	idx.config.EventBus.Unsubscribe(ledger.BlockEventType, idx.subID)
}

// handleBlockEvent is the SubscribeFunc callback; it decodes the block and
// delegates to processBlock or rollbackBlock depending on the action.
func (idx *Indexer) handleBlockEvent(evt event.Event) {
	blockEvt, ok := evt.Data.(ledger.BlockEvent)
	if !ok {
		return
	}
	switch blockEvt.Action {
	case ledger.BlockActionUndo:
		idx.rollbackBlock(blockEvt.Block)
	case ledger.BlockActionApply:
		block := blockEvt.Block
		decoded, err := block.Decode()
		if err != nil {
			if idx.config.Logger != nil {
				idx.config.Logger.Error(
					"midnight indexer: failed to decode block",
					"error", err,
					"slot", block.Slot,
				)
			}
			return
		}
		var timestampMs uint64
		if idx.config.SlotTimer != nil {
			if t, err := idx.config.SlotTimer.SlotToTime(block.Slot); err == nil {
				timestampMs = uint64(t.UnixMilli()) //nolint:gosec
			}
		}
		idx.processBlock(block, decoded.Transactions(), timestampMs)
	}
}

// rollbackBlock undoes all midnight_* rows written for the given block and
// restores the in-memory UTxO tracking sets to their pre-block state.
// Order: undo spends/deregistrations first (restore UTxOs), then undo
// creates/registrations (remove UTxOs), so a UTxO created and spent within
// the same block ends up correctly absent from memory after the rollback.
func (idx *Indexer) rollbackBlock(block models.Block) {
	if idx.cnightEnabled {
		spends, err := idx.config.Metadata.DeleteMidnightAssetSpendsByBlock(nil, block.Number)
		if err != nil && idx.config.Logger != nil {
			idx.config.Logger.Error(
				"midnight indexer: rollback asset spends",
				"error", err,
				"block", block.Number,
			)
		}
		if len(spends) > 0 {
			idx.mu.Lock()
			for _, s := range spends {
				key := utxoKey{TxHash: hex.EncodeToString(s.UtxoTxHash), Index: s.UtxoIndex}
				idx.cNightUTxOs[key] = cNightUTxO{Address: s.Address, Quantity: s.Quantity}
			}
			idx.mu.Unlock()
		}

		creates, err := idx.config.Metadata.DeleteMidnightAssetCreatesByBlock(nil, block.Number)
		if err != nil && idx.config.Logger != nil {
			idx.config.Logger.Error(
				"midnight indexer: rollback asset creates",
				"error", err,
				"block", block.Number,
			)
		}
		if len(creates) > 0 {
			idx.mu.Lock()
			for _, c := range creates {
				delete(idx.cNightUTxOs, utxoKey{TxHash: hex.EncodeToString(c.TxHash), Index: c.OutputIndex})
			}
			idx.mu.Unlock()
		}
	}

	if idx.config.MappingValidatorAddress != "" {
		deregs, err := idx.config.Metadata.DeleteMidnightDeregistrationsByBlock(nil, block.Number)
		if err != nil && idx.config.Logger != nil {
			idx.config.Logger.Error(
				"midnight indexer: rollback deregistrations",
				"error", err,
				"block", block.Number,
			)
		}
		if len(deregs) > 0 {
			idx.mu.Lock()
			for _, d := range deregs {
				key := utxoKey{TxHash: hex.EncodeToString(d.UtxoTxHash), Index: d.UtxoIndex}
				idx.regUTxOs[key] = registrationUTxO{FullDatum: d.FullDatum}
			}
			idx.mu.Unlock()
		}

		regs, err := idx.config.Metadata.DeleteMidnightRegistrationsByBlock(nil, block.Number)
		if err != nil && idx.config.Logger != nil {
			idx.config.Logger.Error(
				"midnight indexer: rollback registrations",
				"error", err,
				"block", block.Number,
			)
		}
		if len(regs) > 0 {
			idx.mu.Lock()
			for _, r := range regs {
				delete(idx.regUTxOs, utxoKey{TxHash: hex.EncodeToString(r.TxHash), Index: r.OutputIndex})
			}
			idx.mu.Unlock()
		}
	}
}

// processBlock iterates the transactions in a block and calls processTx for
// each one. Exposed for direct use in tests.
func (idx *Indexer) processBlock(
	block models.Block,
	txs []lcommon.Transaction,
	timestampMs uint64,
) {
	for i, tx := range txs {
		idx.processTx(block, tx, uint32(i), timestampMs) //nolint:gosec
	}
}

// processTx scans a single transaction's inputs and outputs.
func (idx *Indexer) processTx(
	block models.Block,
	tx lcommon.Transaction,
	txIdx uint32,
	timestampMs uint64,
) {
	txHashBytes := tx.Id().Bytes()

	// Scan inputs for spends of tracked UTxOs.
	// We peek with a read-lock, write the DB row, then remove from memory
	// only on success so that a transient write failure leaves the in-memory
	// state intact and the UTxO reloadable from the DB on restart.
	for _, inp := range tx.Inputs() {
		inpHashHex := hex.EncodeToString(inp.Id().Bytes())
		inpIndex := inp.Index()
		key := utxoKey{TxHash: inpHashHex, Index: inpIndex}

		idx.mu.RLock()
		utxo, isCNight := idx.cNightUTxOs[key]
		reg, isReg := idx.regUTxOs[key]
		idx.mu.RUnlock()

		if isCNight {
			row := &models.MidnightAssetSpend{
				Address:          utxo.Address,
				Quantity:         utxo.Quantity,
				SpendingTxHash:   txHashBytes,
				UtxoTxHash:       inp.Id().Bytes(),
				UtxoIndex:        inpIndex,
				BlockNumber:      block.Number,
				BlockHash:        block.Hash,
				TxIndex:          txIdx,
				BlockTimestampMs: timestampMs,
			}
			if err := idx.config.Metadata.CreateMidnightAssetSpend(nil, row); err != nil {
				if idx.config.Logger != nil {
					idx.config.Logger.Error(
						"midnight indexer: write asset spend",
						"error", err,
						"tx", hex.EncodeToString(txHashBytes),
					)
				}
			} else {
				idx.mu.Lock()
				delete(idx.cNightUTxOs, key)
				idx.mu.Unlock()
			}
		}

		if isReg {
			row := &models.MidnightDeregistration{
				FullDatum:        reg.FullDatum,
				TxHash:           txHashBytes,
				UtxoTxHash:       inp.Id().Bytes(),
				UtxoIndex:        inpIndex,
				BlockNumber:      block.Number,
				BlockHash:        block.Hash,
				TxIndex:          txIdx,
				BlockTimestampMs: timestampMs,
			}
			if err := idx.config.Metadata.CreateMidnightDeregistration(nil, row); err != nil {
				if idx.config.Logger != nil {
					idx.config.Logger.Error(
						"midnight indexer: write deregistration",
						"error", err,
						"tx", hex.EncodeToString(txHashBytes),
					)
				}
			} else {
				idx.mu.Lock()
				delete(idx.regUTxOs, key)
				idx.mu.Unlock()
			}
		}
	}

	// Scan outputs for cNIGHT creates and registration outputs.
	for outIdx, out := range tx.Outputs() {
		idx.processOutput(
			block,
			txHashBytes,
			uint32(outIdx), //nolint:gosec
			txIdx,
			timestampMs,
			out,
		)
	}
}

// processOutput checks a single transaction output for cNIGHT tokens and
// registration auth tokens.
func (idx *Indexer) processOutput(
	block models.Block,
	txHashBytes []byte,
	outIdx uint32,
	txIdx uint32,
	timestampMs uint64,
	out lcommon.TransactionOutput,
) {
	txHashHex := hex.EncodeToString(txHashBytes)
	key := utxoKey{TxHash: txHashHex, Index: outIdx}

	// cNIGHT create scan.
	if idx.cnightEnabled {
		if assets := out.Assets(); assets != nil {
			qty := assets.Asset(idx.cnightPolicyID, idx.cnightAssetName)
			if qty != nil && qty.Cmp(new(big.Int)) > 0 {
				addrBytes, _ := out.Address().Bytes()
				quantity := qty.Uint64()
				row := &models.MidnightAssetCreate{
					Address:          addrBytes,
					Quantity:         quantity,
					TxHash:           txHashBytes,
					OutputIndex:      outIdx,
					BlockNumber:      block.Number,
					BlockHash:        block.Hash,
					TxIndex:          txIdx,
					BlockTimestampMs: timestampMs,
				}
				if err := idx.config.Metadata.CreateMidnightAssetCreate(nil, row); err != nil {
					if idx.config.Logger != nil {
						idx.config.Logger.Error(
							"midnight indexer: write asset create",
							"error", err,
							"tx", txHashHex,
						)
					}
					// Don't track in-memory if the DB write failed; we'd lose
					// the spend pairing without the persisted create row.
					return
				}
				idx.mu.Lock()
				idx.cNightUTxOs[key] = cNightUTxO{
					Address:  addrBytes,
					Quantity: quantity,
				}
				idx.mu.Unlock()
			}
		}
	}

	// Registration scan: output at mapping_validator_address containing
	// an auth token with the configured asset name.
	if idx.config.MappingValidatorAddress == "" {
		return
	}
	addrStr := out.Address().String()
	if addrStr != idx.config.MappingValidatorAddress {
		return
	}
	if !idx.hasAuthToken(out) {
		return
	}
	datum := out.Datum()
	if datum == nil {
		return
	}
	datumCbor := datum.Cbor()
	if len(datumCbor) == 0 {
		return
	}
	row := &models.MidnightRegistration{
		FullDatum:        datumCbor,
		TxHash:           txHashBytes,
		OutputIndex:      outIdx,
		BlockNumber:      block.Number,
		BlockHash:        block.Hash,
		TxIndex:          txIdx,
		BlockTimestampMs: timestampMs,
	}
	if err := idx.config.Metadata.CreateMidnightRegistration(nil, row); err != nil {
		if idx.config.Logger != nil {
			idx.config.Logger.Error(
				"midnight indexer: write registration",
				"error", err,
				"tx", txHashHex,
			)
		}
		return
	}
	idx.mu.Lock()
	idx.regUTxOs[key] = registrationUTxO{FullDatum: datumCbor}
	idx.mu.Unlock()
}

// hasAuthToken returns true if the output assets contain at least one unit of
// the configured auth token. When AuthTokenPolicyID is set the check is
// policy-scoped; otherwise any policy carrying the asset name is accepted.
func (idx *Indexer) hasAuthToken(out lcommon.TransactionOutput) bool {
	if !idx.authEnabled {
		return false
	}
	assets := out.Assets()
	if assets == nil {
		return false
	}
	if idx.authPolicySet {
		qty := assets.Asset(idx.authPolicyID, idx.authAssetName)
		return qty != nil && qty.Cmp(new(big.Int)) > 0
	}
	for _, policyID := range assets.Policies() {
		for _, name := range assets.Assets(policyID) {
			if string(name) == string(idx.authAssetName) {
				qty := assets.Asset(policyID, name)
				if qty != nil && qty.Cmp(new(big.Int)) > 0 {
					return true
				}
			}
		}
	}
	return false
}
