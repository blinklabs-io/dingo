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
// Midnight-relevant transactions (cNIGHT creates/spends, mapping-validator
// registrations/deregistrations, Technical Committee / Council governance
// datums, Ariadne permissioned-candidate parameters, and committee-candidate
// UTxO snapshots) into the database.
package indexer

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	fxcbor "github.com/fxamacker/cbor/v2"
)

const midnightCheckpointPhase = "midnight"

// candidateRollbackDepth is the Cardano security parameter k: the maximum
// number of blocks that Ouroboros can roll back. candidateRemovals journal
// entries older than this depth are pruned after each block to bound memory.
const candidateRollbackDepth uint64 = 2160

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
	// BlockIterator iterates stored blocks in [startSlot, endSlot).
	// When non-nil, Start() runs a backfill pass before subscribing to live
	// events, processing any blocks whose slot is >= the last checkpoint slot.
	// Node.go provides this via database.ForEachBlockInRangeDB.
	BlockIterator func(startSlot, endSlot uint64, fn func(models.Block) error) error
	blockDecoder  func(models.Block) ([]lcommon.Transaction, error)
	// FatalErrorFunc is called when the indexer encounters an error that
	// cannot be recovered without risking a checkpoint gap. Node wiring
	// should cancel the node context so the process exits cleanly.
	FatalErrorFunc func(error)
	// Governance/Ariadne/Candidate scanning
	TechnicalCommitteeAddress   string
	TechnicalCommitteePolicyID  string
	CouncilAddress              string
	CouncilPolicyID             string
	PermissionedCandidatePolicy string
	CommitteeCandidateAddress   string
	SlotToEpoch                 func(uint64) (uint64, error)
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
	// checkpointSlot is the slot of the last block fully processed by
	// backfill or by a live event handler. Persisted via BackfillCheckpoint.
	checkpointSlot uint64
	// Event bus subscription identifier returned by Start.
	subID event.EventSubscriberId

	// governance parsed config
	techCommitteeAddrBytes   []byte
	councilAddrBytes         []byte
	techCommitteePolicyBytes []byte
	councilPolicyBytes       []byte
	permCandidatePolicyBytes []byte
	candidateAddrBytes       []byte
	candidateAddr            lcommon.Address // parsed address for DB queries

	// governance state
	candidates        map[candidateKey][]byte
	candidateRemovals map[uint64]map[candidateKey][]byte
	// epochTransitions journals the currentEpoch value *before* each block that
	// caused an epoch advance via advanceEpochLocked.  On rollback the entry
	// lets us restore currentEpoch to the pre-advance value so that a
	// re-applied block at the same slot is assigned the correct epoch instead
	// of the post-transition one.  Entries are pruned beyond candidateRollbackDepth.
	epochTransitions map[uint64]uint64 // blockNumber to prevCurrentEpoch
	lastAriadneDatum []byte
	currentEpoch     uint64
	hasCurrentEpoch  bool
	snapshotEpoch    uint64
	hasSnapshotEpoch bool
}

// New creates and initialises a new Indexer. It parses the configured policy
// IDs and asset names, then loads the existing unspent UTxO sets from the
// database so that spends arriving in the first block after restart are
// matched correctly.
func New(cfg Config) (*Indexer, error) {
	idx := &Indexer{
		config:            cfg,
		cNightUTxOs:       make(map[utxoKey]cNightUTxO),
		regUTxOs:          make(map[utxoKey]registrationUTxO),
		candidates:        make(map[candidateKey][]byte),
		candidateRemovals: make(map[uint64]map[candidateKey][]byte),
		epochTransitions:  make(map[uint64]uint64),
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

	// Initialise governance state; parseGovernanceConfig populates the
	// address/policy byte slices.
	if err := idx.parseGovernanceConfig(); err != nil {
		return nil, err
	}

	if err := idx.loadTrackedUTxOs(); err != nil {
		return nil, fmt.Errorf("midnight indexer: loading tracked utxos: %w", err)
	}
	return idx, nil
}

// parseGovernanceConfig decodes bech32 addresses and hex policy IDs for the
// governance/Ariadne/candidate scanning paths.
func (idx *Indexer) parseGovernanceConfig() error {
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
		addr, err := lcommon.NewAddress(f.val)
		if err != nil {
			return fmt.Errorf("midnight indexer: parse %s %q: %w", f.name, f.val, err)
		}
		b, err := addr.Bytes()
		if err != nil {
			return fmt.Errorf("midnight indexer: encode %s %q: %w", f.name, f.val, err)
		}
		*f.dst = b
		if f.name == "committee_candidate_address" {
			idx.candidateAddr = addr
		}
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
			return fmt.Errorf("midnight indexer: decode %s %q: %w", f.name, f.val, err)
		}
		*f.dst = b
	}
	return nil
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

	// Load candidate UTxOs from DB if the candidate address is configured.
	if idx.config.CommitteeCandidateAddress != "" {
		utxos, err := idx.config.Metadata.GetMidnightCandidates(idx.candidateAddr, nil)
		if err != nil {
			return fmt.Errorf("querying midnight candidates: %w", err)
		}
		for _, utxo := range utxos {
			var key candidateKey
			copy(key.TxHash[:], utxo.TxId)
			key.OutputIndex = utxo.OutputIdx
			idx.candidates[key] = bytes.Clone(utxo.Datum)
		}
	}

	// Seed lastAriadneDatum so the first real Ariadne datum in a running
	// session is correctly deduplicated against historical data.
	if idx.config.PermissionedCandidatePolicy != "" {
		latest, err := idx.config.Metadata.GetLatestMidnightAriadneParams(nil)
		if err != nil {
			return fmt.Errorf("loading latest ariadne params: %w", err)
		}
		if latest != nil {
			idx.lastAriadneDatum = latest.Datum
		}
	}

	cp, err := idx.config.Metadata.GetBackfillCheckpoint(midnightCheckpointPhase, nil)
	if err != nil {
		return fmt.Errorf("loading midnight checkpoint: %w", err)
	}
	if cp != nil {
		idx.checkpointSlot = cp.LastSlot
	}
	return nil
}

// Subscribe registers the EventBus handlers for live block and epoch events.
// It must be called before LedgerState.Start so that no event is missed.
// If Backfill is called before Subscribe, no ledger block producer should be
// running yet.
func (idx *Indexer) Subscribe() {
	idx.subID = idx.config.EventBus.SubscribeFuncWithBuffer(
		ledger.BlockEventType,
		event.EventQueueSize,
		idx.handleBlockEvent,
	)
}

// Backfill replays all stored blocks from the last checkpoint through the
// database. The configured SlotToEpoch resolver must already be usable before
// this method is called.
func (idx *Indexer) Backfill() error {
	if idx.config.BlockIterator == nil {
		return nil
	}
	return idx.backfill()
}

// Start runs catch-up backfill and then subscribes to live events. Callers must
// ensure SlotToEpoch is ready and that ledger block processing has not started
// yet, so there is no overlap between backfill and live events.
func (idx *Indexer) Start() error {
	if err := idx.Backfill(); err != nil {
		return err
	}
	idx.Subscribe()
	return nil
}

// backfill iterates all blocks stored in the database starting from the last
// checkpoint slot and processes each one through the midnight indexer.
func (idx *Indexer) backfill() error {
	return idx.config.BlockIterator(
		idx.checkpointSlot,
		math.MaxUint64,
		func(block models.Block) error {
			var txs []lcommon.Transaction
			if idx.config.blockDecoder != nil {
				decodedTxs, err := idx.config.blockDecoder(block)
				if err != nil {
					return fmt.Errorf(
						"midnight indexer: backfill: decode block slot=%d block=%d: %w",
						block.Slot, block.Number, err,
					)
				}
				txs = decodedTxs
			} else {
				decoded, err := block.Decode()
				if err != nil {
					if idx.config.Logger != nil {
						idx.config.Logger.Warn(
							"midnight indexer: backfill: skipping undecodable block",
							"slot", block.Slot,
							"error", err,
						)
					}
					return nil
				}
				txs = decoded.Transactions()
			}
			var timestampMs uint64
			if idx.config.SlotTimer != nil {
				if t, tErr := idx.config.SlotTimer.SlotToTime(block.Slot); tErr == nil {
					timestampMs = uint64(t.UnixMilli()) //nolint:gosec
				}
			}
			if err := idx.processBlock(block, txs, timestampMs); err != nil {
				return fmt.Errorf(
					"midnight indexer: backfill: process block slot=%d block=%d: %w",
					block.Slot, block.Number, err,
				)
			}
			if err := idx.updateCheckpoint(block.Slot); err != nil {
				return fmt.Errorf(
					"midnight indexer: backfill: checkpoint slot=%d block=%d: %w",
					block.Slot, block.Number, err,
				)
			}
			return nil
		},
	)
}

// updateCheckpoint persists the last-processed slot to the metadata store.
func (idx *Indexer) updateCheckpoint(slot uint64) error {
	cp := &models.BackfillCheckpoint{
		Phase:    midnightCheckpointPhase,
		LastSlot: slot,
	}
	if err := idx.config.Metadata.SetBackfillCheckpoint(cp, nil); err != nil {
		return err
	}
	idx.checkpointSlot = slot
	return nil
}

// Stop unsubscribes from block events.
func (idx *Indexer) Stop() {
	idx.config.EventBus.Unsubscribe(ledger.BlockEventType, idx.subID)
}

// fatal logs err and forwards it to FatalErrorFunc (typically node cancel).
// Called when the indexer cannot continue without risking a checkpoint gap.
func (idx *Indexer) fatal(err error) {
	if idx.config.Logger != nil {
		idx.config.Logger.Error("midnight indexer fatal error", "error", err)
	}
	if idx.config.FatalErrorFunc != nil {
		idx.config.FatalErrorFunc(err)
	}
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
			idx.fatal(fmt.Errorf(
				"midnight indexer: decode block slot=%d block=%d: %w",
				block.Slot, block.Number, err,
			))
			return
		}
		var timestampMs uint64
		if idx.config.SlotTimer != nil {
			if t, err := idx.config.SlotTimer.SlotToTime(block.Slot); err == nil {
				timestampMs = uint64(t.UnixMilli()) //nolint:gosec
			}
		}
		// Advance epoch context before scanning this block.
		if idx.config.SlotToEpoch != nil {
			epoch, err := idx.config.SlotToEpoch(block.Slot)
			if err != nil {
				if idx.config.Logger != nil {
					idx.config.Logger.Error(
						"midnight indexer: resolve block epoch",
						"slot", block.Slot,
						"error", err,
					)
				}
			} else {
				idx.mu.Lock()
				idx.advanceEpochLocked(epoch, block.Number)
				idx.mu.Unlock()
			}
		}
		if err := idx.processBlock(block, decoded.Transactions(), timestampMs); err != nil {
			idx.fatal(fmt.Errorf(
				"midnight indexer: process block slot=%d block=%d: %w",
				block.Slot, block.Number, err,
			))
			return
		}
		// Persist checkpoint only after all writes for the block succeeded.
		if idx.config.BlockIterator != nil {
			if err := idx.updateCheckpoint(block.Slot); err != nil {
				idx.fatal(fmt.Errorf(
					"midnight indexer: checkpoint slot=%d block=%d: %w",
					block.Slot, block.Number, err,
				))
				return
			}
		}
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

	if len(idx.techCommitteeAddrBytes) > 0 || len(idx.councilAddrBytes) > 0 {
		if err := idx.config.Metadata.DeleteMidnightGovernanceDatumsByBlock(nil, block.Number); err != nil {
			if idx.config.Logger != nil {
				idx.config.Logger.Error(
					"midnight indexer: rollback governance datums",
					"error", err,
					"block", block.Number,
				)
			}
		}
	}

	if len(idx.permCandidatePolicyBytes) > 0 {
		idx.rollbackAriadne(block.Number)
	}

	if len(idx.candidateAddrBytes) > 0 {
		// Snapshot cleanup and spend-journal restore only need block.Number;
		// do both before decoding so a decode error leaves neither stale DB
		// rows nor unrestored in-memory candidates.
		idx.rollbackCandidateSnapshots(block)
		idx.rollbackCandidateSpends(block.Number)
		decoded, err := block.Decode()
		if err != nil {
			if idx.config.Logger != nil {
				idx.config.Logger.Error(
					"midnight indexer: rollback candidate decode block",
					"error", err,
					"block", block.Number,
				)
			}
		} else {
			idx.rollbackCandidateCreates(decoded.Transactions())
		}
	}

	// Restore currentEpoch if this block caused an epoch advance.
	// Without this, a re-applied block at the same slot would see
	// epoch <= currentEpoch and be assigned the wrong (post-transition) epoch.
	idx.mu.Lock()
	if prevEpoch, ok := idx.epochTransitions[block.Number]; ok {
		idx.currentEpoch = prevEpoch
		delete(idx.epochTransitions, block.Number)
	}
	idx.mu.Unlock()
}

func (idx *Indexer) rollbackCandidateSnapshots(block models.Block) {
	if err := idx.config.Metadata.DeleteMidnightEpochCandidatesByBlock(nil, block.Number); err != nil {
		if idx.config.Logger != nil {
			idx.config.Logger.Error(
				"midnight indexer: rollback epoch candidate snapshots",
				"error", err,
				"block", block.Number,
			)
		}
		return
	}

	idx.mu.Lock()
	idx.hasSnapshotEpoch = false
	idx.snapshotEpoch = 0
	idx.mu.Unlock()
}

func (idx *Indexer) rollbackAriadne(blockNumber uint64) {
	entries, err := idx.config.Metadata.FindMidnightAriadneRollbacksByBlock(nil, blockNumber)
	if err != nil {
		if idx.config.Logger != nil {
			idx.config.Logger.Error(
				"midnight indexer: load ariadne rollback journal",
				"error", err,
				"block", blockNumber,
			)
		}
		return
	}

	hasError := false
	for _, entry := range entries {
		var err error
		if entry.PreviousExists {
			err = idx.config.Metadata.UpsertMidnightAriadneParams(
				nil,
				&models.MidnightAriadneParams{
					Epoch: entry.Epoch,
					Datum: bytes.Clone(entry.PreviousDatum),
				},
			)
		} else {
			err = idx.config.Metadata.DeleteMidnightAriadneParamsByEpoch(nil, entry.Epoch)
		}
		if err != nil {
			hasError = true
			if idx.config.Logger != nil {
				idx.config.Logger.Error(
					"midnight indexer: rollback ariadne params",
					"error", err,
					"block", blockNumber,
					"epoch", entry.Epoch,
				)
			}
			continue
		}
	}
	if !hasError {
		if err := idx.config.Metadata.DeleteMidnightAriadneRollbacksByBlock(nil, blockNumber); err != nil &&
			idx.config.Logger != nil {
			idx.config.Logger.Error(
				"midnight indexer: delete ariadne rollback journal",
				"error", err,
				"block", blockNumber,
			)
		}
	}

	latest, err := idx.config.Metadata.GetLatestMidnightAriadneParams(nil)
	if err != nil {
		if idx.config.Logger != nil {
			idx.config.Logger.Error(
				"midnight indexer: refresh ariadne dedupe after rollback",
				"error", err,
				"block", blockNumber,
			)
		}
		return
	}
	idx.mu.Lock()
	if latest == nil {
		idx.lastAriadneDatum = nil
	} else {
		idx.lastAriadneDatum = bytes.Clone(latest.Datum)
	}
	idx.mu.Unlock()
}

// rollbackCandidateSpends restores candidate UTxOs that were spent (removed
// from idx.candidates) by the rolled-back block, using the candidateRemovals
// journal.  It does not require block decoding.
func (idx *Indexer) rollbackCandidateSpends(blockNumber uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if removals := idx.candidateRemovals[blockNumber]; len(removals) > 0 {
		for key, datum := range removals {
			idx.candidates[key] = bytes.Clone(datum)
		}
		delete(idx.candidateRemovals, blockNumber)
	}
}

// rollbackCandidateCreates removes candidate UTxOs that were created by the
// rolled-back block.  It requires the decoded transactions.
func (idx *Indexer) rollbackCandidateCreates(txs []lcommon.Transaction) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for _, tx := range txs {
		txHashBytes := tx.Id().Bytes()
		for outIdx, out := range tx.Outputs() {
			addrBytes, _ := out.Address().Bytes()
			if bytes.Equal(addrBytes, idx.candidateAddrBytes) {
				var key candidateKey
				copy(key.TxHash[:], txHashBytes)
				key.OutputIndex = uint32(outIdx) //nolint:gosec
				delete(idx.candidates, key)
			}
		}
	}
}

// processBlock iterates the transactions in a block and calls processTx for
// each one. Exposed for direct use in tests.
func (idx *Indexer) processBlock(
	block models.Block,
	txs []lcommon.Transaction,
	timestampMs uint64,
) error {
	// Resolve the epoch for this block so Ariadne rows are keyed correctly
	// during both backfill and live processing. The live path (handleBlockEvent)
	// calls advanceEpochLocked before processBlock, but the backfill path calls
	// processBlock directly, so we must resolve and advance here as well.
	var govEpoch uint64
	if idx.config.SlotToEpoch != nil {
		epoch, err := idx.config.SlotToEpoch(block.Slot)
		if err != nil {
			// Ariadne and candidate-snapshot writes are keyed by epoch: writing
			// them under epoch 0 or a stale epoch would silently corrupt the
			// index. Return an error so the caller (backfill or fatal path)
			// can surface it rather than persisting incorrect data.
			if len(idx.permCandidatePolicyBytes) > 0 || len(idx.candidateAddrBytes) > 0 {
				return fmt.Errorf(
					"midnight indexer: epoch resolution required for slot=%d but SlotToEpoch failed: %w",
					block.Slot, err,
				)
			}
			// Governance-only path: epoch is not a write key; fall back safely.
			idx.mu.RLock()
			govEpoch = idx.currentEpoch
			idx.mu.RUnlock()
		} else {
			idx.mu.Lock()
			idx.advanceEpochLocked(epoch, block.Number)
			govEpoch = idx.currentEpoch
			idx.mu.Unlock()
		}
	} else {
		idx.mu.RLock()
		govEpoch = idx.currentEpoch
		idx.mu.RUnlock()
	}

	for i, tx := range txs {
		if err := idx.processTx(block, tx, uint32(i), timestampMs, govEpoch); err != nil { //nolint:gosec
			return err
		}
	}

	// Prune rollback journals that are beyond the rollback window.
	// Ouroboros cannot roll back more than candidateRollbackDepth blocks, so
	// journal entries older than that depth will never be needed for rollback.
	if block.Number > candidateRollbackDepth {
		pruneBelow := block.Number - candidateRollbackDepth
		idx.mu.Lock()
		for bn := range idx.candidateRemovals {
			if bn < pruneBelow {
				delete(idx.candidateRemovals, bn)
			}
		}
		for bn := range idx.epochTransitions {
			if bn < pruneBelow {
				delete(idx.epochTransitions, bn)
			}
		}
		idx.mu.Unlock()
		if len(idx.permCandidatePolicyBytes) > 0 {
			if err := idx.config.Metadata.DeleteMidnightAriadneRollbacksBeforeBlock(nil, pruneBelow); err != nil &&
				idx.config.Logger != nil {
				idx.config.Logger.Error(
					"midnight indexer: prune ariadne rollback journal",
					"error", err,
					"block", block.Number,
					"prune_below", pruneBelow,
				)
			}
		}
	}
	return nil
}

// processTx scans a single transaction's inputs and outputs.
func (idx *Indexer) processTx(
	block models.Block,
	tx lcommon.Transaction,
	txIdx uint32,
	timestampMs uint64,
	govEpoch uint64,
) error {
	txHashBytes := tx.Id().Bytes()

	// Scan inputs for spends of tracked UTxOs.
	// We peek with a read-lock, write the DB row, then remove from memory
	// only on success so that a transient write failure leaves the in-memory
	// state intact and the UTxO reloadable from the DB on restart.
	for _, inp := range tx.Inputs() {
		inpHashBytes := inp.Id().Bytes()
		inpHashHex := hex.EncodeToString(inpHashBytes)
		inpIdx := inp.Index()
		key := utxoKey{TxHash: inpHashHex, Index: inpIdx}

		idx.mu.RLock()
		utxo, isCNight := idx.cNightUTxOs[key]
		reg, isReg := idx.regUTxOs[key]
		idx.mu.RUnlock()

		if isCNight {
			row := &models.MidnightAssetSpend{
				Address:          utxo.Address,
				Quantity:         utxo.Quantity,
				SpendingTxHash:   txHashBytes,
				UtxoTxHash:       inpHashBytes,
				UtxoIndex:        inpIdx,
				BlockNumber:      block.Number,
				BlockHash:        block.Hash,
				TxIndex:          txIdx,
				BlockTimestampMs: timestampMs,
			}
			if err := idx.config.Metadata.CreateMidnightAssetSpend(nil, row); err != nil {
				return fmt.Errorf(
					"write asset spend tx=%s input=%s#%d: %w",
					hex.EncodeToString(txHashBytes), inpHashHex, inpIdx, err,
				)
			}
			idx.mu.Lock()
			delete(idx.cNightUTxOs, key)
			idx.mu.Unlock()
		}

		if isReg {
			row := &models.MidnightDeregistration{
				FullDatum:        reg.FullDatum,
				TxHash:           txHashBytes,
				UtxoTxHash:       inpHashBytes,
				UtxoIndex:        inpIdx,
				BlockNumber:      block.Number,
				BlockHash:        block.Hash,
				TxIndex:          txIdx,
				BlockTimestampMs: timestampMs,
			}
			if err := idx.config.Metadata.CreateMidnightDeregistration(nil, row); err != nil {
				return fmt.Errorf(
					"write deregistration tx=%s input=%s#%d: %w",
					hex.EncodeToString(txHashBytes), inpHashHex, inpIdx, err,
				)
			}
			idx.mu.Lock()
			delete(idx.regUTxOs, key)
			idx.mu.Unlock()
		}

		// Always attempt to remove from candidate set (no-op if not tracked).
		if len(idx.candidateAddrBytes) > 0 {
			idx.mu.Lock()
			if candidateKey, datum, removed := idx.removeCandidate(inpHashBytes, inpIdx); removed {
				idx.recordCandidateRemovalLocked(block.Number, candidateKey, datum)
			}
			idx.mu.Unlock()
		}
	}

	// Scan outputs for cNIGHT creates, registration outputs, and governance.
	for outIdx, out := range tx.Outputs() {
		if err := idx.processOutput(
			block,
			txHashBytes,
			uint32(outIdx), //nolint:gosec
			txIdx,
			timestampMs,
			govEpoch,
			out,
		); err != nil {
			return err
		}
	}
	return nil
}

// processOutput checks a single transaction output for cNIGHT tokens,
// registration auth tokens, and governance/Ariadne/candidate data.
func (idx *Indexer) processOutput(
	block models.Block,
	txHashBytes []byte,
	outIdx uint32,
	txIdx uint32,
	timestampMs uint64,
	govEpoch uint64,
	out lcommon.TransactionOutput,
) error {
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
					return fmt.Errorf(
						"write asset create tx=%s output=%d: %w",
						txHashHex, outIdx, err,
					)
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
	if idx.config.MappingValidatorAddress != "" {
		addrStr := out.Address().String()
		if addrStr == idx.config.MappingValidatorAddress && idx.hasAuthToken(out) {
			datum := out.Datum()
			if datum != nil {
				datumCbor := datum.Cbor()
				if len(datumCbor) > 0 {
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
						return fmt.Errorf(
							"write registration tx=%s output=%d: %w",
							txHashHex, outIdx, err,
						)
					}
					idx.mu.Lock()
					idx.regUTxOs[key] = registrationUTxO{FullDatum: datumCbor}
					idx.mu.Unlock()
				}
			}
		}
	}

	// Governance/Ariadne/Candidate scanning.
	if len(idx.techCommitteeAddrBytes) == 0 &&
		len(idx.councilAddrBytes) == 0 &&
		len(idx.permCandidatePolicyBytes) == 0 &&
		len(idx.candidateAddrBytes) == 0 {
		return nil
	}

	addrBytes, _ := out.Address().Bytes()

	datum := out.Datum()
	var datumCbor []byte
	if datum != nil {
		dc := datum.Cbor()
		if len(dc) > 0 {
			datumCbor = dc
		}
	}

	// Technical Committee governance scan.
	if len(idx.techCommitteeAddrBytes) > 0 &&
		bytes.Equal(addrBytes, idx.techCommitteeAddrBytes) &&
		idx.outputHasPolicy(out, idx.techCommitteePolicyBytes) &&
		datumCbor != nil {
		if err := idx.config.Metadata.InsertMidnightGovernanceDatum(
			nil,
			&models.MidnightGovernanceDatum{
				DatumType:   models.MidnightGovernanceDatumTypeTechnicalCommittee,
				TxHash:      txHashBytes,
				OutputIndex: outIdx,
				Datum:       datumCbor,
				BlockNumber: block.Number,
			},
		); err != nil {
			return fmt.Errorf(
				"write technical committee governance datum tx=%s output=%d block=%d: %w",
				txHashHex, outIdx, block.Number, err,
			)
		}
	}

	// Council governance scan.
	if len(idx.councilAddrBytes) > 0 &&
		bytes.Equal(addrBytes, idx.councilAddrBytes) &&
		idx.outputHasPolicy(out, idx.councilPolicyBytes) &&
		datumCbor != nil {
		if err := idx.config.Metadata.InsertMidnightGovernanceDatum(
			nil,
			&models.MidnightGovernanceDatum{
				DatumType:   models.MidnightGovernanceDatumTypeCouncil,
				TxHash:      txHashBytes,
				OutputIndex: outIdx,
				Datum:       datumCbor,
				BlockNumber: block.Number,
			},
		); err != nil {
			return fmt.Errorf(
				"write council governance datum tx=%s output=%d block=%d: %w",
				txHashHex, outIdx, block.Number, err,
			)
		}
	}

	// Ariadne params scan.
	if len(idx.permCandidatePolicyBytes) > 0 &&
		idx.outputHasPolicy(out, idx.permCandidatePolicyBytes) &&
		datumCbor != nil {
		idx.mu.RLock()
		isDup := bytes.Equal(datumCbor, idx.lastAriadneDatum)
		idx.mu.RUnlock()
		if !isDup {
			if err := idx.recordAriadneRollback(block.Number, govEpoch); err != nil {
				return fmt.Errorf(
					"record ariadne rollback tx=%s output=%d epoch=%d: %w",
					txHashHex, outIdx, govEpoch, err,
				)
			}
			if err := idx.config.Metadata.UpsertMidnightAriadneParams(
				nil,
				&models.MidnightAriadneParams{
					Epoch: govEpoch,
					Datum: datumCbor,
				},
			); err != nil {
				return fmt.Errorf(
					"write ariadne params tx=%s output=%d epoch=%d: %w",
					txHashHex, outIdx, govEpoch, err,
				)
			} else {
				idx.mu.Lock()
				idx.lastAriadneDatum = datumCbor
				idx.mu.Unlock()
			}
		}
	}

	// Committee-candidate tracking.
	if len(idx.candidateAddrBytes) > 0 &&
		bytes.Equal(addrBytes, idx.candidateAddrBytes) {
		var ckey candidateKey
		copy(ckey.TxHash[:], txHashBytes)
		ckey.OutputIndex = outIdx
		idx.mu.Lock()
		idx.candidates[ckey] = bytes.Clone(datumCbor)
		idx.mu.Unlock()
	}

	return nil
}

func (idx *Indexer) recordAriadneRollback(blockNumber, epoch uint64) error {
	existing, err := idx.config.Metadata.GetMidnightAriadneParamsByEpoch(epoch, nil)
	if err != nil {
		return err
	}

	entry := &models.MidnightAriadneRollback{
		BlockNumber: blockNumber,
		Epoch:       epoch,
	}
	if existing != nil {
		entry.PreviousExists = true
		entry.PreviousDatum = bytes.Clone(existing.Datum)
	}
	return idx.config.Metadata.CreateMidnightAriadneRollback(nil, entry)
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

// advanceEpochLocked snapshots all intermediate epochs between the current
// epoch and the new epoch, then sets currentEpoch = epoch.
// If hasCurrentEpoch is false (cold start), it skips snapshotting and just
// sets the current epoch. idx.mu must be held.
func (idx *Indexer) advanceEpochLocked(epoch uint64, blockNumber uint64) {
	if !idx.hasCurrentEpoch {
		idx.currentEpoch = epoch
		idx.hasCurrentEpoch = true
		return
	}
	if epoch <= idx.currentEpoch {
		return
	}
	// Journal the pre-advance value so rollbackBlock can restore it.
	idx.epochTransitions[blockNumber] = idx.currentEpoch
	for e := idx.currentEpoch; e < epoch; e++ {
		idx.snapshotEpochLocked(e, blockNumber)
	}
	idx.currentEpoch = epoch
}

// snapshotEpochLocked writes the current candidate set as the epoch snapshot.
// Skips if this epoch has already been snapshotted. idx.mu must be held.
func (idx *Indexer) snapshotEpochLocked(epoch uint64, blockNumber uint64) {
	if idx.hasSnapshotEpoch && epoch <= idx.snapshotEpoch {
		return
	}

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
		if idx.config.Logger != nil {
			idx.config.Logger.Error(
				"midnight indexer: encode candidate snapshot",
				"epoch", epoch,
				"error", err,
			)
		}
		return
	}
	if err := idx.config.Metadata.UpsertMidnightEpochCandidates(
		nil,
		&models.MidnightEpochCandidates{
			Epoch:          epoch,
			BlockNumber:    blockNumber,
			CandidatesCbor: snapshotCbor,
		},
	); err != nil {
		if idx.config.Logger != nil {
			idx.config.Logger.Error(
				"midnight indexer: upsert epoch candidates",
				"epoch", epoch,
				"error", err,
			)
		}
	} else {
		idx.snapshotEpoch = epoch
		idx.hasSnapshotEpoch = true
	}
}

// removeCandidate removes a UTxO from the in-memory candidate set when it is
// consumed by a transaction. This is a no-op if the key is not in the set.
// idx.mu must be held by the caller.
func (idx *Indexer) removeCandidate(txHashBytes []byte, outputIndex uint32) (candidateKey, []byte, bool) {
	var key candidateKey
	copy(key.TxHash[:], txHashBytes)
	key.OutputIndex = outputIndex
	datum, ok := idx.candidates[key]
	if !ok {
		return key, nil, false
	}
	delete(idx.candidates, key)
	return key, bytes.Clone(datum), true
}

func (idx *Indexer) recordCandidateRemovalLocked(blockNumber uint64, key candidateKey, datum []byte) {
	removals := idx.candidateRemovals[blockNumber]
	if removals == nil {
		removals = make(map[candidateKey][]byte)
		idx.candidateRemovals[blockNumber] = removals
	}
	removals[key] = bytes.Clone(datum)
}

// outputHasPolicy reports whether output carries any asset under policyID.
func (idx *Indexer) outputHasPolicy(
	out lcommon.TransactionOutput,
	policyID []byte,
) bool {
	if len(policyID) == 0 {
		return false
	}
	assets := out.Assets()
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
