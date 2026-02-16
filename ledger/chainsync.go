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
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	cardano "github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/forging"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	// Max number of blocks to fetch in a single blockfetch call
	// This prevents us exceeding the configured recv queue size in the block-fetch protocol
	blockfetchBatchSize = 500

	// Default/fallback slot threshold for blockfetch batches
	blockfetchBatchSlotThresholdDefault = 2500 * 20

	// Timeout for updates on a blockfetch operation. This is based on a 2s BatchStart
	// and a 2s Block timeout for blockfetch
	blockfetchBusyTimeout = 30 * time.Second

	// Interval for rate-limiting non-active connection drop messages
	dropEventLogInterval = 60 * time.Second

	// Interval for periodic sync progress reporting
	syncProgressLogInterval = 30 * time.Second

	// Rollback loop detection thresholds
	rollbackLoopThreshold = 3               // number of rollbacks to same slot before breaking loop
	rollbackLoopWindow    = 5 * time.Minute // time window for rollback loop detection
)

func (ls *LedgerState) handleEventChainsync(evt event.Event) {
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()
	e := evt.Data.(ChainsyncEvent)
	if e.Rollback {
		if err := ls.handleEventChainsyncRollback(e); err != nil {
			ls.config.Logger.Error(
				"failed to handle rollback",
				"component", "ledger",
				"error", err,
				"slot", e.Point.Slot,
				"hash", hex.EncodeToString(e.Point.Hash),
			)
			if ls.config.FatalErrorFunc != nil {
				ls.config.FatalErrorFunc(err)
			}
			return
		}
	} else if e.BlockHeader != nil {
		if err := ls.handleEventChainsyncBlockHeader(e); err != nil {
			ls.config.Logger.Error(
				"failed to handle block header",
				"component", "ledger",
				"error", err,
				"slot", e.Point.Slot,
				"hash", hex.EncodeToString(e.Point.Hash),
			)
			if ls.config.EventBus != nil {
				ls.config.EventBus.Publish(
					LedgerErrorEventType,
					event.NewEvent(
						LedgerErrorEventType,
						LedgerErrorEvent{
							Error:     err,
							Operation: "block_header",
							Point:     e.Point,
						},
					),
				)
			}
			return
		}
	}
}

func (ls *LedgerState) handleEventBlockfetch(evt event.Event) {
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	e := evt.Data.(BlockfetchEvent)
	if e.BatchDone {
		if err := ls.handleEventBlockfetchBatchDone(e); err != nil {
			ls.config.Logger.Error(
				"failed to handle blockfetch batch done",
				"component", "ledger",
				"error", err,
			)
			if ls.config.EventBus != nil {
				ls.config.EventBus.Publish(
					LedgerErrorEventType,
					event.NewEvent(
						LedgerErrorEventType,
						LedgerErrorEvent{
							Error:     err,
							Operation: "blockfetch_batch_done",
						},
					),
				)
			}
		}
	} else if e.Block != nil {
		if err := ls.handleEventBlockfetchBlock(e); err != nil {
			ls.config.Logger.Error(
				"failed to handle block",
				"component", "ledger",
				"error", err,
				"slot", e.Point.Slot,
				"hash", hex.EncodeToString(e.Point.Hash),
			)
			if ls.config.EventBus != nil {
				ls.config.EventBus.Publish(
					LedgerErrorEventType,
					event.NewEvent(
						LedgerErrorEventType,
						LedgerErrorEvent{
							Error:     err,
							Operation: "blockfetch_block",
							Point:     e.Point,
						},
					),
				)
			}
		}
	}
}

// detectConnectionSwitch checks for an active connection change and logs a
// summary of dropped events when a switch is detected. It returns the current
// active connection ID and whether connection filtering is configured. When
// configured is false, callers should skip all connection-based filtering.
func (ls *LedgerState) detectConnectionSwitch() (activeConnId *ouroboros.ConnectionId, configured bool) {
	if ls.config.GetActiveConnectionFunc == nil {
		return nil, false
	}
	activeConnId = ls.config.GetActiveConnectionFunc()
	if activeConnId != nil &&
		(ls.lastActiveConnId == nil || *ls.lastActiveConnId != *activeConnId) {
		if ls.lastActiveConnId != nil {
			ls.config.Logger.Info(
				"active connection changed",
				"component", "ledger",
				"previous_connection_id", ls.lastActiveConnId.String(),
				"new_connection_id", activeConnId.String(),
				"dropped_headers", ls.dropEventCount,
				"dropped_rollbacks", ls.dropRollbackCount,
			)
			ls.dropEventCount = 0
			ls.dropRollbackCount = 0
		}
		ls.lastActiveConnId = activeConnId
		ls.rollbackHistory = nil
	}
	return activeConnId, true
}

func (ls *LedgerState) handleEventChainsyncRollback(e ChainsyncEvent) error {
	// Filter events from non-active connections when chain selection is enabled
	if activeConnId, configured := ls.detectConnectionSwitch(); configured {
		if activeConnId == nil {
			// No active connection set yet, process this event
			ls.config.Logger.Debug(
				"no active connection, processing rollback event",
				"connection_id", e.ConnectionId.String(),
				"slot", e.Point.Slot,
			)
		} else if *activeConnId != e.ConnectionId {
			// Event is from non-active connection, skip
			// Rate-limit this message to once per dropEventLogInterval
			now := time.Now()
			if now.Sub(ls.dropRollbackLastLog) >= dropEventLogInterval {
				suppressed := ls.dropRollbackCount
				ls.dropRollbackCount = 0
				ls.dropRollbackLastLog = now
				ls.config.Logger.Debug(
					"dropping rollback from non-active connection",
					"component", "ledger",
					"event_connection_id", e.ConnectionId.String(),
					"active_connection_id", activeConnId.String(),
					"slot", e.Point.Slot,
					"suppressed_since_last_log", suppressed,
				)
			} else {
				ls.dropRollbackCount++
			}
			return nil
		}
	}

	// Rollback loop detection: track recent rollbacks and skip if
	// the same slot appears too frequently within the detection window.
	now := time.Now()
	ls.rollbackHistory = append(ls.rollbackHistory, rollbackRecord{
		slot:      e.Point.Slot,
		timestamp: now,
	})
	// Prune entries older than the detection window
	cutoff := now.Add(-rollbackLoopWindow)
	pruned := ls.rollbackHistory[:0]
	for _, r := range ls.rollbackHistory {
		if !r.timestamp.Before(cutoff) {
			pruned = append(pruned, r)
		}
	}
	ls.rollbackHistory = pruned
	// Count rollbacks to this specific slot
	var slotCount int
	for _, r := range ls.rollbackHistory {
		if r.slot == e.Point.Slot {
			slotCount++
		}
	}
	if slotCount >= rollbackLoopThreshold {
		ls.config.Logger.Warn(
			"rollback loop detected, skipping rollback to break loop",
			"component", "ledger",
			"slot", e.Point.Slot,
			"count", slotCount,
			"window", rollbackLoopWindow,
		)
		return nil
	}

	if ls.chainsyncState == SyncingChainsyncState {
		ls.config.Logger.Info(
			fmt.Sprintf(
				"ledger: rolling back to %d.%s",
				e.Point.Slot,
				hex.EncodeToString(e.Point.Hash),
			),
		)
		ls.chainsyncState = RollbackChainsyncState
	}
	if err := ls.chain.Rollback(e.Point); err != nil {
		return fmt.Errorf("chain rollback failed: %w", err)
	}
	return nil
}

func (ls *LedgerState) handleEventChainsyncBlockHeader(e ChainsyncEvent) error {
	// Filter events from non-active connections when chain selection is enabled
	if activeConnId, configured := ls.detectConnectionSwitch(); configured {
		if activeConnId == nil {
			// No active connection set yet, process this event
			ls.config.Logger.Debug(
				"no active connection, processing event",
				"connection_id", e.ConnectionId.String(),
				"slot", e.Point.Slot,
			)
		} else if *activeConnId != e.ConnectionId {
			// Event is from non-active connection, skip
			// Rate-limit this message to once per dropEventLogInterval
			now := time.Now()
			if now.Sub(ls.dropEventLastLog) >= dropEventLogInterval {
				suppressed := ls.dropEventCount
				ls.dropEventCount = 0
				ls.dropEventLastLog = now
				ls.config.Logger.Debug(
					"dropping event from non-active connection",
					"component", "ledger",
					"event_connection_id", e.ConnectionId.String(),
					"active_connection_id", activeConnId.String(),
					"slot", e.Point.Slot,
					"suppressed_since_last_log", suppressed,
				)
			} else {
				ls.dropEventCount++
			}
			return nil
		}
	}

	// Track upstream tip for sync progress reporting
	if e.Tip.Point.Slot > ls.syncUpstreamTipSlot.Load() {
		ls.syncUpstreamTipSlot.Store(e.Tip.Point.Slot)
	}

	if ls.chainsyncState == RollbackChainsyncState {
		ls.config.Logger.Info(
			fmt.Sprintf(
				"ledger: switched to fork at %d.%s",
				e.Point.Slot,
				hex.EncodeToString(e.Point.Hash),
			),
		)
		ls.metrics.forks.Add(1)
	}
	ls.chainsyncState = SyncingChainsyncState
	// Allow us to build up a few blockfetch batches worth of headers
	allowedHeaderCount := blockfetchBatchSize * 4
	headerCount := ls.chain.HeaderCount()

	// Add header to chain
	ls.config.Logger.Debug(
		"chainsync header handler entered",
		"component", "ledger",
		"slot", e.Point.Slot,
		"tip_slot", e.Tip.Point.Slot,
		"header_count", headerCount,
		"connection_id", e.ConnectionId.String(),
	)
	if err := ls.chain.AddBlockHeader(e.BlockHeader); err != nil {
		var notFitErr chain.BlockNotFitChainTipError
		if errors.As(err, &notFitErr) {
			// Header doesn't fit current chain tip. This typically happens
			// when the active peer changes and the new peer's chainsync
			// session is at a different chain position. Clear stale queued
			// headers so subsequent headers are evaluated against the
			// block tip rather than perpetuating the mismatch.
			ls.chain.ClearHeaders()
			ls.config.Logger.Debug(
				"block header does not fit chain tip, cleared stale headers",
				"component", "ledger",
				"slot", e.Point.Slot,
				"block_prev_hash", notFitErr.BlockPrevHash(),
				"chain_tip_hash", notFitErr.TipHash(),
			)
			return nil
		}
		return fmt.Errorf("failed adding chain block header: %w", err)
	}
	// Wait for additional block headers before fetching block bodies if we're
	// far enough out from upstream tip
	// Use security window as slot threshold if available
	slotThreshold := ls.calculateStabilityWindow()
	if e.Point.Slot < e.Tip.Point.Slot &&
		(e.Tip.Point.Slot-e.Point.Slot > slotThreshold) &&
		(headerCount+1) < allowedHeaderCount {
		ls.config.Logger.Debug(
			"accumulating headers (far from tip)",
			"component", "ledger",
			"slot", e.Point.Slot,
			"tip_slot", e.Tip.Point.Slot,
			"threshold", slotThreshold,
			"header_count", headerCount+1,
		)
		return nil
	}
	// We use the blockfetch lock to ensure we aren't starting a batch at the same
	// time as blockfetch starts a new one to avoid deadlocks
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	// Don't start fetch if there's already one in progress
	if ls.chainsyncBlockfetchReadyChan != nil {
		ls.config.Logger.Debug(
			"blockfetch in progress, queuing header",
			"component", "ledger",
			"slot", e.Point.Slot,
			"header_count", ls.chain.HeaderCount(),
		)
		return nil
	}
	// Mark blockfetch as in progress
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	// Request next bulk range
	headerStart, headerEnd := ls.chain.HeaderRange(blockfetchBatchSize)
	ls.config.Logger.Debug(
		"starting blockfetch",
		"component", "ledger",
		"header_start_slot", headerStart.Slot,
		"header_end_slot", headerEnd.Slot,
		"header_count", ls.chain.HeaderCount(),
	)
	err := ls.blockfetchRequestRangeStart(
		e.ConnectionId,
		headerStart,
		headerEnd,
	)
	if err != nil {
		ls.blockfetchRequestRangeCleanup()
		return err
	}
	return nil
}

//nolint:unparam
func (ls *LedgerState) handleEventBlockfetchBlock(e BlockfetchEvent) error {
	ls.chainsyncBlockEvents = append(
		ls.chainsyncBlockEvents,
		e,
	)
	// Reset timeout timer since we received a block
	if ls.chainsyncBlockfetchTimeoutTimer != nil {
		ls.chainsyncBlockfetchTimeoutTimer.Reset(blockfetchBusyTimeout)
	}
	return nil
}

func (ls *LedgerState) processBlockEvents() error {
	batchOffset := 0
	for {
		batchSize := min(
			10, // Chosen to stay well under badger transaction size limit
			len(ls.chainsyncBlockEvents)-batchOffset,
		)
		if batchSize <= 0 {
			break
		}
		batch := ls.chainsyncBlockEvents[batchOffset : batchOffset+batchSize]
		// Verify block header cryptographic proofs (VRF, KES) outside
		// the lock to avoid blocking other ledger operations during
		// expensive crypto verification.
		for _, evt := range batch {
			if err := ls.verifyBlockHeaderCrypto(evt.Block); err != nil {
				return fmt.Errorf(
					"block header crypto verification: %w",
					err,
				)
			}
		}
		ls.Lock()
		// Start a transaction
		txn := ls.db.BlobTxn(true)
		err := txn.Do(func(txn *database.Txn) error {
			for _, evt := range batch {
				if err := ls.processBlockEvent(txn, evt); err != nil {
					return fmt.Errorf("failed processing block event: %w", err)
				}
			}
			return nil
		})
		ls.Unlock()
		if err != nil {
			return err
		}
		batchOffset += batchSize
	}
	ls.chainsyncBlockEvents = nil
	return nil
}

// GenesisBlockHash returns the Byron genesis hash from config, which is used
// as the block hash for the synthetic genesis block that holds genesis UTxO data.
// This mirrors how the Shelley epoch nonce uses the Shelley genesis hash.
func GenesisBlockHash(cfg *cardano.CardanoNodeConfig) ([32]byte, error) {
	if cfg == nil || cfg.ByronGenesisHash == "" {
		return [32]byte{}, errors.New(
			"byron genesis hash not available in config",
		)
	}
	hashBytes, err := hex.DecodeString(cfg.ByronGenesisHash)
	if err != nil {
		return [32]byte{}, fmt.Errorf("decode Byron genesis hash: %w", err)
	}
	if len(hashBytes) != 32 {
		return [32]byte{}, fmt.Errorf(
			"invalid Byron genesis hash length: expected 32 bytes, got %d",
			len(hashBytes),
		)
	}
	var hash [32]byte
	copy(hash[:], hashBytes)
	return hash, nil
}

func (ls *LedgerState) createGenesisBlock() error {
	if ls.currentTip.Point.Slot > 0 {
		return nil
	}

	// Get the Byron genesis hash to use as the synthetic block hash.
	// This mirrors how the Shelley epoch nonce uses the Shelley genesis hash.
	genesisHash, err := GenesisBlockHash(ls.config.CardanoNodeConfig)
	if err != nil {
		return fmt.Errorf("get genesis block hash: %w", err)
	}

	txn := ls.db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		// Record genesis UTxOs
		byronGenesis := ls.config.CardanoNodeConfig.ByronGenesis()
		byronGenesisUtxos, err := byronGenesis.GenesisUtxos()
		if err != nil {
			return fmt.Errorf("generate Byron genesis UTxOs: %w", err)
		}
		shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
		shelleyGenesisUtxos, err := shelleyGenesis.GenesisUtxos()
		if err != nil {
			return fmt.Errorf("generate Shelley genesis UTxOs: %w", err)
		}
		if len(byronGenesisUtxos)+len(shelleyGenesisUtxos) == 0 {
			return errors.New("failed to generate genesis UTxOs")
		}
		ls.config.Logger.Info(
			fmt.Sprintf("creating %d genesis UTxOs (%d Byron, %d Shelley)",
				len(byronGenesisUtxos)+len(shelleyGenesisUtxos),
				len(byronGenesisUtxos),
				len(shelleyGenesisUtxos),
			),
			"component", "ledger",
		)

		// Group genesis UTxOs by transaction hash
		genesisUtxos := slices.Concat(byronGenesisUtxos, shelleyGenesisUtxos)
		txUtxos := make(map[[32]byte][]lcommon.Utxo)
		for i := range genesisUtxos {
			txHash := genesisUtxos[i].Id.Id()
			var txHashArray [32]byte
			copy(txHashArray[:], txHash.Bytes())

			// Generate CBOR for genesis UTxO outputs since they don't have original CBOR
			cborData, err := cbor.Encode(genesisUtxos[i].Output)
			if err != nil {
				return fmt.Errorf("encode genesis UTxO output to CBOR: %w", err)
			}

			// Create a new Utxo with CBOR-encoded output
			var newOutput lcommon.TransactionOutput
			switch output := genesisUtxos[i].Output.(type) {
			case byron.ByronTransactionOutput:
				newByronOutput := output
				(&newByronOutput).SetCbor(cborData)
				newOutput = newByronOutput
			case shelley.ShelleyTransactionOutput:
				newShelleyOutput := output
				(&newShelleyOutput).SetCbor(cborData)
				newOutput = newShelleyOutput
			default:
				return fmt.Errorf("unsupported genesis UTxO output type: %T", genesisUtxos[i].Output)
			}

			txUtxos[txHashArray] = append(txUtxos[txHashArray], lcommon.Utxo{
				Id:     genesisUtxos[i].Id,
				Output: newOutput,
			})
		}

		// Build synthetic genesis block with proper structure:
		// Block -> Transactions -> Outputs (UTxOs)
		//
		// CBOR structure:
		// [                                    // block: array of transactions
		//   {0: tx_hash, 1: [output, ...]},    // transaction 1
		//   {0: tx_hash, 1: [output, ...]},    // transaction 2
		//   ...
		// ]
		//
		// We track byte offsets for each output within this structure.
		utxoOffsets := make(map[database.UtxoRef]database.CborOffset)

		// Sort transaction hashes for deterministic ordering
		txHashes := make([][32]byte, 0, len(txUtxos))
		for txHash := range txUtxos {
			txHashes = append(txHashes, txHash)
		}
		slices.SortFunc(txHashes, func(a, b [32]byte) int {
			return bytes.Compare(a[:], b[:])
		})

		// Build the block structure manually to track exact byte offsets
		// We need to know where each output CBOR starts within the block
		blockCbor, err := buildGenesisBlockCbor(
			txHashes,
			txUtxos,
			utxoOffsets,
			genesisHash,
		)
		if err != nil {
			return fmt.Errorf("build genesis block cbor: %w", err)
		}

		// Store synthetic genesis block CBOR.
		// We use SetGenesisCbor to avoid creating a block index entry that
		// would cause the chain iterator to include it (genesis is already
		// handled separately during initialization).
		if err := ls.db.SetGenesisCbor(0, genesisHash[:], blockCbor, txn); err != nil {
			return fmt.Errorf("store genesis cbor: %w", err)
		}

		// Store each genesis transaction with its UTxOs
		for txHashArray, utxos := range txUtxos {
			if err := ls.db.SetGenesisTransaction(
				txHashArray[:],
				genesisHash[:],
				utxos,
				utxoOffsets,
				txn,
			); err != nil {
				return fmt.Errorf(
					"set genesis transaction %x: %w",
					txHashArray[:8],
					err,
				)
			}
		}

		ls.config.Logger.Info(
			fmt.Sprintf("stored %d genesis transactions with %d total UTxOs",
				len(txUtxos),
				len(genesisUtxos),
			),
			"component", "ledger",
		)

		// Load genesis staking data (pool registrations + delegations)
		genesisPools, _, err := shelleyGenesis.InitialPools()
		if err != nil {
			return fmt.Errorf("parse genesis staking: %w", err)
		}
		if len(genesisPools) > 0 ||
			len(shelleyGenesis.Staking.Stake) > 0 {
			ls.config.Logger.Info(
				fmt.Sprintf(
					"loading genesis staking: %d pools, %d delegations",
					len(genesisPools),
					len(shelleyGenesis.Staking.Stake),
				),
				"component", "ledger",
			)
			if err := ls.db.SetGenesisStaking(
				genesisPools,
				shelleyGenesis.Staking.Stake,
				genesisHash[:],
				txn,
			); err != nil {
				return fmt.Errorf("set genesis staking: %w", err)
			}
		}

		return nil
	})
	return err
}

// buildGenesisBlockCbor creates a CBOR structure representing a synthetic
// genesis block containing transactions with outputs. The structure is:
//
//	[                                    // block: array of transactions
//	  {0: tx_hash, 1: [output, ...]},    // transaction 1
//	  {0: tx_hash, 1: [output, ...]},    // transaction 2
//	  ...
//	]
//
// It populates utxoOffsets with the byte offset of each output within the block.
// Unlike a search-based approach, this function tracks exact byte positions during
// CBOR construction to avoid any possibility of false matches.
// The blockHash parameter is the Byron genesis hash used as the synthetic block hash.
func buildGenesisBlockCbor(
	txHashes [][32]byte,
	txUtxos map[[32]byte][]lcommon.Utxo,
	utxoOffsets map[database.UtxoRef]database.CborOffset,
	blockHash [32]byte,
) ([]byte, error) {
	var buf bytes.Buffer

	// Write outer array header for transactions
	writeCborArrayHeader(&buf, len(txHashes))

	for _, txHash := range txHashes {
		utxos := txUtxos[txHash]

		// Sort outputs by index for deterministic ordering
		slices.SortFunc(utxos, func(a, b lcommon.Utxo) int {
			ai, bi := uint64(a.Id.Index()), uint64(b.Id.Index())
			if ai < bi {
				return -1
			} else if ai > bi {
				return 1
			}
			return 0
		})

		// Write map header with 2 entries: {0: txhash, 1: outputs}
		writeCborMapHeader(&buf, 2)

		// Key 0: tx hash
		writeCborUint(&buf, 0)
		writeCborBytes(&buf, txHash[:])

		// Key 1: outputs array
		writeCborUint(&buf, 1)
		writeCborArrayHeader(&buf, len(utxos))

		// Write each output, tracking offsets
		for _, utxo := range utxos {
			outputCbor := utxo.Output.Cbor()
			if len(outputCbor) == 0 {
				var err error
				outputCbor, err = cbor.Encode(utxo.Output)
				if err != nil {
					return nil, fmt.Errorf("encode output: %w", err)
				}
			}

			// Record offset BEFORE writing the output
			offset := buf.Len()
			outputLen := len(outputCbor)

			// Validate sizes fit in uint32 (fail fast instead of silent truncation)
			if offset > math.MaxUint32 {
				return nil, fmt.Errorf(
					"genesis CBOR offset %d exceeds uint32 max",
					offset,
				)
			}
			if outputLen > math.MaxUint32 {
				return nil, fmt.Errorf(
					"genesis output CBOR length %d exceeds uint32 max",
					outputLen,
				)
			}

			buf.Write(outputCbor)

			ref := database.UtxoRef{
				TxId:      txHash,
				OutputIdx: utxo.Id.Index(),
			}
			//nolint:gosec // uint32 bounds checked above
			utxoOffsets[ref] = database.CborOffset{
				BlockSlot:  0,
				BlockHash:  blockHash,
				ByteOffset: uint32(offset),
				ByteLength: uint32(outputLen),
			}
		}
	}

	return buf.Bytes(), nil
}

// writeCborArrayHeader writes a CBOR array header for n elements.
func writeCborArrayHeader(buf *bytes.Buffer, n int) {
	writeCborMajorType(buf, 4, n) // Major type 4 = array
}

// writeCborMapHeader writes a CBOR map header for n pairs.
func writeCborMapHeader(buf *bytes.Buffer, n int) {
	writeCborMajorType(buf, 5, n) // Major type 5 = map
}

// writeCborBytes writes a CBOR byte string.
func writeCborBytes(buf *bytes.Buffer, data []byte) {
	writeCborMajorType(buf, 2, len(data)) // Major type 2 = byte string
	buf.Write(data)
}

// writeCborUint writes a CBOR unsigned integer.
func writeCborUint(buf *bytes.Buffer, n int) {
	writeCborMajorType(buf, 0, n) // Major type 0 = unsigned int
}

// writeCborMajorType writes a CBOR header with the given major type and value.
func writeCborMajorType(buf *bytes.Buffer, majorType, n int) {
	header := byte(majorType << 5)
	switch {
	case n < 24:
		buf.WriteByte(header | byte(n))
	case n < 256:
		buf.WriteByte(header | 24)
		buf.WriteByte(byte(n))
	case n < 65536:
		buf.WriteByte(header | 25)
		buf.WriteByte(byte(n >> 8))
		buf.WriteByte(byte(n))
	case n < 4294967296:
		buf.WriteByte(header | 26)
		buf.WriteByte(byte(n >> 24))
		buf.WriteByte(byte(n >> 16))
		buf.WriteByte(byte(n >> 8))
		buf.WriteByte(byte(n))
	default:
		// 8-byte encoding for values >= 2^32
		buf.WriteByte(header | 27)
		val := uint64(n)
		buf.WriteByte(byte(val >> 56))
		buf.WriteByte(byte(val >> 48))
		buf.WriteByte(byte(val >> 40))
		buf.WriteByte(byte(val >> 32))
		buf.WriteByte(byte(val >> 24))
		buf.WriteByte(byte(val >> 16))
		buf.WriteByte(byte(val >> 8))
		buf.WriteByte(byte(val))
	}
}

func (ls *LedgerState) calculateEpochNonce(
	txn *database.Txn,
	epochStartSlot uint64,
	currentEra eras.EraDesc,
	currentEpoch models.Epoch,
) ([]byte, error) {
	// No epoch nonce in Byron
	if currentEra.Id == 0 {
		return nil, nil
	}
	// Use Shelley genesis hash for initial epoch nonce
	if len(currentEpoch.Nonce) == 0 {
		if ls.config.CardanoNodeConfig.ShelleyGenesisHash == "" {
			return nil, errors.New("could not get Shelley genesis hash")
		}
		genesisHashBytes, err := hex.DecodeString(
			ls.config.CardanoNodeConfig.ShelleyGenesisHash,
		)
		if err != nil {
			return nil, fmt.Errorf("decode genesis hash: %w", err)
		}
		return genesisHashBytes, nil
	}
	// Calculate stability window using the snapshot era
	stabilityWindow := ls.calculateStabilityWindowForEra(currentEra.Id)
	var stabilityWindowStartSlot uint64
	if epochStartSlot > stabilityWindow {
		stabilityWindowStartSlot = epochStartSlot - stabilityWindow
	} else {
		stabilityWindowStartSlot = 0
	}
	// Get last block before stability window
	blockBeforeStabilityWindow, err := database.BlockBeforeSlotTxn(
		txn,
		stabilityWindowStartSlot,
	)
	if err != nil {
		return nil, fmt.Errorf("lookup block before slot: %w", err)
	}
	blockBeforeStabilityWindowNonce, err := ls.db.GetBlockNonce(
		ocommon.Point{
			Hash: blockBeforeStabilityWindow.Hash,
			Slot: blockBeforeStabilityWindow.Slot,
		},
		txn,
	)
	if err != nil {
		return nil, fmt.Errorf("lookup block nonce: %w", err)
	}
	// Get last block in previous epoch using the snapshot epoch
	blockLastPrevEpoch, err := database.BlockBeforeSlotTxn(
		txn,
		currentEpoch.StartSlot,
	)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return blockBeforeStabilityWindowNonce, nil
		}
		return nil, fmt.Errorf("lookup block before slot: %w", err)
	}
	// Calculate nonce from inputs
	ret, err := lcommon.CalculateEpochNonce(
		blockBeforeStabilityWindowNonce,
		blockLastPrevEpoch.PrevHash,
		nil,
	)
	return ret.Bytes(), err
}

// processEpochRollover processes an epoch rollover and returns the result without
// mutating LedgerState. This allows callers to capture the computed state in a
// transaction and apply it to in-memory state after the transaction commits.
// Parameters:
//   - txn: database transaction
//   - currentEpoch: current epoch (read-only input)
//   - currentEra: current era descriptor (read-only input)
//   - currentPParams: current protocol parameters (read-only input)
//
// Returns EpochRolloverResult with all computed state, or an error.
// The caller is responsible for:
//   - Applying the result to in-memory state after successful commit
//   - Starting background cleanup goroutines
//   - Calling Scheduler.ChangeInterval if SchedulerIntervalMs > 0
func (ls *LedgerState) processEpochRollover(
	txn *database.Txn,
	currentEpoch models.Epoch,
	currentEra eras.EraDesc,
	currentPParams lcommon.ProtocolParameters,
) (*EpochRolloverResult, error) {
	epochStartSlot := currentEpoch.StartSlot + uint64(
		currentEpoch.LengthInSlots,
	)
	result := &EpochRolloverResult{
		CheckpointWrittenForEpoch: false,
		NewCurrentEra:             currentEra,
		NewCurrentPParams:         currentPParams,
	}

	// Create initial epoch
	if currentEpoch.SlotLength == 0 {
		// Create initial epoch record
		epochSlotLength, epochLength, err := currentEra.EpochLengthFunc(
			ls.config.CardanoNodeConfig,
		)
		if err != nil {
			return nil, fmt.Errorf("calculate epoch length: %w", err)
		}
		tmpNonce, err := ls.calculateEpochNonce(
			txn,
			0,
			currentEra,
			currentEpoch,
		)
		if err != nil {
			return nil, fmt.Errorf("calculate epoch nonce: %w", err)
		}
		err = ls.db.SetEpoch(
			epochStartSlot,
			0, // epoch
			tmpNonce,
			currentEra.Id,
			epochSlotLength,
			epochLength,
			txn,
		)
		if err != nil {
			return nil, fmt.Errorf("set epoch: %w", err)
		}
		// Load epoch info from DB to populate result
		epochs, err := ls.db.GetEpochs(txn)
		if err != nil {
			return nil, fmt.Errorf("load epochs: %w", err)
		}
		result.NewEpochCache = epochs
		if len(epochs) > 0 {
			result.NewCurrentEpoch = epochs[len(epochs)-1]
			eraDesc := eras.GetEraById(result.NewCurrentEpoch.EraId)
			if eraDesc == nil {
				return nil, fmt.Errorf(
					"unknown era ID %d",
					result.NewCurrentEpoch.EraId,
				)
			}
			result.NewCurrentEra = *eraDesc
			result.NewEpochNum = float64(result.NewCurrentEpoch.EpochId)
		}
		ls.config.Logger.Debug(
			"added initial epoch to DB",
			"epoch", fmt.Sprintf("%+v", result.NewCurrentEpoch),
			"component", "ledger",
		)
		return result, nil
	}
	// Apply pending pparam updates using the non-mutating version
	// Updates target the next epoch, so we pass currentEpoch.EpochId + 1
	// The quorum threshold comes from shelley-genesis.json updateQuorum
	updateQuorum := 0
	if shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis(); shelleyGenesis != nil {
		updateQuorum = shelleyGenesis.UpdateQuorum
	}
	newPParams, err := ls.db.ComputeAndApplyPParamUpdates(
		epochStartSlot,
		currentEpoch.EpochId+1, // Target epoch for updates
		currentEra.Id,
		updateQuorum,
		currentPParams,
		currentEra.DecodePParamsUpdateFunc,
		currentEra.PParamsUpdateFunc,
		txn,
	)
	if err != nil {
		return nil, fmt.Errorf("apply pparam updates: %w", err)
	}
	result.NewCurrentPParams = newPParams

	// Check if the protocol version changed in a way that
	// triggers a hard fork (era transition)
	oldVer, oldErr := GetProtocolVersion(currentPParams)
	newVer, newErr := GetProtocolVersion(newPParams)
	if oldErr != nil {
		ls.config.Logger.Warn(
			"could not extract protocol version from "+
				"current pparams, skipping hard fork "+
				"detection",
			"error", oldErr,
			"pparams_type",
			fmt.Sprintf("%T", currentPParams),
			"component", "ledger",
		)
	}
	if newErr != nil {
		ls.config.Logger.Warn(
			"could not extract protocol version from "+
				"new pparams, skipping hard fork "+
				"detection",
			"error", newErr,
			"pparams_type",
			fmt.Sprintf("%T", newPParams),
			"component", "ledger",
		)
	}
	if oldErr == nil && newErr == nil {
		if IsHardForkTransition(oldVer, newVer) {
			fromEra, _ := EraForVersion(oldVer.Major)
			toEra, _ := EraForVersion(newVer.Major)
			result.HardFork = &HardForkInfo{
				OldVersion: oldVer,
				NewVersion: newVer,
				FromEra:    fromEra,
				ToEra:      toEra,
			}
			ls.config.Logger.Info(
				"hard fork detected via protocol "+
					"parameter update",
				"from_era", fromEra,
				"to_era", toEra,
				"old_version",
				fmt.Sprintf(
					"%d.%d",
					oldVer.Major,
					oldVer.Minor,
				),
				"new_version",
				fmt.Sprintf(
					"%d.%d",
					newVer.Major,
					newVer.Minor,
				),
				"epoch",
				currentEpoch.EpochId+1,
				"component", "ledger",
			)
		}
	}

	// Create next epoch record
	epochSlotLength, epochLength, err := currentEra.EpochLengthFunc(
		ls.config.CardanoNodeConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("calculate epoch length: %w", err)
	}
	tmpNonce, err := ls.calculateEpochNonce(
		txn,
		epochStartSlot,
		currentEra,
		currentEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf("calculate epoch nonce: %w", err)
	}
	err = ls.db.SetEpoch(
		epochStartSlot,
		currentEpoch.EpochId+1,
		tmpNonce,
		currentEra.Id,
		epochSlotLength,
		epochLength,
		txn,
	)
	if err != nil {
		return nil, fmt.Errorf("set epoch: %w", err)
	}
	// Load epoch info from DB to populate result
	epochs, err := ls.db.GetEpochs(txn)
	if err != nil {
		return nil, fmt.Errorf("load epochs: %w", err)
	}
	result.NewEpochCache = epochs
	if len(epochs) > 0 {
		result.NewCurrentEpoch = epochs[len(epochs)-1]
		eraDesc := eras.GetEraById(result.NewCurrentEpoch.EraId)
		if eraDesc == nil {
			return nil, fmt.Errorf(
				"unknown era ID %d",
				result.NewCurrentEpoch.EraId,
			)
		}
		result.NewCurrentEra = *eraDesc
		result.NewEpochNum = float64(result.NewCurrentEpoch.EpochId)
		result.SchedulerIntervalMs = result.NewCurrentEpoch.SlotLength
	}

	ls.config.Logger.Debug(
		"added next epoch to DB",
		"epoch", fmt.Sprintf("%+v", result.NewCurrentEpoch),
		"component", "ledger",
	)
	return result, nil
}

func (ls *LedgerState) cleanupBlockNoncesBefore(startSlot uint64) {
	if startSlot == 0 {
		return
	}
	ls.config.Logger.Debug(
		fmt.Sprintf(
			"cleaning up non-checkpoint block nonces before slot %d",
			startSlot,
		),
		"component",
		"ledger",
	)
	ls.Lock()
	defer ls.Unlock()
	txn := ls.db.Transaction(true)
	if err := txn.Do(func(txn *database.Txn) error {
		return ls.db.DeleteBlockNoncesBeforeSlotWithoutCheckpoints(startSlot, txn)
	}); err != nil {
		ls.config.Logger.Error(
			fmt.Sprintf("failed to clean up old block nonces: %s", err),
			"component", "ledger",
		)
	}
}

func (ls *LedgerState) processBlockEvent(
	txn *database.Txn,
	e BlockfetchEvent,
) error {
	// Note: block header crypto verification (VRF, KES) is performed
	// outside the lock in processBlockEvents before this is called.

	// Add block to chain
	addBlockErr := ls.chain.AddBlock(e.Block, txn)
	if addBlockErr != nil {
		// Ignore and log errors about block not fitting on chain or matching first header
		if !errors.As(addBlockErr, &chain.BlockNotFitChainTipError{}) &&
			!errors.As(addBlockErr, &chain.BlockNotMatchHeaderError{}) {
			return fmt.Errorf("add chain block: %w", addBlockErr)
		}
		ls.config.Logger.Warn(
			fmt.Sprintf(
				"ignoring blockfetch block: %s",
				addBlockErr,
			),
		)
	}

	// Detect slot battles: check if an incoming block occupies a
	// slot for which we forged a block locally
	ls.checkSlotBattle(e, addBlockErr)

	return nil
}

// checkSlotBattle checks whether an incoming block from a peer
// occupies a slot for which the local node has already forged a
// block. If so, it emits a SlotBattleEvent and logs a warning.
//
// The addBlockErr parameter is the error (if any) returned by
// chain.AddBlock for the incoming block. A nil error means the
// remote block was accepted onto the chain (remote won); a
// non-nil error means it was rejected (local won).
//
// The caller must hold ls.Lock() (write lock). This method must not
// acquire ls.RLock(), because sync.RWMutex is not reentrant and
// attempting a read lock while holding the write lock deadlocks.
func (ls *LedgerState) checkSlotBattle(
	e BlockfetchEvent,
	addBlockErr error,
) {
	checker := ls.config.ForgedBlockChecker
	if checker == nil {
		return
	}

	incomingSlot := e.Point.Slot
	localHash, forged := checker.WasForgedByUs(incomingSlot)
	if !forged {
		return
	}

	remoteHash := e.Point.Hash

	// Same hash means same block -- not a battle
	if bytes.Equal(localHash, remoteHash) {
		return
	}

	// Determine winner: if the remote block was rejected (addBlockErr
	// != nil), our local block remains on chain, so we won.
	localWon := addBlockErr != nil

	ls.config.Logger.Warn(
		"slot battle detected",
		"component", "ledger",
		"slot", incomingSlot,
		"local_block_hash", hex.EncodeToString(localHash),
		"remote_block_hash", hex.EncodeToString(remoteHash),
		"local_won", localWon,
	)

	if ls.config.EventBus != nil {
		ls.config.EventBus.PublishAsync(
			forging.SlotBattleEventType,
			event.NewEvent(
				forging.SlotBattleEventType,
				forging.SlotBattleEvent{
					Slot:            incomingSlot,
					LocalBlockHash:  localHash,
					RemoteBlockHash: remoteHash,
					Won:             localWon,
				},
			),
		)
	}
}

func (ls *LedgerState) blockfetchRequestRangeStart(
	connId ouroboros.ConnectionId,
	start ocommon.Point,
	end ocommon.Point,
) error {
	err := ls.config.BlockfetchRequestRangeFunc(
		connId,
		start,
		end,
	)
	if err != nil {
		return fmt.Errorf("request block range: %w", err)
	}

	// Stop any existing timer before creating a new one
	if ls.chainsyncBlockfetchTimeoutTimer != nil {
		ls.chainsyncBlockfetchTimeoutTimer.Stop()
		ls.chainsyncBlockfetchTimeoutTimer = nil
	}

	// Increment generation counter to invalidate any pending timer callbacks
	ls.chainsyncBlockfetchTimerGeneration++
	currentGeneration := ls.chainsyncBlockfetchTimerGeneration

	// Start timeout timer for blockfetch operation
	// The timer fires if no blocks are received within blockfetchBusyTimeout
	// Each received block resets the timer in handleEventBlockfetchBlock
	ls.chainsyncBlockfetchTimeoutTimer = time.AfterFunc(
		blockfetchBusyTimeout,
		func() {
			ls.chainsyncBlockfetchMutex.Lock()
			defer ls.chainsyncBlockfetchMutex.Unlock()
			// Check if this timer callback is stale (a newer timer was started)
			if ls.chainsyncBlockfetchTimerGeneration != currentGeneration {
				return
			}
			ls.blockfetchRequestRangeCleanup()
			ls.config.Logger.Info(
				fmt.Sprintf(
					"blockfetch operation timed out after %s",
					blockfetchBusyTimeout,
				),
				"component",
				"ledger",
			)
		},
	)
	return nil
}

func (ls *LedgerState) blockfetchRequestRangeCleanup() {
	// Stop the timeout timer if running and invalidate any pending callbacks
	if ls.chainsyncBlockfetchTimeoutTimer != nil {
		ls.chainsyncBlockfetchTimeoutTimer.Stop()
		ls.chainsyncBlockfetchTimeoutTimer = nil
	}
	// Increment generation to ensure any pending timer callbacks are ignored
	ls.chainsyncBlockfetchTimerGeneration++
	// Reset buffer
	ls.chainsyncBlockEvents = slices.Delete(
		ls.chainsyncBlockEvents,
		0,
		len(ls.chainsyncBlockEvents),
	)
	// Close our blockfetch done signal channel
	ls.chainsyncBlockfetchReadyMutex.Lock()
	defer ls.chainsyncBlockfetchReadyMutex.Unlock()
	if ls.chainsyncBlockfetchReadyChan != nil {
		close(ls.chainsyncBlockfetchReadyChan)
		ls.chainsyncBlockfetchReadyChan = nil
	}
}

func (ls *LedgerState) handleEventBlockfetchBatchDone(e BlockfetchEvent) error {
	// Stop the blockfetch timeout timer and invalidate any pending callbacks
	if ls.chainsyncBlockfetchTimeoutTimer != nil {
		ls.chainsyncBlockfetchTimeoutTimer.Stop()
		ls.chainsyncBlockfetchTimeoutTimer = nil
	}
	ls.chainsyncBlockfetchTimerGeneration++
	// Process pending block events
	if err := ls.processBlockEvents(); err != nil {
		ls.blockfetchRequestRangeCleanup()
		return fmt.Errorf("process block events: %w", err)
	}
	// Continue fetching as long as there are queued headers
	remainingHeaders := ls.chain.HeaderCount()
	ls.config.Logger.Debug(
		"batch done, checking for more headers",
		"component", "ledger",
		"remaining_headers", remainingHeaders,
	)
	if remainingHeaders == 0 {
		// No more headers to fetch, allow chainsync to collect more
		ls.blockfetchRequestRangeCleanup()
		return nil
	}
	// Clean up from blockfetch batch
	ls.blockfetchRequestRangeCleanup()
	// Mark blockfetch as in progress for next batch
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	// Request next waiting bulk range
	headerStart, headerEnd := ls.chain.HeaderRange(blockfetchBatchSize)
	err := ls.blockfetchRequestRangeStart(
		e.ConnectionId,
		headerStart,
		headerEnd,
	)
	if err != nil {
		ls.blockfetchRequestRangeCleanup()
		return err
	}
	return nil
}

// logSyncProgress logs periodic sync progress at INFO level.
// It reports the current slot, upstream tip slot, percentage complete,
// and sync rate in slots per second. syncUpstreamTipSlot is read
// atomically since it is written by the chainsync handler goroutine.
func (ls *LedgerState) logSyncProgress(currentSlot uint64) {
	now := time.Now()
	if now.Sub(ls.syncProgressLastLog) < syncProgressLogInterval {
		return
	}
	upstreamTip := ls.syncUpstreamTipSlot.Load()
	if upstreamTip == 0 {
		// No upstream tip known yet, skip
		return
	}
	elapsed := now.Sub(ls.syncProgressLastLog).Seconds()
	var slotsPerSec float64
	if elapsed > 0 && ls.syncProgressLastSlot > 0 &&
		currentSlot >= ls.syncProgressLastSlot {
		slotsDelta := currentSlot - ls.syncProgressLastSlot
		slotsPerSec = float64(slotsDelta) / elapsed
	}
	var pct float64
	if upstreamTip > 0 {
		pct = float64(currentSlot) / float64(upstreamTip) * 100
		if pct > 100 {
			pct = 100
		}
	}
	ls.config.Logger.Info(
		fmt.Sprintf(
			"sync progress: slot %d/%d (%.1f%%), %.0f slots/sec",
			currentSlot,
			upstreamTip,
			pct,
			slotsPerSec,
		),
		"component", "ledger",
	)
	ls.syncProgressLastLog = now
	ls.syncProgressLastSlot = currentSlot
}

// SyncProgress returns the current sync progress as a value between
// 0.0 (unknown/just started) and 1.0 (fully synced). This implements
// the peergov.SyncProgressProvider interface, allowing the peer
// governor to exit bootstrap mode once sync reaches its threshold.
func (ls *LedgerState) SyncProgress() float64 {
	upstreamTip := ls.syncUpstreamTipSlot.Load()
	if upstreamTip == 0 {
		return 0
	}
	ls.RLock()
	currentSlot := ls.currentTip.Point.Slot
	ls.RUnlock()
	progress := float64(currentSlot) / float64(upstreamTip)
	if progress > 1.0 {
		progress = 1.0
	}
	return progress
}
