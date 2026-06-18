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

package mithril

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// fetchGapBlocks connects to network relay peers and fetches blocks
// between start and end (inclusive). The start block is skipped since
// it already exists in the blob store. This closes the gap between
// the ImmutableDB tip and the ledger state tip after Mithril bootstrap.
// It tries each bootstrap peer in order, falling back to the next on
// failure.
func fetchGapBlocks(
	ctx context.Context,
	logger *slog.Logger,
	network string,
	start ocommon.Point,
	end ocommon.Point,
) ([]models.Block, error) {
	// Resolve network info for magic and bootstrap peers
	netInfo, ok := ouroboros.NetworkByName(network)
	if !ok {
		return nil, fmt.Errorf("unknown network: %s", network)
	}
	if len(netInfo.BootstrapPeers) == 0 {
		return nil, fmt.Errorf(
			"no bootstrap peers for network %s", network,
		)
	}

	var lastErr error
	for i, peer := range netInfo.BootstrapPeers {
		peerAddr := net.JoinHostPort(
			peer.Address,
			strconv.FormatUint(uint64(peer.Port), 10),
		)

		blocks, err := fetchGapBlocksFromPeer(
			ctx, logger, netInfo.NetworkMagic,
			peerAddr, start, end,
		)
		if err == nil {
			return blocks, nil
		}

		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		logger.Warn(
			"failed to fetch gap blocks from peer, "+
				"trying next",
			"component", "mithril",
			"peer", peerAddr,
			"peer_index", i,
			"total_peers", len(netInfo.BootstrapPeers),
			"error", err,
		)
	}

	return nil, fmt.Errorf(
		"all %d bootstrap peers failed: %w",
		len(netInfo.BootstrapPeers),
		lastErr,
	)
}

// fetchGapBlocksFromPeer fetches gap blocks from a single peer.
func fetchGapBlocksFromPeer(
	ctx context.Context,
	logger *slog.Logger,
	networkMagic uint32,
	peerAddr string,
	start ocommon.Point,
	end ocommon.Point,
) ([]models.Block, error) {
	logger.Info(
		"connecting to relay for volatile blocks",
		"component", "mithril",
		"peer", peerAddr,
	)

	// Collect blocks via callbacks. The start block is skipped
	// because it already exists in the blob store.
	var mu sync.Mutex
	var blocks []models.Block
	done := make(chan struct{}, 1)

	blockFunc := func(
		_ blockfetch.CallbackContext,
		blockType uint,
		block gledger.Block,
	) error {
		if block.SlotNumber() <= start.Slot {
			return nil
		}
		b := models.Block{
			Slot:     block.SlotNumber(),
			Hash:     block.Hash().Bytes(),
			Cbor:     block.Cbor(),
			Number:   block.BlockNumber(),
			Type:     blockType,
			PrevHash: block.PrevHash().Bytes(),
		}
		mu.Lock()
		blocks = append(blocks, b)
		mu.Unlock()
		return nil
	}

	batchDoneFunc := func(_ blockfetch.CallbackContext) error {
		select {
		case done <- struct{}{}:
		default:
		}
		return nil
	}

	// Establish TCP connection to the relay peer
	dialer := net.Dialer{Timeout: 30 * time.Second}
	tcpConn, err := dialer.DialContext(ctx, "tcp", peerAddr)
	if err != nil {
		return nil, fmt.Errorf(
			"connecting to peer %s: %w", peerAddr, err,
		)
	}

	blockfetchCfg, err := blockfetch.NewConfig(
		blockfetch.WithBlockFunc(blockFunc),
		blockfetch.WithBatchDoneFunc(batchDoneFunc),
		blockfetch.WithBatchStartTimeout(30*time.Second),
		blockfetch.WithBlockTimeout(60*time.Second),
	)
	if err != nil {
		tcpConn.Close()
		return nil, fmt.Errorf("creating blockfetch config: %w", err)
	}

	// Create ouroboros connection with blockfetch protocol
	oConn, err := ouroboros.NewConnection(
		ouroboros.WithConnection(tcpConn),
		ouroboros.WithNetworkMagic(networkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithBlockFetchConfig(blockfetchCfg),
	)
	if err != nil {
		tcpConn.Close()
		return nil, fmt.Errorf(
			"creating ouroboros connection: %w", err,
		)
	}
	defer oConn.Close()

	// Request block range [start, end] inclusive
	if err := oConn.BlockFetch().Client.GetBlockRange(
		start, end,
	); err != nil {
		return nil, fmt.Errorf(
			"block range request failed: %w", err,
		)
	}

	// Wait for batch completion with a safety timeout
	// in case the peer disconnects without triggering
	// batchDoneFunc (e.g. protocol error, peer doesn't
	// have the range). Also monitor the ouroboros
	// connection's error channel for immediate failure
	// detection on peer disconnect or protocol error.
	timer := time.NewTimer(5 * time.Minute)
	defer timer.Stop()
	select {
	case <-done:
		// All blocks received
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-oConn.ErrorChan():
		// The peer may close the connection right after
		// sending the BatchDone message. The connection
		// monitor can fire before the protocol handler
		// processes BatchDone. Wait briefly for
		// batchDoneFunc to complete before treating the
		// error as genuine.
		graceTimer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-done:
			graceTimer.Stop()
		case <-graceTimer.C:
			return nil, fmt.Errorf(
				"ouroboros connection error during "+
					"block fetch: %w",
				err,
			)
		}
	case <-timer.C:
		return nil, errors.New(
			"timed out waiting for gap blocks from relay",
		)
	}

	logger.Info(
		"fetched volatile blocks from relay",
		"component", "mithril",
		"count", len(blocks),
		"peer", peerAddr,
	)

	return blocks, nil
}

// deleteBlobBlocksAboveSlot removes every block from the blob store
// whose slot is greater than the given threshold AND drops any
// metadata indexed for those blocks (transactions, UTxOs, governance
// proposals/votes), and restores any UTxOs that the rejected gap
// blocks had marked spent. Used to clean up rejected gap blocks from
// a previous failed Mithril resume so the next BlocksRecent query and
// any slot-ordered iterator (chainsync replay, etc.) cannot resurface
// them, and so leftover metadata from the discarded fork cannot
// shadow the refetched chain.
func deleteBlobBlocksAboveSlot(
	db *database.Database,
	slot uint64,
) error {
	if slot == ^uint64(0) {
		return nil
	}
	iter := db.BlocksInRange(slot+1, ^uint64(0))
	var stale []ocommon.Point
	for {
		next, err := iter.NextRaw()
		if err != nil {
			iter.Close()
			return fmt.Errorf(
				"iterating stale gap blocks above slot %d: %w",
				slot, err,
			)
		}
		if next == nil {
			break
		}
		stale = append(stale, ocommon.Point{
			Slot: next.Slot,
			Hash: append([]byte(nil), next.Hash...),
		})
	}
	iter.Close()
	if len(stale) == 0 {
		return nil
	}
	txn := db.Transaction(true)
	return txn.Do(func(txn *database.Txn) error {
		for _, p := range stale {
			block, err := database.BlockByPointTxn(txn, p)
			if err != nil {
				if errors.Is(err, models.ErrBlockNotFound) {
					continue
				}
				return fmt.Errorf(
					"lookup stale gap block at slot %d: %w",
					p.Slot, err,
				)
			}
			if err := database.BlockDeleteTxn(txn, block); err != nil {
				return fmt.Errorf(
					"delete stale gap block at slot %d: %w",
					p.Slot, err,
				)
			}
		}
		// Drop metadata indexed for the rejected blocks. Without this
		// the refetched chain would inherit stale UTxO consumption,
		// duplicate tx records, and orphan governance proposals/votes
		// from the previous run's gap-block processing.
		if err := db.UtxosDeleteRolledback(slot, txn); err != nil {
			return fmt.Errorf(
				"delete rolled-back UTxOs above slot %d: %w",
				slot, err,
			)
		}
		if err := db.TransactionsDeleteRolledback(slot, txn); err != nil {
			return fmt.Errorf(
				"delete rolled-back transactions above slot %d: %w",
				slot, err,
			)
		}
		if err := db.UtxosUnspend(slot, txn); err != nil {
			return fmt.Errorf(
				"restore UTxOs spent above slot %d: %w",
				slot, err,
			)
		}
		if err := db.DeleteGovernanceVotesAfterSlot(
			slot, txn,
		); err != nil {
			return fmt.Errorf(
				"delete rolled-back governance votes above slot %d: %w",
				slot, err,
			)
		}
		if err := db.DeleteGovernanceProposalsAfterSlot(
			slot, txn,
		); err != nil {
			return fmt.Errorf(
				"delete rolled-back governance proposals above slot %d: %w",
				slot, err,
			)
		}
		return nil
	})
}

func loadGapBlocksFromBlob(
	db *database.Database,
	startSlot uint64,
	endSlot uint64,
) ([]models.Block, error) {
	if startSlot > endSlot {
		return []models.Block{}, nil
	}
	iter := db.BlocksInRange(startSlot, endSlot)
	defer iter.Close()
	ret := make([]models.Block, 0)
	for {
		next, err := iter.NextRaw()
		if err != nil {
			return nil, fmt.Errorf(
				"iterate blob blocks in range [%d,%d]: %w",
				startSlot,
				endSlot,
				err,
			)
		}
		if next == nil {
			break
		}
		ret = append(ret, models.Block{
			Slot:     next.Slot,
			Hash:     append([]byte(nil), next.Hash...),
			PrevHash: append([]byte(nil), next.PrevHash...),
			Cbor:     append([]byte(nil), next.Cbor...),
			Number:   next.Height,
			Type:     next.BlockType,
		})
	}
	return ret, nil
}

// validateStoredGapContinuity verifies the stored gap is non-empty,
// chains from immutableTip, and is internally hash-linked. It does not
// check the terminal block hash, so it is safe to use for partial gaps
// that don't yet reach the ledger state tip.
func validateStoredGapContinuity(
	blocks []models.Block,
	immutableTip models.Block,
) error {
	if len(blocks) == 0 {
		return fmt.Errorf(
			"stored volatile gap is empty after immutable tip slot %d",
			immutableTip.Slot,
		)
	}
	if !bytes.Equal(blocks[0].PrevHash, immutableTip.Hash) {
		return fmt.Errorf(
			"stored gap first block at slot %d prev hash %x does not match immutable tip slot %d hash %x",
			blocks[0].Slot,
			blocks[0].PrevHash,
			immutableTip.Slot,
			immutableTip.Hash,
		)
	}
	for i := 1; i < len(blocks); i++ {
		if bytes.Equal(blocks[i].PrevHash, blocks[i-1].Hash) {
			continue
		}
		return fmt.Errorf(
			"stored gap block at slot %d prev hash %x does not match previous block slot %d hash %x",
			blocks[i].Slot,
			blocks[i].PrevHash,
			blocks[i-1].Slot,
			blocks[i-1].Hash,
		)
	}
	return nil
}

func validateStoredGapBlocks(
	blocks []models.Block,
	immutableTip models.Block,
	ledgerStateHash []byte,
) error {
	if err := validateStoredGapContinuity(blocks, immutableTip); err != nil {
		return err
	}
	if len(blocks) == 0 {
		return fmt.Errorf(
			"stored volatile gap is empty after immutable tip slot %d",
			immutableTip.Slot,
		)
	}
	last := blocks[len(blocks)-1]
	if !bytes.Equal(last.Hash, ledgerStateHash) {
		return fmt.Errorf(
			"stored gap terminal block at slot %d hash %x does not match ledger state hash %x",
			last.Slot,
			last.Hash,
			ledgerStateHash,
		)
	}
	return nil
}

func processPostLedgerStateBlocks(
	ctx context.Context,
	db *database.Database,
	logger *slog.Logger,
	ledgerStateSlot uint64,
	ledgerStateHash []byte,
) error {
	recentBlocks, err := database.BlocksRecent(db, 1)
	if err != nil {
		return fmt.Errorf("reading chain tip for post-ledger processing: %w", err)
	}
	if len(recentBlocks) == 0 || recentBlocks[0].Slot <= ledgerStateSlot {
		return nil
	}
	logger.Info(
		"processing post-ledger-state blocks from blob store",
		"component", "mithril",
		"ledger_state_slot", ledgerStateSlot,
		"stored_tip_slot", recentBlocks[0].Slot,
	)
	postLedgerBlocks, err := loadGapBlocksFromBlob(
		db,
		ledgerStateSlot+1,
		recentBlocks[0].Slot,
	)
	if err != nil {
		return fmt.Errorf(
			"loading post-ledger-state blocks from blob store: %w",
			err,
		)
	}
	if err := validateStoredGapContinuity(
		postLedgerBlocks,
		models.Block{Slot: ledgerStateSlot, Hash: ledgerStateHash},
	); err != nil {
		return fmt.Errorf(
			"validating post-ledger-state block continuity: %w",
			err,
		)
	}
	if err := processGapBlocks(ctx, db, logger, postLedgerBlocks); err != nil {
		return fmt.Errorf(
			"processing post-ledger-state block transactions: %w",
			err,
		)
	}
	return nil
}

// processGapBlocks computes byte offsets and stores transaction
// metadata for gap blocks that were fetched from a relay peer.
// BlockCreate only writes raw CBOR to the blob store; this function
// adds the offset data and metadata records needed for UTxO resolution
// and transaction lookups.
func processGapBlocks(
	ctx context.Context,
	db *database.Database,
	logger *slog.Logger,
	blocks []models.Block,
) error {
	if len(blocks) == 0 {
		return nil
	}
	epochs, err := db.GetEpochs(nil)
	if err != nil {
		return fmt.Errorf("loading epochs for gap blocks: %w", err)
	}
	conwayPParamsCache := make(map[uint64]*conway.ConwayProtocolParameters)
	for _, block := range blocks {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("cancelled: %w", err)
		}
		// Gap blocks were already parsed and body-hash validated by the
		// blockfetch client when fetched from the relay. We only need a
		// second decode here to extract transactions and offsets.
		parsedBlock, err := gledger.NewBlockFromCbor(
			block.Type,
			block.Cbor,
			lcommon.VerifyConfig{SkipBodyHashValidation: true},
		)
		if err != nil {
			return fmt.Errorf(
				"parsing gap block at slot %d: %w",
				block.Slot, err,
			)
		}
		txs := parsedBlock.Transactions()
		if len(txs) == 0 {
			continue
		}
		indexer := database.NewBlockIndexer(
			block.Slot, block.Hash,
		)
		offsets, err := indexer.ComputeOffsets(
			block.Cbor, parsedBlock,
		)
		if err != nil {
			return fmt.Errorf(
				"computing offsets for gap block at slot %d: %w",
				block.Slot, err,
			)
		}
		point := ocommon.NewPoint(block.Slot, block.Hash)
		epoch, err := gapBlockEpoch(epochs, block.Slot)
		if err != nil {
			return fmt.Errorf(
				"resolving epoch for gap block at slot %d: %w",
				block.Slot,
				err,
			)
		}
		blockConwayPParams := (*conway.ConwayProtocolParameters)(nil)
		if epoch.EraId == conway.EraIdConway {
			cached, ok := conwayPParamsCache[epoch.EpochId]
			if !ok {
				pparams, err := db.GetPParams(
					epoch.EpochId,
					eras.ConwayEraDesc.Id,
					eras.ConwayEraDesc.DecodePParamsFunc,
					nil,
				)
				if err != nil {
					return fmt.Errorf(
						"loading protocol parameters for gap block at slot %d (epoch %d): %w",
						block.Slot,
						epoch.EpochId,
						err,
					)
				}
				if pparams != nil {
					cached, ok = pparams.(*conway.ConwayProtocolParameters)
					if !ok {
						return fmt.Errorf(
							"unexpected protocol params %T for gap block at slot %d (epoch %d)",
							pparams,
							block.Slot,
							epoch.EpochId,
						)
					}
				}
				conwayPParamsCache[epoch.EpochId] = cached
			}
			blockConwayPParams = cached
		}
		if err := processGapBlockTransactions(
			db,
			point,
			txs,
			offsets,
			epoch.EpochId,
			blockConwayPParams,
		); err != nil {
			return fmt.Errorf(
				"processing gap block at slot %d: %w",
				block.Slot, err,
			)
		}
		logger.Debug(
			"processed gap block transactions",
			"component", "mithril",
			"slot", block.Slot,
			"tx_count", len(txs),
		)
	}
	return nil
}

func processGapBlockTransactions(
	db *database.Database,
	point ocommon.Point,
	txs []lcommon.Transaction,
	offsets *database.BlockIngestionResult,
	epochId uint64,
	conwayPParams *conway.ConwayProtocolParameters,
) error {
	txn := db.Transaction(true)
	defer txn.Release()
	for i, tx := range txs {
		// Gap blocks are already reflected in the Mithril snapshot's
		// UTxO set, so input UTxOs are already consumed. Store the TX
		// record and blob offsets without re-consuming inputs.
		if err := db.SetGapBlockTransaction(
			tx,
			point,
			uint32(i), // #nosec G115 -- tx index within a block
			offsets,
			txn,
		); err != nil {
			return fmt.Errorf("storing TX: %w", err)
		}
		if !tx.IsValid() {
			continue
		}
		hasGovernance := len(tx.ProposalProcedures()) > 0 ||
			len(tx.VotingProcedures()) > 0
		if !hasGovernance {
			continue
		}
		if conwayPParams == nil {
			return errors.New(
				"missing Conway protocol parameters for governance gap block processing",
			)
		}
		if err := governance.ProcessProposals(
			tx,
			point,
			epochId,
			conwayPParams.GovActionValidityPeriod,
			db,
			txn,
		); err != nil {
			return fmt.Errorf(
				"processing governance proposals: %w",
				err,
			)
		}
		if err := governance.ProcessVotes(
			tx,
			point,
			epochId,
			conwayPParams.DRepInactivityPeriod,
			db,
			txn,
		); err != nil {
			return fmt.Errorf(
				"processing governance votes: %w",
				err,
			)
		}
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

// gapBlockEpoch resolves the epoch containing slot from a slice of
// epochs ordered by ascending StartSlot (as returned by db.GetEpochs).
// It enforces the upper bound StartSlot + LengthInSlots so a slot past
// the last known epoch surfaces an error rather than silently binding
// to the final entry.
func gapBlockEpoch(
	epochs []models.Epoch,
	slot uint64,
) (models.Epoch, error) {
	for _, epoch := range slices.Backward(epochs) {
		if slot < epoch.StartSlot {
			continue
		}
		end := epoch.StartSlot + uint64(epoch.LengthInSlots)
		if epoch.LengthInSlots > 0 && slot >= end {
			return models.Epoch{}, fmt.Errorf(
				"slot %d is past the end of the last known epoch %d (slots %d..%d)",
				slot,
				epoch.EpochId,
				epoch.StartSlot,
				end,
			)
		}
		return epoch, nil
	}
	return models.Epoch{}, fmt.Errorf("no epoch found for slot %d", slot)
}
