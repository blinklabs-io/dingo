// Copyright 2025 Blink Labs Software
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

package utxorpc

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	watch "github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch/watchconnect"
)

const watchTxUndoHistoryBlocks = 256

type watchTxHistoryEntry struct {
	point      ocommon.Point
	prevHash   []byte
	appliedTxs []*watch.AnyChainTx
}

// watchTxBuildForwardMessages returns stream messages for one forward block.
// For a forward block with no predicate matches, out contains a single Idle
// heartbeat with the block reference. The returned appliedTxs are used to build
// Undo messages if a rollback later invalidates this block.
func watchTxBuildForwardMessages(
	blockType uint,
	blockCbor []byte,
	blockSlot, blockHeight uint64,
	blockHash []byte,
	shouldSendTx func(ledger.Transaction) bool,
) (appliedTxs []*watch.AnyChainTx, out []*watch.WatchTxResponse, err error) {
	block, err := ledger.NewBlockFromCbor(blockType, blockCbor)
	if err != nil {
		return nil, nil, err
	}
	txs := block.Transactions()
	applies := make([]*watch.WatchTxResponse, 0, len(txs))
	applied := make([]*watch.AnyChainTx, 0, len(txs))
	for _, tx := range txs {
		tmpTx, err := tx.Utxorpc()
		if err != nil {
			return nil, nil, fmt.Errorf("convert transaction: %w", err)
		}
		if !shouldSendTx(tx) {
			continue
		}
		var act watch.AnyChainTx
		act.Chain = &watch.AnyChainTx_Cardano{Cardano: tmpTx}
		applied = append(applied, &act)
		applies = append(applies, &watch.WatchTxResponse{
			Action: &watch.WatchTxResponse_Apply{Apply: &act},
		})
	}
	if len(applies) == 0 {
		hashCopy := append([]byte(nil), blockHash...)
		idle := &watch.WatchTxResponse{
			Action: &watch.WatchTxResponse_Idle{
				Idle: &watch.BlockRef{
					Slot:   blockSlot,
					Hash:   hashCopy,
					Height: blockHeight,
				},
			},
		}
		return nil, []*watch.WatchTxResponse{idle}, nil
	}
	return applied, applies, nil
}

func pointsEqual(a, b ocommon.Point) bool {
	return a.Slot == b.Slot && bytes.Equal(a.Hash, b.Hash)
}

func watchTxBuildRollbackMessages(
	history *[]watchTxHistoryEntry,
	rollbackPoint ocommon.Point,
) (msgs []*watch.WatchTxResponse, found bool) {
	h := *history
	var out []*watch.WatchTxResponse
	for len(h) > 0 {
		last := h[len(h)-1]
		if pointsEqual(last.point, rollbackPoint) {
			found = true
			break
		}
		for i := len(last.appliedTxs) - 1; i >= 0; i-- {
			out = append(out, &watch.WatchTxResponse{
				Action: &watch.WatchTxResponse_Undo{
					Undo: last.appliedTxs[i],
				},
			})
		}
		h = h[:len(h)-1]
	}
	*history = h
	return out, found
}

func (s *watchServiceServer) watchTxFetchRollbackUndoFromBlocks(
	ctx context.Context,
	startHash []byte,
	rollbackPoint ocommon.Point,
	shouldSendTx func(ledger.Transaction) bool,
) ([]*watch.WatchTxResponse, error) {
	hash := append([]byte(nil), startHash...)
	out := make([]*watch.WatchTxResponse, 0, 64)
	const maxWalkBlocks = 2160
	for range maxWalkBlocks {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if len(hash) == 0 {
			return out, nil
		}
		block, err := s.utxorpc.config.LedgerState.BlockByHash(hash)
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				return out, nil
			}
			return nil, err
		}
		if pointsEqual(
			ocommon.NewPoint(block.Slot, block.Hash),
			rollbackPoint,
		) {
			return out, nil
		}
		// We walk strictly backward via PrevHash. Once we're at-or-before
		// rollback slot without exact point match, the rollback point cannot
		// be reached by continuing this walk.
		if block.Slot < rollbackPoint.Slot ||
			(block.Slot == rollbackPoint.Slot &&
				!bytes.Equal(block.Hash, rollbackPoint.Hash)) {
			return out, nil
		}
		appliedTxs, _, err := watchTxBuildForwardMessages(
			block.Type,
			block.Cbor,
			block.Slot,
			block.Number,
			block.Hash,
			shouldSendTx,
		)
		if err != nil {
			return nil, err
		}
		for j := len(appliedTxs) - 1; j >= 0; j-- {
			out = append(out, &watch.WatchTxResponse{
				Action: &watch.WatchTxResponse_Undo{
					Undo: appliedTxs[j],
				},
			})
		}
		hash = append([]byte(nil), block.PrevHash...)
	}
	return out, fmt.Errorf("rollback fetch exceeded %d blocks", maxWalkBlocks)
}

// watchServiceServer implements the WatchService API
type watchServiceServer struct {
	watchconnect.UnimplementedWatchServiceHandler
	utxorpc *Utxorpc
}

// WatchTx
func (s *watchServiceServer) WatchTx(
	ctx context.Context,
	req *connect.Request[watch.WatchTxRequest],
	stream *connect.ServerStream[watch.WatchTxResponse],
) error {
	predicate := req.Msg.GetPredicate() // Predicate
	fieldMask := req.Msg.GetFieldMask()
	intersect := req.Msg.GetIntersect() // []*BlockRef

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a WatchTx request with predicate %v and fieldMask %v and intersect %v",
			predicate,
			fieldMask,
			intersect,
		),
	)

	var predTree *txPredicateNode
	if predicate != nil {
		predTree = txPredicateFromWatch(predicate)
	}

	// Get our points
	var points []ocommon.Point
	if len(intersect) > 0 {
		for _, blockRef := range intersect {
			blockIdx := blockRef.GetSlot()
			blockHash := blockRef.GetHash()
			slot := blockIdx
			point := ocommon.NewPoint(slot, blockHash)
			points = append(points, point)
		}
	} else {
		point := s.utxorpc.config.LedgerState.Tip().Point
		points = append(points, point)
	}

	// Get our starting point matching our chain
	point, err := s.utxorpc.config.LedgerState.GetIntersectPoint(points)
	if err != nil {
		s.utxorpc.config.Logger.Error(
			"failed to get points",
			"error", err,
		)
		return err
	}
	if point == nil {
		s.utxorpc.config.Logger.Error(
			"nil point returned",
		)
		return errors.New("nil point returned")
	}

	// Create our chain iterator
	chainIter, err := s.utxorpc.config.LedgerState.GetChainFromPoint(
		*point,
		false,
	)
	if err != nil {
		s.utxorpc.config.Logger.Error(
			"failed to get chain iterator",
			"error", err,
		)
		return err
	}

	defer chainIter.Cancel()

	// Cancel the chain iterator when the gRPC stream context is
	// done so that blocking Next() calls unblock immediately.
	go func() {
		<-ctx.Done()
		chainIter.Cancel()
	}()

	shouldSend := func(tx ledger.Transaction) bool {
		if predicate == nil {
			return true
		}
		return s.utxorpc.matchTxPredicateNode(tx, predTree)
	}
	history := make([]watchTxHistoryEntry, 0, 256)

	for {
		next, err := chainIter.Next(true)
		if err != nil {
			if ctx.Err() != nil {
				s.utxorpc.config.Logger.Debug(
					"WatchTx client disconnected",
				)
				return ctx.Err()
			}
			s.utxorpc.config.Logger.Error(
				"failed to iterate chain",
				"error", err,
			)
			return err
		}
		if next == nil {
			continue
		}
		var msgs []*watch.WatchTxResponse
		if next.Rollback {
			var fetchCh chan struct {
				msgs []*watch.WatchTxResponse
				err  error
			}
			if len(history) > 0 {
				startHash := append([]byte(nil), history[0].prevHash...)
				fetchCh = make(chan struct {
					msgs []*watch.WatchTxResponse
					err  error
				}, 1)
				go func() {
					fetchedMsgs, fetchedErr := s.watchTxFetchRollbackUndoFromBlocks(
						ctx,
						startHash,
						next.Point,
						shouldSend,
					)
					fetchCh <- struct {
						msgs []*watch.WatchTxResponse
						err  error
					}{
						msgs: fetchedMsgs,
						err:  fetchedErr,
					}
				}()
			}

			historyMsgs, found := watchTxBuildRollbackMessages(&history, next.Point)
			msgs = append(msgs, historyMsgs...)
			if !found && fetchCh != nil {
				fetched := <-fetchCh
				if fetched.err != nil {
					s.utxorpc.config.Logger.Error(
						"WatchTx rollback fetch failed",
						"error", fetched.err,
					)
					return fetched.err
				}
				msgs = append(msgs, fetched.msgs...)
			}
			s.utxorpc.config.Logger.Debug(
				"WatchTx processed rollback",
				"slot", next.Point.Slot,
				"hash", hex.EncodeToString(next.Point.Hash),
				"undo_count", len(msgs),
			)
		} else {
			appliedTxs, forwardMsgs, err := watchTxBuildForwardMessages(
				next.Block.Type,
				next.Block.Cbor,
				next.Block.Slot,
				next.Block.Number,
				next.Block.Hash,
				shouldSend,
			)
			if err != nil {
				s.utxorpc.config.Logger.Error(
					"failed to get block",
					"error", err,
				)
				return err
			}
			msgs = forwardMsgs
			history = append(history, watchTxHistoryEntry{
				point:      ocommon.NewPoint(next.Block.Slot, next.Block.Hash),
				prevHash:   append([]byte(nil), next.Block.PrevHash...),
				appliedTxs: appliedTxs,
			})
			if len(history) > watchTxUndoHistoryBlocks {
				history = history[len(history)-watchTxUndoHistoryBlocks:]
			}
		}
		for _, resp := range msgs {
			if err := stream.Send(resp); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return err
			}
		}
	}
}
