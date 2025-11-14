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

package ouroboros

import (
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"strconv"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/peergov"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol"
	oblockfetch "github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	olocaltxmonitor "github.com/blinklabs-io/gouroboros/protocol/localtxmonitor"
	olocaltxsubmission "github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
	opeersharing "github.com/blinklabs-io/gouroboros/protocol/peersharing"
	otxsubmission "github.com/blinklabs-io/gouroboros/protocol/txsubmission"
)

type NodeInterface interface {
	ConnManager() *connmanager.ConnectionManager
	LedgerState() *ledger.LedgerState
	Mempool() *mempool.Mempool
	Config() ConfigInterface
	ChainsyncState() *chainsync.State
	PeerGov() *peergov.PeerGovernor
	EventBus() *event.EventBus
}

type ConfigInterface interface {
	IntersectTip() bool
	IntersectPoints() []ocommon.Point
	Logger() *slog.Logger
}

type Ouroboros struct {
	config OuroborosConfig
}

type OuroborosConfig struct {
	NetworkMagic uint32
	Node         NodeInterface
}

func NewOuroboros(cfg OuroborosConfig) *Ouroboros {
	return &Ouroboros{
		config: cfg,
	}
}

// Placeholder methods, will be moved from top-level files
func (o *Ouroboros) ChainsyncClientStart(connId ConnectionId) error {
	return ChainsyncClientStart(
		o.config.Node.ConnManager(),
		o.config.Node.LedgerState(),
		o.config.Node.Config().IntersectTip(),
		o.config.Node.Config().IntersectPoints(),
		connId,
	)
}

func (o *Ouroboros) TxSubmissionClientStart(connId ConnectionId) error {
	return TxsubmissionClientStart(
		o.config.Node.ConnManager(),
		o.config.Node.Mempool(),
		connId,
	)
}

func (o *Ouroboros) ChainsyncServerConnOpts() []ochainsync.ChainSyncOptionFunc {
	return ChainsyncServerConnOpts(
		o.ChainsyncServerFindIntersect,
		o.ChainsyncServerRequestNext,
	)
}

func (o *Ouroboros) ChainsyncClientConnOpts() []ochainsync.ChainSyncOptionFunc {
	return ChainsyncClientConnOpts(
		o.ChainsyncClientRollForward,
		o.ChainsyncClientRollBackward,
	)
}

func (o *Ouroboros) ChainsyncServerFindIntersect(
	ctx ochainsync.CallbackContext,
	points []ocommon.Point,
) (ocommon.Point, ochainsync.Tip, error) {
	o.config.Node.LedgerState().RLock()
	defer o.config.Node.LedgerState().RUnlock()
	var retPoint ocommon.Point
	var retTip ochainsync.Tip
	// Find intersection
	intersectPoint, err := o.config.Node.LedgerState().GetIntersectPoint(points)
	if err != nil {
		return retPoint, retTip, err
	}

	// Populate return tip
	retTip = o.config.Node.LedgerState().Tip()

	if intersectPoint == nil {
		return retPoint, retTip, ochainsync.ErrIntersectNotFound
	}

	// Add our client to the chainsync state
	_, err = o.config.Node.ChainsyncState().AddClient(
		ctx.ConnectionId,
		*intersectPoint,
	)
	if err != nil {
		return retPoint, retTip, err
	}

	// Populate return point
	retPoint = *intersectPoint

	return retPoint, retTip, nil
}

func (o *Ouroboros) ChainsyncServerRequestNext(
	ctx ochainsync.CallbackContext,
) error {
	// Create/retrieve chainsync state for connection
	tip := o.config.Node.LedgerState().Tip()
	clientState, err := o.config.Node.ChainsyncState().AddClient(
		ctx.ConnectionId,
		tip.Point,
	)
	if err != nil {
		return err
	}
	if clientState.NeedsInitialRollback {
		err := ctx.Server.RollBackward(
			clientState.Cursor,
			tip,
		)
		if err != nil {
			return err
		}
		clientState.NeedsInitialRollback = false
		return nil
	}
	// Check for available block
	next, err := clientState.ChainIter.Next(false)
	if err != nil {
		if !errors.Is(err, chain.ErrIteratorChainTip) {
			return err
		}
	}
	if next != nil {
		if next.Rollback {
			err = ctx.Server.RollBackward(
				next.Point,
				tip,
			)
		} else {
			err = ctx.Server.RollForward(
				next.Block.Type,
				next.Block.Cbor,
				tip,
			)
		}
		return err
	}
	// Send AwaitReply
	if err := ctx.Server.AwaitReply(); err != nil {
		return err
	}
	// Wait for next block and send
	go func() {
		next, _ := clientState.ChainIter.Next(true)
		if next == nil {
			return
		}
		tip := o.config.Node.LedgerState().Tip()
		if next.Rollback {
			_ = ctx.Server.RollBackward(
				next.Point,
				tip,
			)
		} else {
			_ = ctx.Server.RollForward(
				next.Block.Type,
				next.Block.Cbor,
				tip,
			)
		}
	}()
	return nil
}

func (o *Ouroboros) ChainsyncClientRollBackward(
	ctx ochainsync.CallbackContext,
	point ocommon.Point,
	tip ochainsync.Tip,
) error {
	// Generate event
	o.config.Node.EventBus().Publish(
		ledger.ChainsyncEventType,
		event.NewEvent(
			ledger.ChainsyncEventType,
			ledger.ChainsyncEvent{
				Rollback: true,
				Point:    point,
				Tip:      tip,
			},
		),
	)
	return nil
}

func (o *Ouroboros) ChainsyncClientRollForward(
	ctx ochainsync.CallbackContext,
	blockType uint,
	blockData any,
	tip ochainsync.Tip,
) error {
	switch v := blockData.(type) {
	case gledger.BlockHeader:
		blockSlot := v.SlotNumber()
		blockHash := v.Hash().Bytes()
		o.config.Node.EventBus().Publish(
			ledger.ChainsyncEventType,
			event.NewEvent(
				ledger.ChainsyncEventType,
				ledger.ChainsyncEvent{
					ConnectionId: ctx.ConnectionId,
					Point:        ocommon.NewPoint(blockSlot, blockHash),
					Type:         blockType,
					BlockHeader:  v,
					Tip:          tip,
				},
			),
		)
	default:
		return fmt.Errorf("unexpected block data type: %T", v)
	}
	return nil
}

func (o *Ouroboros) BlockfetchServerConnOpts() []oblockfetch.BlockFetchOptionFunc {
	return BlockfetchServerConnOpts(o.BlockfetchServerRequestRange)
}

func (o *Ouroboros) BlockfetchClientConnOpts() []oblockfetch.BlockFetchOptionFunc {
	return BlockfetchClientConnOpts(
		o.BlockfetchClientBlock,
		o.BlockfetchClientBatchDone,
	)
}

func (o *Ouroboros) BlockfetchServerRequestRange(
	ctx oblockfetch.CallbackContext,
	start ocommon.Point,
	end ocommon.Point,
) error {
	// TODO: check if we have requested block range available and send NoBlocks if not (#397)
	chainIter, err := o.config.Node.LedgerState().GetChainFromPoint(start, true)
	if err != nil {
		return err
	}
	// Start async process to send requested block range
	go func() {
		if err := ctx.Server.StartBatch(); err != nil {
			return
		}
		for {
			next, _ := chainIter.Next(false)
			if next == nil {
				break
			}
			if next.Block.Slot > end.Slot {
				break
			}
			blockBytes := next.Block.Cbor
			err := ctx.Server.Block(
				next.Block.Type,
				blockBytes,
			)
			if err != nil {
				// TODO: push this error somewhere (#398)
				return
			}
			// Make sure we don't hang waiting for the next block if we've already hit the end
			if next.Block.Slot == end.Slot {
				break
			}
		}
		if err := ctx.Server.BatchDone(); err != nil {
			return
		}
	}()
	return nil
}

func (o *Ouroboros) BlockfetchClientBlock(
	ctx oblockfetch.CallbackContext,
	blockType uint,
	block gledger.Block,
) error {
	// Generate event
	o.config.Node.EventBus().Publish(
		ledger.BlockfetchEventType,
		event.NewEvent(
			ledger.BlockfetchEventType,
			ledger.BlockfetchEvent{
				Point: ocommon.NewPoint(
					block.SlotNumber(),
					block.Hash().Bytes(),
				),
				Type:  blockType,
				Block: block,
			},
		),
	)
	return nil
}

func (o *Ouroboros) BlockfetchClientBatchDone(
	ctx oblockfetch.CallbackContext,
) error {
	// Generate event
	o.config.Node.EventBus().Publish(
		ledger.BlockfetchEventType,
		event.NewEvent(
			ledger.BlockfetchEventType,
			ledger.BlockfetchEvent{
				ConnectionId: ctx.ConnectionId,
				BatchDone:    true,
			},
		),
	)
	return nil
}

func (o *Ouroboros) BlockfetchClientRequestRange(
	connId ConnectionId,
	start ocommon.Point,
	end ocommon.Point,
) error {
	return BlockfetchClientRequestRange(
		o.config.Node.ConnManager(),
		connId,
		start,
		end,
	)
}

func (o *Ouroboros) TxsubmissionServerConnOpts() []otxsubmission.TxSubmissionOptionFunc {
	return TxsubmissionServerConnOpts(o.TxsubmissionServerInit)
}

func (o *Ouroboros) TxsubmissionClientConnOpts() []otxsubmission.TxSubmissionOptionFunc {
	return TxsubmissionClientConnOpts(
		o.TxsubmissionClientRequestTxIds,
		o.TxsubmissionClientRequestTxs,
	)
}

func (o *Ouroboros) TxsubmissionServerInit(
	ctx otxsubmission.CallbackContext,
) error {
	// Start async loop to request transactions from the peer's mempool
	go func() {
		for {
			// Request available TX IDs (era and TX hash) and sizes
			// We make the request blocking to avoid looping on our side
			txIds, err := ctx.Server.RequestTxIds(
				true,
				10, // txsubmissionRequestTxIdsCount
			)
			if err != nil {
				// Peer requested shutdown
				if errors.Is(err, otxsubmission.ErrStopServerProcess) {
					return
				}
				// Don't log on connection close
				if errors.Is(err, protocol.ErrProtocolShuttingDown) {
					return
				}
				o.config.Node.Config().Logger().Error(
					fmt.Sprintf(
						"failed to get TxIds: %s",
						err,
					),
					"component", "network",
					"protocol", "tx-submission",
					"role", "server",
					"connection_id", ctx.ConnectionId.String(),
				)
				return
			}
			if len(txIds) > 0 {
				// Unwrap inner TxId from TxIdAndSize
				var requestTxIds []otxsubmission.TxId
				for _, txId := range txIds {
					requestTxIds = append(requestTxIds, txId.TxId)
				}
				// Request TX content for TxIds from above
				txs, err := ctx.Server.RequestTxs(requestTxIds)
				if err != nil {
					o.config.Node.Config().Logger().Error(
						fmt.Sprintf(
							"failed to get Txs: %s",
							err,
						),
						"component", "network",
						"protocol", "tx-submission",
						"role", "server",
						"connection_id", ctx.ConnectionId.String(),
					)
					return
				}
				for _, txBody := range txs {
					// Decode TX from CBOR
					tx, err := gledger.NewTransactionFromCbor(
						uint(txBody.EraId),
						txBody.TxBody,
					)
					if err != nil {
						o.config.Node.Config().Logger().Error(
							fmt.Sprintf(
								"failed to parse transaction CBOR: %s",
								err,
							),
							"component", "network",
							"protocol", "tx-submission",
							"role", "server",
							"connection_id", ctx.ConnectionId.String(),
						)
						return
					}
					o.config.Node.Config().Logger().Debug(
						"received tx",
						"tx_hash", tx.Hash(),
						"protocol", "tx-submission",
						"role", "server",
						"connection_id", ctx.ConnectionId.String(),
					)
					// Add transaction to mempool
					err = o.config.Node.Mempool().AddTransaction(
						uint(txBody.EraId),
						txBody.TxBody,
					)
					if err != nil {
						o.config.Node.Config().Logger().Error(
							fmt.Sprintf(
								"failed to add tx %x to mempool: %s",
								tx.Hash(),
								err,
							),
							"component", "network",
							"protocol", "tx-submission",
							"role", "server",
							"connection_id", ctx.ConnectionId.String(),
						)
						return
					}
				}
			}
		}
	}()
	return nil
}

func (o *Ouroboros) TxsubmissionClientRequestTxIds(
	ctx otxsubmission.CallbackContext,
	blocking bool,
	ack uint16,
	req uint16,
) ([]otxsubmission.TxIdAndSize, error) {
	connId := ctx.ConnectionId
	ret := []otxsubmission.TxIdAndSize{}
	consumer := o.config.Node.Mempool().Consumer(connId)
	// Clear TX cache
	if ack > 0 {
		consumer.ClearCache()
	}
	// Get available TXs
	var tmpTxs []*mempool.MempoolTransaction
	for {
		// nolint:staticcheck
		// The linter wants to move this up into the loop condition, but it's more readable this way
		if len(tmpTxs) >= int(req) {
			break
		}
		if blocking && len(tmpTxs) == 0 {
			// Wait until we see a TX
			tmpTx := consumer.NextTx(true)
			if tmpTx == nil {
				break
			}
			tmpTxs = append(tmpTxs, tmpTx)
		} else {
			// Return immediately if no TX is available
			tmpTx := consumer.NextTx(false)
			if tmpTx == nil {
				break
			}
			tmpTxs = append(tmpTxs, tmpTx)
		}
	}
	for _, tmpTx := range tmpTxs {
		// Add to return value
		txHashBytes, err := hex.DecodeString(tmpTx.Hash)
		if err != nil {
			return nil, err
		}
		txSize := len(tmpTx.Cbor)
		if txSize > math.MaxUint32 {
			return nil, errors.New("tx impossibly large")
		}
		ret = append(
			ret,
			otxsubmission.TxIdAndSize{
				TxId: otxsubmission.TxId{
					EraId: uint16(tmpTx.Type), // #nosec G115
					TxId:  [32]byte(txHashBytes),
				},
				Size: uint32(txSize),
			},
		)
	}
	return ret, nil
}

func (o *Ouroboros) TxsubmissionClientRequestTxs(
	ctx otxsubmission.CallbackContext,
	txIds []otxsubmission.TxId,
) ([]otxsubmission.TxBody, error) {
	connId := ctx.ConnectionId
	ret := []otxsubmission.TxBody{}
	consumer := o.config.Node.Mempool().Consumer(connId)
	for _, txId := range txIds {
		txHash := hex.EncodeToString(txId.TxId[:])
		tx := consumer.GetTxFromCache(txHash)
		if tx != nil {
			ret = append(
				ret,
				otxsubmission.TxBody{
					EraId:  uint16(tx.Type), // #nosec G115
					TxBody: tx.Cbor,
				},
			)
		}
		consumer.RemoveTxFromCache(txHash)
	}
	return ret, nil
}

func (o *Ouroboros) LocalstatequeryServerConnOpts() []olocalstatequery.LocalStateQueryOptionFunc {
	return LocalstatequeryServerConnOpts(
		o.LocalstatequeryServerAcquire,
		o.LocalstatequeryServerQuery,
		o.LocalstatequeryServerRelease,
	)
}

func (o *Ouroboros) LocalstatequeryServerAcquire(
	ctx olocalstatequery.CallbackContext,
	acquireTarget olocalstatequery.AcquireTarget,
	reAcquire bool,
) error {
	// TODO: create "view" from ledger state (#382)
	return nil
}

func (o *Ouroboros) LocalstatequeryServerQuery(
	ctx olocalstatequery.CallbackContext,
	query olocalstatequery.QueryWrapper,
) (any, error) {
	return o.config.Node.LedgerState().Query(query.Query)
}

func (o *Ouroboros) LocalstatequeryServerRelease(
	ctx olocalstatequery.CallbackContext,
) error {
	// TODO: release "view" from ledger state (#382)
	return nil
}

func (o *Ouroboros) LocaltxmonitorServerConnOpts() []olocaltxmonitor.LocalTxMonitorOptionFunc {
	return LocaltxmonitorServerConnOpts(o.LocaltxmonitorServerGetMempool)
}

func (o *Ouroboros) LocaltxmonitorServerGetMempool(
	ctx olocaltxmonitor.CallbackContext,
) (uint64, uint32, []olocaltxmonitor.TxAndEraId, error) {
	tip := o.config.Node.LedgerState().Tip()
	mempoolTxs := o.config.Node.Mempool().Transactions()
	retTxs := make([]olocaltxmonitor.TxAndEraId, len(mempoolTxs))
	for i := range mempoolTxs {
		retTxs[i] = olocaltxmonitor.TxAndEraId{
			EraId: mempoolTxs[i].Type,
			Tx:    mempoolTxs[i].Cbor,
		}
	}
	return tip.Point.Slot, 10 * 1024 * 1024, retTxs, nil // localtxmonitorMempoolCapacity
}

func (o *Ouroboros) LocaltxsubmissionServerConnOpts() []olocaltxsubmission.LocalTxSubmissionOptionFunc {
	return LocaltxsubmissionServerConnOpts(o.LocaltxsubmissionServerSubmitTx)
}

func (o *Ouroboros) LocaltxsubmissionServerSubmitTx(
	ctx olocaltxsubmission.CallbackContext,
	tx olocaltxsubmission.MsgSubmitTxTransaction,
) error {
	// Add transaction to mempool
	err := o.config.Node.Mempool().AddTransaction(
		uint(tx.EraId),
		tx.Raw.Content.([]byte),
	)
	if err != nil {
		o.config.Node.Config().Logger().Error(
			fmt.Sprintf(
				"failed to add transaction to mempool: %s",
				err,
			),
			"component", "network",
			"protocol", "local-tx-submission",
			"role", "server",
			"connection_id", ctx.ConnectionId.String(),
		)
		return err
	}
	return nil
}

func (o *Ouroboros) PeersharingServerConnOpts() []opeersharing.PeerSharingOptionFunc {
	return PeersharingServerConnOpts(o.PeersharingShareRequest)
}

func (o *Ouroboros) PeersharingClientConnOpts() []opeersharing.PeerSharingOptionFunc {
	return PeersharingClientConnOpts()
}

func (o *Ouroboros) PeersharingShareRequest(
	ctx opeersharing.CallbackContext,
	amount int,
) ([]opeersharing.PeerAddress, error) {
	peers := []opeersharing.PeerAddress{}
	var cnt int
	for _, peer := range o.config.Node.PeerGov().GetPeers() {
		cnt++
		if cnt > amount {
			break
		}
		if peer.Sharable {
			host, port, err := net.SplitHostPort(peer.Address)
			if err != nil {
				// Skip on error
				o.config.Node.Config().
					Logger().
					Debug("failed to split peer address, skipping")
				continue
			}
			portNum, err := strconv.ParseUint(port, 10, 16)
			if err != nil {
				// Skip on error
				o.config.Node.Config().
					Logger().
					Debug("failed to parse peer port, skipping")
				continue
			}
			o.config.Node.Config().Logger().Debug(
				"adding peer for sharing: " + peer.Address,
			)
			peers = append(peers, opeersharing.PeerAddress{
				IP:   net.ParseIP(host),
				Port: uint16(portNum),
			},
			)
		}
	}
	return peers, nil
}
