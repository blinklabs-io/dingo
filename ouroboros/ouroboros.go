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

const (
	localtxmonitorMempoolCapacity = 10 * 1024 * 1024
)

// NodeInterface abstracts the Node's dependencies required by the Ouroboros package.
// This interface enables dependency injection, decoupling the Ouroboros implementation
// from the concrete Node type, and facilitates unit testing.
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

// Ouroboros provides a unified interface for managing Ouroboros protocol interactions.
// It encapsulates client and server implementations for various Ouroboros mini-protocols
// such as ChainSync, BlockFetch, TxSubmission, and others.
type Ouroboros struct {
	config OuroborosConfig
}

// OuroborosConfig holds the configuration for an Ouroboros instance.
type OuroborosConfig struct {
	NetworkMagic           uint32
	Node                   NodeInterface
	ChainsyncPipelineLimit int
}

// NewOuroboros creates a new Ouroboros instance with the given configuration.
func NewOuroboros(cfg OuroborosConfig) (*Ouroboros, error) {
	if cfg.Node == nil {
		return nil, errors.New("OuroborosConfig.Node must not be nil")
	}
	if cfg.ChainsyncPipelineLimit == 0 {
		cfg.ChainsyncPipelineLimit = 50
	}
	return &Ouroboros{
		config: cfg,
	}, nil
}

// ChainsyncClientStart starts the ChainSync client for a connection.
func (o *Ouroboros) ChainsyncClientStart(connId ConnectionId) error {
	connMgr := o.config.Node.ConnManager()
	if connMgr == nil {
		return errors.New("connection manager is nil")
	}
	ledgerState := o.config.Node.LedgerState()
	if ledgerState == nil {
		return errors.New("ledger state is nil")
	}
	config := o.config.Node.Config()
	if config == nil {
		return errors.New("config is nil")
	}
	return ChainsyncClientStart(
		connMgr,
		ledgerState,
		config.IntersectTip(),
		config.IntersectPoints(),
		connId,
	)
}

// TxsubmissionClientStart starts the TxSubmission client for a connection.
func (o *Ouroboros) TxsubmissionClientStart(connId ConnectionId) error {
	connMgr := o.config.Node.ConnManager()
	if connMgr == nil {
		return errors.New("connection manager is nil")
	}
	mempool := o.config.Node.Mempool()
	if mempool == nil {
		return errors.New("mempool is nil")
	}
	return TxsubmissionClientStart(
		connMgr,
		mempool,
		connId,
	)
}

// ChainsyncServerConnOpts returns server connection options for the ChainSync protocol.
func (o *Ouroboros) ChainsyncServerConnOpts() []ochainsync.ChainSyncOptionFunc {
	return ChainsyncServerConnOpts(
		func(ctx ochainsync.CallbackContext, points []ocommon.Point) (ocommon.Point, ochainsync.Tip, error) {
			return ChainsyncServerFindIntersect(ctx, o.config.Node, points)
		},
		func(ctx ochainsync.CallbackContext) error {
			return ChainsyncServerRequestNext(ctx, o.config.Node)
		},
	)
}

// ChainsyncClientConnOpts returns client connection options for the ChainSync protocol.
func (o *Ouroboros) ChainsyncClientConnOpts() []ochainsync.ChainSyncOptionFunc {
	return ChainsyncClientConnOpts(
		func(ctx ochainsync.CallbackContext, blockType uint, blockData any, tip ochainsync.Tip) error {
			return ChainsyncClientRollForward(
				ctx,
				o.config.Node,
				blockType,
				blockData,
				tip,
			)
		},
		func(ctx ochainsync.CallbackContext, point ocommon.Point, tip ochainsync.Tip) error {
			return ChainsyncClientRollBackward(ctx, o.config.Node, point, tip)
		},
		o.config.ChainsyncPipelineLimit,
	)
}

// BlockfetchServerConnOpts returns server connection options for the BlockFetch protocol.
func (o *Ouroboros) BlockfetchServerConnOpts() []oblockfetch.BlockFetchOptionFunc {
	return BlockfetchServerConnOpts(
		func(ctx oblockfetch.CallbackContext, start ocommon.Point, end ocommon.Point) error {
			return BlockfetchServerRequestRange(ctx, o.config.Node, start, end)
		},
	)
}

// BlockfetchClientConnOpts returns client connection options for the BlockFetch protocol.
func (o *Ouroboros) BlockfetchClientConnOpts() []oblockfetch.BlockFetchOptionFunc {
	return BlockfetchClientConnOpts(
		func(ctx oblockfetch.CallbackContext, blockType uint, block gledger.Block) error {
			return BlockfetchClientBlock(ctx, o.config.Node, blockType, block)
		},
		func(ctx oblockfetch.CallbackContext) error {
			return BlockfetchClientBatchDone(ctx, o.config.Node)
		},
	)
}

// BlockfetchClientRequestRange requests a range of blocks from the BlockFetch client.
func (o *Ouroboros) BlockfetchClientRequestRange(
	connId ConnectionId,
	start ocommon.Point,
	end ocommon.Point,
) error {
	connMgr := o.config.Node.ConnManager()
	if connMgr == nil {
		return errors.New("connection manager is nil")
	}
	return BlockfetchClientRequestRange(
		connMgr,
		connId,
		start,
		end,
	)
}

// TxsubmissionServerConnOpts returns server connection options for the TxSubmission protocol.
func (o *Ouroboros) TxsubmissionServerConnOpts() []otxsubmission.TxSubmissionOptionFunc {
	return TxsubmissionServerConnOpts(o.TxsubmissionServerInit)
}

// TxsubmissionClientConnOpts returns client connection options for the TxSubmission protocol.
func (o *Ouroboros) TxsubmissionClientConnOpts() []otxsubmission.TxSubmissionOptionFunc {
	return TxsubmissionClientConnOpts(
		o.TxsubmissionClientRequestTxIds,
		o.TxsubmissionClientRequestTxs,
	)
}

// TxsubmissionServerInit initializes the TxSubmission server for a connection.
func (o *Ouroboros) TxsubmissionServerInit(
	ctx otxsubmission.CallbackContext,
) error {
	connMgr := o.config.Node.ConnManager()
	if connMgr == nil {
		return errors.New("connection manager is nil")
	}
	conn := connMgr.GetConnectionById(ctx.ConnectionId)
	if conn == nil {
		return fmt.Errorf(
			"failed to lookup connection ID: %s",
			ctx.ConnectionId.String(),
		)
	}
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
						continue
					}
				}
			}
		}
	}()
	return nil
}

// TxsubmissionClientRequestTxIds handles TxSubmission client requests for transaction IDs.
func (o *Ouroboros) TxsubmissionClientRequestTxIds(
	ctx otxsubmission.CallbackContext,
	blocking bool,
	ack uint16,
	req uint16,
) ([]otxsubmission.TxIdAndSize, error) {
	connId := ctx.ConnectionId
	ret := []otxsubmission.TxIdAndSize{}
	consumer := o.config.Node.Mempool().Consumer(connId)
	if consumer == nil {
		return nil, fmt.Errorf(
			"no mempool consumer for connection %s",
			connId.String(),
		)
	}
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
		if len(txHashBytes) != 32 {
			return nil, fmt.Errorf(
				"invalid tx hash length: expected 32, got %d",
				len(txHashBytes),
			)
		}
		var txIdArr [32]byte
		copy(txIdArr[:], txHashBytes)
		txSize := len(tmpTx.Cbor)
		if txSize > math.MaxUint32 {
			return nil, errors.New("tx impossibly large")
		}
		ret = append(
			ret,
			otxsubmission.TxIdAndSize{
				TxId: otxsubmission.TxId{
					EraId: uint16(tmpTx.Type), // #nosec G115
					TxId:  txIdArr,
				},
				Size: uint32(txSize),
			},
		)
	}
	return ret, nil
}

// TxsubmissionClientRequestTxs handles TxSubmission client requests for transaction bodies.
func (o *Ouroboros) TxsubmissionClientRequestTxs(
	ctx otxsubmission.CallbackContext,
	txIds []otxsubmission.TxId,
) ([]otxsubmission.TxBody, error) {
	connId := ctx.ConnectionId
	ret := []otxsubmission.TxBody{}
	consumer := o.config.Node.Mempool().Consumer(connId)
	if consumer == nil {
		return nil, fmt.Errorf(
			"no mempool consumer for connection %s",
			connId.String(),
		)
	}
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

// LocalStateQueryServerConnOpts returns server connection options for the LocalStateQuery protocol.
func (o *Ouroboros) LocalStateQueryServerConnOpts() []olocalstatequery.LocalStateQueryOptionFunc {
	return LocalStateQueryServerConnOpts(
		o.LocalstatequeryServerAcquire,
		o.LocalstatequeryServerQuery,
		o.LocalstatequeryServerRelease,
	)
}

// LocalstatequeryServerAcquire acquires a local state query session.
func (o *Ouroboros) LocalstatequeryServerAcquire(
	ctx olocalstatequery.CallbackContext,
	acquireTarget olocalstatequery.AcquireTarget,
	reAcquire bool,
) error {
	// TODO: create "view" from ledger state (#382)
	return nil
}

// LocalstatequeryServerQuery handles LocalStateQuery server queries.
func (o *Ouroboros) LocalstatequeryServerQuery(
	ctx olocalstatequery.CallbackContext,
	query olocalstatequery.QueryWrapper,
) (any, error) {
	return o.config.Node.LedgerState().Query(query.Query)
}

// LocalstatequeryServerRelease releases a local state query session.
func (o *Ouroboros) LocalstatequeryServerRelease(
	ctx olocalstatequery.CallbackContext,
) error {
	// TODO: release "view" from ledger state (#382)
	return nil
}

// LocalTxMonitorServerConnOpts returns server connection options for the LocalTxMonitor protocol.
func (o *Ouroboros) LocalTxMonitorServerConnOpts() []olocaltxmonitor.LocalTxMonitorOptionFunc {
	return LocalTxMonitorServerConnOpts(o.LocaltxmonitorServerGetMempool)
}

// LocaltxmonitorServerGetMempool handles LocalTxMonitor server requests for mempool information.
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
	return tip.Point.Slot, localtxmonitorMempoolCapacity, retTxs, nil
}

// LocalTxSubmissionServerConnOpts returns server connection options for the LocalTxSubmission protocol.
func (o *Ouroboros) LocalTxSubmissionServerConnOpts() []olocaltxsubmission.LocalTxSubmissionOptionFunc {
	return LocalTxSubmissionServerConnOpts(o.LocaltxsubmissionServerSubmitTx)
}

// LocaltxsubmissionServerSubmitTx handles LocalTxSubmission server transaction submissions.
func (o *Ouroboros) LocaltxsubmissionServerSubmitTx(
	ctx olocaltxsubmission.CallbackContext,
	tx olocaltxsubmission.MsgSubmitTxTransaction,
) error {
	// Add transaction to mempool
	raw, ok := tx.Raw.Content.([]byte)
	if !ok {
		return fmt.Errorf(
			"unexpected local-tx-submission content type %T",
			tx.Raw.Content,
		)
	}
	err := o.config.Node.Mempool().AddTransaction(
		uint(tx.EraId),
		raw,
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

// PeersharingServerConnOpts returns server connection options for the PeerSharing protocol.
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
	if amount < 0 {
		return nil, fmt.Errorf("amount must be non-negative, got %d", amount)
	}
	peers := []opeersharing.PeerAddress{}
	for _, peer := range o.config.Node.PeerGov().GetPeers() {
		if amount > 0 && len(peers) >= amount {
			break
		}
		if peer.Sharable {
			host, port, err := net.SplitHostPort(peer.Address)
			if err != nil {
				// Skip on error
				o.config.Node.Config().Logger().Debug(
					"failed to split peer address, skipping",
					"address", peer.Address,
					"error", err,
				)
				continue
			}
			portNum, err := strconv.ParseUint(port, 10, 16)
			if err != nil {
				// Skip on error
				o.config.Node.Config().Logger().Debug(
					"failed to parse peer port, skipping",
					"address", peer.Address,
					"port", port,
					"error", err,
				)
				continue
			}
			ip := net.ParseIP(host)
			if ip == nil {
				// Skip on error
				o.config.Node.Config().Logger().Debug(
					"failed to parse peer IP, skipping",
					"address", peer.Address,
					"host", host,
					"error", "invalid IP address",
				)
				continue
			}
			o.config.Node.Config().Logger().Debug(
				"adding peer for sharing: " + peer.Address,
			)
			peers = append(peers, opeersharing.PeerAddress{
				IP:   ip,
				Port: uint16(portNum),
			},
			)
		}
	}
	return peers, nil
}
