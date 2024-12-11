// Copyright 2024 Blink Labs Software
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

package dingo

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/blinklabs-io/dingo/mempool"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/txsubmission"
	otxsubmission "github.com/blinklabs-io/gouroboros/protocol/txsubmission"
)

const (
	txsubmissionRequestTxIdsCount = 10 // Number of TxIds to request from peer at one time
)

func (n *Node) txsubmissionServerConnOpts() []otxsubmission.TxSubmissionOptionFunc {
	return []otxsubmission.TxSubmissionOptionFunc{
		otxsubmission.WithInitFunc(n.txsubmissionServerInit),
	}
}

func (n *Node) txsubmissionClientConnOpts() []otxsubmission.TxSubmissionOptionFunc {
	return []otxsubmission.TxSubmissionOptionFunc{
		txsubmission.WithRequestTxIdsFunc(n.txsubmissionClientRequestTxIds),
		txsubmission.WithRequestTxsFunc(n.txsubmissionClientRequestTxs),
	}
}

func (n *Node) txsubmissionClientStart(connId ouroboros.ConnectionId) error {
	// Register mempool consumer
	// We don't bother capturing the consumer because we can easily look it up later by connection ID
	_ = n.mempool.AddConsumer(connId)
	// Start TxSubmission loop
	conn := n.connManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf("failed to lookup connection ID: %s", connId.String())
	}
	conn.TxSubmission().Client.Init()
	return nil
}

func (n *Node) txsubmissionServerInit(ctx otxsubmission.CallbackContext) error {
	// Start async loop to request transactions from the peer's mempool
	go func() {
		for {
			// Request available TX IDs (era and TX hash) and sizes
			// We make the request blocking to avoid looping on our side
			txIds, err := ctx.Server.RequestTxIds(
				true,
				txsubmissionRequestTxIdsCount,
			)
			if err != nil {
				n.config.logger.Error(
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
					n.config.logger.Error(
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
					tx, err := ledger.NewTransactionFromCbor(
						uint(txBody.EraId),
						txBody.TxBody,
					)
					if err != nil {
						n.config.logger.Error(
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
					n.config.logger.Debug(
						"received tx",
						"tx_hash", tx.Hash(),
						"protocol", "tx-submission",
						"role", "server",
						"connection_id", ctx.ConnectionId.String(),
					)
					// Add transaction to mempool
					err = n.mempool.AddTransaction(
						mempool.MempoolTransaction{
							Hash:     tx.Hash(),
							Type:     uint(txBody.EraId),
							Cbor:     txBody.TxBody,
							LastSeen: time.Now(),
						},
					)
					if err != nil {
						n.config.logger.Error(
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

func (n *Node) txsubmissionClientRequestTxIds(
	ctx txsubmission.CallbackContext,
	blocking bool,
	ack uint16,
	req uint16,
) ([]txsubmission.TxIdAndSize, error) {
	connId := ctx.ConnectionId
	ret := []txsubmission.TxIdAndSize{}
	consumer := n.mempool.Consumer(connId)
	// Clear TX cache
	if ack > 0 {
		consumer.ClearCache()
	}
	// Get available TXs
	var tmpTxs []*mempool.MempoolTransaction
	for {
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
		tmpTx := tmpTx
		// Add to return value
		txHashBytes, err := hex.DecodeString(tmpTx.Hash)
		if err != nil {
			return nil, err
		}
		ret = append(
			ret,
			txsubmission.TxIdAndSize{
				TxId: txsubmission.TxId{
					EraId: uint16(tmpTx.Type),
					TxId:  [32]byte(txHashBytes),
				},
				Size: uint32(len(tmpTx.Cbor)),
			},
		)
	}
	return ret, nil
}

func (n *Node) txsubmissionClientRequestTxs(
	ctx txsubmission.CallbackContext,
	txIds []txsubmission.TxId,
) ([]txsubmission.TxBody, error) {
	connId := ctx.ConnectionId
	ret := []txsubmission.TxBody{}
	consumer := n.mempool.Consumer(connId)
	for _, txId := range txIds {
		txHash := hex.EncodeToString(txId.TxId[:])
		tx := consumer.GetTxFromCache(txHash)
		if tx != nil {
			ret = append(
				ret,
				txsubmission.TxBody{
					EraId:  uint16(tx.Type),
					TxBody: tx.Cbor,
				},
			)
		}
		consumer.RemoveTxFromCache(txHash)
	}
	return ret, nil
}
