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

package ouroboros

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	gleios "github.com/blinklabs-io/gouroboros/ledger/leios"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
)

func (o *Ouroboros) leiosnotifyServerConnOpts() []oleiosnotify.LeiosNotifyOptionFunc {
	return []oleiosnotify.LeiosNotifyOptionFunc{
		oleiosnotify.WithRequestNextFunc(
			o.instrumentLeiosnotifyRequestNext(o.leiosnotifyServerRequestNext),
		),
	}
}

func (o *Ouroboros) leiosnotifyClientConnOpts() []oleiosnotify.LeiosNotifyOptionFunc {
	return []oleiosnotify.LeiosNotifyOptionFunc{
		oleiosnotify.WithNotificationFunc(
			o.instrumentLeiosnotifyNotification(o.leiosnotifyClientNotification),
		),
		// Disable the Busy-state timeout. LeiosNotify is a push-based
		// notification protocol where the server only sends when it has
		// something to announce. Idle waits of arbitrary length are
		// normal and should not kill the connection.
		oleiosnotify.WithTimeout(time.Duration(0)),
	}
}

func (o *Ouroboros) leiosnotifyClientStart(connId ouroboros.ConnectionId) error {
	conn := o.ConnManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf(
			"failed to lookup connection ID: %s",
			leiosConnectionIdString(connId),
		)
	}
	if conn.LeiosNotify() == nil {
		// Return silently if LeiosNotify protocol is not supported by peer
		return nil
	}
	if err := conn.LeiosNotify().Client.Sync(); err != nil {
		return err
	}
	return nil
}

func leiosConnectionIdString(connId ouroboros.ConnectionId) string {
	if connId.LocalAddr == nil || connId.RemoteAddr == nil {
		return "<unknown>"
	}
	return connId.String()
}

func (o *Ouroboros) instrumentLeiosnotifyNotification(
	fn func(oleiosnotify.CallbackContext, protocol.Message) error,
) func(oleiosnotify.CallbackContext, protocol.Message) error {
	return func(ctx oleiosnotify.CallbackContext, msg protocol.Message) error {
		start := time.Now()
		err := fn(ctx, msg)
		o.recordProtocolMessage("leiosnotify", err, time.Since(start))
		return err
	}
}

func (o *Ouroboros) instrumentLeiosnotifyRequestNext(
	fn func(oleiosnotify.CallbackContext) (protocol.Message, error),
) func(oleiosnotify.CallbackContext) (protocol.Message, error) {
	return func(ctx oleiosnotify.CallbackContext) (protocol.Message, error) {
		start := time.Now()
		msg, err := fn(ctx)
		o.recordProtocolMessage("leiosnotify", err, time.Since(start))
		return msg, err
	}
}

func (o *Ouroboros) leiosnotifyClientNotification(
	ctx oleiosnotify.CallbackContext,
	msg protocol.Message,
) error {
	conn := o.ConnManager.GetConnectionById(ctx.ConnectionId)
	connId := leiosConnectionIdString(ctx.ConnectionId)
	if conn == nil {
		return fmt.Errorf("failed to lookup connection ID: %s", connId)
	}
	switch m := msg.(type) {
	case *oleiosnotify.MsgBlockOffer:
		if conn.LeiosFetch() == nil || conn.LeiosFetch().Client == nil {
			return errors.New("leios-fetch client unavailable")
		}
		resp, err := conn.LeiosFetch().Client.BlockRequest(m.Point)
		if err != nil {
			return err
		}
		respBlock, ok := resp.(*leiosfetch.MsgBlock)
		if !ok {
			return fmt.Errorf(
				"unexpected leios-fetch Block response type %T",
				resp,
			)
		}
		txsRaw, err := o.fetchLeiosEndorserBlockTxs(
			conn,
			m.Point,
			respBlock.BlockRaw,
		)
		if err != nil {
			o.config.Logger.Warn(
				"failed to fetch Leios EB transactions",
				"component", "network",
				"protocol", "leios-fetch",
				"role", "client",
				"connection_id", connId,
				"slot", m.Point.Slot,
				"hash", hex.EncodeToString(m.Point.Hash),
				"error", err,
			)
		}
		if err := o.storeLeiosEndorserBlock(
			m.Point,
			respBlock.BlockRaw,
			txsRaw,
		); err != nil {
			return err
		}
		o.config.Logger.Info(
			fmt.Sprintf(
				"fetched EB %d.%x with size %d and %d txs",
				m.Point.Slot,
				m.Point.Hash,
				len(respBlock.BlockRaw),
				len(txsRaw),
			),
			"component", "network",
			"protocol", "leios-fetch",
			"role", "client",
			"connection_id", connId,
		)
	case *oleiosnotify.MsgBlockTxsOffer:
		txsRaw, err := o.fetchCachedLeiosEndorserBlockTxs(conn, m.Point)
		if err != nil {
			level := slog.LevelWarn
			msg := "failed to fetch Leios EB transactions"
			if errors.Is(err, errLeiosEndorserBlockNotCached) {
				level = slog.LevelDebug
				msg = "skipping Leios EB transactions offer for uncached block"
			}
			o.config.Logger.Log(
				context.Background(),
				level,
				msg,
				"component", "network",
				"protocol", "leios-fetch",
				"role", "client",
				"connection_id", connId,
				"slot", m.Point.Slot,
				"hash", hex.EncodeToString(m.Point.Hash),
				"error", err,
			)
			return nil
		}
		o.config.Logger.Debug(
			"fetched Leios EB transactions",
			"component", "network",
			"protocol", "leios-fetch",
			"role", "client",
			"connection_id", connId,
			"slot", m.Point.Slot,
			"hash", hex.EncodeToString(m.Point.Hash),
			"tx_count", len(txsRaw),
		)
	}
	return nil
}

func (o *Ouroboros) fetchCachedLeiosEndorserBlockTxs(
	conn *ouroboros.Connection,
	point ocommon.Point,
) ([]cbor.RawMessage, error) {
	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	if !ok {
		return nil, fmt.Errorf(
			"%w: %d.%x",
			errLeiosEndorserBlockNotCached,
			point.Slot,
			point.Hash,
		)
	}
	if data.completeTxCache() {
		return cloneRawMessages(data.txsRaw), nil
	}
	txsRaw, err := o.fetchLeiosEndorserBlockTxs(
		conn,
		point,
		cbor.RawMessage(data.blockRaw),
	)
	if err != nil {
		return nil, err
	}
	if err := o.storeLeiosEndorserBlock(point, data.blockRaw, txsRaw); err != nil {
		return nil, err
	}
	return txsRaw, nil
}

func (o *Ouroboros) fetchLeiosEndorserBlockTxs(
	conn *ouroboros.Connection,
	point ocommon.Point,
	blockRaw cbor.RawMessage,
) ([]cbor.RawMessage, error) {
	if conn.LeiosFetch() == nil || conn.LeiosFetch().Client == nil {
		return nil, errors.New("leios-fetch client unavailable")
	}
	block, err := gleios.NewLeiosEndorserBlockFromCbor(blockRaw)
	if err != nil {
		return nil, err
	}
	if block.Body == nil || len(block.Body.TxReferences) == 0 {
		return nil, nil
	}
	bitmaps, err := leiosAllTxBitmap(len(block.Body.TxReferences))
	if err != nil {
		return nil, err
	}
	resp, err := conn.LeiosFetch().Client.BlockTxsRequest(point, bitmaps)
	if err != nil {
		return nil, err
	}
	respTxs, ok := resp.(*leiosfetch.MsgBlockTxs)
	if !ok {
		return nil, fmt.Errorf(
			"unexpected leios-fetch BlockTxs response type %T",
			resp,
		)
	}
	if len(respTxs.TxsRaw) != len(block.Body.TxReferences) {
		return nil, fmt.Errorf(
			"leios-fetch BlockTxs returned %d txs for %d references",
			len(respTxs.TxsRaw),
			len(block.Body.TxReferences),
		)
	}
	return cloneRawMessages(respTxs.TxsRaw), nil
}

func (o *Ouroboros) leiosnotifyServerRequestNext(
	ctx oleiosnotify.CallbackContext,
) (protocol.Message, error) {
	// TODO
	return nil, nil
}
