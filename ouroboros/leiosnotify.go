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
	"sync"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
)

// leiosForgedEBEntry holds one locally-forged endorser block ready to
// be announced to peers via LeiosNotify.
type leiosForgedEBEntry struct {
	point ocommon.Point
}

// leiosForgedEBLog is an append-only log of locally-forged EBs.
// Each per-connection RequestNext goroutine holds a cursor (index) into
// the log; when the log grows past the cursor the goroutine wakes up and
// serves the next entry. The wake channel is closed and replaced on every
// append so all blocked callers unblock at once.
type leiosForgedEBLog struct {
	mu     sync.Mutex
	items  []leiosForgedEBEntry
	wakeCh chan struct{}
}

func newLeiosForgedEBLog() *leiosForgedEBLog {
	return &leiosForgedEBLog{wakeCh: make(chan struct{})}
}

// append adds an entry and wakes all blocked callers.
func (l *leiosForgedEBLog) append(entry leiosForgedEBEntry) {
	l.mu.Lock()
	l.items = append(l.items, entry)
	wake := l.wakeCh
	l.wakeCh = make(chan struct{})
	l.mu.Unlock()
	close(wake)
}

// since returns the entries starting at cursor and the current wake channel.
func (l *leiosForgedEBLog) since(cursor int) ([]leiosForgedEBEntry, chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if cursor < len(l.items) {
		return l.items[cursor:], l.wakeCh
	}
	return nil, l.wakeCh
}

// BroadcastEndorserBlock stores a locally-forged EB and notifies waiting
// LeiosNotify server goroutines so they can announce it to peers.
// It satisfies forging.EndorserBlockBroadcaster.
func (o *Ouroboros) BroadcastEndorserBlock(
	slot uint64,
	hash []byte,
	data []byte,
) error {
	point := ocommon.Point{Slot: slot, Hash: hash}
	if err := o.storeLeiosEndorserBlock(point, data, nil); err != nil {
		return fmt.Errorf("store forged endorser block: %w", err)
	}
	o.leiosEBLog.append(leiosForgedEBEntry{point: point})
	return nil
}

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
		if err := o.storeLeiosEndorserBlock(
			m.Point,
			respBlock.BlockRaw,
			nil,
		); err != nil {
			return err
		}
		o.config.Logger.Info(
			fmt.Sprintf(
				"fetched EB %d.%x with size %d and %d txs",
				m.Point.Slot,
				m.Point.Hash,
				len(respBlock.BlockRaw),
				0,
			),
			"component", "network",
			"protocol", "leios-fetch",
			"role", "client",
			"connection_id", connId,
		)
	case *oleiosnotify.MsgBlockTxsOffer:
		txsRaw, err := o.fetchCachedLeiosEndorserBlockTxs(m.Point)
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
	// In gouroboros v0.180.0 the Leios aliases decode as Dijkstra blocks.
	// The current Dijkstra CDDL has no transaction-reference list, so there
	// is no extra BlockTxsRequest to make here.
	return cloneRawMessages(data.txsRaw), nil
}

func (o *Ouroboros) leiosnotifyServerRequestNext(
	ctx oleiosnotify.CallbackContext,
) (protocol.Message, error) {
	connKey := leiosConnectionIdString(ctx.ConnectionId)

	o.leiosMu.Lock()
	cursor := o.leiosNotifyCursors[connKey]
	o.leiosMu.Unlock()

	var done <-chan struct{}
	if ctx.Server != nil {
		done = ctx.Server.DoneChan()
	}

	for {
		entries, wakeCh := o.leiosEBLog.since(cursor)
		if len(entries) > 0 {
			entry := entries[0]
			o.leiosMu.Lock()
			o.leiosNotifyCursors[connKey] = cursor + 1
			o.leiosMu.Unlock()
			return &oleiosnotify.MsgBlockOffer{Point: entry.point}, nil
		}
		select {
		case <-wakeCh:
			// new EB appended — re-check
		case <-done:
			return nil, nil
		}
	}
}
