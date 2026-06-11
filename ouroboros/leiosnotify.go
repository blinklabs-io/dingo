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

// leiosForgedEBLog is an append-only log of locally-forged EBs with
// per-connection cursors owned by the log itself.
//
// Head entries are pruned whenever every registered connection's cursor
// has advanced past them, so memory scales with the largest per-connection
// backlog rather than total uptime. When no connections are registered the
// log is always empty. A new connection registers at the current tail and
// does not receive EBs forged before it connected. Connections are
// removed via removeConn, which triggers an immediate prune.
//
// The wake channel is closed and replaced on every append so all server
// goroutines waiting for new entries unblock at once.
type leiosForgedEBLog struct {
	mu      sync.Mutex
	items   []leiosForgedEBEntry
	base    int            // logical index of items[0]
	cursors map[string]int // connKey → next logical index to serve
	wakeCh  chan struct{}
}

func newLeiosForgedEBLog() *leiosForgedEBLog {
	return &leiosForgedEBLog{
		cursors: make(map[string]int),
		wakeCh:  make(chan struct{}),
	}
}

// append adds an entry, prunes head entries that all registered connections
// have advanced past (or all entries when none are registered), and signals
// all server goroutines waiting for new entries to wake and retry.
func (l *leiosForgedEBLog) append(entry leiosForgedEBEntry) {
	l.mu.Lock()
	l.items = append(l.items, entry)
	l.pruneLocked()
	wake := l.wakeCh
	l.wakeCh = make(chan struct{})
	l.mu.Unlock()
	close(wake)
}

// next returns the next unserved entry for connKey and the current wake
// channel. If no entry is available it returns (nil, wakeCh); the caller
// should wait on wakeCh and retry. A connKey that has never called next
// is registered at the current tail so it does not receive stale EBs.
func (l *leiosForgedEBLog) next(connKey string) (*leiosForgedEBEntry, chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cursor, exists := l.cursors[connKey]
	if !exists {
		// New connection: start at the current tail.
		cursor = l.base + len(l.items)
		l.cursors[connKey] = cursor
	}
	idx := cursor - l.base
	if idx < len(l.items) {
		entry := l.items[idx]
		l.cursors[connKey] = cursor + 1
		l.pruneLocked()
		return &entry, l.wakeCh
	}
	return nil, l.wakeCh
}

// removeConn unregisters a connection cursor and prunes newly freed entries.
func (l *leiosForgedEBLog) removeConn(connKey string) {
	l.mu.Lock()
	delete(l.cursors, connKey)
	l.pruneLocked()
	l.mu.Unlock()
}

// registerConn pre-registers connKey at the current tail so that EBs
// appended between connection open and the peer's first RequestNext are
// not pruned before the cursor is established. It is a no-op when connKey
// is already registered (e.g. on reconnect within the same session).
func (l *leiosForgedEBLog) registerConn(connKey string) {
	l.mu.Lock()
	if _, exists := l.cursors[connKey]; !exists {
		l.cursors[connKey] = l.base + len(l.items)
	}
	l.mu.Unlock()
}

// leiosEBLogMaxEntries is the maximum number of forged-EB entries the log
// retains. When the log grows beyond this limit, the oldest entries are
// evicted and any lagging cursors are advanced to the new base. This
// bounds memory even when a pre-registered or slow peer never calls next.
const leiosEBLogMaxEntries = 64

// pruneLocked drops head entries whose logical index falls below every
// registered connection's cursor (i.e. all connections have advanced past
// them, whether by consuming the entry or by registering after it). When
// no connections are registered the entire log is pruned. If the log still
// exceeds leiosEBLogMaxEntries after cursor-based pruning, the oldest
// entries are evicted and lagging cursors are advanced to the new base.
// Callers must hold l.mu.
func (l *leiosForgedEBLog) pruneLocked() {
	if len(l.items) == 0 {
		return
	}
	// Start at the tail: if no cursors constrain it, prune the full log.
	minCursor := l.base + len(l.items)
	for _, c := range l.cursors {
		if c < minCursor {
			minCursor = c
		}
	}
	prunable := minCursor - l.base
	// Size cap: if the log still exceeds leiosEBLogMaxEntries after
	// cursor-based pruning, evict the excess from the head. Any cursor
	// that falls behind the new base (e.g. a pre-registered idle peer)
	// is advanced to the new base so it does not pin future entries.
	if capped := len(l.items) - prunable - leiosEBLogMaxEntries; capped > 0 {
		prunable += capped
		newBase := l.base + prunable
		for k, c := range l.cursors {
			if c < newBase {
				l.cursors[k] = newBase
			}
		}
	}
	if prunable <= 0 {
		return
	}
	// Zero pruned slots so the GC can reclaim the point.Hash []byte
	// backing arrays before the backing slice is eventually reallocated.
	clear(l.items[:prunable])
	l.items = l.items[prunable:]
	l.base += prunable
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
		// Peer does not support LeiosNotify; skip cursor registration.
		return nil
	}
	connKey := leiosConnectionIdString(connId)
	// Pre-register the server-side cursor now that we know the peer
	// supports LeiosNotify. This ensures EBs forged between here and
	// the peer's first RequestNext are not pruned.
	o.leiosEBLog.registerConn(connKey)
	if err := conn.LeiosNotify().Client.Sync(); err != nil {
		o.leiosEBLog.removeConn(connKey)
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
	if ctx.Server == nil {
		return nil, nil
	}
	connKey := leiosConnectionIdString(ctx.ConnectionId)
	done := ctx.Server.DoneChan()

	// If the connection is already closing, return without touching the
	// cursor map. This prevents re-registering a stale cursor after
	// removeConn has already run (which would block future log pruning).
	select {
	case <-done:
		return nil, nil
	default:
	}

	for {
		entry, wakeCh := o.leiosEBLog.next(connKey)
		if entry != nil {
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
