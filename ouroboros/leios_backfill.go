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
	"errors"
	"fmt"
	"sync/atomic"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
)

// leiosBackfillConnCursor rotates the starting connection across backfill
// requests so concurrent fetches spread over the available relay connections
// instead of contending on a single connection's fetch guard.
var leiosBackfillConnCursor atomic.Uint64

// FetchEndorserBlockByPoint fetches the endorser block identified by
// (ebSlot, ebHash) -- its manifest and all transaction bodies -- over
// leios-fetch and caches it, so EndorserBlockTxsByHash subsequently returns it.
//
// Unlike the tip path (which waits for the relay to diffuse an endorser block
// it is already pushing), this requests the block by point. The prototype relay
// serves any endorser block by point on demand (MsgLeiosBlockRequest /
// MsgLeiosBlockTxsRequest), including deeply historical ones, so this backfills
// the endorser-resident outputs of older ranking blocks during catch-up rather
// than leaving the UTxO set incomplete and trusting the chain. It satisfies
// ledger.EndorserBlockFetcherFunc.
func (o *Ouroboros) FetchEndorserBlockByPoint(
	ebSlot uint64,
	ebHash []byte,
) error {
	if data, ok := o.lookupLeiosEndorserBlock(ebHash); ok &&
		data.completeTxCache() {
		return nil
	}
	if o.ConnManager == nil {
		return errors.New("leios backfill: no connection manager")
	}
	connIds := o.ConnManager.LeiosFetchConnectionIds()
	if len(connIds) == 0 {
		return errors.New("leios backfill: no leios-fetch connection available")
	}
	point := ocommon.Point{Slot: ebSlot, Hash: ebHash}
	//nolint:gosec // bounded by len(connIds), so it fits in int
	start := int(leiosBackfillConnCursor.Add(1) % uint64(len(connIds)))
	var lastErr error
	for off := range connIds {
		connId := connIds[(start+off)%len(connIds)]
		conn := o.ConnManager.GetConnectionById(connId)
		if conn == nil || conn.LeiosFetch() == nil ||
			conn.LeiosFetch().Client == nil {
			continue
		}
		if err := o.fetchEndorserBlockOnConn(
			connId,
			conn.LeiosFetch().Client,
			point,
		); err != nil {
			lastErr = err
			continue
		}
		if data, ok := o.lookupLeiosEndorserBlock(ebHash); ok &&
			data.completeTxCache() {
			return nil
		}
		lastErr = errors.New(
			"leios backfill: fetch completed but cache incomplete",
		)
	}
	if lastErr == nil {
		lastErr = errors.New("leios backfill: fetch failed")
	}
	return lastErr
}

// fetchEndorserBlockOnConn fetches the manifest (if not already cached) and all
// transaction bodies for point on a single connection, holding that
// connection's fetch guard so the strict request/response leios-fetch client is
// never used concurrently with a tip-driven fetch.
func (o *Ouroboros) fetchEndorserBlockOnConn(
	connId ouroboros.ConnectionId,
	client *oleiosfetch.Client,
	point ocommon.Point,
) error {
	g := o.leiosFetchGuardFor(connId)
	g.mu.Lock()
	defer g.mu.Unlock()
	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	if !ok {
		resp, err := client.BlockRequest(point)
		if err != nil {
			return fmt.Errorf("manifest fetch: %w", err)
		}
		blk, ok := resp.(*oleiosfetch.MsgBlock)
		if !ok {
			return fmt.Errorf(
				"unexpected leios-fetch block response %T",
				resp,
			)
		}
		if err := o.storeLeiosEndorserBlock(
			point,
			blk.BlockRaw,
			nil,
		); err != nil {
			return fmt.Errorf("store manifest: %w", err)
		}
		if data, ok = o.lookupLeiosEndorserBlock(point.Hash); !ok {
			return errors.New("manifest stored but not found in cache")
		}
	}
	if data.txCount == 0 || data.completeTxCache() {
		return nil
	}
	txs, err := o.fetchLeiosEbTxsBatched(client, point, data.txCount)
	if err != nil {
		return fmt.Errorf(
			"tx fetch (%d/%d): %w",
			len(txs),
			data.txCount,
			err,
		)
	}
	if err := o.storeLeiosEndorserBlock(point, data.blockRaw, txs); err != nil {
		return fmt.Errorf("store txs: %w", err)
	}
	return nil
}
