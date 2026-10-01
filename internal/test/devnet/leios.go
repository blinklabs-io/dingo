//go:build linux && devnet

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

package devnet

import (
	"context"
	"fmt"
	"net"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	olsq "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

const leiosDevNetDialTimeout = 10 * time.Second

// LeiosEndorserBlockOfferMonitor follows locally forged EB offers from one
// Dingo node's LeiosNotify server. Errors reports asynchronous connection
// failures; Stop closes the monitor connection.
type LeiosEndorserBlockOfferMonitor struct {
	Offers            <-chan pcommon.Point
	TransactionOffers <-chan pcommon.Point
	Errors            <-chan error
	conn              *ouroboros.Connection
}

func (m *LeiosEndorserBlockOfferMonitor) Stop() {
	if m != nil && m.conn != nil {
		_ = m.conn.Close()
	}
}

// WatchLeiosEndorserBlockOffers opens a Node-to-Node connection and records
// endorser-block offers received over LeiosNotify. It intentionally observes
// the actual mini-protocol instead of inferring a notification from node logs.
func WatchLeiosEndorserBlockOffers(
	ctx context.Context,
	addr string,
	magic uint32,
) (*LeiosEndorserBlockOfferMonitor, error) {
	offers := make(chan pcommon.Point, 1)
	// The relay monitor starts before the test knows which producer EB it
	// needs. Retain a bounded pre-queue so early transaction offers are still
	// available when the scenario reaches its fetch phase.
	txOffers := make(chan pcommon.Point, 4096)
	errorsCh := make(chan error, 1)
	dialer := &net.Dialer{Timeout: leiosDevNetDialTimeout}
	rawConn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial LeiosNotify peer %s: %w", addr, err)
	}
	conn, err := ouroboros.NewConnection(
		ouroboros.WithConnection(rawConn),
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithLeiosNotifyConfig(oleiosnotify.NewConfig(
			oleiosnotify.WithPipelineLimit(1),
			// This push protocol may legitimately stay idle until an EB is
			// forged; the caller's context bounds the observation instead.
			oleiosnotify.WithTimeout(0),
			oleiosnotify.WithNotificationFunc(
				func(_ oleiosnotify.CallbackContext, msg protocol.Message) error {
					var point pcommon.Point
					var dest chan pcommon.Point
					switch offer := msg.(type) {
					case *oleiosnotify.MsgBlockOffer:
						point = offer.Point
						dest = offers
					case *oleiosnotify.MsgBlockTxsOffer:
						point = offer.Point
						dest = txOffers
					default:
						return nil
					}
					if len(point.Hash) != lcommon.Blake2b256Size {
						return fmt.Errorf(
							"LeiosNotify offer has %d-byte hash",
							len(point.Hash),
						)
					}
					point.Hash = append([]byte(nil), point.Hash...)
					select {
					case dest <- point:
					default:
					}
					return nil
				},
			),
		)),
	)
	if err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("handshake with LeiosNotify peer %s: %w", addr, err)
	}
	if protocol := conn.LeiosNotify(); protocol == nil || protocol.Client == nil {
		_ = conn.Close()
		return nil, fmt.Errorf("LeiosNotify client unavailable on %s", addr)
	}
	if err := conn.LeiosNotify().Client.Sync(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("start LeiosNotify client for %s: %w", addr, err)
	}
	monitor := &LeiosEndorserBlockOfferMonitor{
		Offers:            offers,
		TransactionOffers: txOffers,
		Errors:            errorsCh,
		conn:              conn,
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case err, ok := <-conn.ErrorChan():
			if ok && err != nil && ctx.Err() == nil {
				select {
				case errorsCh <- err:
				default:
				}
			}
		}
	}()
	return monitor, nil
}

// FetchLeiosEndorserBlock fetches an EB manifest and every referenced
// transaction body from the selected peer, then verifies the wire size and
// content hash of each body against the manifest.
func FetchLeiosEndorserBlock(
	ctx context.Context,
	addr string,
	magic uint32,
	point pcommon.Point,
) ([][]byte, error) {
	dialer := &net.Dialer{Timeout: leiosDevNetDialTimeout}
	rawConn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial LeiosFetch peer %s: %w", addr, err)
	}
	conn, err := ouroboros.NewConnection(
		ouroboros.WithConnection(rawConn),
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithLeiosFetchConfig(oleiosfetch.NewConfig()),
	)
	if err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("handshake with LeiosFetch peer %s: %w", addr, err)
	}
	defer conn.Close() //nolint:errcheck

	protocol := conn.LeiosFetch()
	if protocol == nil || protocol.Client == nil {
		return nil, fmt.Errorf("LeiosFetch client unavailable on %s", addr)
	}
	requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	manifestResponse, err := protocol.Client.BlockRequest(requestCtx, point)
	if err != nil {
		return nil, fmt.Errorf("fetch EB manifest from %s: %w", addr, err)
	}
	manifestMessage, ok := manifestResponse.(*oleiosfetch.MsgBlock)
	if !ok {
		return nil, fmt.Errorf("unexpected EB manifest response %T", manifestResponse)
	}
	manifest, err := lcommon.NewLeiosEndorserBlockFromCbor(
		manifestMessage.BlockRaw,
	)
	if err != nil {
		return nil, fmt.Errorf("decode EB manifest from %s: %w", addr, err)
	}
	if len(manifest.TransactionReferences) == 0 {
		return nil, fmt.Errorf("EB at slot %d has no transaction references", point.Slot)
	}
	bitmaps, err := leiosFetchBitmaps(len(manifest.TransactionReferences))
	if err != nil {
		return nil, fmt.Errorf("build EB transaction filter: %w", err)
	}
	txsResponse, err := protocol.Client.BlockTxsRequest(
		requestCtx,
		point,
		bitmaps,
	)
	if err != nil {
		return nil, fmt.Errorf("fetch EB transaction bodies from %s: %w", addr, err)
	}
	txsMessage, ok := txsResponse.(*oleiosfetch.MsgBlockTxs)
	if !ok {
		return nil, fmt.Errorf("unexpected EB transaction response %T", txsResponse)
	}
	if len(txsMessage.TxsRaw) != len(manifest.TransactionReferences) {
		return nil, fmt.Errorf(
			"EB at slot %d has %d references but peer returned %d bodies",
			point.Slot,
			len(manifest.TransactionReferences),
			len(txsMessage.TxsRaw),
		)
	}
	bodies := make([][]byte, len(txsMessage.TxsRaw))
	for i, raw := range txsMessage.TxsRaw {
		var body []byte
		bytesRead, err := cbor.Decode(raw, &body)
		if err != nil {
			return nil, fmt.Errorf("decode EB transaction body %d: %w", i, err)
		}
		if bytesRead != len(raw) {
			return nil, fmt.Errorf("EB transaction body %d has trailing bytes", i)
		}
		reference := manifest.TransactionReferences[i]
		if len(body) != int(reference.TransactionSize) {
			return nil, fmt.Errorf(
				"EB transaction body %d has size %d, manifest says %d",
				i,
				len(body),
				reference.TransactionSize,
			)
		}
		if got := lcommon.Blake2b256Hash(body); got != reference.TransactionHash {
			return nil, fmt.Errorf("EB transaction body %d does not match its manifest hash", i)
		}
		bodies[i] = body
	}
	return bodies, nil
}

func leiosFetchBitmaps(count int) (map[uint16]uint64, error) {
	if count <= 0 {
		return nil, fmt.Errorf("transaction reference count must be positive")
	}
	maxCount := (int(^uint16(0)) + 1) * 64
	if count > maxCount {
		return nil, fmt.Errorf("too many transaction references: %d", count)
	}
	bitmaps := make(map[uint16]uint64, (count+63)/64)
	for i := range count {
		word := uint16(i / 64)
		bit := uint(63 - i%64)
		bitmaps[word] |= uint64(1) << bit
	}
	return bitmaps, nil
}

// LeiosTransactionOutputsApplied reports whether the acquired NtC ledger view
// contains any output created by one of the fetched Dijkstra-era EB bodies.
func LeiosTransactionOutputsApplied(
	ctx context.Context,
	addr string,
	magic uint32,
	bodies [][]byte,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	conn, err := ouroboros.New(
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(false),
		ouroboros.WithLocalStateQueryConfig(olsq.NewConfig(
			olsq.WithAcquireTimeout(15*time.Second),
			olsq.WithQueryTimeout(15*time.Second),
		)),
	)
	if err != nil {
		return false, fmt.Errorf("create NtC connection: %w", err)
	}
	defer conn.Close() //nolint:errcheck
	connectionDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-connectionDone:
		}
	}()
	defer close(connectionDone)
	if err := conn.DialTimeout("tcp", addr, leiosDevNetDialTimeout); err != nil {
		return false, fmt.Errorf("dial NtC peer %s: %w", addr, err)
	}
	query := conn.LocalStateQuery()
	if query == nil || query.Client == nil {
		return false, fmt.Errorf("LocalStateQuery client unavailable on %s", addr)
	}
	if err := query.Client.AcquireVolatileTip(); err != nil {
		return false, fmt.Errorf("acquire ledger tip on %s: %w", addr, err)
	}
	defer query.Client.Release() //nolint:errcheck

	var inputs []gledger.TransactionInput
	for i, body := range bodies {
		tx, err := gledger.NewTransactionFromCbor(
			uint(gledger.TxTypeDijkstra),
			body,
		)
		if err != nil {
			return false, fmt.Errorf("decode fetched Dijkstra transaction %d: %w", i, err)
		}
		for _, output := range tx.Produced() {
			inputs = append(inputs, output.Id)
		}
	}
	if len(inputs) == 0 {
		return false, fmt.Errorf("fetched EB transactions produce no UTxO outputs")
	}
	result, err := query.Client.GetUTxOByTxIn(inputs)
	if err != nil {
		return false, fmt.Errorf("query EB transaction outputs on %s: %w", addr, err)
	}
	return len(result.Results) > 0, nil
}
