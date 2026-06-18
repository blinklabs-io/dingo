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

// Package archivedemo provides shared helpers for the archive-node demo
// at internal/test/archive-demo/. The integration test (under build tag
// archive_demo) and the demo-fetch CLI both consume it.
package archivedemo

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	gcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// DefaultNetworkMagic matches the value baked into testnet.yaml.
const DefaultNetworkMagic uint32 = 42

// Endpoint is a named TCP target speaking Ouroboros NtN.
type Endpoint struct {
	Name    string
	Address string
}

// DefaultEndpoints returns the archive and history-expiry Dingo endpoints,
// honoring env overrides for CI flexibility.
func DefaultEndpoints() (archive, pruning Endpoint) {
	arch := os.Getenv("ARCHIVEDEMO_DINGO_ARCHIVE_ADDR")
	if arch == "" {
		arch = "localhost:3111"
	}
	prn := os.Getenv("ARCHIVEDEMO_DINGO_PRUNING_ADDR")
	if prn == "" {
		prn = "localhost:3113"
	}
	return Endpoint{Name: "dingo-archive", Address: arch},
		Endpoint{Name: "dingo-pruning", Address: prn}
}

// Tip is a small subset of gouroboros' chainsync.Tip used by the harness.
type Tip struct {
	Slot  uint64
	Block uint64
	Hash  []byte
}

// Logger is a minimal printf-style logger callers can plug in. Both
// testing.T.Logf and a stdlib log helper satisfy this signature.
type Logger func(format string, args ...any)

// dial opens an Ouroboros NtN connection to ep with optional extra config.
// The caller is responsible for Close().
func dial(ep Endpoint, magic uint32, extra ...ouroboros.ConnectionOptionFunc) (*ouroboros.Connection, error) {
	conn, err := net.DialTimeout("tcp", ep.Address, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", ep.Name, err)
	}
	opts := append([]ouroboros.ConnectionOptionFunc{
		ouroboros.WithConnection(conn),
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
	}, extra...)
	oConn, err := ouroboros.NewConnection(opts...)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ouroboros %s: %w", ep.Name, err)
	}
	return oConn, nil
}

// GetTip returns the current chain tip as reported by ep over a fresh
// connection. cardano-node only updates the tip on a new connection, so
// callers should not cache the connection.
func GetTip(ep Endpoint, magic uint32) (Tip, error) {
	oc, err := dial(ep, magic)
	if err != nil {
		return Tip{}, err
	}
	defer oc.Close()
	tip, err := oc.ChainSync().Client.GetCurrentTip()
	if err != nil {
		return Tip{}, fmt.Errorf("tip from %s: %w", ep.Name, err)
	}
	return Tip{
		Slot:  tip.Point.Slot,
		Block: tip.BlockNumber,
		Hash:  tip.Point.Hash,
	}, nil
}

// WaitForSlot polls ep until its tip slot reaches target or timeout fires.
// Returns the final observed tip and an error on timeout.
func WaitForSlot(ep Endpoint, magic uint32, target uint64, timeout time.Duration, log Logger) (Tip, error) {
	deadline := time.Now().Add(timeout)
	var last Tip
	for {
		tip, err := GetTip(ep, magic)
		if err == nil {
			last = tip
			if log != nil {
				log("WaitForSlot %s: slot=%d block=%d", ep.Name, tip.Slot, tip.Block)
			}
			if tip.Slot >= target {
				return tip, nil
			}
		} else if log != nil {
			log("WaitForSlot %s: %v", ep.Name, err)
		}
		if time.Now().After(deadline) {
			return last, fmt.Errorf("%s did not reach slot %d within %s", ep.Name, target, timeout)
		}
		time.Sleep(2 * time.Second)
	}
}

// FindBlockAtOrAfterSlot ChainSync-walks ep from origin and returns the
// first block point whose slot is >= targetSlot.
func FindBlockAtOrAfterSlot(
	ep Endpoint,
	magic uint32,
	targetSlot uint64,
	timeout time.Duration,
	log Logger,
) (pcommon.Point, error) {
	var (
		mu        sync.Mutex
		found     pcommon.Point
		foundOnce sync.Once
		done      = make(chan struct{})
	)

	rollForward := func(_ chainsync.CallbackContext, blockType uint, blockOrHeader any, _ chainsync.Tip) error {
		hdr, ok := blockOrHeader.(gcommon.BlockHeader)
		if !ok {
			if log != nil {
				log("FindBlockAtOrAfterSlot: roll-forward got %T (blockType=%d), expected BlockHeader", blockOrHeader, blockType)
			}
			return nil
		}
		if hdr.SlotNumber() < targetSlot {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		foundOnce.Do(func() {
			found = pcommon.Point{
				Slot: hdr.SlotNumber(),
				Hash: hdr.Hash().Bytes(),
			}
			close(done)
		})
		return errors.New("archivedemo: target reached")
	}

	rollBackward := func(_ chainsync.CallbackContext, _ pcommon.Point, _ chainsync.Tip) error {
		// Server sends RollBackward to origin on Sync, before any
		// RollForward. We don't care; just acknowledge.
		return nil
	}

	cfg := chainsync.NewConfig(
		chainsync.WithRollForwardFunc(rollForward),
		chainsync.WithRollBackwardFunc(rollBackward),
	)
	oc, err := dial(ep, magic, ouroboros.WithChainSyncConfig(cfg))
	if err != nil {
		return pcommon.Point{}, fmt.Errorf("dial %s: %w", ep.Name, err)
	}
	defer oc.Close()

	// Origin point tells the peer to start streaming from genesis on the
	// first roll-forward.
	syncErr := make(chan error, 1)
	go func() {
		if err := oc.ChainSync().Client.Sync([]pcommon.Point{pcommon.NewPointOrigin()}); err != nil {
			syncErr <- err
		}
	}()

	select {
	case <-done:
		return found, nil
	case err := <-syncErr:
		// rollForward returns a sentinel error to stop streaming once
		// we have a match; treat that as success.
		select {
		case <-done:
			return found, nil
		default:
		}
		return pcommon.Point{}, fmt.Errorf("chainsync %s: %w", ep.Name, err)
	case <-time.After(timeout):
		return pcommon.Point{}, fmt.Errorf("FindBlockAtOrAfterSlot: did not find block at/after slot %d on %s within %s",
			targetSlot, ep.Name, timeout)
	}
}

// FetchBlock issues BlockFetch for the given point on ep and returns the
// raw block CBOR. Returns an error if BlockFetch fails or no block is
// delivered before the timeout.
func FetchBlock(
	ep Endpoint,
	magic uint32,
	point pcommon.Point,
	timeout time.Duration,
) ([]byte, error) {
	var (
		mu       sync.Mutex
		gotCbor  []byte
		gotBlock = make(chan struct{}, 1)
		batch    = make(chan struct{}, 1)
	)
	blockFunc := func(_ blockfetch.CallbackContext, _ uint, b gledger.Block) error {
		mu.Lock()
		gotCbor = b.Cbor()
		mu.Unlock()
		select {
		case gotBlock <- struct{}{}:
		default:
		}
		return nil
	}
	batchDone := func(_ blockfetch.CallbackContext) error {
		select {
		case batch <- struct{}{}:
		default:
		}
		return nil
	}

	cfg, err := blockfetch.NewConfig(
		blockfetch.WithBlockFunc(blockFunc),
		blockfetch.WithBatchDoneFunc(batchDone),
		blockfetch.WithBatchStartTimeout(timeout),
		blockfetch.WithBlockTimeout(timeout),
	)
	if err != nil {
		return nil, err
	}
	oc, err := dial(ep, magic, ouroboros.WithBlockFetchConfig(cfg))
	if err != nil {
		return nil, err
	}
	defer oc.Close()

	// Single-point fetch: start == end is inclusive on both ends.
	if err := oc.BlockFetch().Client.GetBlockRange(point, point); err != nil {
		return nil, fmt.Errorf("blockfetch %s: %w", ep.Name, err)
	}

	select {
	case <-gotBlock:
		mu.Lock()
		defer mu.Unlock()
		return gotCbor, nil
	case <-batch:
		// Prefer a delivered block if blockFunc and batchDone raced.
		select {
		case <-gotBlock:
			mu.Lock()
			defer mu.Unlock()
			return gotCbor, nil
		default:
			return nil, fmt.Errorf("blockfetch %s: batch completed with no block delivered", ep.Name)
		}
	case err := <-oc.ErrorChan():
		return nil, fmt.Errorf("blockfetch %s connection error: %w", ep.Name, err)
	case <-time.After(timeout):
		return nil, fmt.Errorf("blockfetch %s timed out after %s", ep.Name, timeout)
	}
}
