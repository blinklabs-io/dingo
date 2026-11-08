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

package nodeparity

import (
	"bufio"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// FromGenesisOption configures optional RunFromGenesis behaviour.
type FromGenesisOption func(*fromGenesisOptions)

type fromGenesisOptions struct {
	captureDir string
	// follower replaces runBoundaryFollower; only tests set it, since the
	// fake Dingo cannot serve a second chainsync session.
	follower func(context.Context, *boundaryCaptures)
	// afterCapturedUTxORead runs once a capture's UTxO set is read, or found
	// unreadable; only tests set it, to end the session before the boundary
	// is reported.
	afterCapturedUTxORead func(context.Context)
}

// WithBoundaryCaptureDir makes RunFromGenesis read Dingo's side of every
// epoch-boundary check as Dingo itself crosses the boundary, rather than when
// the UTxO reconstruction gets there, and keep each boundary's whole UTxO set
// in dir until the reconstruction compares against it.
//
// Dingo answers only for points within its rollback window of its tip, and
// the reconstruction moves at the speed Koios answers /tx_info, which on a
// busy chain is far slower than Dingo syncs. Without a capture every check at
// a boundary the reconstruction reaches late is refused as too old. Each
// capture file holds one whole UTxO set and is removed once compared.
func WithBoundaryCaptureDir(dir string) FromGenesisOption {
	return func(o *fromGenesisOptions) { o.captureDir = dir }
}

// boundaryCapture is Dingo's side of one epoch boundary's checks, read at
// point while Dingo could still answer for it. The protocol-parameter and
// stake results are complete checks, since their Koios side does not depend
// on the reconstruction.
type boundaryCapture struct {
	epoch           uint64
	point           pcommon.Point
	ppMismatches    []koiosparity.CheckMismatch
	ppErr           error
	stakeMismatches []StakeMismatch
	stakeErr        error
	psElapsed       time.Duration
	// utxoPath is the file holding Dingo's whole UTxO set at point; empty
	// when utxoErr says why it could not be read.
	utxoPath string
	utxoErr  error
}

// boundaryCaptures hands captures from the follower to the main loop.
type boundaryCaptures struct {
	mu      sync.Mutex
	dir     string
	byEpoch map[uint64]*boundaryCapture
}

func newBoundaryCaptures(dir string) *boundaryCaptures {
	return &boundaryCaptures{dir: dir, byEpoch: map[uint64]*boundaryCapture{}}
}

func (b *boundaryCaptures) put(c *boundaryCapture) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if old := b.byEpoch[c.epoch]; old != nil && old.utxoPath != "" &&
		old.utxoPath != c.utxoPath {
		_ = os.Remove(old.utxoPath)
	}
	b.byEpoch[c.epoch] = c
}

// take returns the capture for epoch, only if it was read at exactly point:
// a capture from a block that a rollback later replaced answers for a
// different chain. It also discards every capture for an earlier epoch, which
// the main loop has passed and will never ask for.
func (b *boundaryCaptures) take(
	epoch uint64, point pcommon.Point,
) *boundaryCapture {
	b.mu.Lock()
	defer b.mu.Unlock()
	for e, c := range b.byEpoch {
		if e < epoch {
			if c.utxoPath != "" {
				_ = os.Remove(c.utxoPath)
			}
			delete(b.byEpoch, e)
		}
	}
	c := b.byEpoch[epoch]
	if c == nil || !pointsEqual(c.point, point) {
		return nil
	}
	delete(b.byEpoch, epoch)
	return c
}

// epochAt returns the epoch of the capture read at exactly point, if one is
// held. A nil receiver holds none.
func (b *boundaryCaptures) epochAt(point pcommon.Point) (uint64, bool) {
	if b == nil {
		return 0, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range b.byEpoch {
		if pointsEqual(c.point, point) {
			return c.epoch, true
		}
	}
	return 0, false
}

// close removes every capture file still held.
func (b *boundaryCaptures) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for e, c := range b.byEpoch {
		if c.utxoPath != "" {
			_ = os.Remove(c.utxoPath)
		}
		delete(b.byEpoch, e)
	}
}

func writeUTxOSetFile(path string, set UTxOSet) (err error) {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			_ = os.Remove(path)
		}
	}()
	// One gob message per entry: encoding the map as a single value would
	// buffer the whole encoding, a second copy of a mainnet-sized set.
	w := bufio.NewWriterSize(f, 1<<20)
	enc := gob.NewEncoder(w)
	if err := enc.Encode(len(set)); err != nil {
		return err
	}
	for ref, entry := range set {
		if err := enc.Encode(utxoSetFileEntry{Ref: ref, Entry: entry}); err != nil {
			return err
		}
	}
	return w.Flush()
}

type utxoSetFileEntry struct {
	Ref   string
	Entry string
}

func readUTxOSetFile(path string) (UTxOSet, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck
	dec := gob.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	var n int
	if err := dec.Decode(&n); err != nil {
		return nil, err
	}
	set := make(UTxOSet, n)
	for range n {
		var e utxoSetFileEntry
		if err := dec.Decode(&e); err != nil {
			return nil, err
		}
		set[e.Ref] = e.Entry
	}
	return set, nil
}

// captureBoundary reads Dingo's side of epoch's checks at point. The UTxO set
// is read first: the protocol-parameter and stake checks wait on Koios, and
// on mainnet that wait has outlasted Dingo's rollback window, so a UTxO read
// after them was refused as too old.
func captureBoundary(
	ctx context.Context,
	dingoAddr string,
	magic uint32,
	point pcommon.Point,
	epoch uint64,
	koios *koiosparity.KoiosClient,
	cache *koiosparity.Cache,
	network string,
	dir string,
) *boundaryCapture {
	return captureBoundaryInOrder(
		epoch, point,
		func() (string, error) {
			return captureUTxOSetFile(ctx, dingoAddr, magic, point, epoch, dir)
		},
		func(c *boundaryCapture) {
			c.ppMismatches, c.ppErr, c.stakeMismatches, c.stakeErr = runProtocolParamsAndStake(
				ctx, dingoAddr, magic, point, koios, cache, network, epoch,
			)
		},
	)
}

// captureBoundaryInOrder runs readUTxO to completion before checks, which
// fills in the protocol-parameter and stake results.
func captureBoundaryInOrder(
	epoch uint64,
	point pcommon.Point,
	readUTxO func() (string, error),
	checks func(*boundaryCapture),
) *boundaryCapture {
	c := &boundaryCapture{epoch: epoch, point: point}
	c.utxoPath, c.utxoErr = readUTxO()
	psStart := time.Now()
	checks(c)
	c.psElapsed = time.Since(psStart)
	return c
}

func captureUTxOSetFile(
	ctx context.Context,
	dingoAddr string,
	magic uint32,
	point pcommon.Point,
	epoch uint64,
	dir string,
) (string, error) {
	set, err := captureGenesisBaseline(ctx, dingoAddr, magic, point)
	if err != nil {
		return "", fmt.Errorf("read Dingo's UTxO set at slot %d: %w", point.Slot, err)
	}
	path := filepath.Join(
		dir, fmt.Sprintf(
			"utxo-epoch-%d-slot-%d-%x.gob", epoch, point.Slot, point.Hash,
		),
	)
	if err := writeUTxOSetFile(path, set); err != nil {
		return "", fmt.Errorf("store Dingo's UTxO set at slot %d: %w", point.Slot, err)
	}
	return path, nil
}

// boundaryFollowerRetryDelay is the wait before the boundary follower
// reconnects after a session ends. It is a variable only so tests can
// shorten it; no production code changes it.
var boundaryFollowerRetryDelay = 5 * time.Second

// runBoundaryFollower follows Dingo's chain from start on its own chainsync
// connection, doing no per-block work, and at the first block of each epoch
// calls capture and hands the result to captures. It runs until ctx ends,
// reconnecting from the last block it saw.
func runBoundaryFollower(
	ctx context.Context,
	dingoAddr string,
	magic uint32,
	start pcommon.Point,
	epochOf func(context.Context, uint64) (uint64, error),
	capture func(context.Context, pcommon.Point, uint64) *boundaryCapture,
	captures *boundaryCaptures,
	logf func(format string, args ...any),
) {
	var (
		mu        sync.Mutex
		lastPoint = start
		prevEpoch uint64
		haveEpoch bool
	)
	if start.Slot != 0 || len(start.Hash) != 0 {
		if e, err := epochOf(ctx, start.Slot); err == nil {
			prevEpoch, haveEpoch = e, true
		}
	}
	for ctx.Err() == nil {
		sessionCtx, endSession := context.WithCancel(ctx)
		err := followBoundariesOnce(sessionCtx, dingoAddr, magic, &mu, &lastPoint,
			func(cbCtx context.Context, block lcommon.Block) error {
				if block.Era().Id == byron.EraIdByron {
					return nil
				}
				// An error ends the session before the block counts as
				// seen, so the next session delivers it again: skipping it
				// would capture the epoch's second block instead, which the
				// main loop never asks for.
				epoch, err := epochOf(cbCtx, block.SlotNumber())
				if err != nil {
					return fmt.Errorf("epoch of slot %d: %w", block.SlotNumber(), err)
				}
				if haveEpoch && epoch <= prevEpoch {
					return nil
				}
				point := pcommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
				c := capture(cbCtx, point, epoch)
				if err := cbCtx.Err(); err != nil {
					if c.utxoPath != "" {
						_ = os.Remove(c.utxoPath)
					}
					return err
				}
				captures.put(c)
				prevEpoch, haveEpoch = epoch, true
				if c.utxoErr != nil {
					logf(
						"nodeparity: captured Dingo's epoch %d boundary checks at "+
							"slot %d, but not its UTxO set: %v",
						epoch, point.Slot, c.utxoErr,
					)
				} else {
					logf(
						"nodeparity: captured Dingo's epoch %d boundary at slot %d "+
							"for the UTxO comparison",
						epoch, point.Slot,
					)
				}
				return nil
			},
			func(cbCtx context.Context, point pcommon.Point) {
				if point.Slot == 0 && len(point.Hash) == 0 {
					haveEpoch = false
					return
				}
				if e, err := epochOf(cbCtx, point.Slot); err == nil &&
					(!haveEpoch || e < prevEpoch) {
					prevEpoch, haveEpoch = e, true
				}
			},
		)
		endSession()
		if ctx.Err() != nil {
			return
		}
		logf("nodeparity: boundary follower session ended, reconnecting: %v", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(boundaryFollowerRetryDelay):
		}
	}
}

// followBoundariesOnce runs one chainsync session from *lastPoint until it
// ends, calling onBlock and onRollback under mu. A block becomes *lastPoint
// only once onBlock succeeds; an onBlock error ends the session.
func followBoundariesOnce(
	ctx context.Context,
	dingoAddr string,
	magic uint32,
	mu *sync.Mutex,
	lastPoint *pcommon.Point,
	onBlock func(context.Context, lcommon.Block) error,
	onRollback func(context.Context, pcommon.Point),
) error {
	rawConn, err := dialRaw(ctx, protoFromAddr(dingoAddr), dingoAddr)
	if err != nil {
		return err
	}
	conn, err := ouroboros.New(
		ouroboros.WithConnection(rawConn),
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(false),
		ouroboros.WithMuxerSegmentReadTimeout(0),
		ouroboros.WithChainSyncConfig(chainsync.NewConfig(
			chainsync.WithPipelineLimit(1),
			chainsync.WithRollForwardFunc(func(
				_ chainsync.CallbackContext, _ uint, blockData any, _ chainsync.Tip,
			) error {
				block, ok := blockData.(lcommon.Block)
				if !ok {
					return fmt.Errorf("unexpected roll-forward payload type %T", blockData)
				}
				mu.Lock()
				defer mu.Unlock()
				if err := onBlock(ctx, block); err != nil {
					return err
				}
				*lastPoint = pcommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
				return nil
			}),
			chainsync.WithRollBackwardFunc(func(
				_ chainsync.CallbackContext, point pcommon.Point, _ chainsync.Tip,
			) error {
				mu.Lock()
				defer mu.Unlock()
				*lastPoint = point
				onRollback(ctx, point)
				return nil
			}),
		)),
	)
	if err != nil {
		rawConn.Close() //nolint:errcheck
		return err
	}
	defer conn.Close()                                      //nolint:errcheck
	stop := context.AfterFunc(ctx, func() { conn.Close() }) //nolint:errcheck
	defer stop()
	cs := conn.ChainSync()
	if cs == nil || cs.Client == nil {
		return errors.New("ChainSync client unavailable")
	}
	mu.Lock()
	from := *lastPoint
	mu.Unlock()
	if err := cs.Client.Sync([]pcommon.Point{from}); err != nil {
		return err
	}
	go keepNtCConnAlive(ctx, conn, ntcKeepaliveInterval)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err, ok := <-conn.ErrorChan():
		if !ok {
			return errDingoClosedConnection
		}
		return err
	}
}
