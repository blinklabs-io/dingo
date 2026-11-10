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
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// take must refuse a capture read at another block of the same epoch, since
// after a rollback that block answers for a different chain, and must drop
// every earlier epoch's file, which the main loop has already passed.
func TestBoundaryCapturesTake(t *testing.T) {
	dir := t.TempDir()
	captures := newBoundaryCaptures(dir)
	file := func(name string) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, nil, 0o600))
		return path
	}
	p5 := pcommon.NewPoint(500, []byte{5})
	p6 := pcommon.NewPoint(600, []byte{6})
	old := file("old")
	cur := file("cur")
	require.NoError(t, captures.put(&boundaryCapture{epoch: 5, point: p5, utxoPath: old}))
	require.NoError(t, captures.put(&boundaryCapture{epoch: 6, point: p6, utxoPath: cur}))

	assert.Nil(t, captures.take(6, pcommon.NewPoint(600, []byte{9})))
	_, err := os.Stat(old)
	assert.True(t, os.IsNotExist(err), "an earlier epoch's capture file is removed")

	c := captures.take(6, p6)
	require.NotNil(t, c)
	assert.Equal(t, cur, c.utxoPath)
	assert.Nil(t, captures.take(6, p6), "a capture is handed over once")
}

// requireCaptureDirEmpty waits for a compared capture's files to be removed,
// which happens just after its epoch is reported.
func requireCaptureDirEmpty(t *testing.T, dir string) {
	t.Helper()
	require.Eventually(t, func() bool {
		entries, err := os.ReadDir(dir)
		return err == nil && len(entries) == 0
	}, 5*time.Second, 10*time.Millisecond, "a compared capture leaves no file behind")
}

// The follower captures at the first block of each epoch after the one it
// starts in, and at no other block.
func TestRunBoundaryFollowerCapturesFirstBlockOfEachEpoch(t *testing.T) {
	const magic = 42
	server := newGenesisFakeServer(t, 6)
	server.ungated = true
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	epochBySlot := map[uint64]uint64{}
	for i, p := range server.chain.Points {
		epochBySlot[p.Slot] = uint64(1 + i/2)
	}
	epochOf := func(_ context.Context, slot uint64) (uint64, error) {
		return epochBySlot[slot], nil
	}

	var (
		mu       sync.Mutex
		captured []uint64
	)
	captures := newBoundaryCaptures(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		runBoundaryFollower(
			ctx, listener.Addr().String(), magic, pcommon.NewPointOrigin(),
			epochOf,
			func(_ context.Context, point pcommon.Point, epoch uint64) *boundaryCapture {
				mu.Lock()
				captured = append(captured, epoch)
				mu.Unlock()
				return &boundaryCapture{epoch: epoch, point: point}
			},
			captures, t.Logf,
		)
	}()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(captured) >= 2
	}, 10*time.Second, 10*time.Millisecond)
	cancel()
	<-done

	assert.Equal(t, []uint64{2, 3}, captured)
	assert.NotNil(t, captures.take(2, server.chain.Points[2]))
	assert.NotNil(t, captures.take(3, server.chain.Points[4]))
}

// TestRunBoundaryFollowerRetriesABoundaryItCouldNotPlace covers an epoch
// lookup that fails on an epoch's first block: the session ends before the
// block counts as seen, so the reconnect delivers it again and the capture
// lands on that block, the one the main loop asks for, not on the next one.
func TestRunBoundaryFollowerRetriesABoundaryItCouldNotPlace(t *testing.T) {
	const magic = 42
	server := newGenesisFakeServer(t, 6)
	server.ungated = true
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	epochBySlot := map[uint64]uint64{}
	for i, p := range server.chain.Points {
		epochBySlot[p.Slot] = uint64(1 + i/2)
	}
	var (
		mu       sync.Mutex
		failed   bool
		captured []uint64
	)
	boundary := server.chain.Points[2]
	epochOf := func(_ context.Context, slot uint64) (uint64, error) {
		mu.Lock()
		defer mu.Unlock()
		if slot == boundary.Slot && !failed {
			failed = true
			return 0, errors.New("simulated era history lookup failure")
		}
		return epochBySlot[slot], nil
	}

	captures := newBoundaryCaptures(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runBoundaryFollower(
			ctx, listener.Addr().String(), magic, pcommon.NewPointOrigin(),
			epochOf,
			func(_ context.Context, point pcommon.Point, epoch uint64) *boundaryCapture {
				mu.Lock()
				captured = append(captured, epoch)
				mu.Unlock()
				return &boundaryCapture{epoch: epoch, point: point}
			},
			captures, t.Logf,
		)
	}()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(captured) >= 2
	}, 20*time.Second, 10*time.Millisecond)
	cancel()
	<-done

	assert.Equal(t, []uint64{2, 3}, captured)
	assert.NotNil(t, captures.take(2, boundary),
		"the epoch 2 capture must be at its first block")
	assert.NotNil(t, captures.take(3, server.chain.Points[4]))
}

// TestCaptureBoundaryReadsUTxOSetFirst covers the mainnet failure where the
// stake check's Koios wait outlasted Dingo's rollback window and the UTxO
// read that followed it was refused as too old.
func TestCaptureBoundaryReadsUTxOSetFirst(t *testing.T) {
	t.Parallel()
	var order []string
	point := pcommon.NewPoint(10, []byte{0x01})
	c := captureBoundaryInOrder(
		3, point,
		func() (string, error) {
			order = append(order, "utxo")
			return "utxo-epoch-3-slot-10.gob", nil
		},
		func(c *boundaryCapture) {
			order = append(order, "checks")
			assert.Equal(t, "utxo-epoch-3-slot-10.gob", c.utxoPath,
				"the UTxO set is already read when the Koios-bound checks start")
		},
	)
	assert.Equal(t, []string{"utxo", "checks"}, order)
	assert.Equal(t, uint64(3), c.epoch)
	assert.True(t, pointsEqual(point, c.point))
}

// TestCaptureBoundaryRunsChecksWhenUTxOReadFails proves a failed UTxO read
// still leaves the protocol-parameter and stake checks to run.
func TestCaptureBoundaryRunsChecksWhenUTxOReadFails(t *testing.T) {
	t.Parallel()
	ran := false
	c := captureBoundaryInOrder(
		3, pcommon.NewPoint(10, []byte{0x01}),
		func() (string, error) { return "", errors.New("point too old") },
		func(*boundaryCapture) { ran = true },
	)
	assert.True(t, ran)
	assert.Empty(t, c.utxoPath)
	assert.EqualError(t, c.utxoErr, "point too old")
}

func withBoundaryFollower(
	f func(context.Context, *boundaryCaptures),
) FromGenesisOption {
	return func(o *fromGenesisOptions) { o.follower = f }
}

// TestRunFromGenesis_ComparesAgainstBoundaryCapture drives the main loop
// through an epoch boundary that has a capture. The capture's UTxO set holds
// one ref the fake Dingo's live answer lacks, and its protocol-parameter
// result is a marker error, so both reaching the epoch result proves the
// boundary was checked against the capture rather than queried live. The
// capture file is removed once compared.
func TestRunFromGenesis_ComparesAgainstBoundaryCapture(t *testing.T) {
	const magic = 42
	server := newGenesisFakeServer(t, 3)
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 1,
		server.chain.Points[1].Slot: 2,
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
	require.NoError(t, err)

	dir := t.TempDir()
	path := filepath.Join(dir, "utxo-epoch-2.gob")
	ppMarker := errors.New("protocol params read at the boundary capture")
	put := make(chan struct{})
	// The file is written once the run has started, as the real follower
	// does: the run removes a UTxO file no capture record names.
	follower := func(_ context.Context, captures *boundaryCaptures) {
		require.NoError(t, writeUTxOSetFile(path, UTxOSet{"ff#0": "addr|1"}))
		_ = captures.put(&boundaryCapture{
			epoch:    2,
			point:    server.chain.Points[1],
			ppErr:    ppMarker,
			utxoPath: path,
		})
		close(put)
	}

	results := make(chan EpochResult, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RunFromGenesis(
			ctx, listener.Addr().String(), "preview", magic, koios, nil,
			func(r EpochResult) { results <- r },
			nil, nil, false,
			WithBoundaryCaptureDir(dir), withBoundaryFollower(follower),
		)
	}()
	testutil.RequireReceive(t, put, 5*time.Second, "capture handed to the main loop")

	server.allowStep(t) // block 0: the UTxO baseline
	server.allowStep(t) // block 1: the epoch 2 boundary
	r := testutil.RequireReceive(t, results, 10*time.Second, "epoch 2 result")

	assert.Equal(t, uint64(2), r.Epoch)
	assert.ErrorIs(t, r.ProtocolParamsErr, ppMarker)
	assert.True(t, r.UTxOAttempted)
	require.NoError(t, r.UTxOErr)
	assert.Equal(t, []string{"ff#0"}, r.UTxOExtra)
	assert.Empty(t, r.UTxOMissing)
	requireCaptureDirEmpty(t, dir)

	cancel()
	err = testutil.RequireReceive(t, done, 10*time.Second, "RunFromGenesis exit")
	assert.True(t, err == nil || errors.Is(err, context.Canceled))
}

// TestRunFromGenesis_HandsBackACaptureInterruptedAfterItsRead ends the session
// right after the epoch 2 boundary's capture is read, before the boundary is
// reported. The resumed session delivers that block again and must check it
// against the same capture, not live: the protocol-parameter marker proves
// the capture's checks were kept, for a capture with a UTxO file and for one
// whose UTxO read had failed.
func TestRunFromGenesis_HandsBackACaptureInterruptedAfterItsRead(t *testing.T) {
	ppMarker := errors.New("protocol params read at the boundary capture")
	utxoMarker := errors.New("UTxO set unreadable at the boundary capture")
	for _, tc := range []struct {
		name    string
		utxoErr error
	}{
		{name: "with UTxO file"},
		{name: "without UTxO set", utxoErr: utxoMarker},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const magic = 42
			server := newGenesisFakeServer(t, 3)
			server.epochBySlot = map[uint64]int{
				server.chain.Points[0].Slot: 1,
				server.chain.Points[1].Slot: 2,
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(func() { _ = listener.Close() })
			server.serve(t, listener, magic)

			koiosSrv := httptest.NewServer(http.NotFoundHandler())
			t.Cleanup(koiosSrv.Close)
			koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
			require.NoError(t, err)

			dir := t.TempDir()
			capture := &boundaryCapture{
				epoch:   2,
				point:   server.chain.Points[1],
				ppErr:   ppMarker,
				utxoErr: tc.utxoErr,
			}
			if tc.utxoErr == nil {
				capture.utxoPath = filepath.Join(dir, "utxo-epoch-2.gob")
			}
			put := make(chan struct{})
			follower := func(_ context.Context, captures *boundaryCaptures) {
				if capture.utxoPath != "" {
					require.NoError(t, writeUTxOSetFile(
						capture.utxoPath, UTxOSet{"ff#0": "addr|1"},
					))
				}
				_ = captures.put(capture)
				close(put)
			}
			var interrupted atomic.Bool
			interrupt := func(sessionCtx context.Context) {
				if !interrupted.CompareAndSwap(false, true) {
					return
				}
				server.mu.Lock()
				conn := server.conns[server.chainSyncAddr]
				server.mu.Unlock()
				require.NotNil(t, conn)
				_ = conn.Close()
				select {
				case <-sessionCtx.Done():
				case <-time.After(10 * time.Second):
					t.Error("closing the chainsync connection did not end the session")
				}
			}

			logs := newLogCollector()
			results := make(chan EpochResult, 8)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- RunFromGenesis(
					ctx, listener.Addr().String(), "preview", magic, koios, nil,
					func(r EpochResult) { results <- r },
					logs.logf, nil, false,
					WithBoundaryCaptureDir(dir), withBoundaryFollower(follower),
					func(o *fromGenesisOptions) {
						o.afterCapturedUTxORead = interrupt
					},
				)
			}()
			testutil.RequireReceive(t, put, 5*time.Second, "capture handed to the main loop")

			server.allowStep(t) // block 0: the UTxO baseline
			server.allowStep(t) // block 1: the epoch 2 boundary, interrupted
			server.waitForSessions(t, 2)
			server.allowStep(t) // block 1 again, on the resumed session
			t.Cleanup(func() { t.Logf("logs: %q", logs.all()) })
			r := testutil.RequireReceive(t, results, 20*time.Second, "epoch 2 result")

			require.True(t, interrupted.Load())
			assert.Equal(t, uint64(2), r.Epoch)
			assert.ErrorIs(t, r.ProtocolParamsErr, ppMarker,
				"the boundary was checked live, not against its capture")
			if tc.utxoErr != nil {
				assert.ErrorIs(t, r.UTxOErr, utxoMarker)
			} else {
				require.NoError(t, r.UTxOErr)
				assert.Equal(t, []string{"ff#0"}, r.UTxOExtra)
			}
			requireCaptureDirEmpty(t, dir)

			cancel()
			err = testutil.RequireReceive(t, done, 10*time.Second, "RunFromGenesis exit")
			assert.True(t, err == nil || errors.Is(err, context.Canceled))
		})
	}
}

// TestRunFromGenesis_ResolvesACapturedBoundaryWithoutDingo covers a boundary
// Dingo has moved too far past to answer for, reached while its era history
// is unavailable. The epoch must come from the capture rather than from Dingo
// at that point, or the lookup is refused, the session resumes past the
// boundary block, and the capture is never used.
func TestRunFromGenesis_ResolvesACapturedBoundaryWithoutDingo(t *testing.T) {
	const magic = 42
	server := newGenesisFakeServer(t, 3)
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 1,
		server.chain.Points[1].Slot: 2,
		server.chain.Points[2].Slot: 2,
	}
	boundary := server.chain.Points[1]
	server.refuseAcquireFn = func(p pcommon.Point) bool {
		return pointsEqual(p, boundary)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
	require.NoError(t, err)

	dir := t.TempDir()
	path := filepath.Join(dir, "utxo-epoch-2.gob")
	ppMarker := errors.New("protocol params read at the boundary capture")
	put := make(chan struct{})
	// The file is written once the run has started, as the real follower
	// does: the run removes a UTxO file no capture record names.
	follower := func(_ context.Context, captures *boundaryCaptures) {
		require.NoError(t, writeUTxOSetFile(path, UTxOSet{"ff#0": "addr|1"}))
		_ = captures.put(&boundaryCapture{
			epoch:    2,
			point:    boundary,
			ppErr:    ppMarker,
			utxoPath: path,
		})
		close(put)
	}

	results := make(chan EpochResult, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RunFromGenesis(
			ctx, listener.Addr().String(), "preview", magic, koios, nil,
			func(r EpochResult) { results <- r },
			nil, nil, false,
			WithBoundaryCaptureDir(dir), withBoundaryFollower(follower),
		)
	}()
	testutil.RequireReceive(t, put, 5*time.Second, "capture handed to the main loop")

	server.allowStep(t) // block 0: the UTxO baseline
	server.allowStep(t) // block 1: the epoch 2 boundary
	r := testutil.RequireReceive(t, results, 10*time.Second, "epoch 2 result")

	assert.Equal(t, uint64(2), r.Epoch)
	assert.ErrorIs(t, r.ProtocolParamsErr, ppMarker,
		"the boundary was not checked against its capture")
	require.NoError(t, r.UTxOErr)
	assert.Equal(t, []string{"ff#0"}, r.UTxOExtra)

	cancel()
	err = testutil.RequireReceive(t, done, 10*time.Second, "RunFromGenesis exit")
	assert.True(t, err == nil || errors.Is(err, context.Canceled))
}

// TestRunFromGenesis_ComparesACaptureFromAnEarlierProcess captures the epoch
// 2 boundary in one RunFromGenesis, ends it before the reconstruction gets
// there, and starts a second one on the same directory. The second run must
// check the boundary against the first run's capture: Dingo may no longer
// answer for that point, so a capture lost on restart is an epoch lost.
func TestRunFromGenesis_ComparesACaptureFromAnEarlierProcess(t *testing.T) {
	const magic = 42
	// Each process gets its own Dingo serving the same chain, so nothing
	// the first left in flight on its connection reaches the second.
	newDingo := func(chain *genesisFakeServer) (*genesisFakeServer, string) {
		server := newGenesisFakeServer(t, 3)
		if chain != nil {
			server.chain = chain.chain
		}
		server.epochBySlot = map[uint64]int{
			server.chain.Points[0].Slot: 1,
			server.chain.Points[1].Slot: 2,
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = listener.Close() })
		server.serve(t, listener, magic)
		return server, listener.Addr().String()
	}
	server, firstAddr := newDingo(nil)

	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
	require.NoError(t, err)

	dir := t.TempDir()
	logs := newLogCollector()
	t.Cleanup(func() { t.Logf("logs: %q", logs.all()) })
	run := func(
		ctx context.Context, addr string,
		follower func(context.Context, *boundaryCaptures),
		results chan<- EpochResult,
	) <-chan error {
		done := make(chan error, 1)
		go func() {
			done <- RunFromGenesis(
				ctx, addr, "preview", magic, koios, nil,
				func(r EpochResult) { results <- r },
				logs.logf, nil, false,
				WithBoundaryCaptureDir(dir), withBoundaryFollower(follower),
			)
		}()
		return done
	}

	ppMarker := "protocol params read at the boundary capture"
	put := make(chan struct{})
	firstCtx, stopFirst := context.WithCancel(context.Background())
	defer stopFirst()
	first := run(firstCtx, firstAddr, func(_ context.Context, captures *boundaryCaptures) {
		path := filepath.Join(dir, captureUTxOFileName(2, server.chain.Points[1]))
		require.NoError(t, writeUTxOSetFile(path, UTxOSet{"ff#0": "addr|1"}))
		require.NoError(t, captures.put(&boundaryCapture{
			epoch:    2,
			point:    server.chain.Points[1],
			ppErr:    errors.New(ppMarker),
			utxoPath: path,
		}))
		close(put)
	}, make(chan EpochResult, 8))
	testutil.RequireReceive(t, put, 5*time.Second, "capture handed to the main loop")
	stopFirst()
	err = testutil.RequireReceive(t, first, 10*time.Second, "first RunFromGenesis exit")
	require.True(t, err == nil || errors.Is(err, context.Canceled), "%v", err)

	results := make(chan EpochResult, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	second, secondAddr := newDingo(server)
	done := run(ctx, secondAddr, func(context.Context, *boundaryCaptures) {}, results)

	second.allowStep(t) // block 0: the UTxO baseline
	second.allowStep(t) // block 1: the epoch 2 boundary
	r := testutil.RequireReceive(t, results, 10*time.Second, "epoch 2 result")

	assert.Equal(t, uint64(2), r.Epoch)
	require.Error(t, r.ProtocolParamsErr)
	assert.Equal(t, ppMarker, r.ProtocolParamsErr.Error(),
		"the boundary was checked live, not against the earlier capture")
	require.NoError(t, r.UTxOErr)
	assert.Equal(t, []string{"ff#0"}, r.UTxOExtra)
	requireCaptureDirEmpty(t, dir)

	cancel()
	err = testutil.RequireReceive(t, done, 10*time.Second, "RunFromGenesis exit")
	assert.True(t, err == nil || errors.Is(err, context.Canceled))
}

// A boundary the follower already holds a capture for, such as one an earlier
// run left behind, is not captured again: Dingo may no longer answer for it,
// and a refused capture would replace a good one.
func TestRunBoundaryFollowerKeepsAHeldCapture(t *testing.T) {
	const magic = 42
	server := newGenesisFakeServer(t, 6)
	server.ungated = true
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	epochBySlot := map[uint64]uint64{}
	for i, p := range server.chain.Points {
		epochBySlot[p.Slot] = uint64(1 + i/2)
	}
	epochOf := func(_ context.Context, slot uint64) (uint64, error) {
		return epochBySlot[slot], nil
	}

	captures := newBoundaryCaptures(t.TempDir())
	held := &boundaryCapture{
		epoch: 2, point: server.chain.Points[2], utxoErr: errors.New("held"),
	}
	require.NoError(t, captures.put(held))
	var (
		mu       sync.Mutex
		captured []uint64
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		runBoundaryFollower(
			ctx, listener.Addr().String(), magic, pcommon.NewPointOrigin(),
			epochOf,
			func(_ context.Context, point pcommon.Point, epoch uint64) *boundaryCapture {
				mu.Lock()
				captured = append(captured, epoch)
				mu.Unlock()
				return &boundaryCapture{
					epoch: epoch, point: point, utxoErr: errors.New("new"),
				}
			},
			captures, t.Logf,
		)
	}()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(captured) >= 1
	}, 10*time.Second, 10*time.Millisecond)
	cancel()
	<-done

	assert.Equal(t, []uint64{3}, captured)
	assert.Same(t, held, captures.take(2, server.chain.Points[2]))
}

// load picks up the captures a run left behind for boundaries after its start
// point and removes everything else, and never reads back a check that
// failed, or a record it cannot read, as a passed check.
func TestBoundaryCapturesLoad(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer := newBoundaryCaptures(dir)
	p := func(slot uint64, hash byte) pcommon.Point {
		return pcommon.NewPoint(slot, []byte{hash, 0xab})
	}
	utxo := func(epoch uint64, point pcommon.Point) string {
		path := filepath.Join(dir, captureUTxOFileName(epoch, point))
		require.NoError(t, writeUTxOSetFile(path, UTxOSet{"aa#0": "addr|1"}))
		return path
	}
	exists := func(path string) bool { return fileExists(path) }

	// Epoch 4's boundary is at the start point, so never delivered again.
	passed := &boundaryCapture{epoch: 4, point: p(400, 4)}
	passed.utxoPath = utxo(4, passed.point)
	require.NoError(t, writer.put(passed))

	// Epoch 5 round-trips every result, including an error with no message.
	mismatch := StakeMismatch{PoolIDBech32: "pool1x", DingoStake: 7, KoiosStake: "8"}
	future := &boundaryCapture{
		epoch:           5,
		point:           p(500, 5),
		ppErr:           errors.New(""),
		stakeMismatches: []StakeMismatch{mismatch},
		psElapsed:       3 * time.Second,
	}
	future.utxoPath = utxo(5, future.point)
	require.NoError(t, writer.put(future))

	// Epoch 6 has two records, from a boundary block a rollback replaced.
	// put itself keeps only one, so the stale one is written directly.
	replaced := &boundaryCapture{epoch: 6, point: p(600, 1), utxoErr: errors.New("x")}
	replacedPath, err := writeCaptureRecord(dir, replaced)
	require.NoError(t, err)
	var rec boundaryCaptureRecord
	data, err := os.ReadFile(replacedPath)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &rec))
	rec.CapturedAt = rec.CapturedAt.Add(-time.Hour)
	data, err = json.Marshal(rec)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(replacedPath, data, 0o600))
	latest := &boundaryCapture{epoch: 6, point: p(601, 2)}
	latest.utxoPath = utxo(6, latest.point)
	_, err = writeCaptureRecord(dir, latest)
	require.NoError(t, err)

	// Epoch 7's record was cut short.
	corrupt := p(700, 7)
	corruptUTxO := utxo(7, corrupt)
	corruptRecord := filepath.Join(dir, captureRecordFileName(7, corrupt))
	require.NoError(t, os.WriteFile(corruptRecord, []byte(`{"Version":1,"Ep`), 0o600))

	orphan := utxo(8, p(800, 8))
	partial := filepath.Join(dir, "capture-epoch-9-slot-900-09.json"+partialSuffix)
	require.NoError(t, os.WriteFile(partial, []byte("{"), 0o600))
	unrelated := filepath.Join(dir, "notes.txt")
	require.NoError(t, os.WriteFile(unrelated, nil, 0o600))

	captures := newBoundaryCaptures(dir)
	loaded, removed, err := captures.load(passed.point)
	require.NoError(t, err)
	assert.Equal(t, 3, loaded)
	// passed, the superseded epoch 6 record, the orphan and the partial file.
	assert.Equal(t, 4, removed)

	assert.False(t, exists(passed.utxoPath), "a passed boundary's UTxO file is removed")
	assert.False(t, exists(passed.recordPath), "a passed boundary's record is removed")
	assert.False(t, exists(orphan), "a UTxO file no record names is removed")
	assert.False(t, exists(partial), "a partly written file is removed")
	assert.True(t, exists(unrelated), "a file load does not own is kept")

	c := captures.take(5, future.point)
	require.NotNil(t, c)
	require.Error(t, c.ppErr, "a failed check must not read back as passed")
	assert.Equal(t, "", c.ppErr.Error())
	require.NoError(t, c.stakeErr)
	assert.Equal(t, []StakeMismatch{mismatch}, c.stakeMismatches)
	assert.Equal(t, 3*time.Second, c.psElapsed)
	assert.Equal(t, future.utxoPath, c.utxoPath)
	require.NoError(t, c.utxoErr)

	assert.Nil(t, captures.take(6, replaced.point))
	c = captures.take(6, latest.point)
	require.NotNil(t, c, "the later of two records for one epoch is kept")
	assert.False(t, exists(replacedPath))

	c = captures.take(7, corrupt)
	require.NotNil(t, c, "an unreadable record still holds its epoch")
	for _, err := range []error{c.ppErr, c.stakeErr, c.utxoErr} {
		require.Error(t, err)
		assert.Contains(t, err.Error(), "did not run")
	}
	assert.Equal(t, corruptUTxO, c.utxoPath)
	c.removeFiles()
	assert.False(t, exists(corruptRecord))
	assert.False(t, exists(corruptUTxO))
}

// TestRunFromGenesis_WaitsForACaptureStillBeingMade covers the reconstruction
// reaching a boundary while the follower is still capturing it: the capture's
// protocol-parameter and stake checks wait on Koios, on mainnet for over an
// hour after the UTxO set was read. Dingo has moved past the boundary, so the
// main loop must wait for the capture rather than ask Dingo, which refuses
// the point as too old and leaves every check of the epoch not run.
func TestRunFromGenesis_WaitsForACaptureStillBeingMade(t *testing.T) {
	const magic = 42
	server := newGenesisFakeServer(t, 3)
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 1,
		server.chain.Points[1].Slot: 2,
		server.chain.Points[2].Slot: 2,
	}
	boundary := server.chain.Points[1]
	server.refuseAcquireFn = func(p pcommon.Point) bool {
		return pointsEqual(p, boundary)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
	require.NoError(t, err)

	dir := t.TempDir()
	ppMarker := errors.New("protocol params read at the boundary capture")
	begun := make(chan struct{})
	finishCapture := make(chan struct{})
	follower := func(ctx context.Context, captures *boundaryCaptures) {
		captures.begin(2, boundary)
		close(begun)
		select {
		case <-finishCapture:
		case <-ctx.Done():
			_ = captures.finish(2, boundary, nil)
			return
		}
		path := filepath.Join(dir, captureUTxOFileName(2, boundary))
		assert.NoError(t, writeUTxOSetFile(path, UTxOSet{"ff#0": "addr|1"}))
		assert.NoError(t, captures.finish(2, boundary, &boundaryCapture{
			epoch: 2, point: boundary, ppErr: ppMarker, utxoPath: path,
		}))
	}

	logs := newLogCollector()
	t.Cleanup(func() { t.Logf("logs: %q", logs.all()) })
	results := make(chan EpochResult, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RunFromGenesis(
			ctx, listener.Addr().String(), "preview", magic, koios, nil,
			func(r EpochResult) { results <- r },
			logs.logf, nil, false,
			WithBoundaryCaptureDir(dir), withBoundaryFollower(follower),
		)
	}()
	testutil.RequireReceive(t, begun, 5*time.Second, "capture begun")

	server.allowStep(t) // block 0: the UTxO baseline
	server.allowStep(t) // block 1: the epoch 2 boundary, still being captured
	require.True(t,
		logs.waitFor("waiting for Dingo's epoch 2 boundary capture", 10*time.Second),
		"the main loop did not wait for the capture in progress",
	)
	close(finishCapture)
	r := testutil.RequireReceive(t, results, 10*time.Second, "epoch 2 result")

	assert.Equal(t, uint64(2), r.Epoch)
	assert.ErrorIs(t, r.ProtocolParamsErr, ppMarker,
		"the boundary was not checked against its capture")
	require.NoError(t, r.UTxOErr)
	assert.Equal(t, []string{"ff#0"}, r.UTxOExtra)
	requireCaptureDirEmpty(t, dir)

	cancel()
	err = testutil.RequireReceive(t, done, 10*time.Second, "RunFromGenesis exit")
	assert.True(t, err == nil || errors.Is(err, context.Canceled))
}

// TestRunFromGenesis_RedeliversACapturedBoundaryAfterAFailedFlush covers the
// session ending while a captured boundary block's transactions are fetched,
// before its checks run. The capture answers only for that exact block, so
// the resumed session must deliver it again: resuming after it would check
// the epoch live at a later block, which Dingo refuses as too old. The
// block's own hash must also not be buffered twice.
func TestRunFromGenesis_RedeliversACapturedBoundaryAfterAFailedFlush(t *testing.T) {
	for _, tc := range []struct {
		name string
		// epochs gives each block's epoch; the last block of epoch 1 is
		// followed by the captured epoch 2 boundary.
		epochs []int
		// limits, when set, lowers the flush threshold to 4 hashes so the
		// boundary block's own flush is the threshold one.
		limits bool
	}{
		{name: "flush before the checks", epochs: []int{1, 2, 2}},
		{
			name:   "flush at the threshold",
			epochs: []int{1, 1, 1, 1, 1, 2, 2},
			limits: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const magic = 42
			server := newGenesisFakeServerWithTransactions(t, len(tc.epochs))
			server.epochBySlot = map[uint64]int{}
			boundaryIdx := -1
			for i, e := range tc.epochs {
				server.epochBySlot[server.chain.Points[i].Slot] = e
				if e == 2 && boundaryIdx < 0 {
					boundaryIdx = i
				}
			}
			boundary := server.chain.Points[boundaryIdx]
			boundaryHash := server.chain.Blocks[boundaryIdx].Transactions()[0].Hash().String()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(func() { _ = listener.Close() })
			server.serve(t, listener, magic)

			// /tx_info answers every hash with a transaction that spends and
			// creates nothing, except that the first request carrying the
			// boundary block's hash ends the chainsync session and is never
			// answered.
			var (
				interrupted  atomic.Bool
				answeredMu   sync.Mutex
				boundarySeen int
			)
			koiosSrv := httptest.NewServer(http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					if !strings.HasSuffix(r.URL.Path, "/tx_info") {
						http.NotFound(w, r)
						return
					}
					var body struct {
						TxHashes []string `json:"_tx_hashes"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					if slices.Contains(body.TxHashes, boundaryHash) &&
						interrupted.CompareAndSwap(false, true) {
						server.mu.Lock()
						conn := server.conns[server.chainSyncAddr]
						server.mu.Unlock()
						if conn != nil {
							_ = conn.Close()
						}
						select {
						case <-r.Context().Done():
						case <-time.After(10 * time.Second):
						}
						return
					}
					items := make([]koiosparity.KoiosTxInfoItem, 0, len(body.TxHashes))
					answeredMu.Lock()
					for _, h := range body.TxHashes {
						if h == boundaryHash {
							boundarySeen++
						}
						items = append(items, koiosparity.KoiosTxInfoItem{TxHash: h})
					}
					answeredMu.Unlock()
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(items)
				},
			))
			t.Cleanup(koiosSrv.Close)
			koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
			require.NoError(t, err)
			if tc.limits {
				require.NoError(t, koios.SetTxInfoLimits(1, 1))
			}

			ppMarker := errors.New("protocol params read at the boundary capture")
			capture := &boundaryCapture{
				epoch:   2,
				point:   boundary,
				ppErr:   ppMarker,
				utxoErr: errors.New("no UTxO set at the boundary capture"),
			}
			put := make(chan struct{})
			follower := func(_ context.Context, captures *boundaryCaptures) {
				_ = captures.put(capture)
				close(put)
			}

			logs := newLogCollector()
			t.Cleanup(func() { t.Logf("logs: %q", logs.all()) })
			results := make(chan EpochResult, 16)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- RunFromGenesis(
					ctx, listener.Addr().String(), "preview", magic, koios, nil,
					func(r EpochResult) { results <- r },
					logs.logf, nil, false,
					WithBoundaryCaptureDir(t.TempDir()),
					withBoundaryFollower(follower),
				)
			}()
			testutil.RequireReceive(t, put, 5*time.Second, "capture handed to the main loop")

			for range boundaryIdx + 1 {
				server.allowStep(t)
			}
			server.waitForSessions(t, 2)
			require.True(t, interrupted.Load(), "the boundary flush was never interrupted")
			server.allowStep(t) // the boundary again, or the block after it

			var r EpochResult
			for r.Epoch != 2 {
				r = testutil.RequireReceive(t, results, 20*time.Second, "epoch 2 result")
			}
			assert.ErrorIs(t, r.ProtocolParamsErr, ppMarker,
				"the boundary was checked live at a later block, not against its capture")
			answeredMu.Lock()
			assert.Equal(t, 1, boundarySeen,
				"the boundary block's hash was fetched more than once")
			answeredMu.Unlock()

			cancel()
			err = testutil.RequireReceive(t, done, 10*time.Second, "RunFromGenesis exit")
			assert.True(t, err == nil || errors.Is(err, context.Canceled))
		})
	}
}

// TestBoundaryCapturesLoadRejectsForeignAndMalformedRecords covers records no
// boundary can use: one naming another boundary's UTxO file would compare
// this boundary against that set, and one whose name does not parse can never
// be matched to a boundary at all.
func TestBoundaryCapturesLoadRejectsForeignAndMalformedRecords(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	own := pcommon.NewPoint(500, []byte{0x05})
	other := pcommon.NewPoint(600, []byte{0x06})
	foreign := filepath.Join(dir, captureUTxOFileName(6, other))
	require.NoError(t, writeUTxOSetFile(foreign, UTxOSet{"aa#0": "addr|1"}))
	_, err := writeCaptureRecord(dir, &boundaryCapture{
		epoch: 5, point: own, utxoPath: foreign,
	})
	require.NoError(t, err)
	malformed := filepath.Join(dir, "capture-epoch-99999999999999999999999-slot-1-ab.json")
	require.NoError(t, os.WriteFile(malformed, []byte("{}"), 0o600))

	captures := newBoundaryCaptures(dir)
	loaded, removed, err := captures.load(pcommon.NewPoint(0, nil))
	require.NoError(t, err)
	assert.Equal(t, 1, loaded)
	// The malformed record, and the foreign UTxO file no usable record names.
	assert.Equal(t, 2, removed)
	assert.False(t, fileExists(malformed), "a record no boundary can match is removed")

	c := captures.take(5, own)
	require.NotNil(t, c, "the record still holds its epoch")
	for _, err := range []error{c.ppErr, c.stakeErr, c.utxoErr} {
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not this boundary's")
	}
	assert.Empty(t, c.utxoPath, "the other boundary's set is never read")
}

// TestReadUTxOSetFileRejectsADamagedCount covers a UTxO set file whose entry
// count cannot be right: it must be reported as unreadable, not panic or
// allocate for the count.
func TestReadUTxOSetFileRejectsADamagedCount(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		count int
	}{
		{name: "negative", count: -1},
		{name: "beyond the file size", count: 1 << 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "utxo-epoch-1-slot-1-01.gob")
			f, err := os.Create(path)
			require.NoError(t, err)
			require.NoError(t, gob.NewEncoder(f).Encode(tc.count))
			require.NoError(t, f.Close())
			_, err = readUTxOSetFile(path)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "claims")
		})
	}
}
