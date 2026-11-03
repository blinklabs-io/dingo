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
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUTxOSetFileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "set.gob")
	want := UTxOSet{"aa#0": "addr1|5|", "bb#1": "addr2|7|x"}
	require.NoError(t, writeUTxOSetFile(path, want))
	got, err := readUTxOSetFile(path)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

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
	captures.put(&boundaryCapture{epoch: 5, point: p5, utxoPath: old})
	captures.put(&boundaryCapture{epoch: 6, point: p6, utxoPath: cur})

	assert.Nil(t, captures.take(6, pcommon.NewPoint(600, []byte{9})))
	_, err := os.Stat(old)
	assert.True(t, os.IsNotExist(err), "an earlier epoch's capture file is removed")

	c := captures.take(6, p6)
	require.NotNil(t, c)
	assert.Equal(t, cur, c.utxoPath)
	assert.Nil(t, captures.take(6, p6), "a capture is handed over once")
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
	require.NoError(t, writeUTxOSetFile(path, UTxOSet{"ff#0": "addr|1"}))
	ppMarker := errors.New("protocol params read at the boundary capture")
	put := make(chan struct{})
	follower := func(_ context.Context, captures *boundaryCaptures) {
		captures.put(&boundaryCapture{
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
	_, err = os.Stat(path)
	assert.ErrorIs(t, err, os.ErrNotExist, "a compared capture is removed")

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
				require.NoError(t, writeUTxOSetFile(
					capture.utxoPath, UTxOSet{"ff#0": "addr|1"},
				))
			}
			put := make(chan struct{})
			follower := func(_ context.Context, captures *boundaryCaptures) {
				captures.put(capture)
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
				_, err = os.Stat(capture.utxoPath)
				assert.ErrorIs(t, err, os.ErrNotExist,
					"a compared capture is removed")
			}

			cancel()
			err = testutil.RequireReceive(t, done, 10*time.Second, "RunFromGenesis exit")
			assert.True(t, err == nil || errors.Is(err, context.Canceled))
		})
	}
}
