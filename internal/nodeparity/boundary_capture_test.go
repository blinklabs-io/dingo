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
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

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
