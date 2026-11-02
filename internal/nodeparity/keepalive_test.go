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
	"sync/atomic"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol/localtxmonitor"
	"github.com/stretchr/testify/require"
)

// TestKeepNtCConnAliveSendsUntilCancelled checks that the keepalive keeps
// writing to the connection it shares with chainsync, which is what resets
// Dingo's 2-minute idle deadline, and that it stops once the session ends.
func TestKeepNtCConnAliveSendsUntilCancelled(t *testing.T) {
	const magic = 764824073
	var acquires atomic.Int64

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				oconn, err := ouroboros.New(
					ouroboros.WithConnection(conn),
					ouroboros.WithServer(true),
					ouroboros.WithNetworkMagic(magic),
					ouroboros.WithNodeToNode(false),
					ouroboros.WithLocalTxMonitorConfig(localtxmonitor.NewConfig(
						localtxmonitor.WithGetMempoolFunc(func(
							localtxmonitor.CallbackContext,
						) (uint64, uint32, []localtxmonitor.TxAndEraId, error) {
							acquires.Add(1)
							return 1, 1 << 20, nil, nil
						}),
					)),
				)
				if err != nil {
					_ = conn.Close()
					return
				}
				defer oconn.Close() //nolint:errcheck
				<-oconn.ErrorChan()
			}()
		}
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	client, err := ouroboros.New(
		ouroboros.WithConnection(conn),
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(false),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	go func() {
		keepNtCConnAlive(ctx, client, 10*time.Millisecond)
		close(done)
	}()

	require.Eventually(t, func() bool {
		return acquires.Load() >= 3
	}, 5*time.Second, 5*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("keepNtCConnAlive did not return after its context ended")
	}
}
