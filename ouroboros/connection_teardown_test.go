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
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

// parkedBlockfetchIterator blocks inside Next until released, holding the
// batch loop past its teardown check.
type parkedBlockfetchIterator struct {
	stubBlockfetchIterator
	entered     chan struct{}
	enteredOnce sync.Once
	release     chan struct{}
}

func (i *parkedBlockfetchIterator) Next(
	bool,
) (*chain.ChainIteratorResult, error) {
	i.enteredOnce.Do(func() { close(i.entered) })
	<-i.release
	return testBlockfetchIteratorBlock(1), nil
}

// TestBlockfetchServerSendBatchStopsOnManagerTeardown runs the batch loop over
// a connection the real manager owns, in both orders of "handler inside the
// loop" and "manager has consumed the error". The loop must exit on the
// manager's teardown signal and must not take the error from the manager:
// ConnClosedFunc has to receive the injected value.
func TestBlockfetchServerSendBatchStopsOnManagerTeardown(t *testing.T) {
	t.Parallel()

	for _, handlerFirst := range []bool{false, true} {
		name := "manager first"
		if handlerFirst {
			name = "handler first"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			closedErr := make(chan error, 1)
			cm := connmanager.NewConnectionManager(
				connmanager.ConnectionManagerConfig{
					ConnClosedFunc: func(_ ouroboros.ConnectionId, _ bool, err error) {
						closedErr <- err
					},
				},
			)
			rawConn, err := ouroboros.New()
			require.NoError(t, err)
			require.True(t, cm.AddConnection(rawConn, false, "127.0.0.1:1234"))
			conn, done := cm.GetConnectionWithDone(rawConn.Id())
			require.Same(t, rawConn, conn)

			iter := &parkedBlockfetchIterator{
				entered: make(chan struct{}),
				release: make(chan struct{}),
			}
			server := &stubBlockfetchBatchServer{}
			node := newOuroboros(OuroborosConfig{
				Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			})
			start := testBlockfetchIteratorBlock(1).Point
			// An end beyond the served block keeps the loop going, so only
			// the teardown signal can end it.
			end := testBlockfetchIteratorBlock(99).Point
			batchErr := make(chan error, 1)
			runBatch := func() {
				batchErr <- node.blockfetchServerSendBatch(
					rawConn.Id().String(),
					start,
					end,
					iter,
					server,
					connWithDone{conn: conn, done: done},
					testMaxBlocksUnbounded,
				)
			}

			injected := errors.New("peer reset")
			if handlerFirst {
				go runBatch()
				testutil.RequireReceive(t, iter.entered, 5*time.Second,
					"batch loop never reached the iterator")
				rawConn.ErrorChan() <- injected
			} else {
				rawConn.ErrorChan() <- injected
				testutil.RequireReceive(t, done, 5*time.Second,
					"manager never signalled teardown")
				go runBatch()
			}
			require.ErrorIs(t,
				testutil.RequireReceive(t, closedErr, 5*time.Second,
					"manager never received the connection error"),
				injected,
			)
			testutil.RequireReceive(t, done, 5*time.Second,
				"manager never signalled teardown")
			if handlerFirst {
				close(iter.release)
			}
			require.NoError(t, testutil.RequireReceive(t, batchErr, 5*time.Second,
				"batch loop did not exit on teardown"))
			require.Zero(t, server.batchDoneCalls,
				"a torn-down connection must not be sent BatchDone")
		})
	}
}
