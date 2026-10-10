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

package utxorpc

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit"
)

// TestBulkSlot_WriteDeadlineOnRealServer serves a ReadMempool response larger
// than the client's HTTP/2 flow-control window to a client that never reads
// its body, through the handler the listener actually serves, over h2c and
// over TLS. The write deadline must free the slot on both: if the server's
// response writer did not support it, the slot would stay held.
func TestBulkSlot_WriteDeadlineOnRealServer(t *testing.T) {
	t.Parallel()
	for _, useTLS := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%t", useTLS), func(t *testing.T) {
			t.Parallel()
			// Incompressible bodies, so the response stays larger than the
			// client's flow-control window even if compression is negotiated.
			rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec
			mp := newBoundsRecordingMempool(0)
			for i := range 160 {
				body := make([]byte, 64<<10)
				for j := range body {
					body[j] = byte(rng.Uint32())
				}
				mp.txs = append(mp.txs, mempool.MempoolTransaction{
					Hash: fmt.Sprintf("hash-%d", i),
					Cbor: body,
				})
			}
			// Buffered so the probe that finally gets a slot does not block.
			mp.entered = make(chan struct{}, 2)
			mp.release = make(chan struct{})
			u := NewUtxorpc(UtxorpcConfig{
				Logger:                    discardLogger(),
				Mempool:                   mp,
				MaxConcurrentBulkRequests: 1,
				BulkResponseWriteTimeout:  time.Second,
			})
			srv := httptest.NewUnstartedServer(u.buildServer().Handler)
			var client *http.Client
			if useTLS {
				srv.EnableHTTP2 = true
				srv.StartTLS()
				client = srv.Client()
			} else {
				srv.Config.Protocols = unencryptedHTTP2Protocols()
				srv.Start()
				client = newConnectH2CClient()
			}
			t.Cleanup(srv.Close)

			req, err := http.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				srv.URL+"/utxorpc.v1alpha.submit.SubmitService/ReadMempool",
				bytes.NewReader(nil),
			)
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/proto")
			// Go's transport otherwise asks for gzip, and Connect then
			// compresses the whole response before its first write. Under
			// -race on a CI runner that takes several seconds, all of it
			// before the deadline can fire, so the slot outlives the wait
			// below for reasons unrelated to the deadline.
			req.Header.Set("Accept-Encoding", "identity")
			// On a slow runner the deadline can still expire before the
			// response headers are flushed, in which case the server resets
			// the stream and Do fails. Either outcome leaves the body unread,
			// which is all this test needs.
			type doResult struct {
				resp *http.Response
				err  error
			}
			done := make(chan doResult, 1)
			go func() {
				resp, err := client.Do(req)
				done <- doResult{resp, err}
			}()
			t.Cleanup(func() {
				// Drop the connections, rather than Close, which waits for a
				// handler that may still be blocked writing, so a Do still
				// waiting on headers returns.
				srv.CloseClientConnections()
				if r := <-done; r.err == nil {
					r.resp.Body.Close()
				}
			})

			// The handler is parked inside the mempool read, so the slot is
			// provably held before the probe.
			testutil.RequireReceive(t, mp.entered, 5*time.Second,
				"ReadMempool reached the mempool")
			submitSrv := &submitServiceServer{utxorpc: u}
			_, err = submitSrv.ReadMempool(
				context.Background(),
				connect.NewRequest(&submit.ReadMempoolRequest{}),
			)
			require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err),
				"the in-flight request holds its slot")
			close(mp.release)
			testutil.WaitForCondition(t, func() bool {
				_, err := submitSrv.ReadMempool(
					context.Background(),
					connect.NewRequest(&submit.ReadMempoolRequest{}),
				)
				return err == nil
			}, 5*time.Second,
				"the write deadline frees the slot of an unread response")
		})
	}
}
