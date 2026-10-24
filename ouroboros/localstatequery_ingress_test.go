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
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/gouroboros/muxer"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

type localQueryIngressPacedConn struct {
	net.Conn
	mux     *muxer.Muxer
	enabled atomic.Bool
}

func (c *localQueryIngressPacedConn) Write(buf []byte) (int, error) {
	n, err := c.Conn.Write(buf)
	if err != nil || !c.enabled.Load() {
		return n, err
	}
	// Each write is one mux segment. Drain its queue before sending another
	// so the test reaches reassembly's cap rather than racing the mux cap.
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for c.mux.IngressInUse() > 0 {
		select {
		case <-tick.C:
		case <-timeout.C:
			return n, errors.New("local query ingress did not drain")
		}
	}
	return n, nil
}

// testTxIns returns n distinct inputs. Each encodes to roughly 38 bytes, so
// the request size scales with n independently of the ledger contents.
func testTxIns(t *testing.T, n int) []gledger.TransactionInput {
	t.Helper()
	ret := make([]gledger.TransactionInput, n)
	for i := range n {
		hash := make([]byte, 32)
		copy(hash, fmt.Appendf(nil, "%08d", i))
		in, err := shelley.NewShelleyTransactionInput(
			hex.EncodeToString(hash),
			0,
		)
		require.NoError(t, err)
		ret[i] = in
	}
	return ret
}

// TestLocalstatequeryProtocol_RequestAboveIngressCapIsRefused drives the
// production server options over a real connection: a GetUTxOByTxIn request
// above localStateQueryRequestMaxBytes must not be reassembled and answered,
// on trusted and untrusted listeners alike, while one well under the cap is.
func TestLocalstatequeryProtocol_RequestAboveIngressCapIsRefused(
	t *testing.T,
) {
	t.Parallel()
	for _, trusted := range []bool{false, true} {
		t.Run(fmt.Sprintf("trusted=%t", trusted), func(t *testing.T) {
			t.Parallel()
			o := &Ouroboros{
				localstatequeryAcquiredPoints: make(
					map[ouroboros.ConnectionId]ledger.QueryPoint,
				),
			}
			ls, db := newTestLedgerStateWithChain(t, 2)
			o.ledgerState = ls
			require.NoError(t, db.SetTip(ochainsync.Tip{
				Point: ocommon.NewPoint(1, bytes.Repeat([]byte{1}, 32)),
			}, nil))

			rawServer, rawClient := net.Pipe()
			pacedClient := &localQueryIngressPacedConn{Conn: rawClient}
			serverDone := make(chan error, 1)
			var srvConn *ouroboros.Connection
			go func() {
				conn, err := ouroboros.New(
					ouroboros.WithConnection(rawServer),
					ouroboros.WithNetworkMagic(42),
					ouroboros.WithNodeToNode(false),
					ouroboros.WithServer(true),
					ouroboros.WithLocalStateQueryConfig(
						olocalstatequery.NewConfig(
							o.localstatequeryServerConnOpts(trusted)...,
						),
					),
				)
				srvConn = conn
				serverDone <- err
			}()
			cliConn, err := ouroboros.New(
				ouroboros.WithConnection(pacedClient),
				ouroboros.WithNetworkMagic(42),
				ouroboros.WithNodeToNode(false),
			)
			require.NoError(t, err)
			defer cliConn.Close() //nolint:errcheck
			select {
			case err = <-serverDone:
			case <-time.After(10 * time.Second):
				t.Fatal("server handshake never completed")
			}
			require.NoError(t, err)
			defer srvConn.Close() //nolint:errcheck

			client := cliConn.LocalStateQuery().Client
			require.NoError(t, client.AcquireVolatileTip())

			_, err = client.GetUTxOByTxIn(testTxIns(t, 10))
			require.NoError(t, err, "a request under the cap must be answered")
			pacedClient.mux = srvConn.Muxer()
			pacedClient.enabled.Store(true)

			// 1.5x the cap in ~38-byte inputs. The item count is above
			// gouroboros' decode-time filter limit too, so only the read
			// buffer cap refusing the request before decode produces the
			// reassembly error asserted here.
			oversized := testTxIns(
				t,
				localStateQueryRequestMaxBytes*3/2/38,
			)
			go func() {
				// The connection is torn down under this call; its error
				// is the client-side view of the same refusal.
				_, _ = client.GetUTxOByTxIn(oversized)
			}()
			select {
			case srvErr := <-srvConn.ErrorChan():
				require.ErrorContains(
					t,
					srvErr,
					"read buffer exceeded maximum size",
				)
			case <-time.After(30 * time.Second):
				t.Fatal("oversized request was not refused")
			}
		})
	}
}
