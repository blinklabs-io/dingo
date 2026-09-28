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

package txpump

import (
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/blinklabs-io/gouroboros/protocol/localtxmonitor"
	"github.com/stretchr/testify/require"
)

// TestReconcileWalletObservesMonitorBeforeLSQ uses both real local protocol
// servers over net.Pipe. The monitor acquisition models confirmation before
// the LSQ acquisition; the resulting parent output must replace the spent
// funding input rather than reviving it from an older snapshot.
func TestReconcileWalletObservesMonitorBeforeLSQ(t *testing.T) {
	address, err := ledger.NewAddress(
		"addr_test1vrk294czhxhglflvxla7vxj2cjz7wyrdpxl3fj0vych5wws77xuc7",
	)
	require.NoError(t, err)
	addressBytes, err := address.Bytes()
	require.NoError(t, err)

	confirmed := false
	queryCount := 0
	lsqConfig := localstatequery.NewConfig(
		localstatequery.WithAcquireFunc(func(
			localstatequery.CallbackContext,
			localstatequery.AcquireTarget,
			bool,
		) error {
			return nil
		}),
		localstatequery.WithQueryFunc(func(
			localstatequery.CallbackContext,
			localstatequery.QueryWrapper,
		) (any, error) {
			queryCount++
			if queryCount == 1 {
				return 6, nil // Conway current era
			}
			id := localstatequery.UtxoId{
				Hash: ledger.NewBlake2b256([]byte{0x22}),
				Idx:  0,
			}
			if !confirmed {
				id.Hash = ledger.NewBlake2b256([]byte{0x11})
			}
			return localstatequery.UTxOsResult{
				Results: map[localstatequery.UtxoId]ledger.BabbageTransactionOutput{
					id: {
						OutputAddress: address,
						OutputAmount: ledger.MaryTransactionOutputValue{
							Amount: 5_000_000,
						},
					},
				},
			}, nil
		}),
		localstatequery.WithReleaseFunc(func(localstatequery.CallbackContext) error {
			return nil
		}),
	)
	monitorConfig := localtxmonitor.NewConfig(
		localtxmonitor.WithGetMempoolFunc(func(localtxmonitor.CallbackContext) (uint64, uint32, []localtxmonitor.TxAndEraId, error) {
			confirmed = true
			return 0, 100, nil, nil
		}),
	)

	client := newProtocolTestClient(t,
		ouroboros.WithLocalStateQueryConfig(lsqConfig),
		ouroboros.WithLocalTxMonitorConfig(monitorConfig),
	)
	snapshot, presence, err := client.ReconcileWallet([][]byte{addressBytes}, nil)
	require.NoError(t, err)
	require.Empty(t, presence)
	require.Len(t, snapshot, 1)
	require.Equal(t, ledger.NewBlake2b256([]byte{0x22}).String(), snapshot[0].TxHash)
	wallet := NewWallet()
	source := UTxO{
		TxHash: ledger.NewBlake2b256([]byte{0x11}).String(),
		Index:  0,
		Amount: 5_000_000,
	}
	wallet.Add(source)
	wallet.Reserve("confirmed-tx", []UTxO{source}, nil, 0)
	wallet.ReconcileSnapshot(snapshot, presence)
	selected, _, err := wallet.SelectCoins(1)
	require.NoError(t, err)
	require.Len(t, selected, 1)
	require.Equal(t, snapshot[0].TxHash, selected[0].TxHash)
}

func newProtocolTestClient(t *testing.T, serverOptions ...ouroboros.ConnectionOptionFunc) *NodeClient {
	t.Helper()
	serverPipe, clientPipe := net.Pipe()
	t.Cleanup(func() {
		_ = clientPipe.Close()
		_ = serverPipe.Close()
	})
	require.NoError(t, serverPipe.SetDeadline(time.Now().Add(10*time.Second)))
	require.NoError(t, clientPipe.SetDeadline(time.Now().Add(10*time.Second)))
	serverCh := make(chan *ouroboros.Connection, 1)
	serverErrCh := make(chan error, 1)
	go func() {
		opts := []ouroboros.ConnectionOptionFunc{
			ouroboros.WithConnection(serverPipe),
			ouroboros.WithServer(true),
			ouroboros.WithNetworkMagic(999999),
		}
		conn, serverErr := ouroboros.New(append(opts, serverOptions...)...)
		if serverErr != nil {
			serverErrCh <- serverErr
			return
		}
		serverCh <- conn
	}()
	clientConn, err := ouroboros.New(
		ouroboros.WithConnection(clientPipe),
		ouroboros.WithNetworkMagic(999999),
		ouroboros.WithNodeToNode(false),
		ouroboros.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		ouroboros.WithLocalStateQueryConfig(localstatequery.NewConfig()),
		ouroboros.WithLocalTxMonitorConfig(localtxmonitor.NewConfig()),
	)
	var serverConn *ouroboros.Connection
	select {
	case serverErr := <-serverErrCh:
		require.NoError(t, serverErr)
	case serverConn = <-serverCh:
	case <-time.After(15 * time.Second):
		t.Fatal("protocol server construction did not finish")
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientConn.Close() })
	require.NotNil(t, serverConn)
	t.Cleanup(func() { _ = serverConn.Close() })
	return &NodeClient{conn: clientConn, addr: "pipe"}
}
