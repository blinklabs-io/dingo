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

package mesh

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/apiauth"
	"github.com/blinklabs-io/dingo/mempool"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

// stubChain/stubDatabase/stubLedgerState/stubMempool are minimal
// interface-satisfying doubles for the package-local dependency
// interfaces (node_interface.go), used only to get past NewServer's
// non-nil checks for HTTP-layer tests below. They deliberately do not
// simulate any real chain/ledger/mempool behavior.
type stubChain struct{}

func (stubChain) Tip() ochainsync.Tip { return ochainsync.Tip{} }

type stubDatabase struct{}

func (stubDatabase) BlockByHash(hash []byte) (models.Block, error) {
	return models.Block{}, nil
}

func (stubDatabase) BlockByIndex(idx uint64) (models.Block, error) {
	return models.Block{}, nil
}

func (stubDatabase) GetTransactionByHash(
	hash []byte,
) (*models.Transaction, error) {
	return nil, nil
}

func (stubDatabase) GetTransactionsByBlockHash(
	hash []byte,
) ([]models.Transaction, error) {
	return nil, nil
}

type stubLedgerState struct{}

func (stubLedgerState) GetCurrentPParams() lcommon.ProtocolParameters {
	return nil
}

func (stubLedgerState) SlotToTime(slot uint64) (time.Time, error) {
	return time.Time{}, nil
}

func (stubLedgerState) UtxosByAddress(
	addr lcommon.Address,
) ([]models.Utxo, error) {
	return nil, nil
}

type stubMempool struct{}

func (stubMempool) AddTransaction(txType uint, txBytes []byte) error {
	return nil
}

func (stubMempool) GetTransaction(
	hash string,
) (mempool.MempoolTransaction, bool) {
	return mempool.MempoolTransaction{}, false
}

func (stubMempool) Transactions() []mempool.MempoolTransaction { return nil }

func newTestServer(t *testing.T, auth apiauth.Policy) *Server {
	t.Helper()
	srv, err := NewServer(ServerConfig{
		Chain:               stubChain{},
		Database:            stubDatabase{},
		LedgerState:         stubLedgerState{},
		Mempool:             stubMempool{},
		Network:             "testnet",
		GenesisHash:         "00",
		GenesisStartTimeSec: 1,
		Auth:                auth,
	})
	require.NoError(t, err)
	return srv
}

// doRequest performs req and asserts it completed without a transport
// error, returning a guaranteed non-nil response. Centralizing the nil
// guard here (rather than relying on require.NoError alone before
// dereferencing resp at each call site) keeps every call site provably
// safe.
func doRequest(
	t *testing.T,
	client *http.Client,
	req *http.Request,
) *http.Response {
	t.Helper()
	resp, err := client.Do(req)
	require.NoError(t, err)
	if resp == nil {
		t.Fatal("http.Client.Do returned a nil response with a nil error")
	}
	return resp
}

func newNetworkListRequest(t *testing.T, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequest( //nolint:noctx
		http.MethodPost,
		url+"/network/list",
		strings.NewReader("{}"),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	return req
}

// TestServerHandlerAuthModeNone covers the default (no in-process
// authentication) behavior: requests reach the mux unauthenticated.
func TestServerHandlerAuthModeNone(t *testing.T) {
	s := newTestServer(t, apiauth.Policy{})
	verifier, err := apiauth.NewVerifier(s.config.Auth)
	require.NoError(t, err)
	srv := httptest.NewServer(s.handler(verifier))
	defer srv.Close()

	resp := doRequest(
		t,
		http.DefaultClient,
		newNetworkListRequest(t, srv.URL),
	)
	defer resp.Body.Close()
	require.NotEqual(t, http.StatusUnauthorized, resp.StatusCode)
}

// TestServerHandlerAuthModeToken covers dingo #2996's fail-closed token
// enforcement at the actual listener level (real TCP connection through
// net/http, not a direct handler call).
func TestServerHandlerAuthModeToken(t *testing.T) {
	tokenPath := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenPath, []byte("s3cret"), 0o600))

	s := newTestServer(t, apiauth.Policy{
		Mode:          apiauth.ModeToken,
		TokenFilePath: tokenPath,
	})
	verifier, err := apiauth.NewVerifier(s.config.Auth)
	require.NoError(t, err)
	srv := httptest.NewServer(s.handler(verifier))
	defer srv.Close()

	// No credential: fails closed with 401.
	resp := doRequest(
		t,
		http.DefaultClient,
		newNetworkListRequest(t, srv.URL),
	)
	resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	// Correct bearer credential: reaches the mux.
	req := newNetworkListRequest(t, srv.URL)
	req.Header.Set("Authorization", "Bearer s3cret")
	resp = doRequest(t, http.DefaultClient, req)
	resp.Body.Close()
	require.NotEqual(t, http.StatusUnauthorized, resp.StatusCode)

	// Wrong credential: still fails closed.
	req2 := newNetworkListRequest(t, srv.URL)
	req2.Header.Set("Authorization", "Bearer wrong")
	resp = doRequest(t, http.DefaultClient, req2)
	resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}
