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
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/mempool"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// admittingMempool adds mempool.SubmissionAdmitter to fakeMempool and counts
// reservations so tests can see when the handler takes one.
type admittingMempool struct {
	*fakeMempool
	admitErr error
	admits   atomic.Int32
}

func (a *admittingMempool) AdmitSubmission(
	ctx context.Context,
) (context.Context, func(), error) {
	a.admits.Add(1)
	if a.admitErr != nil {
		return ctx, nil, a.admitErr
	}
	return ctx, func() {}, nil
}

func newAdmittingHandler(
	t *testing.T,
	admitErr error,
) (http.Handler, *admittingMempool) {
	t.Helper()
	deps := newTestDeps()
	pool := &admittingMempool{fakeMempool: deps.mempool, admitErr: admitErr}
	h := newTestHandler(t, deps, func(cfg *ServerConfig) {
		cfg.Mempool = pool
	})
	return h, pool
}

// A request that fails body decoding or network validation must not take a
// validation slot: the reservation is for transaction work, and a client
// that is still sending its body would otherwise hold one.
func TestConstructionSubmitAdmitsAfterRequestBody(t *testing.T) {
	t.Parallel()

	h, pool := newAdmittingHandler(t, nil)
	rec := postRaw(t, h, "/construction/submit", `{"signed_transaction":`)
	requireMeshError(
		t, rec, ErrInvalidRequest, http.StatusInternalServerError,
	)
	require.Zero(t, pool.admits.Load())

	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x29), nil,
	)
	txCbor, _ := testSimpleSignedTx(t, addr)
	rec = postJSON(
		t, h, "/construction/submit",
		submitRequest(hexString(txCbor)),
	)
	decodeResponse[ConstructionSubmitResponse](t, rec)
	require.Equal(t, int32(1), pool.admits.Load())
	require.Len(t, pool.submissions(), 1)
}

func TestConstructionSubmitBusyIsUnavailable(t *testing.T) {
	t.Parallel()

	h, pool := newAdmittingHandler(t, mempool.ErrSubmissionBusy)
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x2a), nil,
	)
	txCbor, _ := testSimpleSignedTx(t, addr)
	rec := postJSON(
		t, h, "/construction/submit",
		submitRequest(hexString(txCbor)),
	)
	got := requireMeshError(
		t, rec, ErrUnavailable, http.StatusInternalServerError,
	)
	require.True(t, got.Retriable)
	require.Empty(t, pool.submissions())
}
