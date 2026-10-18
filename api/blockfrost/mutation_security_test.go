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

package blockfrost

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockfrostMutationRoutesRequireAuthentication(t *testing.T) {
	t.Parallel()

	for _, route := range []struct {
		path        string
		contentType string
		body        string
	}{
		{"/api/v0/tx/submit", "application/cbor", ""},
		{"/api/v0/utils/txs/evaluate", "application/cbor", ""},
		{"/api/v0/utils/txs/evaluate/utxos", "application/json", `{}`},
	} {
		t.Run(route.path, func(t *testing.T) {
			t.Parallel()
			b := New(BlockfrostConfig{AuthToken: "secret"}, &mockNode{}, nil)
			handler := b.handler()

			for _, auth := range []struct {
				name   string
				header string
				value  string
				extra  string
				want   int
			}{
				{name: "missing", want: http.StatusUnauthorized},
				{name: "wrong", header: "project_id", value: "wrong", want: http.StatusUnauthorized},
				{name: "project id", header: "project_id", value: "secret", want: http.StatusBadRequest},
				{name: "project id alias", header: "X-Blockfrost-Project-Id", value: "secret", want: http.StatusBadRequest},
				{name: "bearer", header: "Authorization", value: "Bearer secret", want: http.StatusBadRequest},
				{name: "wrong project id with valid bearer", header: "project_id", value: "wrong", extra: "Bearer secret", want: http.StatusBadRequest},
			} {
				t.Run(auth.name, func(t *testing.T) {
					req := httptest.NewRequest(
						http.MethodPost,
						route.path,
						strings.NewReader(route.body),
					)
					req.Header.Set("Content-Type", route.contentType)
					if auth.header != "" {
						req.Header.Set(auth.header, auth.value)
					}
					if auth.extra != "" {
						req.Header.Set("Authorization", auth.extra)
					}
					w := httptest.NewRecorder()
					handler.ServeHTTP(w, req)
					assert.Equal(t, auth.want, w.Code)
					assert.NotContains(t, w.Body.String(), "secret")
				})
			}
		})
	}
}

func TestBlockfrostMutationRoutesStayClosedWithoutConfiguredToken(t *testing.T) {
	t.Parallel()

	b := New(BlockfrostConfig{}, &mockNode{}, nil)
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v0/tx/submit",
		strings.NewReader("\x80"),
	)
	req.Header.Set("Content-Type", "application/cbor")
	req.Header.Set("project_id", "any-value")
	w := httptest.NewRecorder()
	b.handler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

type blockingMutationNode struct {
	*mockNode
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (n *blockingMutationNode) TransactionSubmit(
	ctx context.Context,
	_ []byte,
) (string, error) {
	n.calls.Add(1)
	n.started <- struct{}{}
	select {
	case <-n.release:
		return "hash", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

type observedBody struct {
	read atomic.Bool
}

func (b *observedBody) Read([]byte) (int, error) {
	b.read.Store(true)
	return 0, io.EOF
}

func (*observedBody) Close() error { return nil }

func TestBlockfrostMutationAdmissionPrecedesBodyRead(t *testing.T) {
	t.Parallel()

	node := &blockingMutationNode{
		mockNode: &mockNode{},
		started:  make(chan struct{}, 2),
		release:  make(chan struct{}),
	}
	b := New(BlockfrostConfig{
		AuthToken:                       "secret",
		MaxConcurrentMutations:          2,
		MaxConcurrentMutationsPerClient: 1,
	}, node, nil)
	handler := b.handler()

	startRequest := func(remoteAddr string) <-chan struct{} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			req := httptest.NewRequest(
				http.MethodPost,
				"/api/v0/tx/submit",
				strings.NewReader("\x80"),
			)
			req.RemoteAddr = remoteAddr
			req.Header.Set("Content-Type", "application/cbor")
			req.Header.Set("project_id", "secret")
			handler.ServeHTTP(httptest.NewRecorder(), req)
		}()
		return done
	}
	assertRejectedWithoutRead := func(remoteAddr string) {
		t.Helper()
		body := &observedBody{}
		req := httptest.NewRequest(
			http.MethodPost,
			"/api/v0/tx/submit",
			body,
		)
		req.RemoteAddr = remoteAddr
		req.Header.Set("Content-Type", "application/cbor")
		req.Header.Set("project_id", "secret")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		assert.Equal(t, http.StatusTooManyRequests, w.Code)
		assert.False(t, body.read.Load())
	}

	firstDone := startRequest("192.0.2.1:1000")
	testutil.RequireReceive(t, node.started, 5*time.Second, "mutation did not start")
	assertRejectedWithoutRead("192.0.2.1:1001")

	secondDone := startRequest("192.0.2.2:1000")
	testutil.RequireReceive(t, node.started, 5*time.Second, "mutation did not start")
	assertRejectedWithoutRead("192.0.2.3:1000")

	close(node.release)
	<-firstDone
	<-secondDone
	assert.Equal(t, int32(2), node.calls.Load())
}

func TestBlockfrostMutationAdmissionHonorsCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	release, ok := newMutationAdmission(1, 1).acquire(ctx, "client")
	assert.False(t, ok)
	assert.Nil(t, release)
}

func TestBlockfrostProviderSecurityDefaults(t *testing.T) {
	t.Parallel()

	cfg := DefaultProviderConfig()
	assert.Equal(t, "127.0.0.1", cfg.Host)
	assert.Positive(t, cfg.MaxConcurrentMutations)
	assert.Positive(t, cfg.MaxConcurrentMutationsPerClient)
	assert.LessOrEqual(
		t,
		cfg.MaxConcurrentMutationsPerClient,
		cfg.MaxConcurrentMutations,
	)
}

func TestBlockfrostRemoteMutationAuthenticationRequiresTLS(t *testing.T) {
	t.Parallel()

	require.ErrorContains(
		t,
		validateListenSecurity(
			"0.0.0.0",
			"secret",
			apiconfig.EffectiveTLS{},
		),
		"requires server TLS",
	)
	assert.NoError(t, validateListenSecurity(
		"0.0.0.0",
		"secret",
		apiconfig.EffectiveTLS{Enabled: true},
	))
	assert.NoError(t, validateListenSecurity(
		"127.0.0.1",
		"secret",
		apiconfig.EffectiveTLS{},
	))
	assert.NoError(t, validateListenSecurity(
		"0.0.0.0",
		"",
		apiconfig.EffectiveTLS{},
	))
}

func TestBlockfrostStartRejectsRemoteMutationAuthWithoutTLS(t *testing.T) {
	t.Parallel()

	b := New(BlockfrostConfig{
		ListenAddress: "0.0.0.0:0",
		AuthToken:     "secret",
	}, &mockNode{}, nil)
	err := b.Start(t.Context())
	require.ErrorContains(t, err, "requires server TLS")
	assert.Nil(t, b.listener.Server())
}

func TestNormalizeListenHostStripsOneBracketPair(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "::1", normalizeListenHost("[::1]"))
	assert.Equal(t, "::1", normalizeListenHost("::1"))
	assert.Equal(t, "127.0.0.1", normalizeListenHost("127.0.0.1"))
	assert.Equal(
		t,
		"[::1]:3000",
		net.JoinHostPort(normalizeListenHost("[::1]"), "3000"),
	)
}
