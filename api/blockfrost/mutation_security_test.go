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

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/assert"
)

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
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		assert.Equal(t, http.StatusTooManyRequests, w.Code)
		assert.False(t, body.read.Load())
	}

	firstDone := startRequest("192.0.2.1:1000")
	testutil.RequireReceive(
		t,
		node.started,
		5*time.Second,
		"mutation did not start",
	)
	assertRejectedWithoutRead("192.0.2.1:1001")

	secondDone := startRequest("192.0.2.2:1000")
	testutil.RequireReceive(
		t,
		node.started,
		5*time.Second,
		"mutation did not start",
	)
	assertRejectedWithoutRead("192.0.2.3:1000")

	close(node.release)
	<-firstDone
	<-secondDone
	assert.Equal(t, int32(2), node.calls.Load())
}

func TestBlockfrostProviderSecurityDefaults(t *testing.T) {
	t.Parallel()

	cfg := DefaultProviderConfig()
	assert.Equal(t, "127.0.0.1", cfg.Host)
	assert.Zero(t, cfg.MaxConcurrentMutations)
	assert.Zero(t, cfg.MaxConcurrentMutationsPerClient)
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
