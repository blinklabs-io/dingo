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

package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunRequestedShutdown(t *testing.T) {
	t.Parallel()

	errReExec := errors.New("re-exec failed")
	tests := []struct {
		name       string
		restart    bool
		shutdown   func(release <-chan struct{}) error
		reExecErr  error
		wantReExec bool
		wantErr    error
		wantText   string
	}{
		{
			name:     "stop completes gracefully",
			shutdown: func(<-chan struct{}) error { return nil },
		},
		{
			name:       "restart re-executes after a graceful shutdown",
			restart:    true,
			shutdown:   func(<-chan struct{}) error { return nil },
			wantReExec: true,
		},
		{
			name: "stop past the timeout is abandoned",
			shutdown: func(release <-chan struct{}) error {
				<-release
				return nil
			},
			wantText: "graceful shutdown exceeded",
		},
		{
			name:    "restart past the timeout still re-executes",
			restart: true,
			shutdown: func(release <-chan struct{}) error {
				<-release
				return nil
			},
			wantReExec: true,
			wantText:   "graceful shutdown exceeded",
		},
		{
			name:       "re-exec failure is reported",
			restart:    true,
			shutdown:   func(<-chan struct{}) error { return nil },
			reExecErr:  errReExec,
			wantReExec: true,
			wantErr:    errReExec,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			reExecCalls := 0
			result := make(chan error, 1)
			go func() {
				result <- runRequestedShutdown(
					dingo.ShutdownRequest{
						Restart: tc.restart,
						Timeout: 50 * time.Millisecond,
					},
					func() error { return tc.shutdown(release) },
					func() error {
						reExecCalls++
						return tc.reExecErr
					},
				)
			}()
			err := testutil.RequireReceive(
				t, result, 5*time.Second, "requested shutdown result",
			)
			switch {
			case tc.wantErr != nil:
				require.ErrorIs(t, err, tc.wantErr)
			case tc.wantText != "":
				require.ErrorContains(t, err, tc.wantText)
			default:
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantReExec, reExecCalls == 1)
		})
	}
}

// TestWaitForStopKeepsRemoteRequestWhenRunReturnsFirst covers the remote
// request whose cancellation makes Node.Run return nil before waitForStop
// observes the cancelled context: the request must still be returned, or a
// restart degrades to a plain stop.
func TestWaitForStopKeepsRemoteRequestWhenRunReturnsFirst(t *testing.T) {
	t.Parallel()

	signalCtx, cancel := context.WithCancel(t.Context())
	cancel()
	errChan := make(chan error, 1)
	errChan <- nil
	remoteRequest := make(chan dingo.ShutdownRequest, 1)
	want := dingo.ShutdownRequest{Restart: true, Timeout: time.Second}
	remoteRequest <- want

	req, _, err := waitForStop(signalCtx, errChan, remoteRequest)
	require.NoError(t, err)
	require.NotNil(t, req)
	assert.Equal(t, want, *req)
}

func TestWaitForStopPrefersComponentError(t *testing.T) {
	t.Parallel()

	errComponent := errors.New("component failed")
	errChan := make(chan error, 1)
	errChan <- errComponent
	remoteRequest := make(chan dingo.ShutdownRequest, 1)
	remoteRequest <- dingo.ShutdownRequest{Timeout: time.Second}

	req, signaled, err := waitForStop(t.Context(), errChan, remoteRequest)
	require.ErrorIs(t, err, errComponent)
	assert.False(t, signaled)
	assert.Nil(t, req)
}
