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

package connmanager

import (
	"bytes"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func acceptedNtCLog(t *testing.T, level slog.Level) string {
	t.Helper()
	out := &syncBuffer{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	manager := startNtCAdmissionManager(t, ConnectionManagerConfig{
		Logger: slog.New(slog.NewJSONHandler(out, &slog.HandlerOptions{
			Level: level,
		})),
	}, listener)
	connectNtCAdmissionClient(t, listener)
	requireNtCAdmissionCount(t, manager, 1)
	return out.String()
}

// A node-to-client connection is opened per request by local tools, so an
// accepted connection is routine and belongs at debug level: at info level a
// transaction generator alone produced thousands of lines a minute.
func TestAcceptedNtCConnectionIsNotLoggedAtInfo(t *testing.T) {
	const msg = "listener: accepted NtC connection"
	require.NotContains(t, acceptedNtCLog(t, slog.LevelInfo), msg)
	require.True(
		t,
		strings.Contains(acceptedNtCLog(t, slog.LevelDebug), msg),
		"the accepted connection must still be logged at debug level",
	)
}
