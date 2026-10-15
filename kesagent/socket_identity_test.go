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

package kesagent

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestClientRejectsWorldWritableServiceSocket(t *testing.T) {
	t.Parallel()

	ln, socketPath := listenUnix(t)
	require.NoError(t, os.Chmod(socketPath, 0o662))

	client, err := NewClient(Config{SocketPath: socketPath})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	err = client.CheckReady()
	require.ErrorIs(t, err, errUnsafeSocket)

	// Nothing should have reached the listener: rejection happens before
	// the unsafe pathname is trusted for a dial.
	require.NoError(t, ln.(*net.UnixListener).SetDeadline(testDeadline()))
	_, err = ln.Accept()
	require.Error(t, err)
}

func TestClientRejectsWritableServiceSocketDirectory(t *testing.T) {
	t.Parallel()
	testutil.SkipIfBlockProducerUnsupported(t)

	ln, socketPath := listenUnix(t)
	parent := filepath.Dir(socketPath)
	info, err := os.Stat(parent)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(parent, 0o777))
	t.Cleanup(func() { _ = os.Chmod(parent, info.Mode().Perm()) })

	client, err := NewClient(Config{SocketPath: socketPath})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	err = client.CheckReady()
	require.ErrorIs(t, err, errUnsafeSocket)

	// The untrusted directory is rejected before a connection reaches the
	// listener, so an attacker cannot establish the first pinned identity.
	require.NoError(t, ln.(*net.UnixListener).SetDeadline(testDeadline()))
	_, err = ln.Accept()
	require.Error(t, err)
}

func TestClientRejectsSymlinkedServiceSocket(t *testing.T) {
	t.Parallel()
	testutil.SkipIfBlockProducerUnsupported(t)

	ln, socketPath := listenUnix(t)
	aliasPath := filepath.Join(filepath.Dir(socketPath), "agent-link.sock")
	require.NoError(t, os.Symlink(socketPath, aliasPath))

	client, err := NewClient(Config{SocketPath: aliasPath})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	err = client.CheckReady()
	require.ErrorIs(t, err, errUnsafeSocket)

	require.NoError(t, ln.(*net.UnixListener).SetDeadline(testDeadline()))
	_, err = ln.Accept()
	require.Error(t, err)
}

func TestPinnedServerOwnerRejectsSubstitutedPeer(t *testing.T) {
	t.Parallel()

	ln, socketPath := listenUnix(t)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		sendHello(t, conn, ModeServeKey)
	}()

	client, err := NewClient(Config{SocketPath: socketPath})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.CheckReady())

	client.mu.Lock()
	serverUIDSet := client.serverUIDSet
	client.serverUID++
	client.mu.Unlock()
	require.True(t, serverUIDSet)

	err = client.CheckReady()
	require.ErrorIs(t, err, errWrongPeer)
}

func testDeadline() time.Time {
	return time.Now().Add(100 * time.Millisecond)
}
