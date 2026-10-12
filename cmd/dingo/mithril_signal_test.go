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

//go:build unix

package main

import (
	"context"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// TestMithrilServeShutsDownOnSIGTERM sends SIGTERM to the test process while
// `mithril serve` runs. Without a handler the signal terminates the process;
// with one, the command shuts the server down and returns cleanly.
//
// Not t.Parallel: it signals the whole process, and the command replaces the
// default logger.
func TestMithrilServeShutsDownOnSIGTERM(t *testing.T) {
	cfg, _ := snapshotTestConfig(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	port := ln.Addr().(*net.TCPAddr).Port
	cfg.Mithril.Server.Port = uint(port) //nolint:gosec // a bound TCP port
	require.NoError(t, ln.Close())

	cmd := mithrilServeCommand()
	cmd.SetContext(config.WithContext(context.Background(), cfg))
	done := make(chan error, 1)
	go func() { done <- cmd.RunE(cmd, nil) }()

	testutil.WaitForCondition(t, func() bool {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 10*time.Second, "mithril serve did not start listening")

	require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGTERM))
	require.NoError(t, testutil.RequireReceive(
		t, done, 40*time.Second, "mithril serve did not stop on SIGTERM",
	))
}
