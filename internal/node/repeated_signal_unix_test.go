//go:build !windows

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
	"log/slog"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// SIGUSR1 is used because no other test or production path in this package
// registers it, so delivering it to the test process is isolated.
func TestLogRepeatedShutdownSignals(t *testing.T) {
	t.Parallel()

	logs := &lockedLogBuffer{}
	stop := logRepeatedShutdownSignals(
		slog.New(slog.NewTextHandler(logs, nil)), syscall.SIGUSR1,
	)
	defer stop()

	proc, err := os.FindProcess(os.Getpid())
	require.NoError(t, err)
	require.NoError(t, proc.Signal(syscall.SIGUSR1))
	testutil.WaitForCondition(t, func() bool {
		return logs.contains("send SIGKILL to abandon it")
	}, 5*time.Second, "repeated signal was not logged")
}
