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

//go:build !windows

package badger

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestWaitForDirLockReleaseReportsTimeout pins that the bounded wait reports a
// lock held past its timeout instead of claiming the directory is free.
func TestWaitForDirLockReleaseReportsTimeout(t *testing.T) {
	dir := t.TempDir()
	holder, err := os.Open(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = holder.Close() })
	fd := int(holder.Fd()) //nolint:gosec // descriptor values fit in int
	require.NoError(t, unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB))

	require.False(t, waitForDirLockRelease(dir, 20*time.Millisecond))

	require.NoError(t, unix.Flock(fd, unix.LOCK_UN))
	require.True(t, waitForDirLockRelease(dir, time.Second))
}
