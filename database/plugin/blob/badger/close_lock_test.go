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

package badger

import (
	"os"
	"os/exec"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCloseReleasesDirectoryLockDuringConcurrentExec pins that Close returns
// only once the on-disk directory lock is free, even while other goroutines
// in the process are starting child processes.
//
// Badger releases its flock by closing the directory descriptor. A child
// forked while that descriptor is open holds a copy of it until the child
// execs, so unless Close waits for that lock an immediate in-process reopen of
// the same directory fails with "Cannot acquire directory lock".
func TestCloseReleasesDirectoryLockDuringConcurrentExec(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows children do not inherit the parent's lock handle")
	}
	require.NotEmpty(t, os.Args)
	dataDir := t.TempDir()

	stop := make(chan struct{})
	var spawners sync.WaitGroup
	for range 4 {
		spawners.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				// -test.run=^$ makes the child exit without running tests.
				cmd := exec.Command(os.Args[0], "-test.run=^$")
				_ = cmd.Run()
			}
		})
	}
	t.Cleanup(func() {
		close(stop)
		spawners.Wait()
	})

	for i := range 40 {
		store, err := New(
			WithDataDir(dataDir),
			WithGc(false),
			WithValueLogFileSize(16*1024*1024),
			WithMemTableSize(8*1024*1024),
		)
		require.NoError(t, err, "reopen %d after Close", i)
		require.NoError(t, store.Close())
	}
}
