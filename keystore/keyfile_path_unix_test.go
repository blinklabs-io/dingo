//go:build unix

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

package keystore

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestKeyFileLoadersRejectFIFOWithoutWriter(t *testing.T) {
	tests := []struct {
		name string
		load func(string) (*loadedKey, error)
	}{
		{name: "secret key", load: loadKeyFromFile},
		{name: "operational certificate", load: loadOpCertFromFile},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "key.fifo")
			require.NoError(t, syscall.Mkfifo(path, 0o600))

			result := make(chan error, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, err := test.load(path)
				result <- err
			}()
			t.Cleanup(func() {
				select {
				case <-done:
					return
				default:
				}
				writer, err := os.OpenFile(
					path,
					os.O_WRONLY|syscall.O_NONBLOCK,
					0,
				)
				if err == nil {
					_ = writer.Close()
				}
				testutil.RequireReceive(
					t, done, testutil.AsyncWait, "blocked FIFO loader cleanup",
				)
			})

			err := testutil.RequireReceive(
				t, result, testutil.AsyncWait, "nonblocking FIFO rejection",
			)
			require.ErrorIs(t, err, ErrNotRegularFile)
		})
	}
}
