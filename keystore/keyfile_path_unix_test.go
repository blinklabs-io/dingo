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

	"github.com/stretchr/testify/require"
)

func TestKeyFileLoadersRejectFIFOContent(t *testing.T) {
	tests := []struct {
		name    string
		content string
		load    func(string) (*loadedKey, error)
	}{
		{name: "secret key", content: testVRFSKeyJSON, load: loadKeyFromFile},
		{
			name:    "operational certificate",
			content: testOpCertJSON,
			load:    loadOpCertFromFile,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "key.fifo")
			require.NoError(t, syscall.Mkfifo(path, 0o600))

			writerDone := make(chan struct{})
			go func() {
				defer close(writerDone)
				f, err := os.OpenFile(path, os.O_WRONLY, 0)
				if err != nil {
					return
				}
				_, _ = f.WriteString(test.content)
				_ = f.Close()
			}()

			_, err := test.load(path)
			require.ErrorIs(t, err, ErrNotRegularFile)

			// If the validated reader closed before the writer was scheduled,
			// connect a nonblocking reader so the writer can finish.
			drain, openErr := os.OpenFile(
				path,
				os.O_RDONLY|syscall.O_NONBLOCK,
				0,
			)
			require.NoError(t, openErr)
			<-writerDone
			require.NoError(t, drain.Close())
		})
	}
}
