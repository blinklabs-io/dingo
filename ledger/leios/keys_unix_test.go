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

package leios

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/blinklabs-io/dingo/keystore"
	"github.com/stretchr/testify/require"
)

func TestLoadVoteSigningKeyFileRejectsFIFOContent(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "vote.fifo")
	require.NoError(t, syscall.Mkfifo(path, 0o600))

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		f, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			return
		}
		_, _ = fmt.Fprintf(f, "%064x", 42)
		_ = f.Close()
	}()

	_, err := LoadVoteSigningKeyFile(path)
	require.ErrorIs(t, err, keystore.ErrNotRegularFile)

	drain, openErr := os.OpenFile(
		path,
		os.O_RDONLY|syscall.O_NONBLOCK,
		0,
	)
	require.NoError(t, openErr)
	<-writerDone
	require.NoError(t, drain.Close())
}
