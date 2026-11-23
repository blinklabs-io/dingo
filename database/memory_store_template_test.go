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

package database

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// An in-memory test database must start from the migrated schema: the
// migration runner's upgrade check reports the version it found before
// running anything, and a database that migrates from scratch reports 0.
func TestInMemoryTestDatabaseStartsFromMigratedSchema(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"", "api"} {
		t.Run("mode="+mode, func(t *testing.T) {
			t.Parallel()
			var logs syncLogBuffer
			db, err := newTestDatabase(t, &Config{
				DataDir:     "",
				StorageMode: mode,
				Logger:      slog.New(slog.NewTextHandler(&logs, nil)),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })

			var check string
			for line := range strings.SplitSeq(logs.String(), "\n") {
				if strings.Contains(line, "metadata schema upgrade check") {
					check = line
				}
			}
			require.NotEmpty(t, check, "migration runner never reported")
			require.NotContains(
				t, check, "current_version=0 ",
				"in-memory database ran its own migrations",
			)
		})
	}
}
