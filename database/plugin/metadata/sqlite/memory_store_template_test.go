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

package sqlite

import (
	"bytes"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
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

// An in-memory store must start from the migrated schema: the migration
// runner's upgrade check reports the schema version it found before running
// anything, and a store that migrates from scratch reports 0.
func TestInMemoryStoreStartsFromMigratedSchema(t *testing.T) {
	t.Parallel()
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)

	var logs syncBuffer
	store, _, _, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{
			Logger: slog.New(slog.NewTextHandler(&logs, nil)),
		},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	var check string
	for line := range strings.SplitSeq(logs.String(), "\n") {
		if strings.Contains(line, "metadata schema upgrade check") {
			check = line
		}
	}
	require.NotEmpty(t, check, "migration runner never reported")
	require.Contains(
		t, check, "completed_migrations="+strconv.Itoa(len(registry)),
		"in-memory store ran its own migrations",
	)
}
