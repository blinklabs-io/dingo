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

package sqlstore

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

func integrationMigrationLocker(
	dialect string,
	namespace string,
) migrations.Locker {
	// PostgreSQL and MySQL advisory locks are server-scoped. These tests use
	// isolated schemas or databases, so their locks must be isolated too.
	return migrations.NewAdvisoryLocker(
		dialect,
		integrationMigrationLockKey(namespace),
		time.Second,
	)
}

func integrationMigrationLockKey(namespace string) int64 {
	digest := sha256.Sum256([]byte(namespace))
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

func TestIntegrationMigrationLockKey(t *testing.T) {
	t.Parallel()

	first := integrationMigrationLockKey("sqlstore_pool_1")
	require.Equal(
		t,
		first,
		integrationMigrationLockKey("sqlstore_pool_1"),
	)
	require.NotEqual(
		t,
		first,
		integrationMigrationLockKey("sqlstore_pool_2"),
	)
}
