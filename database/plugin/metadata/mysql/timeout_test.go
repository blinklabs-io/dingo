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

//go:build dingo_extra_plugins

package mysql

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestAssembledDSNSetsStatementAndLockTimeoutParams(t *testing.T) {
	dsn, err := assembleDSN(Config{
		StatementTimeout: 5 * time.Second,
		LockTimeout:      1500 * time.Millisecond,
	})
	require.NoError(t, err)

	require.Contains(t, dsn, "max_execution_time=5000")
	// A sub-second LockTimeout rounds up to a whole second rather than
	// truncating to zero (unbounded) -- innodb_lock_wait_timeout has no
	// sub-second resolution.
	require.Contains(t, dsn, "innodb_lock_wait_timeout=2")
}

func TestAssembledDSNOmitsUnsetTimeouts(t *testing.T) {
	dsn, err := assembleDSN(Config{})
	require.NoError(t, err)

	require.NotContains(t, dsn, "max_execution_time")
	require.NotContains(t, dsn, "innodb_lock_wait_timeout")
}

func TestAssembledDSNSetsTransportTimeouts(t *testing.T) {
	dsn, err := assembleDSN(Config{
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
	require.NoError(t, err)

	require.Contains(t, dsn, "readTimeout=2s")
	require.Contains(t, dsn, "writeTimeout=3s")
}

func TestOpenStoreIgnoresTimeoutsWhenDSNIsExplicit(t *testing.T) {
	// A schema-less explicit DSN must not trigger CREATE DATABASE or
	// require a live server; StatementTimeout/LockTimeout are only applied
	// while assembling a provider-generated DSN, so an explicit DSN's
	// timeouts (or lack of them) pass through untouched.
	store, err := openStore(
		t.Context(),
		Config{
			DSN:              "user:pass@tcp(localhost:3306)/",
			StatementTimeout: 5 * time.Second,
			LockTimeout:      time.Second,
		},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
}

func TestOpenStoreRejectsNegativeTimeouts(t *testing.T) {
	for _, cfg := range []Config{
		{DSN: "user:pass@tcp(localhost:3306)/", StatementTimeout: -time.Second},
		{DSN: "user:pass@tcp(localhost:3306)/", LockTimeout: -time.Second},
		{DSN: "user:pass@tcp(localhost:3306)/", ReadTimeout: -time.Second},
		{DSN: "user:pass@tcp(localhost:3306)/", WriteTimeout: -time.Second},
	} {
		_, err := openStore(t.Context(), cfg, metadata.ProviderDependencies{})
		require.Error(t, err)
	}
}

func TestConfigYAMLDecodesTimeouts(t *testing.T) {
	var cfg Config
	require.NoError(t, yaml.Unmarshal([]byte(`
statementTimeout: 5s
lockTimeout: 1500ms
readTimeout: 2s
writeTimeout: 3s
`), &cfg))
	require.Equal(t, 5*time.Second, cfg.StatementTimeout)
	require.Equal(t, 1500*time.Millisecond, cfg.LockTimeout)
	require.Equal(t, 2*time.Second, cfg.ReadTimeout)
	require.Equal(t, 3*time.Second, cfg.WriteTimeout)
}
