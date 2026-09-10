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

package postgres

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestDefaultConfigLeavesTimeoutsUnset(t *testing.T) {
	cfg := defaultConfig()

	require.Zero(t, cfg.StatementTimeout)
	require.Zero(t, cfg.LockTimeout)
}

func TestAssembledDSNSetsStatementAndLockTimeoutInMilliseconds(t *testing.T) {
	cfg := defaultConfig()
	cfg.StatementTimeout = 5 * time.Second
	cfg.LockTimeout = 1500 * time.Millisecond

	dsn := assembleDSN(cfg)

	require.Contains(t, dsn, "statement_timeout=5000")
	require.Contains(t, dsn, "lock_timeout=1500")
}

// TestAssembledDSNRoundsUpSubMillisecondTimeouts guards a real gap:
// time.Duration.Milliseconds() truncates, so a positive sub-millisecond
// value (500us) would format as "0" -- indistinguishable from unset, and
// so silently disabling the timeout instead of applying the shortest one
// Postgres can express (1ms).
func TestAssembledDSNRoundsUpSubMillisecondTimeouts(t *testing.T) {
	cfg := defaultConfig()
	cfg.StatementTimeout = 500 * time.Microsecond
	cfg.LockTimeout = 1500 * time.Microsecond

	dsn := assembleDSN(cfg)

	require.Contains(t, dsn, "statement_timeout=1")
	require.Contains(t, dsn, "lock_timeout=2")
}

func TestAssembledDSNOmitsUnsetTimeouts(t *testing.T) {
	dsn := assembleDSN(defaultConfig())

	require.NotContains(t, dsn, "statement_timeout")
	require.NotContains(t, dsn, "lock_timeout")
}

func TestAssembledDSNIgnoresTimeoutsWhenDSNIsExplicit(t *testing.T) {
	cfg := Config{
		DSN:              "postgres://user:pass@localhost:5432/dingo",
		StatementTimeout: 5 * time.Second,
		LockTimeout:      time.Second,
	}

	require.Equal(t, cfg.DSN, assembleDSN(cfg))
}

func TestOpenStoreRejectsNegativeTimeouts(t *testing.T) {
	for _, cfg := range []Config{
		{
			DSN:              "postgres://user:pass@localhost:5432/dingo",
			StatementTimeout: -time.Second,
		},
		{
			DSN:         "postgres://user:pass@localhost:5432/dingo",
			LockTimeout: -time.Second,
		},
	} {
		_, err := openStore(cfg, metadata.ProviderDependencies{})
		require.Error(t, err)
	}
}

func TestConfigYAMLDecodesTimeouts(t *testing.T) {
	var cfg Config
	require.NoError(t, yaml.Unmarshal([]byte(`
statementTimeout: 5s
lockTimeout: 1500ms
`), &cfg))
	require.Equal(t, 5*time.Second, cfg.StatementTimeout)
	require.Equal(t, 1500*time.Millisecond, cfg.LockTimeout)
}
