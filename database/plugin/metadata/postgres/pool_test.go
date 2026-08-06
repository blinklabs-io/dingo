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

func TestOpenStoreAppliesPoolSettings(t *testing.T) {
	store, err := openStore(
		Config{
			DSN:                 "postgres://user:pass@localhost:5432/dingo",
			PoolMaxOpenConns:    17,
			PoolMaxIdleConns:    3,
			PoolConnMaxLifetime: 30 * time.Minute,
		},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	require.Equal(t, 17, store.WritePoolStats().MaxOpenConnections)
}

func TestOpenStoreRejectsNegativePoolSettings(t *testing.T) {
	for _, cfg := range []Config{
		{PoolMaxOpenConns: -1},
		{PoolMaxIdleConns: -1},
		{PoolConnMaxLifetime: -time.Second},
	} {
		_, err := openStore(cfg, metadata.ProviderDependencies{})
		require.Error(t, err)
	}
}

func TestConfigYAMLDecodesPoolSettings(t *testing.T) {
	var cfg Config
	require.NoError(t, yaml.Unmarshal([]byte(`
poolMaxOpenConns: 250
poolMaxIdleConns: 25
poolConnMaxLifetime: 1h30m
`), &cfg))
	require.Equal(t, 250, cfg.PoolMaxOpenConns)
	require.Equal(t, 25, cfg.PoolMaxIdleConns)
	require.Equal(t, 90*time.Minute, cfg.PoolConnMaxLifetime)
}
