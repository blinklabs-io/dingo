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

	"gopkg.in/yaml.v3"
)

// TestNewWithOptionsPoolDefaults verifies that unset pool settings fall back
// to the documented defaults, preserving existing behavior for deployments
// that do not configure them.
func TestNewWithOptionsPoolDefaults(t *testing.T) {
	store, err := NewWithOptions()
	if err != nil {
		t.Fatalf("expected default pool settings to succeed, got: %v", err)
	}
	if store.poolMaxOpen != defaultPoolMaxOpenConns {
		t.Errorf(
			"expected default poolMaxOpen %d, got %d",
			defaultPoolMaxOpenConns, store.poolMaxOpen,
		)
	}
	if store.poolMaxIdle != defaultPoolMaxIdleConns {
		t.Errorf(
			"expected default poolMaxIdle %d, got %d",
			defaultPoolMaxIdleConns, store.poolMaxIdle,
		)
	}
	if store.poolConnMaxLifetime != defaultPoolConnMaxLifetime {
		t.Errorf(
			"expected default poolConnMaxLifetime %s, got %s",
			defaultPoolConnMaxLifetime, store.poolConnMaxLifetime,
		)
	}
}

// TestNewWithOptionsPoolOverrides verifies that explicit pool settings
// override the defaults.
func TestNewWithOptionsPoolOverrides(t *testing.T) {
	store, err := NewWithOptions(
		WithPoolMaxOpenConns(200),
		WithPoolMaxIdleConns(20),
		WithPoolConnMaxLifetime(30*time.Minute),
	)
	if err != nil {
		t.Fatalf("expected overridden pool settings to succeed, got: %v", err)
	}
	if store.poolMaxOpen != 200 {
		t.Errorf("expected poolMaxOpen 200, got %d", store.poolMaxOpen)
	}
	if store.poolMaxIdle != 20 {
		t.Errorf("expected poolMaxIdle 20, got %d", store.poolMaxIdle)
	}
	if store.poolConnMaxLifetime != 30*time.Minute {
		t.Errorf(
			"expected poolConnMaxLifetime 30m, got %s",
			store.poolConnMaxLifetime,
		)
	}
}

// TestNewWithOptionsRejectsNegativePoolSettings verifies startup validation
// of the pool settings.
func TestNewWithOptionsRejectsNegativePoolSettings(t *testing.T) {
	if _, err := NewWithOptions(WithPoolMaxOpenConns(-1)); err == nil {
		t.Error("expected error for negative poolMaxOpen, got nil")
	}
	if _, err := NewWithOptions(WithPoolMaxIdleConns(-1)); err == nil {
		t.Error("expected error for negative poolMaxIdle, got nil")
	}
	if _, err := NewWithOptions(WithPoolConnMaxLifetime(-time.Second)); err == nil {
		t.Error("expected error for negative poolConnMaxLifetime, got nil")
	}
}

// TestConfigYAMLDecodesPoolSettings verifies that the provider Config struct
// parses pool settings from YAML, including a time.Duration string for
// poolConnMaxLifetime.
func TestConfigYAMLDecodesPoolSettings(t *testing.T) {
	var cfg Config
	data := []byte(`
host: db.local
poolMaxOpenConns: 250
poolMaxIdleConns: 25
poolConnMaxLifetime: 1h30m
`)
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("failed to decode config: %v", err)
	}
	if cfg.PoolMaxOpenConns != 250 {
		t.Errorf("expected PoolMaxOpenConns 250, got %d", cfg.PoolMaxOpenConns)
	}
	if cfg.PoolMaxIdleConns != 25 {
		t.Errorf("expected PoolMaxIdleConns 25, got %d", cfg.PoolMaxIdleConns)
	}
	if cfg.PoolConnMaxLifetime != 90*time.Minute {
		t.Errorf(
			"expected PoolConnMaxLifetime 1h30m, got %s",
			cfg.PoolConnMaxLifetime,
		)
	}
}

// TestConfigYAMLDecodesPoolSettingsDefaultZero verifies that omitted pool
// settings decode to zero, which NewWithOptions/RegisterProvider treat as
// "use the provider default."
func TestConfigYAMLDecodesPoolSettingsDefaultZero(t *testing.T) {
	var cfg Config
	if err := yaml.Unmarshal([]byte(`host: db.local`), &cfg); err != nil {
		t.Fatalf("failed to decode config: %v", err)
	}
	if cfg.PoolMaxOpenConns != 0 || cfg.PoolMaxIdleConns != 0 ||
		cfg.PoolConnMaxLifetime != 0 {
		t.Errorf(
			"expected zero-value pool settings when unset, got %+v",
			cfg,
		)
	}
}

// TestMysqlStartAppliesPoolSettings verifies that Start() applies the
// resolved pool settings to the underlying sql.DB rather than the previously
// hardcoded literals.
func TestMysqlStartAppliesPoolSettings(t *testing.T) {
	if !isMysqlConfigured() {
		t.Skip(
			"Skipping mysql integration test: mysql not configured (set MYSQL_PASSWORD or configure via cmdline options)",
		)
	}
	opts := append(
		getTestMysqlOptions(),
		WithPoolMaxOpenConns(17),
		WithPoolMaxIdleConns(3),
	)
	store, err := NewWithOptions(opts...)
	if err != nil {
		t.Fatalf("failed to create mysql store: %v", err)
	}
	if err := store.Start(); err != nil {
		t.Fatalf("failed to start mysql store: %v", err)
	}
	defer store.Close() //nolint:errcheck

	sqlDB, err := store.DB().DB()
	if err != nil {
		t.Fatalf("failed to get sql.DB handle: %v", err)
	}
	if stats := sqlDB.Stats(); stats.MaxOpenConnections != 17 {
		t.Errorf(
			"expected MaxOpenConnections 17, got %d",
			stats.MaxOpenConnections,
		)
	}
}
