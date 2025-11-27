// Copyright 2025 Blink Labs Software
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

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/database/plugin/blob/aws"
	_ "github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/blob/gcs"
	_ "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/internal/config"
)

func TestPluginSystemIntegration(t *testing.T) {
	// Test that all plugins are registered
	blobPlugins := plugin.GetPlugins(plugin.PluginTypeBlob)
	if len(blobPlugins) == 0 {
		t.Fatal("no blob plugins registered")
	}

	metadataPlugins := plugin.GetPlugins(plugin.PluginTypeMetadata)
	if len(metadataPlugins) == 0 {
		t.Fatal("no metadata plugins registered")
	}

	// Test that we can get specific plugins
	badgerPlugin := plugin.GetPlugin(plugin.PluginTypeBlob, "badger")
	if badgerPlugin == nil {
		t.Fatal("badger plugin not found")
	}

	sqlitePlugin := plugin.GetPlugin(plugin.PluginTypeMetadata, "sqlite")
	if sqlitePlugin == nil {
		t.Fatal("sqlite plugin not found")
	}

	// Test that plugins can start and stop (basic lifecycle)
	if err := badgerPlugin.Start(); err != nil {
		t.Fatalf("failed to start badger plugin: %v", err)
	}
	defer func() {
		if err := badgerPlugin.Stop(); err != nil {
			t.Errorf("failed to stop badger plugin: %v", err)
		}
	}()

	if err := sqlitePlugin.Start(); err != nil {
		t.Fatalf("failed to start sqlite plugin: %v", err)
	}
	defer func() {
		if err := sqlitePlugin.Stop(); err != nil {
			t.Errorf("failed to stop sqlite plugin: %v", err)
		}
	}()

	// Plugins will be stopped automatically by deferred calls
}

func TestPluginDescriptions(t *testing.T) {
	blobPlugins := plugin.GetPlugins(plugin.PluginTypeBlob)
	metadataPlugins := plugin.GetPlugins(plugin.PluginTypeMetadata)

	// Check that all plugins have descriptions
	for _, p := range blobPlugins {
		if p.Description == "" {
			t.Errorf("blob plugin %q has empty description", p.Name)
		}
	}

	for _, p := range metadataPlugins {
		if p.Description == "" {
			t.Errorf("metadata plugin %q has empty description", p.Name)
		}
	}

	// Check specific descriptions
	foundBadger := false
	foundSqlite := false

	for _, p := range blobPlugins {
		if p.Name == "badger" {
			foundBadger = true
			if p.Description != "BadgerDB local key-value store" {
				t.Errorf("badger description mismatch: got %q, want %q",
					p.Description, "BadgerDB local key-value store")
			}
		}
	}

	for _, p := range metadataPlugins {
		if p.Name == "sqlite" {
			foundSqlite = true
			if p.Description != "SQLite relational database" {
				t.Errorf("sqlite description mismatch: got %q, want %q",
					p.Description, "SQLite relational database")
			}
		}
	}

	if !foundBadger {
		t.Error("badger plugin not found in blob plugins")
	}
	if !foundSqlite {
		t.Error("sqlite plugin not found in metadata plugins")
	}
}

func TestPluginLifecycleEndToEnd(t *testing.T) {
	// Test the complete plugin lifecycle: registration → instantiation → config → usage
	// Note: This test focuses on plugins that can be instantiated with default/empty config

	// 1. Verify plugins are registered
	blobPlugins := plugin.GetPlugins(plugin.PluginTypeBlob)
	// Check for expected plugins by name instead of count
	expectedBlobs := []string{"badger", "gcs", "s3"}
	for _, name := range expectedBlobs {
		if findPluginEntry(blobPlugins, name) == nil {
			t.Errorf("expected blob plugin %q not found", name)
		}
	}

	metadataPlugins := plugin.GetPlugins(plugin.PluginTypeMetadata)
	// Check for expected metadata plugins
	expectedMetadata := []string{"sqlite"}
	for _, name := range expectedMetadata {
		if findPluginEntry(metadataPlugins, name) == nil {
			t.Errorf("expected metadata plugin %q not found", name)
		}
	}

	// 2. Test instantiation of plugins that can work with defaults
	// Badger should work with defaults (creates temp directory)
	badgerPlugin := plugin.GetPlugin(plugin.PluginTypeBlob, "badger")
	if badgerPlugin == nil {
		t.Error("failed to instantiate badger plugin")
	} else {
		// Test lifecycle methods
		if err := badgerPlugin.Start(); err != nil {
			t.Errorf("failed to start badger plugin: %v", err)
		}
		if err := badgerPlugin.Stop(); err != nil {
			t.Errorf("failed to stop badger plugin: %v", err)
		}
	}

	// SQLite should work with defaults (creates temp database)
	sqlitePlugin := plugin.GetPlugin(plugin.PluginTypeMetadata, "sqlite")
	if sqlitePlugin == nil {
		t.Error("failed to instantiate sqlite plugin")
	} else {
		// Test lifecycle methods
		if err := sqlitePlugin.Start(); err != nil {
			t.Errorf("failed to start sqlite plugin: %v", err)
		}
		if err := sqlitePlugin.Stop(); err != nil {
			t.Errorf("failed to stop sqlite plugin: %v", err)
		}
	}

	// Note: GCS and S3 plugins require configuration (bucket, credentials) and cannot
	// be tested in this basic lifecycle test without proper setup
}

func TestPluginConfigurationIntegration(t *testing.T) {
	// Test that plugin configuration works end-to-end
	// This tests the integration between config loading and plugin instantiation

	// Create a temporary config that specifies plugins
	configContent := `
database:
  blob:
    plugin: "badger"
    badger:
      data-dir: "/tmp/test-config-badger"
      block-cache-size: 1000000
  metadata:
    plugin: "sqlite"
    sqlite:
      data-dir: "/tmp/test-config-sqlite.db"
`

	// Write config to a temporary file
	tmpFile, err := os.CreateTemp("", "dingo-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp config file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(configContent); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}
	tmpFile.Close()

	// Test that we can load this config and verify plugin settings are applied
	cfg, err := config.LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Verify that the config was loaded with the expected plugin settings
	if cfg.BlobPlugin != "badger" {
		t.Errorf("expected BlobPlugin to be 'badger', got '%s'", cfg.BlobPlugin)
	}
	if cfg.MetadataPlugin != "sqlite" {
		t.Errorf(
			"expected MetadataPlugin to be 'sqlite', got '%s'",
			cfg.MetadataPlugin,
		)
	}

	// Note: Plugin-specific options like data-dir are stored in cmdlineOptions
	// and would be used when creating plugin instances
}

func TestPluginSwitching(t *testing.T) {
	// Test that we can switch between different plugins of the same type
	// This validates that the plugin system allows runtime plugin selection

	// For now, test that we can at least get different plugin entries
	blobPlugins := plugin.GetPlugins(plugin.PluginTypeBlob)
	if len(blobPlugins) < 2 {
		t.Skip("need at least 2 blob plugins to test switching")
	}

	// Test that we can retrieve different plugin entries
	badgerEntry := findPluginEntry(blobPlugins, "badger")
	if badgerEntry == nil {
		t.Error("badger plugin entry not found")
	}

	// Note: We don't instantiate cloud plugins here as they require configuration
	// This test validates that the plugin registry works correctly
}

func TestCloudPluginGCS(t *testing.T) {
	// Test GCS plugin with real credentials if available
	if !hasGCSCredentials() {
		t.Skip("GCS credentials not found, skipping test")
	}

	// Set up test configuration
	testBucket := os.Getenv("DINGO_TEST_GCS_BUCKET")
	if testBucket == "" {
		testBucket = "dingo-test-bucket"
	}

	// Create GCS plugin directly with test configuration
	gcsPlugin, err := gcs.NewWithOptions(
		gcs.WithBucket(testBucket),
	)
	if err != nil {
		t.Fatalf("failed to create GCS plugin: %v", err)
	}

	// Test basic lifecycle
	if err := gcsPlugin.Start(); err != nil {
		t.Fatalf("failed to start GCS plugin: %v", err)
	}
	defer func() {
		if err := gcsPlugin.Stop(); err != nil {
			t.Errorf("failed to stop GCS plugin: %v", err)
		}
	}()
}

func TestCloudPluginS3(t *testing.T) {
	// Test S3 plugin with real credentials if available
	if !hasS3Credentials() {
		t.Skip("S3 credentials not found, skipping test")
	}

	// Set up test configuration
	testBucket := os.Getenv("DINGO_TEST_S3_BUCKET")
	if testBucket == "" {
		testBucket = "dingo-test-bucket"
	}

	// Create S3 plugin directly with test configuration
	s3Plugin, err := aws.NewWithOptions(
		aws.WithBucket(testBucket),
	)
	if err != nil {
		t.Fatalf("failed to create S3 plugin: %v", err)
	}

	// Test basic lifecycle
	if err := s3Plugin.Start(); err != nil {
		t.Fatalf("failed to start S3 plugin: %v", err)
	}
	defer func() {
		if err := s3Plugin.Stop(); err != nil {
			t.Errorf("failed to stop S3 plugin: %v", err)
		}
	}()
}

func hasGCSCredentials() bool {
	// Check for GCS credentials in various locations
	credentials := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if credentials != "" {
		return true
	}

	// Check if gcloud is configured
	home := os.Getenv("HOME")
	if home != "" {
		// Check for application default credentials
		adcPath := filepath.Join(
			home,
			".config",
			"gcloud",
			"application_default_credentials.json",
		)
		if _, err := os.Stat(adcPath); err == nil {
			return true
		}
	}

	return false
}

func hasS3Credentials() bool {
	// Check for AWS credentials in standard locations
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if accessKey != "" && secretKey != "" {
		return true
	}

	// Check for AWS profile or IAM role
	// This is a basic check - AWS SDK will handle the full credential chain
	home := os.Getenv("HOME")
	if home != "" {
		credentialsPath := filepath.Join(home, ".aws", "credentials")
		if _, err := os.Stat(credentialsPath); err == nil {
			return true
		}
	}

	// Check for EC2 instance metadata (IAM role)
	// This would require a network call, so we'll assume if AWS_REGION is set,
	// there might be credentials available
	if os.Getenv("AWS_REGION") != "" {
		return true
	}

	return false
}

func findPluginEntry(
	plugins []plugin.PluginEntry,
	name string,
) *plugin.PluginEntry {
	for i := range plugins {
		p := &plugins[i]
		if p.Name == name {
			return p
		}
	}
	return nil
}
