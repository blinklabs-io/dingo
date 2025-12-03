package plugin_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin"
	_ "github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	_ "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/internal/config"
)

// Basic tests for SetPluginOption to ensure programmatic option setting works
func TestSetPluginOption_SuccessAndTypeCheck(t *testing.T) {
	// Note: This test mutates global plugin state (cmdlineOptions in subpackages).
	// If tests run in parallel, this could cause interference. Currently, tests
	// are run sequentially, but consider adding cleanup if parallelism is enabled.

	// Set data-dir for sqlite plugin to an empty string (in-memory) and ensure no error
	if err := plugin.SetPluginOption(plugin.PluginTypeMetadata, config.DefaultMetadataPlugin, "data-dir", ""); err != nil {
		t.Fatalf("unexpected error setting sqlite data-dir: %v", err)
	}

	// Setting with wrong type should return an error
	if err := plugin.SetPluginOption(plugin.PluginTypeMetadata, config.DefaultMetadataPlugin, "data-dir", 123); err == nil {
		t.Fatalf(
			"expected type error when setting sqlite data-dir with int, got nil",
		)
	}

	// Setting an unknown option is a no-op (non-fatal) so should not return an error
	if err := plugin.SetPluginOption(plugin.PluginTypeMetadata, config.DefaultMetadataPlugin, "does-not-exist", "x"); err != nil {
		t.Fatalf("unexpected error when setting unknown option: %v", err)
	}

	// Test setting data-dir for badger plugin (blob type)
	if err := plugin.SetPluginOption(plugin.PluginTypeBlob, config.DefaultBlobPlugin, "data-dir", t.TempDir()); err != nil {
		t.Fatalf("unexpected error setting badger data-dir: %v", err)
	}

	// Test uint option handling for badger block-cache-size
	if err := plugin.SetPluginOption(plugin.PluginTypeBlob, config.DefaultBlobPlugin, "block-cache-size", uint64(100000000)); err != nil {
		t.Fatalf("unexpected error setting badger block-cache-size: %v", err)
	}

	// Test bool option handling for badger gc
	if err := plugin.SetPluginOption(plugin.PluginTypeBlob, config.DefaultBlobPlugin, "gc", true); err != nil {
		t.Fatalf("unexpected error setting badger gc: %v", err)
	}

	// Test plugin not found error
	if err := plugin.SetPluginOption(plugin.PluginTypeMetadata, "nonexistent", "data-dir", t.TempDir()); err == nil {
		t.Fatalf(
			"expected error when setting option for nonexistent plugin, got nil",
		)
	}
}
