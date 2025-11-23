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

package plugin

import (
	"testing"
)

// Mock plugin implementation for testing
type mockPlugin struct{}

func (m *mockPlugin) Start() error { return nil }
func (m *mockPlugin) Stop() error  { return nil }

func TestRegister(t *testing.T) {
	// Save original plugin entries
	originalEntries := pluginEntries
	defer func() { pluginEntries = originalEntries }()

	// Reset plugin entries for this test
	pluginEntries = []PluginEntry{}

	// Register a test plugin
	testEntry := PluginEntry{
		Type:               PluginTypeBlob,
		Name:               "test-plugin",
		NewFromOptionsFunc: func() Plugin { return &mockPlugin{} },
	}

	Register(testEntry)

	// Verify the plugin was registered
	if len(pluginEntries) != 1 {
		t.Errorf("Expected 1 plugin entry, got %d", len(pluginEntries))
	}

	if pluginEntries[0].Type != PluginTypeBlob {
		t.Errorf(
			"Expected plugin type %d, got %d",
			PluginTypeBlob,
			pluginEntries[0].Type,
		)
	}

	if pluginEntries[0].Name != "test-plugin" {
		t.Errorf(
			"Expected plugin name 'test-plugin', got '%s'",
			pluginEntries[0].Name,
		)
	}
}

func TestGetPlugins(t *testing.T) {
	// Save original plugin entries
	originalEntries := pluginEntries
	defer func() { pluginEntries = originalEntries }()

	// Reset plugin entries for this test
	pluginEntries = []PluginEntry{}

	// Register test plugins
	Register(PluginEntry{
		Type:               PluginTypeBlob,
		Name:               "blob-plugin-1",
		NewFromOptionsFunc: func() Plugin { return &mockPlugin{} },
	})

	Register(PluginEntry{
		Type:               PluginTypeBlob,
		Name:               "blob-plugin-2",
		NewFromOptionsFunc: func() Plugin { return &mockPlugin{} },
	})

	Register(PluginEntry{
		Type:               PluginTypeMetadata,
		Name:               "metadata-plugin-1",
		NewFromOptionsFunc: func() Plugin { return &mockPlugin{} },
	})

	// Test getting blob plugins
	blobPlugins := GetPlugins(PluginTypeBlob)
	if len(blobPlugins) != 2 {
		t.Errorf("Expected 2 blob plugins, got %d", len(blobPlugins))
	}

	// Test getting metadata plugins
	metadataPlugins := GetPlugins(PluginTypeMetadata)
	if len(metadataPlugins) != 1 {
		t.Errorf("Expected 1 metadata plugin, got %d", len(metadataPlugins))
	}
}

func TestGetPlugin(t *testing.T) {
	// Save original plugin entries
	originalEntries := pluginEntries
	defer func() { pluginEntries = originalEntries }()

	// Reset plugin entries for this test
	pluginEntries = []PluginEntry{}

	// Register a test plugin
	Register(PluginEntry{
		Type:               PluginTypeBlob,
		Name:               "test-plugin",
		NewFromOptionsFunc: func() Plugin { return &mockPlugin{} },
	})

	// Test getting the plugin
	plugin := GetPlugin(PluginTypeBlob, "test-plugin")
	if plugin == nil {
		t.Fatal("Expected plugin instance, got nil")
	}

	if _, ok := plugin.(*mockPlugin); !ok {
		t.Errorf("Expected plugin of type *mockPlugin, got %T", plugin)
	}

	// Test getting non-existent plugin
	nonExistentPlugin := GetPlugin(PluginTypeBlob, "non-existent")
	if nonExistentPlugin != nil {
		t.Errorf(
			"Expected nil for non-existent plugin, got %v",
			nonExistentPlugin,
		)
	}
}
