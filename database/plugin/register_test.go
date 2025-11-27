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

package plugin_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin"
)

// Mock plugin implementation for testing
type mockPlugin struct{}

func (m *mockPlugin) Start() error { return nil }
func (m *mockPlugin) Stop() error  { return nil }

func TestRegister(t *testing.T) {
	pluginName := "test-plugin-" + t.Name()
	testEntry := plugin.PluginEntry{
		Type:               plugin.PluginTypeBlob,
		Name:               pluginName,
		NewFromOptionsFunc: func() plugin.Plugin { return &mockPlugin{} },
	}

	plugin.Register(testEntry)

	// Check that GetPlugin finds it
	p := plugin.GetPlugin(plugin.PluginTypeBlob, pluginName)
	if p == nil {
		t.Error("plugin not found")
	}

	// Check that GetPlugins includes it
	plugins := plugin.GetPlugins(plugin.PluginTypeBlob)
	found := false
	for _, pl := range plugins {
		if pl.Name == pluginName && pl.Type == plugin.PluginTypeBlob {
			found = true
			break
		}
	}
	if !found {
		t.Error("plugin not in GetPlugins list")
	}
}

func TestGetPlugins(t *testing.T) {
	blobName1 := "blob-test-1-" + t.Name()
	blobName2 := "blob-test-2-" + t.Name()
	metaName := "meta-test-" + t.Name()

	plugin.Register(plugin.PluginEntry{
		Type:               plugin.PluginTypeBlob,
		Name:               blobName1,
		NewFromOptionsFunc: func() plugin.Plugin { return &mockPlugin{} },
	})

	plugin.Register(plugin.PluginEntry{
		Type:               plugin.PluginTypeBlob,
		Name:               blobName2,
		NewFromOptionsFunc: func() plugin.Plugin { return &mockPlugin{} },
	})

	plugin.Register(plugin.PluginEntry{
		Type:               plugin.PluginTypeMetadata,
		Name:               metaName,
		NewFromOptionsFunc: func() plugin.Plugin { return &mockPlugin{} },
	})

	blobPlugins := plugin.GetPlugins(plugin.PluginTypeBlob)
	// Check that the registered blob plugins are in the list
	found1 := false
	found2 := false
	for _, pl := range blobPlugins {
		if pl.Name == blobName1 && pl.Type == plugin.PluginTypeBlob {
			found1 = true
		}
		if pl.Name == blobName2 && pl.Type == plugin.PluginTypeBlob {
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Error("not all blob plugins found")
	}

	metaPlugins := plugin.GetPlugins(plugin.PluginTypeMetadata)
	foundMeta := false
	for _, pl := range metaPlugins {
		if pl.Name == metaName && pl.Type == plugin.PluginTypeMetadata {
			foundMeta = true
			break
		}
	}
	if !foundMeta {
		t.Error("metadata plugin not found")
	}
}

func TestGetPlugin(t *testing.T) {
	pluginName := "test-get-plugin-" + t.Name()
	plugin.Register(plugin.PluginEntry{
		Type:               plugin.PluginTypeBlob,
		Name:               pluginName,
		NewFromOptionsFunc: func() plugin.Plugin { return &mockPlugin{} },
	})

	// Test getting the plugin
	p := plugin.GetPlugin(plugin.PluginTypeBlob, pluginName)
	if p == nil {
		t.Fatal("Expected plugin instance, got nil")
	}

	if _, ok := p.(*mockPlugin); !ok {
		t.Errorf("Expected plugin of type *mockPlugin, got %T", p)
	}

	// Test getting non-existent plugin
	nonExistentPlugin := plugin.GetPlugin(
		plugin.PluginTypeBlob,
		"non-existent-"+t.Name(),
	)
	if nonExistentPlugin != nil {
		t.Errorf(
			"Expected nil for non-existent plugin, got %v",
			nonExistentPlugin,
		)
	}
}
