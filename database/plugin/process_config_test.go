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

package plugin

import (
	"strings"
	"testing"
)

func withTestPluginEntries(t *testing.T, entries []PluginEntry) {
	t.Helper()
	original := pluginEntries
	t.Cleanup(func() { pluginEntries = original })
	pluginEntries = entries
}

// TestProcessConfig_RejectsUnknownKey verifies that a typo'd or otherwise
// unrecognized key in a plugin's config block fails config load instead of
// being silently ignored (e.g. "buckit" instead of "bucket").
func TestProcessConfig_RejectsUnknownKey(t *testing.T) {
	var bucket string
	withTestPluginEntries(t, []PluginEntry{
		{
			Type: PluginTypeBlob,
			Name: "test-plugin",
			Options: []PluginOption{
				{
					Name: "bucket",
					Type: PluginOptionTypeString,
					Dest: &bucket,
				},
			},
		},
	})

	err := ProcessConfig(map[string]map[string]map[string]any{
		"blob": {
			"test-plugin": {
				"buckit": "typo-value",
			},
		},
	})
	if err == nil {
		t.Fatal("expected error for unknown plugin config key, got nil")
	}
	if !strings.Contains(err.Error(), "buckit") {
		t.Errorf("error %q does not mention the unknown key", err.Error())
	}
}

// TestProcessConfig_AcceptsKnownKey is a control: a recognized key must
// still apply cleanly.
func TestProcessConfig_AcceptsKnownKey(t *testing.T) {
	var bucket string
	withTestPluginEntries(t, []PluginEntry{
		{
			Type: PluginTypeBlob,
			Name: "test-plugin",
			Options: []PluginOption{
				{
					Name: "bucket",
					Type: PluginOptionTypeString,
					Dest: &bucket,
				},
			},
		},
	})

	err := ProcessConfig(map[string]map[string]map[string]any{
		"blob": {
			"test-plugin": {
				"bucket": "my-bucket",
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error for known plugin config key: %v", err)
	}
	if bucket != "my-bucket" {
		t.Errorf("bucket = %q, want my-bucket", bucket)
	}
}
