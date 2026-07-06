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

// TestProcessConfig_NoPartialApplicationOnUnknownKey verifies that when a
// config block mixes a valid key with a typo'd one, the valid key's option
// is not applied before the error for the unknown key is returned. All
// keys must be validated before any of a plugin's options are mutated.
func TestProcessConfig_NoPartialApplicationOnUnknownKey(t *testing.T) {
	bucket := "unset"
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
				"bucket": "should-not-apply",
				"buckit": "typo-value",
			},
		},
	})
	if err == nil {
		t.Fatal("expected error for unknown plugin config key, got nil")
	}
	if bucket != "unset" {
		t.Errorf(
			"bucket = %q, want unset (valid key must not apply before the unknown-key error)",
			bucket,
		)
	}
}

// TestProcessConfig_RejectsUnknownPluginName verifies that a config
// section whose plugin name doesn't match any registered plugin (e.g. a
// typo like "badgre" instead of "badger") fails config load instead of
// being silently ignored, since the outer loop only ever visits
// registered plugin names.
func TestProcessConfig_RejectsUnknownPluginName(t *testing.T) {
	var bucket string
	withTestPluginEntries(t, []PluginEntry{
		{
			Type: PluginTypeBlob,
			Name: "test-plugin",
			Options: []PluginOption{
				{Name: "bucket", Type: PluginOptionTypeString, Dest: &bucket},
			},
		},
	})

	err := ProcessConfig(map[string]map[string]map[string]any{
		"blob": {
			"tset-plugin": {
				"bucket": "my-bucket",
			},
		},
	})
	if err == nil {
		t.Fatal("expected error for unknown plugin name, got nil")
	}
	if !strings.Contains(err.Error(), "tset-plugin") {
		t.Errorf("error %q does not mention the unknown plugin name", err.Error())
	}
}

// TestProcessConfig_TolerantOfBuildTagGatedPluginNames verifies that a
// config section for a plugin name that's valid in other builds (behind
// -tags dingo_extra_plugins) is accepted even when that plugin isn't
// registered in the current build, so shipping a single example config
// covering all optional plugins doesn't break plain builds.
func TestProcessConfig_TolerantOfBuildTagGatedPluginNames(t *testing.T) {
	withTestPluginEntries(t, []PluginEntry{
		{Type: PluginTypeBlob, Name: "badger"},
	})

	err := ProcessConfig(map[string]map[string]map[string]any{
		"blob": {
			"gcs": {
				"bucket": "my-bucket",
			},
		},
	})
	if err != nil {
		t.Errorf(
			"unexpected error for build-tag-gated plugin name not compiled into this build: %v",
			err,
		)
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
