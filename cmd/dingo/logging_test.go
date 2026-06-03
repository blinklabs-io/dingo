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

package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLogger_JSONFormatProducesValidJSON(t *testing.T) {
	var buf bytes.Buffer
	logger, levelOK, formatOK := newLogger(&buf, "json", "info", false)
	require.True(t, levelOK)
	require.True(t, formatOK)

	logger.Info("hello", "component", "test", "k", "v")

	line := strings.TrimSpace(buf.String())
	require.NotEmpty(t, line)
	require.True(t, json.Valid([]byte(line)), "expected valid JSON, got: %s", line)
	var rec map[string]any
	require.NoError(t, json.Unmarshal([]byte(line), &rec))
	assert.Equal(t, "hello", rec["msg"])
	assert.Equal(t, "test", rec["component"])
}

func TestNewLogger_TextFormatIsNotJSON(t *testing.T) {
	var buf bytes.Buffer
	logger, _, formatOK := newLogger(&buf, "text", "info", false)
	require.True(t, formatOK)

	logger.Info("hello", "component", "test")

	line := strings.TrimSpace(buf.String())
	require.NotEmpty(t, line)
	assert.False(
		t, json.Valid([]byte(line)),
		"text output should not be valid JSON: %s", line,
	)
	assert.Contains(t, line, "component=test")
}

func TestNewLogger_EmptyFormatDefaultsToText(t *testing.T) {
	var buf bytes.Buffer
	logger, _, formatOK := newLogger(&buf, "", "info", false)
	require.True(t, formatOK)

	logger.Info("hello")
	assert.False(t, json.Valid(bytes.TrimSpace(buf.Bytes())))
}

func TestNewLogger_UnknownFormatFallsBackToTextAndReportsNotOK(t *testing.T) {
	var buf bytes.Buffer
	logger, _, formatOK := newLogger(&buf, "xml", "info", false)
	assert.False(t, formatOK)

	logger.Info("hello")
	assert.False(t, json.Valid(bytes.TrimSpace(buf.Bytes())))
}

func TestNewLogger_LevelFiltersBelowThreshold(t *testing.T) {
	var buf bytes.Buffer
	logger, levelOK, _ := newLogger(&buf, "text", "warn", false)
	require.True(t, levelOK)

	logger.Info("info-msg")
	logger.Warn("warn-msg")

	out := buf.String()
	assert.NotContains(t, out, "info-msg")
	assert.Contains(t, out, "warn-msg")
}

func TestNewLogger_UnknownLevelReportsNotOKAndUsesInfo(t *testing.T) {
	var buf bytes.Buffer
	logger, levelOK, _ := newLogger(&buf, "text", "bogus", false)
	assert.False(t, levelOK)

	logger.Debug("debug-msg") // below info => suppressed
	logger.Info("info-msg")
	out := buf.String()
	assert.NotContains(t, out, "debug-msg")
	assert.Contains(t, out, "info-msg")
}

func TestNewLogger_DebugFlagOverridesLevel(t *testing.T) {
	var buf bytes.Buffer
	// level=error, but --debug must force debug level.
	logger, _, _ := newLogger(&buf, "text", "error", true)

	logger.Debug("debug-msg")
	assert.Contains(t, buf.String(), "debug-msg")
}

func TestParseLogLevel(t *testing.T) {
	cases := []struct {
		in     string
		want   slog.Level
		wantOK bool
	}{
		{"debug", slog.LevelDebug, true},
		{"info", slog.LevelInfo, true},
		{"", slog.LevelInfo, true},
		{"WARN", slog.LevelWarn, true},
		{"warning", slog.LevelWarn, true},
		{"error", slog.LevelError, true},
		{"bogus", slog.LevelInfo, false},
	}
	for _, c := range cases {
		got, ok := parseLogLevel(c.in)
		assert.Equalf(t, c.want, got, "level %q", c.in)
		assert.Equalf(t, c.wantOK, ok, "ok %q", c.in)
	}
}
