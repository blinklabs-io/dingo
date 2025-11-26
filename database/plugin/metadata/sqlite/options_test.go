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

package sqlite

import (
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestWithDataDir(t *testing.T) {
	m := &MetadataStoreSqlite{}
	option := WithDataDir("/tmp/test")

	option(m)

	if m.dataDir != "/tmp/test" {
		t.Errorf("Expected dataDir to be '/tmp/test', got '%s'", m.dataDir)
	}
}

func TestWithLogger(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(nil, nil))
	m := &MetadataStoreSqlite{}
	option := WithLogger(logger)

	option(m)

	if m.logger != logger {
		t.Errorf("Expected logger to be set")
	}
}

func TestWithPromRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := &MetadataStoreSqlite{}
	option := WithPromRegistry(reg)

	option(m)

	if m.promRegistry != reg {
		t.Errorf("Expected promRegistry to be set")
	}
}
