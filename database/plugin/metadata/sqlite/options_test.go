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

package sqlite_test

import (
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/prometheus/client_golang/prometheus"
)

func TestWithDataDir(t *testing.T) {
	m := &sqlite.MetadataStoreSqlite{}
	option := sqlite.WithDataDir("/tmp/test")

	option(m)

	if m.DataDir != "/tmp/test" {
		t.Errorf("Expected DataDir to be '/tmp/test', got '%s'", m.DataDir)
	}
}

func TestWithLogger(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := &sqlite.MetadataStoreSqlite{}
	option := sqlite.WithLogger(logger)

	option(m)

	if m.Logger != logger {
		t.Errorf("Expected logger to be set")
	}
}

func TestWithPromRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := &sqlite.MetadataStoreSqlite{}
	option := sqlite.WithPromRegistry(reg)

	option(m)

	if m.PromRegistry != reg {
		t.Errorf("Expected PromRegistry to be set")
	}
}
