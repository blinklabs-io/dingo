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

	"github.com/prometheus/client_golang/prometheus"
)

type SqliteOptionFunc func(*MetadataStoreSqlite)

// WithLogger specifies the logger object to use for logging messages
func WithLogger(logger *slog.Logger) SqliteOptionFunc {
	return func(m *MetadataStoreSqlite) {
		m.logger = logger
	}
}

// WithPromRegistry specifies the prometheus registry to use for metrics
func WithPromRegistry(
	registry prometheus.Registerer,
) SqliteOptionFunc {
	return func(m *MetadataStoreSqlite) {
		m.promRegistry = registry
	}
}

// WithDataDir specifies the data directory to use for storage
func WithDataDir(dataDir string) SqliteOptionFunc {
	return func(m *MetadataStoreSqlite) {
		m.dataDir = dataDir
	}
}
