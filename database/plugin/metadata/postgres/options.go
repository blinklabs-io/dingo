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

package postgres

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

type PostgresOptionFunc func(*MetadataStorePostgres)

// WithLogger specifies the logger object to use for logging messages
func WithLogger(logger *slog.Logger) PostgresOptionFunc {
	return func(m *MetadataStorePostgres) {
		m.logger = logger
	}
}

// WithPromRegistry specifies the prometheus registry to use for metrics
func WithPromRegistry(
	registry prometheus.Registerer,
) PostgresOptionFunc {
	return func(m *MetadataStorePostgres) {
		m.promRegistry = registry
	}
}

// WithHost specifies the Postgres host
func WithHost(host string) PostgresOptionFunc {
	return func(m *MetadataStorePostgres) {
		m.host = host
	}
}

// WithPort specifies the Postgres port
func WithPort(port uint) PostgresOptionFunc {
	return func(m *MetadataStorePostgres) {
		m.port = port
	}
}

// WithUser specifies the Postgres user
func WithUser(user string) PostgresOptionFunc {
	return func(m *MetadataStorePostgres) {
		m.user = user
	}
}

// WithPassword specifies the Postgres password
func WithPassword(password string) PostgresOptionFunc {
	return func(m *MetadataStorePostgres) {
		m.password = password
	}
}

// WithDatabase specifies the Postgres database name
func WithDatabase(database string) PostgresOptionFunc {
	return func(m *MetadataStorePostgres) {
		m.database = database
	}
}

// WithSSLMode specifies the Postgres sslmode
func WithSSLMode(sslMode string) PostgresOptionFunc {
	return func(m *MetadataStorePostgres) {
		m.sslMode = sslMode
	}
}

// WithTimeZone specifies the Postgres TimeZone
func WithTimeZone(timeZone string) PostgresOptionFunc {
	return func(m *MetadataStorePostgres) {
		m.timeZone = timeZone
	}
}

// WithDSN specifies a full Postgres DSN string and takes precedence over
// individual connection options.
func WithDSN(dsn string) PostgresOptionFunc {
	return func(m *MetadataStorePostgres) {
		m.dsn = dsn
	}
}
