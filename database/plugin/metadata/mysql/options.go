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

//go:build dingo_extra_plugins

package mysql

import (
	"log/slog"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type MysqlOptionFunc func(*MetadataStoreMysql)

// WithLogger specifies the logger object to use for logging messages
func WithLogger(logger *slog.Logger) MysqlOptionFunc {
	return func(m *MetadataStoreMysql) {
		m.logger = logger
	}
}

// WithPromRegistry specifies the prometheus registry to use for metrics
func WithPromRegistry(
	registry prometheus.Registerer,
) MysqlOptionFunc {
	return func(m *MetadataStoreMysql) {
		m.promRegistry = registry
	}
}

// WithHost specifies the MySQL host
func WithHost(host string) MysqlOptionFunc {
	return func(m *MetadataStoreMysql) {
		m.host = host
	}
}

// WithPort specifies the MySQL port
func WithPort(port uint) MysqlOptionFunc {
	return func(m *MetadataStoreMysql) {
		m.port = port
	}
}

// WithUser specifies the MySQL user
func WithUser(user string) MysqlOptionFunc {
	return func(m *MetadataStoreMysql) {
		m.user = user
	}
}

// WithPassword specifies the MySQL password
func WithPassword(password string) MysqlOptionFunc {
	return func(m *MetadataStoreMysql) {
		m.password = password
	}
}

// WithDatabase specifies the MySQL database name
func WithDatabase(database string) MysqlOptionFunc {
	return func(m *MetadataStoreMysql) {
		m.database = database
	}
}

// WithSSLMode specifies the MySQL TLS option (mapped to tls= in the DSN)
func WithSSLMode(sslMode string) MysqlOptionFunc {
	return func(m *MetadataStoreMysql) {
		m.sslMode = sslMode
	}
}

// WithTimeZone specifies the MySQL time zone location
func WithTimeZone(timeZone string) MysqlOptionFunc {
	return func(m *MetadataStoreMysql) {
		m.timeZone = timeZone
	}
}

// WithDSN specifies a full MySQL DSN string and takes precedence over
// individual connection options.
func WithDSN(dsn string) MysqlOptionFunc {
	return func(m *MetadataStoreMysql) {
		m.dsn = dsn
	}
}

// WithStorageMode specifies the storage tier.
// "core" = consensus only, "api" = full tx metadata.
func WithStorageMode(mode string) MysqlOptionFunc {
	return func(m *MetadataStoreMysql) {
		m.storageMode = strings.ToLower(mode)
	}
}

// WithPoolMaxOpenConns specifies the maximum number of open connections to
// the database. A value of 0 selects the provider default.
func WithPoolMaxOpenConns(maxOpenConns int) MysqlOptionFunc {
	return func(m *MetadataStoreMysql) {
		m.poolMaxOpen = maxOpenConns
	}
}

// WithPoolMaxIdleConns specifies the maximum number of idle connections
// retained in the pool. A value of 0 selects the provider default.
func WithPoolMaxIdleConns(maxIdleConns int) MysqlOptionFunc {
	return func(m *MetadataStoreMysql) {
		m.poolMaxIdle = maxIdleConns
	}
}

// WithPoolConnMaxLifetime specifies the maximum amount of time a connection
// may be reused. A value of 0 selects the provider default.
func WithPoolConnMaxLifetime(connMaxLifetime time.Duration) MysqlOptionFunc {
	return func(m *MetadataStoreMysql) {
		m.poolConnMaxLifetime = connMaxLifetime
	}
}
