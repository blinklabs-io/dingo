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
	"io"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestWithHost(t *testing.T) {
	m := &MetadataStorePostgres{}
	option := WithHost("db.local")

	option(m)

	if m.host != "db.local" {
		t.Errorf("Expected host to be 'db.local', got '%s'", m.host)
	}
}

func TestWithPort(t *testing.T) {
	m := &MetadataStorePostgres{}
	option := WithPort(uint(5432))

	option(m)

	if m.port != 5432 {
		t.Errorf("Expected port to be 5432, got '%d'", m.port)
	}
}

func TestWithUser(t *testing.T) {
	m := &MetadataStorePostgres{}
	option := WithUser("postgres")

	option(m)

	if m.user != "postgres" {
		t.Errorf("Expected user to be 'postgres', got '%s'", m.user)
	}
}

func TestWithPassword(t *testing.T) {
	m := &MetadataStorePostgres{}
	option := WithPassword("secret")

	option(m)

	if m.password != "secret" {
		t.Errorf("Expected password to be set")
	}
}

func TestWithDatabase(t *testing.T) {
	m := &MetadataStorePostgres{}
	option := WithDatabase("dingo")

	option(m)

	if m.database != "dingo" {
		t.Errorf("Expected database to be 'dingo', got '%s'", m.database)
	}
}

func TestWithSSLMode(t *testing.T) {
	m := &MetadataStorePostgres{}
	option := WithSSLMode("require")

	option(m)

	if m.sslMode != "require" {
		t.Errorf("Expected sslMode to be 'require', got '%s'", m.sslMode)
	}
}

func TestWithTimeZone(t *testing.T) {
	m := &MetadataStorePostgres{}
	option := WithTimeZone("UTC")

	option(m)

	if m.timeZone != "UTC" {
		t.Errorf("Expected timeZone to be 'UTC', got '%s'", m.timeZone)
	}
}

func TestWithDSN(t *testing.T) {
	m := &MetadataStorePostgres{}
	option := WithDSN("host=localhost dbname=dingo")

	option(m)

	if m.dsn != "host=localhost dbname=dingo" {
		t.Errorf("Expected dsn to be set")
	}
}

func TestWithLogger(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := &MetadataStorePostgres{}
	option := WithLogger(logger)

	option(m)

	if m.logger != logger {
		t.Errorf("Expected logger to be set")
	}
}

func TestWithPromRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := &MetadataStorePostgres{}
	option := WithPromRegistry(reg)

	option(m)

	if m.promRegistry != reg {
		t.Errorf("Expected promRegistry to be set")
	}
}
