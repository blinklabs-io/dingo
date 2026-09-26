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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mcp

import (
	"database/sql"
	"log/slog"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mempool"
)

// ProviderConfig holds the configuration for the MCP API server.
type ProviderConfig struct {
	Port         uint                `yaml:"port"`
	Host         string              `yaml:"host"`
	AuthToken    string              `yaml:"authToken"`
	RateLimit    float64             `yaml:"rateLimit"` // requests per second (0 = unlimited)
	Burst        int                 `yaml:"burst"`     // rate limiter burst allowance (default 10)
	QueryTimeout time.Duration       `yaml:"queryTimeout"`
	MaxRows      int                 `yaml:"maxRows"`
	TLS          apiconfig.TLSPolicy `yaml:"tls"`
}

// ProviderDependencies contains the dependencies injected by the Node coordinator.
type ProviderDependencies struct {
	Logger             *slog.Logger
	Database           *database.Database
	DataDir            string
	SQLDB              *sql.DB // optional direct SQLite read-only connection
	LedgerState        *ledger.LedgerState
	Mempool            mempool.Service
	Host               string
	Network            string
	CORSAllowedOrigins []string
}

// DefaultProviderConfig returns default settings for the MCP server.
func DefaultProviderConfig() ProviderConfig {
	return ProviderConfig{
		Port:         8088,
		Host:         "127.0.0.1",
		AuthToken:    "",
		RateLimit:    60.0, // 60 requests per minute
		Burst:        15,
		QueryTimeout: 5 * time.Second,
		MaxRows:      100,
	}
}
