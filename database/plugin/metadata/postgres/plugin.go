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
	"sync"

	"github.com/blinklabs-io/dingo/database/plugin"
)

var (
	cmdlineOptions struct {
		host     string
		port     uint64
		user     string
		password string
		database string
		sslMode  string
		timeZone string
		dsn      string
	}
	cmdlineOptionsMutex sync.RWMutex
)

// initCmdlineOptions sets default values for cmdlineOptions.
// Note: password is intentionally empty - users must provide their own credentials.
func initCmdlineOptions() {
	cmdlineOptionsMutex.Lock()
	defer cmdlineOptionsMutex.Unlock()
	cmdlineOptions.host = "localhost"
	cmdlineOptions.port = 5432
	cmdlineOptions.user = "postgres"
	cmdlineOptions.password = ""
	cmdlineOptions.database = "postgres"
	cmdlineOptions.sslMode = "disable"
	cmdlineOptions.timeZone = "UTC"
	cmdlineOptions.dsn = ""
}

// Register plugin
func init() {
	initCmdlineOptions()
	plugin.Register(
		plugin.PluginEntry{
			Type:               plugin.PluginTypeMetadata,
			Name:               "postgres",
			Description:        "Postgres relational database",
			NewFromOptionsFunc: NewFromCmdlineOptions,
			Options: []plugin.PluginOption{
				{
					Name:         "host",
					Type:         plugin.PluginOptionTypeString,
					Description:  "Postgres host",
					DefaultValue: "localhost",
					Dest:         &(cmdlineOptions.host),
				},
				{
					Name:         "port",
					Type:         plugin.PluginOptionTypeUint,
					Description:  "Postgres port",
					DefaultValue: uint64(5432),
					Dest:         &(cmdlineOptions.port),
				},
				{
					Name:         "user",
					Type:         plugin.PluginOptionTypeString,
					Description:  "Postgres user",
					DefaultValue: "postgres",
					Dest:         &(cmdlineOptions.user),
				},
				{
					Name:         "password",
					Type:         plugin.PluginOptionTypeString,
					Description:  "Postgres password (required)",
					DefaultValue: "",
					Dest:         &(cmdlineOptions.password),
				},
				{
					Name:         "database",
					Type:         plugin.PluginOptionTypeString,
					Description:  "Postgres database name",
					DefaultValue: "postgres",
					Dest:         &(cmdlineOptions.database),
				},
				{
					Name:         "ssl-mode",
					Type:         plugin.PluginOptionTypeString,
					Description:  "Postgres sslmode",
					DefaultValue: "disable",
					Dest:         &(cmdlineOptions.sslMode),
				},
				{
					Name:         "timezone",
					Type:         plugin.PluginOptionTypeString,
					Description:  "Postgres TimeZone",
					DefaultValue: "UTC",
					Dest:         &(cmdlineOptions.timeZone),
				},
				{
					Name:         "dsn",
					Type:         plugin.PluginOptionTypeString,
					Description:  "Full Postgres DSN (overrides other options when set)",
					DefaultValue: "",
					Dest:         &(cmdlineOptions.dsn),
				},
			},
		},
	)
}

func NewFromCmdlineOptions() plugin.Plugin {
	cmdlineOptionsMutex.RLock()
	host := cmdlineOptions.host
	port := uint(cmdlineOptions.port)
	user := cmdlineOptions.user
	password := cmdlineOptions.password
	database := cmdlineOptions.database
	sslMode := cmdlineOptions.sslMode
	timeZone := cmdlineOptions.timeZone
	dsn := cmdlineOptions.dsn
	cmdlineOptionsMutex.RUnlock()

	opts := []PostgresOptionFunc{
		WithHost(host),
		WithPort(port),
		WithUser(user),
		WithPassword(password),
		WithDatabase(database),
		WithSSLMode(sslMode),
		WithTimeZone(timeZone),
		WithDSN(dsn),
		// Logger and promRegistry will use defaults if nil
	}
	p, err := NewWithOptions(opts...)
	if err != nil {
		// Return a plugin that defers the error to Start()
		return plugin.NewErrorPlugin(err)
	}
	return p
}
