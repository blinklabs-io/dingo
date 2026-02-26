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

package mysql

import (
	"sync"

	"github.com/blinklabs-io/dingo/database/plugin"
)

var (
	cmdlineOptions struct {
		dataDir     string
		maxConns    int
		host        string
		port        uint64
		user        string
		password    string
		database    string
		sslMode     string
		timeZone    string
		dsn         string
		storageMode string
	}
	cmdlineOptionsMutex sync.RWMutex
)

// initCmdlineOptions sets default values for cmdlineOptions.
// Note: password is intentionally empty - users must provide their own credentials.
func initCmdlineOptions() {
	cmdlineOptionsMutex.Lock()
	defer cmdlineOptionsMutex.Unlock()
	cmdlineOptions.dataDir = ""
	cmdlineOptions.maxConns = 0
	cmdlineOptions.host = "localhost"
	cmdlineOptions.port = 3306
	cmdlineOptions.user = "root"
	cmdlineOptions.password = ""
	cmdlineOptions.database = "dingo"
	cmdlineOptions.sslMode = ""
	cmdlineOptions.timeZone = "UTC"
	cmdlineOptions.dsn = ""
	cmdlineOptions.storageMode = "core"
}

// Register plugin
func init() {
	initCmdlineOptions()
	plugin.Register(
		plugin.PluginEntry{
			Type:               plugin.PluginTypeMetadata,
			Name:               "mysql",
			Description:        "MySQL relational database",
			NewFromOptionsFunc: NewFromCmdlineOptions,
			Options: []plugin.PluginOption{
				{
					Name:         "data-dir",
					Type:         plugin.PluginOptionTypeString,
					Description:  "Metadata data directory (unused for mysql)",
					DefaultValue: "",
					Dest:         &(cmdlineOptions.dataDir),
				},
				{
					Name:         "max-connections",
					Type:         plugin.PluginOptionTypeInt,
					Description:  "Maximum number of connections (unused for mysql)",
					DefaultValue: 0,
					Dest:         &(cmdlineOptions.maxConns),
				},
				{
					Name:         "host",
					Type:         plugin.PluginOptionTypeString,
					Description:  "MySQL host",
					DefaultValue: "localhost",
					CustomEnvVar: "MYSQL_HOST",
					Dest:         &(cmdlineOptions.host),
				},
				{
					Name:         "port",
					Type:         plugin.PluginOptionTypeUint,
					Description:  "MySQL port",
					DefaultValue: uint64(3306),
					CustomEnvVar: "MYSQL_PORT",
					Dest:         &(cmdlineOptions.port),
				},
				{
					Name:         "user",
					Type:         plugin.PluginOptionTypeString,
					Description:  "MySQL user",
					DefaultValue: "root",
					CustomEnvVar: "MYSQL_USER",
					Dest:         &(cmdlineOptions.user),
				},
				{
					Name:         "password",
					Type:         plugin.PluginOptionTypeString,
					Description:  "MySQL password (required)",
					DefaultValue: "",
					CustomEnvVar: "MYSQL_PASSWORD",
					Dest:         &(cmdlineOptions.password),
				},
				{
					Name:         "database",
					Type:         plugin.PluginOptionTypeString,
					Description:  "MySQL database name",
					DefaultValue: "dingo",
					CustomEnvVar: "MYSQL_DATABASE",
					Dest:         &(cmdlineOptions.database),
				},
				{
					Name:         "ssl-mode",
					Type:         plugin.PluginOptionTypeString,
					Description:  "MySQL TLS mode (mapped to tls= in DSN)",
					DefaultValue: "",
					CustomEnvVar: "MYSQL_SSLMODE",
					Dest:         &(cmdlineOptions.sslMode),
				},
				{
					Name:         "timezone",
					Type:         plugin.PluginOptionTypeString,
					Description:  "MySQL time zone location",
					DefaultValue: "UTC",
					CustomEnvVar: "MYSQL_TIMEZONE",
					Dest:         &(cmdlineOptions.timeZone),
				},
				{
					Name:         "dsn",
					Type:         plugin.PluginOptionTypeString,
					Description:  "Full MySQL DSN (overrides other options when set)",
					DefaultValue: "",
					CustomEnvVar: "MYSQL_DSN",
					Dest:         &(cmdlineOptions.dsn),
				},
				{
					Name:         "storage-mode",
					Type:         plugin.PluginOptionTypeString,
					Description:  "Storage tier: core or api",
					DefaultValue: "core",
					Dest:         &(cmdlineOptions.storageMode),
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
	storageMode := cmdlineOptions.storageMode
	cmdlineOptionsMutex.RUnlock()

	opts := []MysqlOptionFunc{
		WithHost(host),
		WithPort(port),
		WithUser(user),
		WithPassword(password),
		WithDatabase(database),
		WithSSLMode(sslMode),
		WithTimeZone(timeZone),
		WithDSN(dsn),
		WithStorageMode(storageMode),
		// Logger and promRegistry will use defaults if nil
	}
	p, err := NewWithOptions(opts...)
	if err != nil {
		// Return a plugin that defers the error to Start()
		return plugin.NewErrorPlugin(err)
	}
	return p
}
