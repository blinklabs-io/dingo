//go:build dingo_extra_plugins

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

package mysql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func coreDSN(dsn string) (string, string, error) {
	parsed, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return "", "", fmt.Errorf("parse MySQL DSN: %w", err)
	}
	if parsed.DBName == "" {
		return "", "", errors.New(
			"the API MySQL DSN must name its database before a consensus database can be selected",
		)
	}
	parsed.DBName = metadata.CoreNamespaceName(parsed.DBName, "")
	return parsed.FormatDSN(), parsed.DBName, nil
}

// configuredUser returns the user cfg connects as, for privilege errors.
func configuredUser(cfg Config) string {
	dsn := cfg.DSN
	if dsn == "" {
		var err error
		if dsn, err = assembleDSN(cfg); err != nil {
			return cfg.User
		}
	}
	parsed, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return cfg.User
	}
	return parsed.User
}

// describeCorePrivilegeError names the consensus database and the GRANT a
// privilege refusal while creating or migrating it needs. Creating the
// database needs CREATE on it; its schema migrations need the table and
// index privileges ALL grants. The API database is never modified first.
func describeCorePrivilegeError(err error, user, database string) error {
	var refused *mysqldriver.MySQLError
	if errors.As(err, &refused) {
		switch refused.Number {
		case 1044, 1142, 1227, 1143, 1045:
			if refused.Number == 1045 {
				return fmt.Errorf(
					"authenticate MySQL user %q: check the configured credentials: %w",
					user, err,
				)
			}
			return fmt.Errorf(
				"prepare the MySQL consensus database %q: user %q lacks a privilege on it; run GRANT ALL PRIVILEGES ON %s.* TO %s and restart, the API database is unchanged: %w",
				database, user,
				"`"+strings.ReplaceAll(database, "`", "``")+"`",
				quoteMySQLUser(user),
				err,
			)
		}
	}
	return fmt.Errorf(
		"prepare the MySQL consensus database %q: %w", database, err,
	)
}

func quoteMySQLUser(user string) string {
	return "'" + strings.ReplaceAll(user, "'", "''") + "'"
}
