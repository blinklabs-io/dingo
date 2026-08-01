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

//go:build dingo_extra_plugins

package migrations

import (
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestIsDDLAlreadyAppliedOnlyAcceptsDuplicateDefinitions(t *testing.T) {
	t.Parallel()
	require.True(t, isDDLAlreadyApplied(&mysqldriver.MySQLError{Number: 1061}))
	require.True(t, isDDLAlreadyApplied(&mysqldriver.MySQLError{Number: 1826}))
	require.False(t, isDDLAlreadyApplied(&mysqldriver.MySQLError{
		Number:  1005,
		Message: "already exists but is incompatible",
	}))
	require.False(t, isDDLAlreadyApplied(
		&mysqldriver.MySQLError{Number: 1062, Message: "duplicate key name"},
	))
}
