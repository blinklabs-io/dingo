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

//go:build !dingo_extra_plugins

package migrations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeMySQLIndexColumnsDefault(t *testing.T) {
	t.Parallel()
	require.Equal(
		t,
		"hash,slot",
		normalizeMySQLIndexColumnsDefault("`hash`(255) DESC, `slot` ASC"),
	)
}

func TestMySQLIndexDefinitionPatternDefault(t *testing.T) {
	t.Parallel()
	match := mysqlIndexDefinitionPatternDefault.FindStringSubmatch(
		"CREATE UNIQUE INDEX IF NOT EXISTS `hash_slot` ON `block_nonce` (`hash`, `slot`)",
	)
	require.Len(t, match, 5)
	require.Equal(t, "UNIQUE ", match[1])
	require.Equal(t, "hash_slot", match[2])
	require.Equal(t, "block_nonce", match[3])
	require.Equal(t, "`hash`, `slot`", match[4])
}
