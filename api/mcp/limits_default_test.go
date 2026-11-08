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

package mcp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDefaultOffsetLimitIsMaxToolOffset pins that the lowered-bound test seam
// leaves production behavior unchanged: default limits enforce maxToolOffset,
// the note boundary sits exactly at it, and the SQLite tool rejects one past
// it.
func TestDefaultOffsetLimitIsMaxToolOffset(t *testing.T) {
	t.Parallel()
	limits := newReadLimits()
	require.Equal(t, maxToolOffset, limits.maxOffset)
	require.Empty(t, offsetLimitNote(maxToolOffset-1, 1, 1, limits.maxOffset))
	require.Contains(
		t,
		offsetLimitNote(maxToolOffset, 1, 1, limits.maxOffset),
		"Offset limit reached",
	)
	cs := newToolSession(t, newSchemaDB(t), 100)
	text := callTool(t, cs, "sqlite_query", map[string]any{
		"query": "SELECT 1", "offset": maxToolOffset + 1,
	}, true)
	require.Contains(t, text, errOffsetTooLarge.Error())
	callTool(t, cs, "sqlite_query", map[string]any{
		"query": "SELECT 1", "offset": maxToolOffset,
	}, false)
}
