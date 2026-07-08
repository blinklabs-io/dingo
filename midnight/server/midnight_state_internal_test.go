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

package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEffectivePageSize verifies zero/omitted capacity requests fall back
// to a bounded default instead of being forwarded to the store as an
// unbounded scan (limit <= 0 means "no SQL LIMIT" in the store contract),
// and that oversized requests are clamped rather than honored as-is.
func TestEffectivePageSize(t *testing.T) {
	t.Parallel()
	require.Equal(t, defaultEventPageSize, effectivePageSize(0))
	require.Equal(t, 1, effectivePageSize(1))
	require.Equal(t, maxEventPageSize, effectivePageSize(maxEventPageSize))
	require.Equal(t, maxEventPageSize, effectivePageSize(maxEventPageSize+1))
}
