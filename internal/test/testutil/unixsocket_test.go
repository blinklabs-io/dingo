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

package testutil

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCheckUnixSocketPathLenRejectsOverLongPath proves the guard fires rather
// than merely existing. The length it rejects is the portable budget, which
// is below this platform's own limit, so this assertion holds everywhere.
func TestCheckUnixSocketPathLenRejectsOverLongPath(t *testing.T) {
	t.Parallel()

	tooLong := "/tmp/" + strings.Repeat("x", MaxPortableUnixSocketPathLen)
	require.Greater(t, len(tooLong), MaxPortableUnixSocketPathLen)
	err := CheckUnixSocketPathLen(tooLong)
	require.Error(t, err)
	require.ErrorContains(t, err, "portable limit")

	require.NoError(
		t,
		CheckUnixSocketPathLen(
			"/tmp/"+strings.Repeat(
				"x",
				MaxPortableUnixSocketPathLen-len("/tmp/"),
			),
		),
	)
}

// TestBlockProducerUnsupportedReason proves the skip fires on the platform it
// names and nowhere else. The branch it covers can never be reached by this
// project's own runs on Linux or macOS, so without passing the platform in
// explicitly the Windows decision would ship unexercised.
func TestBlockProducerUnsupportedReason(t *testing.T) {
	t.Parallel()

	require.Contains(
		t,
		blockProducerUnsupportedReason("windows"),
		"not a supported configuration on Windows",
	)
	require.Empty(t, blockProducerUnsupportedReason("linux"))
	require.Empty(t, blockProducerUnsupportedReason("darwin"))
}
