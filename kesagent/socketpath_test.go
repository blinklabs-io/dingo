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

package kesagent

import (
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewClientRejectsOverLongSocketPath pins the operator-facing half of this
// check: a path too long for the platform is refused at construction, with an
// error naming the length and the limit, rather than surfacing much later as
// a bare "invalid argument" from connect().
func TestNewClientRejectsOverLongSocketPath(t *testing.T) {
	t.Parallel()

	tooLong := "/" + strings.Repeat("x", sunPathLenOther)
	_, err := NewClient(Config{SocketPath: tooLong, Mode: ModeServeKey})
	require.Error(t, err)
	require.ErrorContains(t, err, "over this platform's")
	require.ErrorContains(t, err, "use a shorter path")
}

// TestNewClientAcceptsPathAtTheLimit is the boundary control: the check must
// reject what the platform rejects and nothing more, or it becomes its own
// bug. The longest acceptable non-abstract path is sun_path minus one byte
// for the NUL terminator.
func TestNewClientAcceptsPathAtTheLimit(t *testing.T) {
	t.Parallel()

	sunPath := sunPathLenOther
	if runtime.GOOS == "darwin" {
		sunPath = sunPathLenDarwin
	}
	atLimit := "/" + strings.Repeat("x", sunPath-2)
	require.Len(t, atLimit, sunPath-1)
	_, err := NewClient(Config{SocketPath: atLimit, Mode: ModeServeKey})
	require.NoError(t, err)
}

// TestSocketPathLimitIsPlatformCorrect pins the two constants against the
// limits the platforms actually impose, and the NUL-terminator rule that
// separates a named address from a Linux abstract one. Without the abstract
// case, a check that is one byte too strict would silently reject a valid
// agent address.
func TestSocketPathLimitIsPlatformCorrect(t *testing.T) {
	t.Parallel()

	named := socketPathLimit("/run/kes-agent.sock")
	abstract := socketPathLimit("@kes-agent")
	require.Equal(t, named+1, abstract, "abstract addresses carry no NUL")

	if runtime.GOOS == "darwin" {
		require.Equal(t, sunPathLenDarwin-1, named)
	} else {
		require.Equal(t, sunPathLenOther-1, named)
	}
}

// TestCheckSocketPathLenAtDarwinLimit exercises the macOS budget from any
// platform. macOS is 4 bytes tighter than Linux, so a path that is fine on a
// Linux CI runner can still be refused on a developer's Mac; this is the
// assertion that the tighter limit is enforced rather than merely documented.
func TestCheckSocketPathLenAtDarwinLimit(t *testing.T) {
	t.Parallel()

	const darwinNamedLimit = sunPathLenDarwin - 1
	justOver := strings.Repeat("x", darwinNamedLimit+1)
	require.Error(t, checkSocketPathLen(justOver, darwinNamedLimit))
	require.NoError(
		t,
		checkSocketPathLen(
			strings.Repeat("x", darwinNamedLimit),
			darwinNamedLimit,
		),
	)

	// The same path is accepted under Linux's looser limit, which is exactly
	// the gap that makes this platform-specific rather than universal.
	require.NoError(t, checkSocketPathLen(justOver, sunPathLenOther-1))
}
