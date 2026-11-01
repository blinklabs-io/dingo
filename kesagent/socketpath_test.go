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
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewClientPinsRelativeSocketPath(t *testing.T) {
	root, err := os.MkdirTemp("", "kesagent-endpoint-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(root)) })
	first := filepath.Join(root, "first")
	second := filepath.Join(root, "second")
	require.NoError(t, os.Mkdir(first, 0o700))
	require.NoError(t, os.Mkdir(second, 0o700))

	t.Chdir(first)
	client, err := NewClient(Config{
		SocketPath: "kes-agent.sock",
		Mode:       ModeServeKey,
	})
	require.NoError(t, err)
	t.Chdir(second)
	require.Equal(t, filepath.Join(first, "kes-agent.sock"), client.cfg.SocketPath)
}

func TestNewClientCleansAbsoluteSocketPath(t *testing.T) {
	t.Parallel()

	separator := string(filepath.Separator)
	root := filepath.VolumeName(os.TempDir()) + separator
	want := filepath.Join(root, "run", "kes-agent.sock")
	configured := root + "var" + separator + ".." + separator +
		"run" + separator + "kes-agent.sock"
	client, err := NewClient(Config{
		SocketPath: configured,
		Mode:       ModeServeKey,
	})
	require.NoError(t, err)
	require.Equal(t, want, client.cfg.SocketPath)
}

func TestNewClientResolvesAtSocketPathByPlatform(t *testing.T) {
	t.Parallel()

	const configured = "@kes-agent"
	client, err := NewClient(Config{SocketPath: configured, Mode: ModeServeKey})
	require.NoError(t, err)
	if hasAbstractSocketNamespace(runtime.GOOS) {
		require.Equal(t, configured, client.cfg.SocketPath)
		return
	}
	want, err := filepath.Abs(configured)
	require.NoError(t, err)
	require.Equal(t, want, client.cfg.SocketPath)
}

func TestNewClientRejectsNULPrefixedSocketAddress(t *testing.T) {
	t.Parallel()

	_, err := NewClient(Config{
		SocketPath: "\x00kes-agent",
		Mode:       ModeServeKey,
	})
	require.ErrorContains(t, err, "NUL-prefixed")
	require.ErrorContains(t, err, "@ prefix")
}

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

	if runtime.GOOS == "windows" {
		t.Skip("the sun_path limit applies to Unix-style paths; Windows resolves the path with a drive letter")
	}

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
	atPrefix := socketPathLimit("@kes-agent")
	if hasAbstractSocketNamespace(runtime.GOOS) {
		require.Equal(t, named+1, atPrefix, "abstract addresses carry no NUL")
	} else {
		require.Equal(t, named, atPrefix, "@ is not abstract on this platform, so it costs a NUL too")
	}

	if runtime.GOOS == "darwin" {
		require.Equal(t, sunPathLenDarwin-1, named)
	} else {
		require.Equal(t, sunPathLenOther-1, named)
	}
}

// TestAtPrefixIsAbstractWhereGoRewritesIt pins the platform set. Go's syscall
// package rewrites a leading "@" to the abstract form on Linux (and Android,
// which builds the same file), Solaris and illumos, and Windows. Elsewhere
// "@name" is an ordinary relative filename.
func TestAtPrefixIsAbstractWhereGoRewritesIt(t *testing.T) {
	t.Parallel()

	for _, goos := range []string{"linux", "android", "illumos", "solaris", "windows"} {
		require.True(t, isAbstractSocketPathFor(goos, "@kes-agent"), goos)
	}
	for _, goos := range []string{"darwin", "freebsd", "openbsd", "netbsd", "dragonfly"} {
		require.False(t, isAbstractSocketPathFor(goos, "@kes-agent"), goos)
	}
	require.False(t, isAbstractSocketPathFor("linux", "/run/kes-agent.sock"))
}

// TestAtPrefixIsRelativeWhereGoDoesNotRewriteIt pins the other half of the rule.
// Where Go rewrites "@" the address is abstract and stays as written. Elsewhere
// it is a relative filename, so it must be resolved like any other path and
// must keep the NUL byte in the length budget.
func TestAtPrefixIsRelativeWhereGoDoesNotRewriteIt(t *testing.T) {
	t.Parallel()
	if hasAbstractSocketNamespace(runtime.GOOS) {
		t.Skip("@ is an abstract address on this platform")
	}

	resolved, err := resolveSocketPath("@kesprobe")
	require.NoError(t, err)
	want, err := filepath.Abs("@kesprobe")
	require.NoError(t, err)
	require.Equal(t, want, resolved)

	wantLimit := sunPathLenOther - 1
	if runtime.GOOS == "darwin" {
		wantLimit = sunPathLenDarwin - 1
	}
	require.Equal(t, wantLimit, socketPathLimit("@kesprobe"))
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
