//go:build linux

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

package devnet

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	dockerfileUIDRe = regexp.MustCompile(`adduser\s+--system\s+--uid\s+(\d+)`)
	dockerfileGIDRe = regexp.MustCompile(`addgroup\s+--system\s+--gid\s+(\d+)`)
	composeUIDRe    = regexp.MustCompile(
		`DINGO_UID:\s*"\$\{DEVNET_DINGO_UID:-(\d+)\}"`,
	)
	composeGIDRe = regexp.MustCompile(
		`DINGO_GID:\s*"\$\{DEVNET_DINGO_GID:-(\d+)\}"`,
	)
)

// repoRootDir walks up from the package directory to the module root.
func repoRootDir(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for range 10 {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "reached the filesystem root")
		dir = parent
	}
	t.Fatal("could not locate the module root")
	return ""
}

func matchAll(t *testing.T, re *regexp.Regexp, text, what string) []string {
	t.Helper()
	matches := re.FindAllStringSubmatch(text, -1)
	require.NotEmpty(t, matches, "no %s found", what)
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, m[1])
	}
	return out
}

// TestConfiguratorUIDMatchesDockerfile keeps the DevNet configurator's
// key-ownership target in agreement with the user the Dingo image
// actually runs as.
//
// These drifted apart once, and the failure was expensive to read: the
// image moved from uid 100 to a pinned 1000 while configurator.sh still
// chowned each pool's key directory to 100:101. That directory has to be
// 0700 — cardano-node refuses to start when vrf.skey is readable by
// group or other — so every Dingo block producer in the DevNet died at
// startup with "failed to read key file .../vrf.skey: permission denied",
// and the whole network was unusable with nothing pointing at the cause.
//
// The compose file now passes the ids in, and this test derives the
// expectation from the Dockerfile rather than restating a number, so the
// next time the image's user changes this fails immediately.
func TestConfiguratorUIDMatchesDockerfile(t *testing.T) {
	root := repoRootDir(t)

	dockerfile, err := os.ReadFile(filepath.Join(root, "Dockerfile"))
	require.NoError(t, err)
	compose, err := os.ReadFile(
		filepath.Join(root, "internal", "test", "devnet", "docker-compose.yml"),
	)
	require.NoError(t, err)

	imageUIDs := matchAll(
		t, dockerfileUIDRe, string(dockerfile), "adduser --uid in Dockerfile",
	)
	imageGIDs := matchAll(
		t, dockerfileGIDRe, string(dockerfile), "addgroup --gid in Dockerfile",
	)
	require.Len(t, imageUIDs, 1, "expected exactly one dingo user")
	require.Len(t, imageGIDs, 1, "expected exactly one dingo group")

	// Both configurator services (dingo and conformance profiles) must
	// carry the ids, or the profile without them silently regresses.
	composeUIDs := matchAll(
		t, composeUIDRe, string(compose), "DINGO_UID default in compose",
	)
	composeGIDs := matchAll(
		t, composeGIDRe, string(compose), "DINGO_GID default in compose",
	)
	require.Len(t, composeUIDs, 2,
		"both configurator services must set DINGO_UID")
	require.Len(t, composeGIDs, 2,
		"both configurator services must set DINGO_GID")

	for _, got := range composeUIDs {
		require.Equal(t, imageUIDs[0], got,
			"compose DINGO_UID must match the Dockerfile's pinned dingo uid;"+
				" a mismatch makes every DevNet block producer fail to read"+
				" its VRF key")
	}
	for _, got := range composeGIDs {
		require.Equal(t, imageGIDs[0], got,
			"compose DINGO_GID must match the Dockerfile's pinned dingo gid")
	}
}

// TestConfiguratorChownsUsingPassedIds guards the other half of the
// contract: compose can pass the ids in, but the script has to use them
// rather than a hardcoded pair.
func TestConfiguratorChownsUsingPassedIds(t *testing.T) {
	root := repoRootDir(t)
	script, err := os.ReadFile(
		filepath.Join(root, "internal", "test", "devnet", "configurator.sh"),
	)
	require.NoError(t, err)

	require.Contains(t, string(script), `chown -R "${DINGO_UID}:${DINGO_GID}"`,
		"configurator.sh must chown pool keys to the ids compose passes in")
	// Scoped to the pool-key chown this contract covers, so an unrelated
	// numeric chown elsewhere in the script does not fail the guard.
	require.NotRegexp(t,
		regexp.MustCompile(`chown -R \d+:\d+ "?/configs/`),
		string(script),
		"configurator.sh must not hardcode a uid:gid for the pool keys")
}
