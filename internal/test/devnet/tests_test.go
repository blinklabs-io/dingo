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
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// A pinned container_name is global on the Docker host: two worktrees
// running the same compose file would fight over the same container, and
// the second `docker compose up` would recreate (or delete) the first
// worktree's container. Every service must leave Compose to name the
// container per-project instead.
func TestComposeServicesDoNotPinContainerNames(t *testing.T) {
	data, err := os.ReadFile("docker-compose.yml")
	require.NoError(t, err)

	var compose struct {
		Services map[string]map[string]any `yaml:"services"`
	}
	require.NoError(t, yaml.Unmarshal(data, &compose))
	require.NotEmpty(t, compose.Services)
	for service, config := range compose.Services {
		require.NotContains(t, config, "container_name",
			"service %s must let Compose project-scope its container", service)
	}
}

// devnet_compose_project must derive the same project name every time for
// one worktree (so a re-run's teardown still matches its own start), a
// different name for a different worktree (so concurrent runs don't share a
// project), and must respect a caller-supplied COMPOSE_PROJECT_NAME override
// unchanged.
func TestComposeProjectIsStableAndWorktreeSpecific(t *testing.T) {
	helper, err := filepath.Abs("compose-project.sh")
	require.NoError(t, err)

	worktreeA := filepath.Join(t.TempDir(), "worktree-a")
	worktreeB := filepath.Join(t.TempDir(), "worktree-b")
	for _, root := range []string{worktreeA, worktreeB} {
		require.NoError(t, os.MkdirAll(
			filepath.Join(root, "internal", "test", "devnet"), 0o755,
		))
	}

	projectA := deriveComposeProject(t, helper, worktreeA, "")
	require.Equal(t, projectA, deriveComposeProject(t, helper, worktreeA, ""))
	projectB := deriveComposeProject(t, helper, worktreeB, "")
	require.NotEqual(t, projectA, projectB)
	require.True(t, strings.HasPrefix(projectA, "dingo-devnet-"))
	require.Equal(t, "caller-selected", deriveComposeProject(
		t, helper, worktreeA, "caller-selected",
	))
}

// The isolation only works end to end if every entry point actually wires
// the helper functions in: start.sh/run-tests.sh/stop.sh must all pick a
// compose project, start.sh/run-tests.sh must render worktree-specific
// topology before bringing containers up, and stop.sh must be able to find
// that rendered directory again to remove it.
func TestDevNetScriptsSelectComposeProject(t *testing.T) {
	for _, file := range []string{"run-tests.sh", "start.sh", "stop.sh"} {
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		require.Contains(
			t,
			string(data),
			`source "${SCRIPT_DIR}/compose-project.sh"`,
		)
		require.Contains(t, string(data), "devnet_compose_project")
	}
	for _, file := range []string{"run-tests.sh", "start.sh"} {
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		require.Contains(
			t,
			string(data),
			"devnet_render_topology",
			"%s must render worktree-specific topology before bringing containers up",
			file,
		)
		require.Contains(t, string(data), "devnet_ports",
			"%s must derive a worktree-specific host port block, or a second"+
				" worktree's `docker compose up` fails with"+
				" \"port is already allocated\"", file)
		require.Contains(t, string(data), "devnet_compose_up",
			"%s must bring containers up through devnet_compose_up, which"+
				" retries a subnet collision instead of failing the run", file)
	}
	data, err := os.ReadFile("stop.sh")
	require.NoError(t, err)
	require.Contains(
		t,
		string(data),
		"devnet_topology_dir",
		"stop.sh must locate this run's rendered topology directory to remove it",
	)
}

// A distinct Compose project name scopes containers, volumes, and the
// network's own name, but not its subnet: Docker refuses to create two
// networks with the same subnet regardless of project ("Pool overlaps with
// other one on this address space"). docker-compose.yml must therefore
// derive both the subnet and every static ipv4_address from DEVNET_NET_BASE.
func TestComposeNetworkSubnetIsParameterizedPerWorktree(t *testing.T) {
	data, err := os.ReadFile("docker-compose.yml")
	require.NoError(t, err)
	content := string(data)

	require.Contains(t, content, "${DEVNET_NET_BASE:-172.20.0}.0/24",
		"the network subnet must derive from DEVNET_NET_BASE")
	for _, octet := range []string{
		"10", "11", "12", "13", "14", "15", "16", "20", "21",
	} {
		require.Contains(t, content, "${DEVNET_NET_BASE:-172.20.0}."+octet,
			"the service pinned to .%s must derive its address from"+
				" DEVNET_NET_BASE, not a hardcoded 172.20.0.x literal", octet)
	}
}

// The topology/*.json peer lists are static, checked-in files that address
// peers by IP (net.SplitHostPort et al. in peergov do resolve hostnames,
// but these files predate that and pin literal 172.20.0.x addresses).
// Concurrent worktrees need their own rendered copy at their own
// DEVNET_NET_BASE, so the compose file must mount from
// DEVNET_TOPOLOGY_DIR rather than the checked-in ./topology directly.
func TestComposeTopologyMountsUseRenderedDirectory(t *testing.T) {
	data, err := os.ReadFile("docker-compose.yml")
	require.NoError(t, err)
	content := string(data)

	for _, file := range []string{
		"dingo-1.json", "dingo-2.json", "dingo-3.json", "dingo-relay.json",
		"dingo-producer.json", "cardano-producer.json", "relay.json",
	} {
		require.Contains(t, content,
			"${DEVNET_TOPOLOGY_DIR:-./topology}/"+file,
			"the mount for %s must come from DEVNET_TOPOLOGY_DIR", file)
	}
}

// Mirrors TestComposeProjectIsStableAndWorktreeSpecific for the network
// subnet: devnet_net_base must be stable for one worktree, fall inside the
// reserved 172.24-172.31 range, and respect a caller override.
//
// It does NOT assert that two worktrees hash to different starting
// candidates — with 2048 possible /24s, an occasional hash collision
// between two arbitrary paths is expected, not a bug: neither call here
// creates a real Docker network, so nothing actually collides yet.
// TestNetBaseAvoidsSubnetsDockerReports and TestComposeUpRetriesOnPoolOverlap
// cover what devnet_net_base and devnet_compose_up actually guarantee —
// that a subnet already in use gets skipped, and a collision surfaced by
// `docker compose up` gets retried onto a different one.
func TestNetBaseIsStableAndWorktreeSpecific(t *testing.T) {
	helper, err := filepath.Abs("compose-project.sh")
	require.NoError(t, err)

	worktreeA := filepath.Join(t.TempDir(), "worktree-a")
	require.NoError(t, os.MkdirAll(
		filepath.Join(worktreeA, "internal", "test", "devnet"), 0o755,
	))

	baseA := deriveNetBase(t, helper, worktreeA, "")
	require.Equal(t, baseA, deriveNetBase(t, helper, worktreeA, ""))
	require.Regexp(t, `^172\.(2[4-9]|3[01])\.\d{1,3}$`, baseA)
	require.Equal(t, "10.0.0", deriveNetBase(t, helper, worktreeA, "10.0.0"))
}

// _devnet_cidr_overlaps must catch every way two /24-or-wider CIDRs can
// intersect (identical, one nested inside a wider block), and correctly
// clear two blocks that plainly don't.
func TestCidrOverlapDetection(t *testing.T) {
	helper, err := filepath.Abs("compose-project.sh")
	require.NoError(t, err)

	cases := []struct {
		name        string
		a, b        string
		wantOverlap bool
	}{
		{"identical /24s", "172.20.0.0/24", "172.20.0.0/24", true},
		{"adjacent /24s", "172.20.0.0/24", "172.21.0.0/24", false},
		{
			"candidate inside a wider existing /12",
			"172.24.5.0/24",
			"172.16.0.0/12",
			true,
		},
		{"unrelated ranges", "172.24.5.0/24", "10.0.0.0/8", false},
		{
			"wider candidate containing a narrower existing block",
			"172.17.0.0/16",
			"172.17.5.0/24",
			true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cmd := exec.Command(
				"bash", "-c",
				`source "$1"; _devnet_cidr_overlaps "$2" "$3"`,
				"bash", helper, c.a, c.b,
			)
			err := cmd.Run()
			if c.wantOverlap {
				require.NoError(t, err, "%s and %s should overlap", c.a, c.b)
			} else {
				require.Error(t, err, "%s and %s should not overlap", c.a, c.b)
			}
		})
	}
}

// A hash of the worktree path is only a starting point: two different
// worktrees can hash to the same /24, and this range isn't reserved for
// DevNet, so an unrelated Docker network could already sit on it. Stub
// `docker network ls`/`inspect` to report the exact subnet a fake
// worktree's hash would otherwise pick, and confirm devnet_net_base walks
// forward to a different, still-in-range candidate instead of assuming
// the hash was free.
func TestNetBaseAvoidsSubnetsDockerReports(t *testing.T) {
	helper, err := filepath.Abs("compose-project.sh")
	require.NoError(t, err)
	worktree := filepath.Join(t.TempDir(), "worktree")
	require.NoError(t, os.MkdirAll(
		filepath.Join(worktree, "internal", "test", "devnet"), 0o755,
	))

	unblocked := deriveNetBase(t, helper, worktree, "")

	fakeBin := t.TempDir()
	writeExecutable(
		t,
		filepath.Join(fakeBin, "docker"),
		fakeDockerNetworkScript(
			unblocked+".0/24",
		),
	)

	blocked := deriveNetBaseWithPath(t, helper, worktree, fakeBin)
	require.NotEqual(t, unblocked, blocked,
		"a subnet Docker already reports as taken must not be reused")
	require.Regexp(t, `^172\.(2[4-9]|3[01])\.\d{1,3}$`, blocked)
}

// Docker networks can be IPv6 (a ULA /64, /8, etc.), and the CIDR helpers
// only understand IPv4. devnet_net_base must skip a subnet like that
// instead of aborting on it with a bash arithmetic error, and still land
// on a valid IPv4 candidate.
func TestNetBaseIgnoresIPv6Subnets(t *testing.T) {
	helper, err := filepath.Abs("compose-project.sh")
	require.NoError(t, err)
	worktree := filepath.Join(t.TempDir(), "worktree")
	require.NoError(t, os.MkdirAll(
		filepath.Join(worktree, "internal", "test", "devnet"), 0o755,
	))

	fakeBin := t.TempDir()
	writeExecutable(
		t,
		filepath.Join(fakeBin, "docker"),
		fakeDockerNetworkScript("fd00::/64"),
	)

	base := deriveNetBaseWithPath(t, helper, worktree, fakeBin)
	require.Regexp(t, `^172\.(2[4-9]|3[01])\.\d{1,3}$`, base)
}

// devnet_ports must skip a host port something is already listening on,
// shifting its whole block forward rather than handing out a port that
// would make `docker compose up` fail with "port is already allocated".
func TestPortsAvoidOccupiedPorts(t *testing.T) {
	helper, err := filepath.Abs("compose-project.sh")
	require.NoError(t, err)
	worktree := filepath.Join(t.TempDir(), "worktree")
	require.NoError(t, os.MkdirAll(
		filepath.Join(worktree, "internal", "test", "devnet"), 0o755,
	))

	unblocked := derivePorts(t, helper, worktree, nil)
	occupiedPort := unblocked["DEVNET_DINGO1_PORT"]

	listener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(occupiedPort))
	require.NoError(t, err)
	defer listener.Close()

	blocked := derivePorts(t, helper, worktree, nil)
	for name, port := range blocked {
		require.NotEqual(t, occupiedPort, port,
			"%s reused the occupied port %d instead of shifting the block",
			name, occupiedPort)
	}
	require.Equal(
		t,
		unblocked["DEVNET_DINGO1_PORT"]+len(unblocked),
		blocked["DEVNET_DINGO1_PORT"],
		"the whole block should shift forward by its own size, not just skip one port",
	)
}

// A caller who has already set even one of the port variables gets full
// manual control: devnet_ports must not touch any of the others either.
func TestPortsRespectPartialOverride(t *testing.T) {
	helper, err := filepath.Abs("compose-project.sh")
	require.NoError(t, err)
	worktree := filepath.Join(t.TempDir(), "worktree")
	require.NoError(t, os.MkdirAll(
		filepath.Join(worktree, "internal", "test", "devnet"), 0o755,
	))

	ports := derivePorts(t, helper, worktree, map[string]string{
		"DEVNET_DINGO1_PORT": "9999",
	})
	require.Equal(t, 9999, ports["DEVNET_DINGO1_PORT"])
	_, stillUnset := ports["DEVNET_DINGO2_PORT"]
	require.False(t, stillUnset,
		"a partial override must leave every other port var untouched")
}

// The window between devnet_net_base checking a subnet and `docker compose
// up` actually creating the network is a real race: two worktrees can both
// see the same subnet as free and only one wins. devnet_compose_up must
// recover from that by recomputing DEVNET_NET_BASE (which will now see the
// winner's network via docker network ls) and retrying, rather than
// failing the whole run over a race it can detect and correct.
func TestComposeUpRetriesOnPoolOverlap(t *testing.T) {
	repoDevnetDir, err := filepath.Abs(".")
	require.NoError(t, err)

	tempRoot := t.TempDir()
	fakeBin := filepath.Join(tempRoot, "bin")
	require.NoError(t, os.Mkdir(fakeBin, 0o755))
	writeExecutable(
		t,
		filepath.Join(fakeBin, "docker"),
		fakeDockerFailsOnceWithPoolOverlap,
	)

	countFile := filepath.Join(tempRoot, "up-attempts")
	blockedSubnetFile := filepath.Join(tempRoot, "blocked-subnet")
	// The fake docker doesn't know, ahead of time, which subnet the hash
	// will pick, so the script tells it: write out the first pick, then
	// have `docker network ls/inspect` report it as taken from then on,
	// modeling the concurrent worktree that won the race.
	script := `source "$1"
devnet_render_topology
before="$DEVNET_NET_BASE"
printf '%s' "$before" >"${FAKE_BLOCKED_SUBNET_FILE}"
devnet_compose_up "/fake/compose.yml"
status=$?
printf '%s %s %s\n' "$before" "$DEVNET_NET_BASE" "$status"`
	cmd := exec.Command(
		"bash",
		"-c",
		script,
		"bash",
		filepath.Join(repoDevnetDir, "compose-project.sh"),
	)
	cmd.Env = append(os.Environ(),
		"SCRIPT_DIR="+repoDevnetDir,
		"TMPDIR="+tempRoot,
		"COMPOSE_PROJECT_NAME=dingo-devnet-retry-test",
		"DEVNET_NET_BASE=",
		"FAKE_UP_COUNT_FILE="+countFile,
		"FAKE_BLOCKED_SUBNET_FILE="+blockedSubnetFile,
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	require.NoError(t, err, "stderr: %s", stderr.String())

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	last := lines[len(lines)-1]
	fields := strings.Fields(last)
	require.Len(
		t,
		fields,
		3,
		"unexpected final line: %q (full stdout: %q, stderr: %q)",
		last,
		out,
		stderr.String(),
	)
	before, after, status := fields[0], fields[1], fields[2]

	require.Equal(
		t,
		"0",
		status,
		"devnet_compose_up must succeed once the retry lands on a free subnet",
	)
	require.NotEqual(t, before, after,
		"a retry after a pool-overlap failure must pick a different subnet")

	attempts, err := os.ReadFile(countFile)
	require.NoError(t, err)
	require.Equal(
		t,
		"2",
		strings.TrimSpace(string(attempts)),
		"docker compose up should have been tried exactly twice: once to hit the collision, once to succeed",
	)
}

const fakeDockerFailsOnceWithPoolOverlap = `#!/usr/bin/env bash
case " $* " in
  *" up -d "*)
    count=0
    [[ -f "${FAKE_UP_COUNT_FILE}" ]] && count=$(cat "${FAKE_UP_COUNT_FILE}")
    count=$((count + 1))
    printf '%s' "${count}" >"${FAKE_UP_COUNT_FILE}"
    if [[ "${count}" -eq 1 ]]; then
      echo "Error response from daemon: invalid pool request: Pool overlaps with other one on this address space" >&2
      exit 1
    fi
    exit 0
    ;;
  *" network ls "*)
    printf 'busy-net\n'
    ;;
  *" network inspect "*)
    if [[ -f "${FAKE_BLOCKED_SUBNET_FILE:-}" ]]; then
      printf '%s.0/24\n' "$(cat "${FAKE_BLOCKED_SUBNET_FILE}")"
    fi
    ;;
  *) exit 0 ;;
esac
`

// A host port can slip through devnet_ports' check-then-bind window the
// same way a subnet can slip through devnet_net_base's: two concurrent
// runs can both see a port as free and only one wins. devnet_compose_up
// must react to a port-bind failure by unsetting every _DEVNET_PORT_VARS
// entry and calling devnet_ports again — but only when devnet_ports
// allocated the ports in the first place (DEVNET_PORTS_AUTO=1); a caller's
// explicit port override must never be retried away.
//
// This shadows devnet_ports with a stub rather than relying on a real,
// timing-sensitive socket race (which is already covered, independently,
// by TestPortsAvoidOccupiedPorts): the stub proves devnet_compose_up's own
// retry wiring — that it unsets the vars first (the real devnet_ports
// would otherwise see them still set and silently no-op) and calls
// devnet_ports again exactly once per port-conflict failure.
func TestComposeUpRetriesOnPortConflict(t *testing.T) {
	repoDevnetDir, err := filepath.Abs(".")
	require.NoError(t, err)

	t.Run("auto-derived ports are retried", func(t *testing.T) {
		tempRoot := t.TempDir()
		fakeBin := filepath.Join(tempRoot, "bin")
		require.NoError(t, os.Mkdir(fakeBin, 0o755))
		upCountFile := filepath.Join(tempRoot, "up-attempts")
		portsCountFile := filepath.Join(tempRoot, "ports-calls")
		wasUnsetFile := filepath.Join(tempRoot, "was-unset")
		writeExecutable(
			t,
			filepath.Join(fakeBin, "docker"),
			fakeDockerFailsOnceWithPortConflict,
		)

		script := `source "$1"
devnet_ports() {
  local count=0
  [[ -f "${FAKE_PORTS_COUNT_FILE}" ]] && count=$(cat "${FAKE_PORTS_COUNT_FILE}")
  count=$((count + 1))
  printf '%s' "${count}" >"${FAKE_PORTS_COUNT_FILE}"
  if [[ -n "${DEVNET_DINGO1_PORT:-}" ]]; then
    printf 'not-unset' >"${FAKE_WAS_UNSET_FILE}"
  else
    printf 'unset' >"${FAKE_WAS_UNSET_FILE}"
  fi
  DEVNET_PORTS_AUTO=1
  export DEVNET_DINGO1_PORT=$((30000 + count))
}
devnet_ports
before="$DEVNET_DINGO1_PORT"
devnet_compose_up "/fake/compose.yml"
status=$?
printf '%s %s %s\n' "$before" "$DEVNET_DINGO1_PORT" "$status"`
		cmd := exec.Command(
			"bash",
			"-c",
			script,
			"bash",
			filepath.Join(repoDevnetDir, "compose-project.sh"),
		)
		cmd.Env = append(os.Environ(),
			"SCRIPT_DIR="+repoDevnetDir,
			"COMPOSE_PROJECT_NAME=dingo-devnet-port-retry-test",
			"DEVNET_NET_BASE=172.30.99",
			"FAKE_UP_COUNT_FILE="+upCountFile,
			"FAKE_PORTS_COUNT_FILE="+portsCountFile,
			"FAKE_WAS_UNSET_FILE="+wasUnsetFile,
			"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		require.NoError(t, err, "stderr: %s", stderr.String())

		fields := strings.Fields(strings.TrimSpace(string(out)))
		require.Len(
			t,
			fields,
			3,
			"unexpected output: %q (stderr: %q)",
			out,
			stderr.String(),
		)
		before, after, status := fields[0], fields[1], fields[2]

		require.Equal(
			t,
			"0",
			status,
			"devnet_compose_up must succeed once the retry calls devnet_ports again",
		)
		require.NotEqual(t, before, after,
			"a retry after a port-bind failure must call devnet_ports again")

		portsCalls, err := os.ReadFile(portsCountFile)
		require.NoError(t, err)
		require.Equal(
			t,
			"2",
			strings.TrimSpace(string(portsCalls)),
			"devnet_ports should have been called exactly twice: once to derive, once to retry",
		)

		wasUnset, err := os.ReadFile(wasUnsetFile)
		require.NoError(t, err)
		require.Equal(
			t,
			"unset",
			string(wasUnset),
			"devnet_compose_up must unset the port vars before retrying, or the"+
				" real devnet_ports would see them still set and silently no-op",
		)
	})

	t.Run("a caller's port override is never retried away", func(t *testing.T) {
		tempRoot := t.TempDir()
		fakeBin := filepath.Join(tempRoot, "bin")
		require.NoError(t, os.Mkdir(fakeBin, 0o755))
		upCountFile := filepath.Join(tempRoot, "up-attempts")
		writeExecutable(
			t,
			filepath.Join(fakeBin, "docker"),
			fakeDockerFailsOnceWithPortConflict,
		)

		script := `source "$1"
devnet_ports
devnet_compose_up "/fake/compose.yml"
printf '%s\n' "$?"`
		cmd := exec.Command(
			"bash",
			"-c",
			script,
			"bash",
			filepath.Join(repoDevnetDir, "compose-project.sh"),
		)
		cmd.Env = append(os.Environ(),
			"SCRIPT_DIR="+repoDevnetDir,
			"COMPOSE_PROJECT_NAME=dingo-devnet-port-retry-test-override",
			"DEVNET_NET_BASE=172.30.99",
			"FAKE_UP_COUNT_FILE="+upCountFile,
			"DEVNET_DINGO1_PORT=9999",
			"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		require.NoError(t, err, "stderr: %s", stderr.String())
		require.Equal(
			t,
			"1",
			strings.TrimSpace(string(out)),
			"devnet_compose_up must not retry away a caller-supplied port override",
		)
	})
}

const fakeDockerFailsOnceWithPortConflict = `#!/usr/bin/env bash
case " $* " in
  *" up -d "*)
    count=0
    [[ -f "${FAKE_UP_COUNT_FILE}" ]] && count=$(cat "${FAKE_UP_COUNT_FILE}")
    count=$((count + 1))
    printf '%s' "${count}" >"${FAKE_UP_COUNT_FILE}"
    if [[ "${count}" -eq 1 ]]; then
      echo "Error response from daemon: driver failed programming external" \
        "connectivity: Bind for 0.0.0.0:${DEVNET_DINGO1_PORT}:" \
        "port is already allocated" >&2
      exit 1
    fi
    exit 0
    ;;
  *" network ls "*) printf 'x\n' ;;
  *" network inspect "*) printf '' ;;
  *) exit 0 ;;
esac
`

// devnet_render_topology must rewrite every checked-in topology/*.json file
// into DEVNET_TOPOLOGY_DIR with this run's DEVNET_NET_BASE substituted for
// the hardcoded 172.20.0.x addresses, and must never modify the checked-in
// source files themselves (they're shared by every worktree, including ones
// running concurrently).
func TestRenderTopologyRewritesAddressesWithoutMutatingSource(t *testing.T) {
	repoDevnetDir, err := filepath.Abs(".")
	require.NoError(t, err)
	sourcePath := filepath.Join(repoDevnetDir, "topology", "dingo-1.json")
	before, err := os.ReadFile(sourcePath)
	require.NoError(t, err)
	require.Contains(t, string(before), "172.20.0.")

	tempRoot := t.TempDir()
	cmd := exec.Command(
		"bash",
		"-c",
		`source "$1"; devnet_render_topology; printf '%s' "$DEVNET_TOPOLOGY_DIR"`,
		"bash",
		filepath.Join(repoDevnetDir, "compose-project.sh"),
	)
	cmd.Env = append(os.Environ(),
		"SCRIPT_DIR="+repoDevnetDir,
		"TMPDIR="+tempRoot,
		"COMPOSE_PROJECT_NAME=dingo-devnet-isolation-test",
		"DEVNET_NET_BASE=",
	)
	out, err := cmd.Output()
	require.NoError(t, err)
	renderedDir := string(out)
	require.NotEmpty(t, renderedDir)

	rendered, err := os.ReadFile(filepath.Join(renderedDir, "dingo-1.json"))
	require.NoError(t, err)
	require.NotContains(t, string(rendered), "172.20.0.",
		"rendered topology must use this run's DEVNET_NET_BASE, not the"+
			" checked-in address")
	require.Regexp(t, `172\.(2[4-9]|3[01])\.\d{1,3}\.14`, string(rendered))

	after, err := os.ReadFile(sourcePath)
	require.NoError(t, err)
	require.Equal(t, before, after,
		"rendering must not mutate the checked-in topology file")

	entries, err := os.ReadDir(renderedDir)
	require.NoError(t, err)
	require.Len(t, entries, 7,
		"every checked-in topology file must be rendered")
}

// deriveNetBase sources compose-project.sh with SCRIPT_DIR pointed at a
// fake worktree and returns the DEVNET_NET_BASE it computes (or the
// override, if one is supplied), without touching Docker.
func deriveNetBase(
	t *testing.T,
	helper string,
	worktree string,
	override string,
) string {
	t.Helper()
	scriptDir := filepath.Join(worktree, "internal", "test", "devnet")
	cmd := exec.Command(
		"bash", "-c",
		`source "$1"; devnet_net_base; printf '%s' "$DEVNET_NET_BASE"`,
		"bash", helper,
	)
	cmd.Env = append(os.Environ(), "SCRIPT_DIR="+scriptDir)
	if override == "" {
		cmd.Env = append(cmd.Env, "DEVNET_NET_BASE=")
	} else {
		cmd.Env = append(cmd.Env, "DEVNET_NET_BASE="+override)
	}
	out, err := cmd.Output()
	require.NoError(t, err)
	return string(out)
}

// deriveNetBaseWithPath is deriveNetBase with an extra directory prepended
// to PATH, so a stubbed `docker` (see fakeDockerNetworkScript) is used
// instead of the real one.
func deriveNetBaseWithPath(
	t *testing.T,
	helper string,
	worktree string,
	extraPathDir string,
) string {
	t.Helper()
	scriptDir := filepath.Join(worktree, "internal", "test", "devnet")
	cmd := exec.Command(
		"bash", "-c",
		`source "$1"; devnet_net_base; printf '%s' "$DEVNET_NET_BASE"`,
		"bash", helper,
	)
	cmd.Env = append(os.Environ(),
		"SCRIPT_DIR="+scriptDir,
		"DEVNET_NET_BASE=",
		"PATH="+extraPathDir+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	out, err := cmd.Output()
	require.NoError(t, err)
	return string(out)
}

// fakeDockerNetworkScript is a stand-in `docker` that reports a single
// existing network with the given subnet — enough for
// _devnet_used_subnets, which only calls `docker network ls` and
// `docker network inspect`.
func fakeDockerNetworkScript(subnet string) string {
	return "#!/usr/bin/env bash\n" +
		"case \"$1 $2\" in\n" +
		"  \"network ls\") printf 'busy-net\\n' ;;\n" +
		"  \"network inspect\") printf '" + subnet + "\\n' ;;\n" +
		"esac\n"
}

var devnetPortVarNames = []string{
	"DEVNET_DINGO1_PORT", "DEVNET_DINGO2_PORT", "DEVNET_DINGO3_PORT",
	"DEVNET_DINGO_RELAY_PORT", "DEVNET_DINGO1_NTC_PORT",
	"DEVNET_DINGO2_NTC_PORT", "DEVNET_DINGO3_NTC_PORT",
	"DEVNET_DINGO_RELAY_NTC_PORT", "DEVNET_DINGO_PORT",
	"DEVNET_CARDANO_PORT", "DEVNET_RELAY_PORT",
	"DEVNET_DINGO_NTC_PORT", "DEVNET_CARDANO_NTC_PORT",
}

// derivePorts sources compose-project.sh with SCRIPT_DIR pointed at a fake
// worktree, calls devnet_ports, and returns whichever of the 13 port vars
// ended up set (only the caller-supplied ones, if any override is given
// and devnet_ports therefore leaves the rest alone).
//
// devnetPortVarNames must stay in sync with compose-project.sh's own
// _DEVNET_PORT_VARS: derivePorts clears every name in this list from the
// subprocess's inherited environment before calling devnet_ports, and
// devnet_ports bails out early (leaving its vars alone) if any
// _DEVNET_PORT_VARS entry is already set. A name present in
// _DEVNET_PORT_VARS but missing here would leak through from whatever the
// test process's own environment happened to have (run-tests.sh/start.sh
// export these), making derivePorts's results depend on the outside
// environment instead of purely on devnet_ports' own logic.
func derivePorts(
	t *testing.T,
	helper string,
	worktree string,
	overrides map[string]string,
) map[string]int {
	t.Helper()
	scriptDir := filepath.Join(worktree, "internal", "test", "devnet")
	script := `source "$1"; devnet_ports
for v in "${_DEVNET_PORT_VARS[@]}"; do
  if [[ -n "${!v:-}" ]]; then printf '%s=%s\n' "$v" "${!v}"; fi
done`
	cmd := exec.Command("bash", "-c", script, "bash", helper)
	env := append(os.Environ(), "SCRIPT_DIR="+scriptDir)
	for _, name := range devnetPortVarNames {
		env = append(env, name+"=")
	}
	for k, v := range overrides {
		env = append(env, k+"="+v)
	}
	cmd.Env = env
	out, err := cmd.Output()
	require.NoError(t, err)

	result := map[string]int{}
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		require.True(t, ok, "malformed output line %q", line)
		port, err := strconv.Atoi(value)
		require.NoError(t, err)
		result[name] = port
	}
	return result
}

// deriveComposeProject sources compose-project.sh with SCRIPT_DIR pointed
// at a fake worktree and returns the COMPOSE_PROJECT_NAME it computes (or
// the override, if one is supplied), without touching Docker.
func deriveComposeProject(
	t *testing.T,
	helper string,
	worktree string,
	override string,
) string {
	t.Helper()
	scriptDir := filepath.Join(worktree, "internal", "test", "devnet")
	cmd := exec.Command(
		"bash",
		"-c",
		`source "$1"; devnet_compose_project; printf '%s' "$COMPOSE_PROJECT_NAME"`,
		"bash",
		helper,
	)
	cmd.Env = append(os.Environ(), "SCRIPT_DIR="+scriptDir)
	if override == "" {
		cmd.Env = append(cmd.Env, "COMPOSE_PROJECT_NAME=")
	} else {
		cmd.Env = append(cmd.Env, "COMPOSE_PROJECT_NAME="+override)
	}
	out, err := cmd.Output()
	require.NoError(t, err)
	return string(out)
}

const fakeDockerScript = `#!/usr/bin/env bash
set -euo pipefail

printf '%q ' "$@" >>"${FAKE_DOCKER_LOG}"
printf '\n' >>"${FAKE_DOCKER_LOG}"

case "${1:-}" in
  inspect)
    printf 'healthy\n'
    ;;
  volume)
    # The stake-key volume exists. An empty response to "volume ls" also
    # prevents the failure-artifact path from trying to copy a config volume.
    ;;
  compose)
    printf 'TXPUMP_WINDOW=%s\n' "${DEVNET_TXPUMP_CONFIRMATION_SLOTS:-600}" >>"${FAKE_DOCKER_LOG}"
    printf 'DINGO_SPEC=%s\nLEIOS_ENABLED=%s\nLEIOS_KEY_FILE=%s\n' \
      "${DEVNET_DINGO_SPEC:-}" "${DEVNET_LEIOS_ENABLED:-}" \
      "${DEVNET_LEIOS_VOTE_SIGNING_KEY_FILE:-}" >>"${FAKE_DOCKER_LOG}"
    printf 'DINGO_RUN_MODE=%s\n' "${DEVNET_DINGO_RUN_MODE:-}" >>"${FAKE_DOCKER_LOG}"
    printf 'DINGO_START_ERA=%s\n' "${DEVNET_DINGO_START_ERA:-}" >>"${FAKE_DOCKER_LOG}"
    printf 'TXPUMP_TRANSACTION_ERA=%s\n' "${DEVNET_TXPUMP_TRANSACTION_ERA:-}" >>"${FAKE_DOCKER_LOG}"
    case " $* " in
      *" ps --status running --quiet "*) printf 'fake-container\n' ;;
      *" exec -T "*) printf '1 0 0\n' ;;
    esac
    ;;
  run)
    shift
    host_user=false
    host_uid=''
    host_gid=''
    output_dir=''
    while (( $# > 0 )); do
      case "$1" in
        --user)
          if [[ "$2" != '0:0' ]]; then
            host_user=true
          fi
          shift 2
          ;;
        -e)
          case "$2" in
            HOST_UID=*) host_uid="${2#HOST_UID=}" ;;
            HOST_GID=*) host_gid="${2#HOST_GID=}" ;;
          esac
          shift 2
          ;;
        -v)
          mount="$2"
          if [[ "${mount}" == *:/out ]]; then
            output_dir="${mount%:/out}"
          fi
          shift 2
          ;;
        *) shift ;;
      esac
    done
    if [[ -n "${host_uid}" && -n "${host_gid}" ]]; then
      # Model the runner's chown after the root-only source copy.
      host_user=true
    fi
    if [[ -n "${output_dir}" ]]; then
      mkdir -p "${output_dir}/stake"
      printf 'fake stake key\n' >"${output_dir}/stake/genesis.skey"
      if [[ "${host_user}" != "true" ]]; then
        # Model a root-created container directory that the host user can read
        # but cannot remove recursively.
        chmod 0555 "${output_dir}/stake"
      fi
    fi
    ;;
esac
`

const fakeGoScript = `#!/usr/bin/env bash
printf 'GO_ARGS=' >>"${FAKE_DOCKER_LOG}"
printf '%q ' "$@" >>"${FAKE_DOCKER_LOG}"
printf '\n' >>"${FAKE_DOCKER_LOG}"
if [[ " $* " == *' -run ^$ '* ]]; then
  printf 'compile-tests\n' >>"${FAKE_DOCKER_LOG}"
  exit "${FAKE_GO_COMPILE_EXIT:-0}"
fi
printf 'run-tests\n' >>"${FAKE_DOCKER_LOG}"
exit "${FAKE_GO_EXIT}"
`

const failingRmScript = `#!/usr/bin/env bash
exit 42
`

type fakeDevnetResult struct {
	exitCode     int
	output       string
	dockerLog    string
	stakeDirs    []string
	artifactDirs []string
}

func TestRunTestsCompilesBeforeGenesis(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{nil, {"--conformance", "--accelerated"}} {
		result := runFakeDevnet(t, 0, false, args...)
		require.Zero(t, result.exitCode, result.output)
		compile := strings.Index(result.dockerLog, "compile-tests\n")
		start := strings.Index(result.dockerLog, " up -d")
		run := strings.Index(result.dockerLog, "run-tests\n")
		require.NotEqual(t, -1, compile, "tests must compile before genesis")
		require.NotEqual(t, -1, start, "network did not start")
		require.NotEqual(t, -1, run, "tests did not execute")
		require.Less(t, compile, start, "cold compilation consumed chain time")
		require.Less(t, start, run, "tests must execute against running nodes")
	}
}

func TestRunTestsLeiosSelectsItsFullDijkstraScenario(t *testing.T) {
	result := runFakeDevnetWithEnv(t, 0, false, map[string]string{
		"DEVNET_LEIOS_ENABLED":               "0",
		"DEVNET_LEIOS_VOTE_SIGNING_KEY_FILE": "/stale/key",
		"DEVNET_DINGO_SPEC":                  "./testnet-dingo.yaml",
		"DEVNET_DINGO_RUN_MODE":              "serve",
		"DEVNET_DINGO_START_ERA":             "",
		"DEVNET_TXPUMP_TRANSACTION_ERA":      "conway",
	}, "--leios")
	require.Zero(t, result.exitCode, result.output)
	require.Contains(t, result.dockerLog,
		"DINGO_SPEC=./testnet-dingo-leios.yaml\n")
	require.Contains(t, result.dockerLog, "LEIOS_ENABLED=1\n")
	require.Contains(t, result.dockerLog,
		"LEIOS_KEY_FILE=/configs/keys/leios-vote.skey\n")
	require.Contains(t, result.dockerLog, "DINGO_RUN_MODE=leios\n")
	require.Contains(t, result.dockerLog, "DINGO_START_ERA=dijkstra\n")
	require.Contains(t, result.dockerLog, "TXPUMP_TRANSACTION_ERA=dijkstra\n")
	require.Contains(t, result.dockerLog, "TXPUMP_WINDOW=1000\n")
	require.Contains(t, result.dockerLog, "GO_ARGS=test -tags devnet")
	require.Contains(t, result.dockerLog, "-timeout 12m")
	require.Contains(t, result.dockerLog,
		"TestLeiosEndorserBlockProducerToPeer")
	require.Contains(t, result.dockerLog, "./internal/test/devnet/...")
}

func TestRunTestsDefaultClearsDijkstraOverrides(t *testing.T) {
	result := runFakeDevnetWithEnv(t, 0, false, map[string]string{
		"DEVNET_DINGO_RUN_MODE":         "leios",
		"DEVNET_DINGO_START_ERA":        "dijkstra",
		"DEVNET_TXPUMP_TRANSACTION_ERA": "dijkstra",
	}, "--keep-up")
	require.Zero(t, result.exitCode, result.output)
	require.Contains(t, result.dockerLog, "DINGO_RUN_MODE=\n")
	require.Contains(t, result.dockerLog, "DINGO_START_ERA=\n")
	require.Contains(t, result.dockerLog, "TXPUMP_TRANSACTION_ERA=\n")
}

func TestRunTestsLeiosRejectsIncompatibleModes(t *testing.T) {
	for _, args := range [][]string{
		{"--leios", "--conformance"},
		{"--leios", "--accelerated"},
		{"--leios", "-run", "TestAnything"},
	} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			result := runFakeDevnet(t, 0, false, args...)
			require.NotZero(t, result.exitCode, result.output)
			require.NotContains(t, result.dockerLog, " up -d")
		})
	}
}

func TestRunTestsCompileFailureDoesNotStartGenesis(t *testing.T) {
	t.Parallel()
	result := runFakeDevnetWithEnv(t, 0, false, map[string]string{
		"FAKE_GO_COMPILE_EXIT": "29",
	})
	require.Equal(t, 29, result.exitCode, result.output)
	require.NotContains(t, result.dockerLog, " up -d")
	require.NotContains(t, result.dockerLog, "run-tests\n")
}

func TestRunTestsPreservesTestStatusWhenCleanupFails(t *testing.T) {
	for _, test := range []struct {
		name     string
		testExit int
	}{
		{name: "success", testExit: 0},
		{name: "failure", testExit: 23},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := runFakeDevnet(t, test.testExit, true)
			assert.Equal(t, test.testExit, result.exitCode, result.output)
		})
	}
}

func TestRunTestsKeepUpPreservesSuccess(t *testing.T) {
	result := runFakeDevnet(t, 0, true, "--keep-up")
	assert.Equal(t, 0, result.exitCode, result.output)
	assert.NotContains(t, result.dockerLog, " down -v",
		"--keep-up should not tear down a passing network")
}

func TestRunTestsCleansContainerCreatedTemporaryFiles(t *testing.T) {
	wantUserMapping := bashUserMapping(t)
	wantHostUID, wantHostGID, ok := strings.Cut(wantUserMapping, ":")
	require.True(t, ok)
	wantCopy := "run --rm --user 0:0 -e HOST_UID=" + wantHostUID +
		" -e HOST_GID=" + wantHostGID

	for _, test := range []struct {
		name              string
		testExit          int
		wantArtifactCount int
	}{
		{name: "success", testExit: 0, wantArtifactCount: 0},
		{name: "failure", testExit: 23, wantArtifactCount: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := runFakeDevnet(t, test.testExit, false)
			assert.Equal(t, test.testExit, result.exitCode, result.output)
			assert.Contains(t, result.dockerLog, wantCopy,
				"key copy must restore temporary files to the host uid:gid")
			assert.Contains(t, result.dockerLog, "chown -R",
				"container-created files must be returned to the host user")
			assert.Contains(t, result.dockerLog, "chmod 0600",
				"copied signing keys must remain private")
			assert.Empty(t, result.stakeDirs,
				"runner left its stake-key temp tree behind\n%s", result.output)
			assert.Len(
				t,
				result.artifactDirs,
				test.wantArtifactCount,
				"runner did not apply its artifact retention policy\n%s",
				result.output,
			)
		})
	}
}

// TestRunTestsPreservesCallerOwnedStakeDirectory verifies cleanup ownership:
// the runner may inherit STAKE_KEYS_HOST_DIR from its caller, but it must only
// remove a stake-key directory that this invocation created with mktemp.
func TestRunTestsPreservesCallerOwnedStakeDirectory(t *testing.T) {
	callerDir := t.TempDir()
	marker := filepath.Join(callerDir, "keep")
	require.NoError(t, os.WriteFile(marker, []byte("caller-owned"), 0o600))

	result := runFakeDevnetWithEnv(t, 0, false, map[string]string{
		"MODE":                "conformance",
		"STAKE_KEYS_HOST_DIR": callerDir,
	})
	require.Equal(t, 0, result.exitCode, result.output)
	contents, err := os.ReadFile(marker)
	require.NoError(
		t,
		err,
		"runner removed a stake directory it did not create",
	)
	assert.Equal(t, "caller-owned", string(contents))
}

func runFakeDevnet(
	t *testing.T,
	testExit int,
	failRm bool,
	runnerArgs ...string,
) fakeDevnetResult {
	t.Helper()
	return runFakeDevnetWithEnv(t, testExit, failRm, nil, runnerArgs...)
}

// runFakeDevnetWithEnv runs the shell harness against fake Docker and Go
// binaries while allowing a test to model inherited runner state. Keeping the
// overrides inside cleanRunnerEnv prevents the developer's real environment
// from accidentally deciding which directory the cleanup trap removes.
func runFakeDevnetWithEnv(
	t *testing.T,
	testExit int,
	failRm bool,
	envOverrides map[string]string,
	runnerArgs ...string,
) fakeDevnetResult {
	t.Helper()
	return runFakeDevnetScript(
		t,
		"run-tests.sh",
		testExit,
		failRm,
		envOverrides,
		runnerArgs...)
}

func runFakeDevnetScript(
	t *testing.T,
	script string,
	testExit int,
	failRm bool,
	envOverrides map[string]string,
	runnerArgs ...string,
) fakeDevnetResult {
	t.Helper()

	root := repoRootDir(t)
	tempRoot := t.TempDir()
	// A fail-before run intentionally leaves a read-only directory behind.
	// Restore owner permissions before testing.TempDir performs final cleanup.
	t.Cleanup(func() {
		_ = filepath.Walk(
			tempRoot,
			func(path string, info os.FileInfo, err error) error {
				if err == nil && info.IsDir() {
					_ = os.Chmod(path, 0o700)
				}
				return nil
			},
		)
	})

	fakeBin := filepath.Join(tempRoot, "bin")
	require.NoError(t, os.Mkdir(fakeBin, 0o700))
	writeExecutable(t, filepath.Join(fakeBin, "docker"), fakeDockerScript)
	writeExecutable(t, filepath.Join(fakeBin, "go"), fakeGoScript)
	if failRm {
		writeExecutable(t, filepath.Join(fakeBin, "rm"), failingRmScript)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	args := []string{
		filepath.Join(root, "internal", "test", "devnet", script),
	}
	args = append(args, runnerArgs...)
	cmd := exec.CommandContext(ctx, "bash", args...)
	cmd.Dir = root
	env := map[string]string{
		"FAKE_DOCKER_LOG":      filepath.Join(tempRoot, "docker.log"),
		"FAKE_GO_EXIT":         strconv.Itoa(testExit),
		"FAKE_GO_COMPILE_EXIT": "0",
		"MODE":                 "dingo",
		"PATH": fakeBin + string(
			os.PathListSeparator,
		) + os.Getenv(
			"PATH",
		),
		"TMPDIR": tempRoot,
	}
	maps.Copy(env, envOverrides)
	cmd.Env = cleanRunnerEnv(env)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	err := cmd.Run()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("run-tests.sh did not finish:\n%s", output.String())
	}

	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		require.ErrorAs(t, err, &exitErr, output.String())
		exitCode = exitErr.ExitCode()
	}
	stakeDirs, err := filepath.Glob(
		filepath.Join(tempRoot, "dingo-devnet-stake-keys.*"),
	)
	require.NoError(t, err)
	artifactDirs, err := filepath.Glob(
		filepath.Join(tempRoot, "dingo-devnet-artifacts.*"),
	)
	require.NoError(t, err)
	dockerLog, err := os.ReadFile(filepath.Join(tempRoot, "docker.log"))
	require.NoError(t, err)
	return fakeDevnetResult{
		exitCode:     exitCode,
		output:       output.String(),
		dockerLog:    string(dockerLog),
		stakeDirs:    stakeDirs,
		artifactDirs: artifactDirs,
	}
}

func bashUserMapping(t *testing.T) string {
	t.Helper()
	cmd := exec.Command("bash", "-c", `printf '%s:%s' "$(id -u)" "$(id -g)"`)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	return string(output)
}

func cleanRunnerEnv(overrides map[string]string) []string {
	blocked := map[string]struct{}{
		"COMPOSE_PROFILES":                   {},
		"DEVNET_ACCELERATED":                 {},
		"DEVNET_DINGO_SPEC":                  {},
		"DEVNET_LEIOS_ENABLED":               {},
		"DEVNET_LEIOS_VOTE_SIGNING_KEY_FILE": {},
		"DEVNET_DINGO_RUN_MODE":              {},
		"DEVNET_DINGO_START_ERA":             {},
		"DEVNET_TXPUMP_TRANSACTION_ERA":      {},
		"DEVNET_ARTIFACT_DIR":                {},
		"DEVNET_CIP50_TEST":                  {},
		"DEVNET_TESTNET_YAML":                {},
		"FAKE_DOCKER_LOG":                    {},
		"FAKE_GO_EXIT":                       {},
		"FAKE_GO_COMPILE_EXIT":               {},
		"MODE":                               {},
		"PATH":                               {},
		"STAKE_KEYS_HOST_DIR":                {},
		"TMPDIR":                             {},
		"DEVNET_RUNTIME":                     {},
	}
	env := make([]string, 0, len(os.Environ())+len(overrides))
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		if _, found := blocked[key]; !found {
			env = append(env, item)
		}
	}
	for key, value := range overrides {
		env = append(env, fmt.Sprintf("%s=%s", key, value))
	}
	return env
}

func writeExecutable(t *testing.T, path, contents string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o700))
}

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
