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
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

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
}

// derivePorts sources compose-project.sh with SCRIPT_DIR pointed at a fake
// worktree, calls devnet_ports, and returns whichever of the 11 port vars
// ended up set (only the caller-supplied ones, if any override is given
// and devnet_ports therefore leaves the rest alone).
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
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
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
