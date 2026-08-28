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
	"os/exec"
	"path/filepath"
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
		require.Contains(t, string(data), `source "${SCRIPT_DIR}/compose-project.sh"`)
		require.Contains(t, string(data), "devnet_compose_project")
	}
	for _, file := range []string{"run-tests.sh", "start.sh"} {
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		require.Contains(t, string(data), "devnet_render_topology",
			"%s must render worktree-specific topology before bringing containers up", file)
	}
	data, err := os.ReadFile("stop.sh")
	require.NoError(t, err)
	require.Contains(t, string(data), "devnet_topology_dir",
		"stop.sh must locate this run's rendered topology directory to remove it")
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
// subnet: devnet_net_base must be stable for one worktree, distinct across
// worktrees (this is what a live `docker network create` collision test
// confirmed manually — two worktrees deriving the same base would race to
// create the same Docker network), fall inside the reserved 172.24-172.31
// range, and respect a caller override.
func TestNetBaseIsStableAndWorktreeSpecific(t *testing.T) {
	helper, err := filepath.Abs("compose-project.sh")
	require.NoError(t, err)

	worktreeA := filepath.Join(t.TempDir(), "worktree-a")
	worktreeB := filepath.Join(t.TempDir(), "worktree-b")
	for _, root := range []string{worktreeA, worktreeB} {
		require.NoError(t, os.MkdirAll(
			filepath.Join(root, "internal", "test", "devnet"), 0o755,
		))
	}

	baseA := deriveNetBase(t, helper, worktreeA, "")
	require.Equal(t, baseA, deriveNetBase(t, helper, worktreeA, ""))
	baseB := deriveNetBase(t, helper, worktreeB, "")
	require.NotEqual(t, baseA, baseB,
		"two worktrees derived the same subnet; concurrent runs would race"+
			" to create the same Docker network")
	require.Regexp(t, `^172\.(2[4-9]|3[01])\.\d{1,3}$`, baseA)
	require.Equal(t, "10.0.0", deriveNetBase(t, helper, worktreeA, "10.0.0"))
}

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
		"bash", "-c",
		`source "$1"; devnet_render_topology; printf '%s' "$DEVNET_TOPOLOGY_DIR"`,
		"bash", filepath.Join(repoDevnetDir, "compose-project.sh"),
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
		"bash", "-c",
		`source "$1"; devnet_compose_project; printf '%s' "$COMPOSE_PROJECT_NAME"`,
		"bash", helper,
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
