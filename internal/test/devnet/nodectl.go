//go:build linux && devnet

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
	"os"
	"os/exec"
	"strconv"
	"time"
)

// defaultComposeFile is where the scenarios package finds the DevNet
// compose file. run-tests.sh exports DEVNET_COMPOSE_FILE explicitly; this
// is the fallback for a hand-run `go test` from the scenarios directory.
const defaultComposeFile = "../docker-compose.yml"

// stopGracePeriod is how long a container gets to exit on SIGTERM before
// it is killed. The scenario is testing recovery, not shutdown, so this
// stays short to keep the interruption inside the k-block window.
const stopGracePeriod = 2 * time.Second

// NodeControl stops and starts DevNet containers so a scenario can
// exercise peer interruption and relay restart against the real
// topology.
type NodeControl struct {
	composeFile    string
	composeProject string
	logf           func(format string, args ...any)
}

// NewNodeControl returns a controller for the running DevNet. It fails
// when Docker Compose or the compose file are not reachable, since a
// scenario that silently skipped its disruption phases would report a
// pass it did not earn.
func NewNodeControl(
	logf func(format string, args ...any),
) (*NodeControl, error) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	composeFile := os.Getenv("DEVNET_COMPOSE_FILE")
	if composeFile == "" {
		composeFile = defaultComposeFile
	}
	composeProject := os.Getenv("DEVNET_COMPOSE_PROJECT")
	if composeProject == "" {
		composeProject = os.Getenv("COMPOSE_PROJECT_NAME")
	}
	if composeProject == "" {
		return nil, errors.New(
			"devnet: compose project unset; run the scenario via" +
				" internal/test/devnet/run-tests.sh, or set" +
				" DEVNET_COMPOSE_PROJECT",
		)
	}
	//nolint:gosec // compose path comes from the harness environment
	if _, err := os.Stat(composeFile); err != nil {
		return nil, fmt.Errorf(
			"devnet: compose file %q not readable (%w); run the scenario"+
				" via internal/test/devnet/run-tests.sh, or set"+
				" DEVNET_COMPOSE_FILE",
			composeFile, err,
		)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := runCompose(
		ctx, composeFile, composeProject, "version",
	); err != nil {
		return nil, fmt.Errorf("devnet: docker compose unusable: %w", err)
	}
	return &NodeControl{
		composeFile: composeFile, composeProject: composeProject, logf: logf,
	}, nil
}

// Stop halts one node, leaving its volumes intact so a later Start
// resumes the same node rather than bootstrapping a fresh one.
//
// This resolves the service through this run's Compose project, then drives
// `docker` directly rather than `docker compose`, because
// compose resolves depends_on: `docker compose start dingo-3` also starts
// the configurator it depends on, which regenerates genesis into the
// shared config volumes. The restarted node then refuses to start at all,
// with a genesis-hash mismatch against the database it already has —
// a network-wide reset dressed up as a single-node restart.
func (n *NodeControl) Stop(ctx context.Context, service string) error {
	container, err := n.containerID(ctx, service)
	if err != nil {
		return err
	}
	n.logf("nodectl: stopping %s (%s)", service, container)
	if _, err := runDocker(
		ctx,
		"stop", "-t", strconv.Itoa(int(stopGracePeriod.Seconds())), container,
	); err != nil {
		return fmt.Errorf("stop %s: %w", service, err)
	}
	return nil
}

// Start brings a stopped node back up. See Stop for why this bypasses
// compose.
func (n *NodeControl) Start(ctx context.Context, service string) error {
	container, err := n.containerID(ctx, service)
	if err != nil {
		return err
	}
	n.logf("nodectl: starting %s (%s)", service, container)
	if _, err := runDocker(ctx, "start", container); err != nil {
		return fmt.Errorf("start %s: %w", service, err)
	}
	return nil
}

// ContainerStatus returns `docker compose ps` output for the active
// profile.
func (n *NodeControl) ContainerStatus(ctx context.Context) (string, error) {
	out, err := runCompose(
		ctx, n.composeFile, n.composeProject, "ps", "--all",
	)
	return out, err
}

// Logs returns a service's container logs. A positive tailLines limits
// the result to that many trailing lines; see CapturedLogTailLines for
// why a capture asks for a bound rather than everything the daemon holds.
func (n *NodeControl) Logs(
	ctx context.Context,
	service string,
	tailLines int,
) (string, error) {
	args := []string{"logs", "--no-color", "--no-log-prefix"}
	if tailLines > 0 {
		args = append(args, "--tail", strconv.Itoa(tailLines))
	}
	args = append(args, service)
	return runCompose(ctx, n.composeFile, n.composeProject, args...)
}

func (n *NodeControl) containerID(
	ctx context.Context,
	service string,
) (string, error) {
	out, err := runCompose(
		ctx, n.composeFile, n.composeProject,
		"ps", "--all", "--quiet", service,
	)
	if err != nil {
		return "", fmt.Errorf("resolve %s container: %w", service, err)
	}
	container := string(bytes.TrimSpace([]byte(out)))
	if container == "" {
		return "", fmt.Errorf(
			"resolve %s container: no container found",
			service,
		)
	}
	return container, nil
}

// CaptureFailureArtifacts writes the evidence a failed scenario needs to
// be diagnosable after the network is gone: what every node's chain
// actually did, which containers were up, and each service's logs.
//
// The writing itself lives in artifacts.go without the optional devnet tag,
// so what a failure preserves is covered by an ordinary Linux test run. This
// method supplies the Docker side of it.
func (n *NodeControl) CaptureFailureArtifacts(
	ctx context.Context,
	name string,
	snapshots []ChainSnapshot,
	services []string,
) {
	dir, ok := ArtifactDir()
	if !ok {
		n.logf(
			"nodectl: DEVNET_ARTIFACT_DIR unset; skipping artifact capture",
		)
		return
	}
	WriteFailureArtifacts(
		ctx,
		n,
		FailureCapturePlan{
			Root:     dir,
			Name:     ArtifactName(name),
			Services: services,
		},
		snapshots,
		n.logf,
	)
}

// runCompose invokes docker compose against the DevNet compose file. The
// active COMPOSE_PROFILES is inherited from the environment run-tests.sh
// set up, so the command targets the topology that is actually running.
// Use it for whole-topology reads (status, logs), never to start or stop
// an individual node — see Stop.
func runCompose(
	ctx context.Context,
	composeFile string,
	composeProject string,
	args ...string,
) (string, error) {
	return runDocker(
		ctx,
		append(
			[]string{"compose", "-f", composeFile, "-p", composeProject},
			args...,
		)...,
	)
}

// runDocker runs the docker CLI and returns its stdout.
func runDocker(ctx context.Context, args ...string) (string, error) {
	//nolint:gosec // args are container/service names owned by the harness
	cmd := exec.CommandContext(ctx, "docker", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf(
			"docker %v: %w: %s", args, err, stderr.String(),
		)
	}
	return stdout.String(), nil
}
