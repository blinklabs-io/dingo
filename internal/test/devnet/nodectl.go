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

//go:build devnet

package devnet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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
	composeFile string
	logf        func(format string, args ...any)
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
		ctx, composeFile, "version",
	); err != nil {
		return nil, fmt.Errorf("devnet: docker compose unusable: %w", err)
	}
	return &NodeControl{composeFile: composeFile, logf: logf}, nil
}

// Stop halts one node, leaving its volumes intact so a later Start
// resumes the same node rather than bootstrapping a fresh one.
//
// This drives `docker` directly rather than `docker compose`, because
// compose resolves depends_on: `docker compose start dingo-3` also starts
// the configurator it depends on, which regenerates genesis into the
// shared config volumes. The restarted node then refuses to start at all,
// with a genesis-hash mismatch against the database it already has —
// a network-wide reset dressed up as a single-node restart. Every DevNet
// service pins container_name to its service name, so addressing the
// container by name is exact and touches nothing else.
func (n *NodeControl) Stop(ctx context.Context, container string) error {
	n.logf("nodectl: stopping %s", container)
	if _, err := runDocker(
		ctx,
		"stop", "-t", strconv.Itoa(int(stopGracePeriod.Seconds())), container,
	); err != nil {
		return fmt.Errorf("stop %s: %w", container, err)
	}
	return nil
}

// Start brings a stopped node back up. See Stop for why this bypasses
// compose.
func (n *NodeControl) Start(ctx context.Context, container string) error {
	n.logf("nodectl: starting %s", container)
	if _, err := runDocker(ctx, "start", container); err != nil {
		return fmt.Errorf("start %s: %w", container, err)
	}
	return nil
}

// ContainerStatus returns `docker compose ps` output for the active
// profile.
func (n *NodeControl) ContainerStatus(ctx context.Context) (string, error) {
	out, err := runCompose(ctx, n.composeFile, "ps", "--all")
	return out, err
}

// Logs returns a service's container logs.
func (n *NodeControl) Logs(
	ctx context.Context,
	service string,
) (string, error) {
	return runCompose(
		ctx, n.composeFile, "logs", "--no-color", "--no-log-prefix", service,
	)
}

// ArtifactDir returns the directory failure evidence is written to, and
// whether one is configured. run-tests.sh creates it and preserves it on
// failure.
func ArtifactDir() (string, bool) {
	dir := os.Getenv("DEVNET_ARTIFACT_DIR")
	return dir, dir != ""
}

// CaptureFailureArtifacts writes the evidence a failed scenario needs to
// be diagnosable after the network is gone: what every node's chain
// actually did, which containers were up, and each service's logs.
//
// It is best-effort by design — a capture error must not mask the test
// failure that triggered it — so problems are logged rather than
// returned.
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
	dir = filepath.Join(dir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		n.logf("nodectl: creating %s: %v", dir, err)
		return
	}

	if data, err := json.MarshalIndent(snapshots, "", "  "); err != nil {
		n.logf("nodectl: encoding chain events: %v", err)
	} else {
		n.writeArtifact(dir, "observed-chains.json", data)
	}

	if status, err := n.ContainerStatus(ctx); err != nil {
		n.logf("nodectl: container status: %v", err)
	} else {
		n.writeArtifact(dir, "container-status.txt", []byte(status))
	}

	for _, svc := range services {
		logs, err := n.Logs(ctx, svc)
		if err != nil {
			n.logf("nodectl: logs for %s: %v", svc, err)
			continue
		}
		n.writeArtifact(dir, svc+".log", []byte(logs))
	}
	n.logf("nodectl: failure artifacts written to %s", dir)
}

func (n *NodeControl) writeArtifact(dir, name string, data []byte) {
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		n.logf("nodectl: writing %s: %v", path, err)
	}
}

// runCompose invokes docker compose against the DevNet compose file. The
// active COMPOSE_PROFILES is inherited from the environment run-tests.sh
// set up, so the command targets the topology that is actually running.
// Use it for whole-topology reads (status, logs), never to start or stop
// an individual node — see Stop.
func runCompose(
	ctx context.Context,
	composeFile string,
	args ...string,
) (string, error) {
	return runDocker(
		ctx, append([]string{"compose", "-f", composeFile}, args...)...,
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
