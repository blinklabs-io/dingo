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
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
)

// This file carries no build tag on purpose. What a failed scenario
// preserves, and where, is decided here so it is covered by an ordinary
// `go test ./...` run; only the Docker calls that supply the evidence
// need the devnet tag and a live network.

// ArtifactDir returns the directory failure evidence is written to, and
// whether one is configured. run-tests.sh creates it and preserves it on
// failure.
func ArtifactDir() (string, bool) {
	dir := os.Getenv("DEVNET_ARTIFACT_DIR")
	return dir, dir != ""
}

// CapturedLogTailLines bounds the per-service logs a failed scenario
// copies out. A DevNet node logs at debug level and emits tens of
// megabytes a minute, so an unbounded copy per failed scenario is a
// multi-gigabyte artifact directory on a long canonical run.
// run-tests.sh separately preserves the complete compose log for the
// whole run under network/, so this copy only has to carry the window
// around the failure.
const CapturedLogTailLines = 2000

// ArtifactSource supplies the Docker-side evidence a capture writes out.
// NodeControl implements it.
type ArtifactSource interface {
	ContainerStatus(ctx context.Context) (string, error)
	// Logs returns a service's container logs, limited to the last
	// tailLines lines when tailLines is positive.
	Logs(
		ctx context.Context,
		service string,
		tailLines int,
	) (string, error)
}

// FailureCapturePlan is what a scenario preserves when it fails: the
// directory to write under, the name that separates one scenario's
// evidence from another's, and the compose services whose logs to keep.
type FailureCapturePlan struct {
	Root     string
	Name     string
	Services []string
}

// PlanFailureCapture decides whether a scenario can preserve evidence and
// what it would preserve. Capture needs somewhere to write and at least
// one container to read, so an unset DEVNET_ARTIFACT_DIR or a topology of
// endpoints that name no container disables it rather than producing an
// empty directory or asking Docker for services that do not exist.
func PlanFailureCapture(
	root string,
	testName string,
	endpoints []NodeEndpoint,
) (FailureCapturePlan, bool) {
	if root == "" {
		return FailureCapturePlan{}, false
	}
	services := make([]string, 0, len(endpoints))
	seen := make(map[string]struct{}, len(endpoints))
	for _, ep := range endpoints {
		if ep.Container == "" {
			continue
		}
		if _, dup := seen[ep.Container]; dup {
			continue
		}
		seen[ep.Container] = struct{}{}
		services = append(services, ep.Container)
	}
	if len(services) == 0 {
		return FailureCapturePlan{}, false
	}
	return FailureCapturePlan{
		Root:     root,
		Name:     ArtifactName(testName),
		Services: services,
	}, true
}

// ArtifactName reduces a Go test name to one path segment. t.Name()
// renders a subtest as parent/child, which would otherwise scatter a
// scenario's evidence across nested directories, and a name carrying ..
// would write outside the artifact root.
//
// That flattening is lossy — TestX/a-b and TestX/a/b both reduce to
// TestX-a-b — so a rewritten name carries a digest of what it came from
// and two failures cannot land in one directory. A name that needed no
// rewriting keeps its exact test name, which is every scenario in the
// canonical suite and is what makes the directory findable from the
// failure output.
func ArtifactName(testName string) string {
	name := strings.Map(func(r rune) rune {
		switch r {
		case '/', '\\':
			return '-'
		}
		return r
	}, testName)
	name = strings.ReplaceAll(name, "..", "-")
	name = strings.Trim(name, ". ")
	if name == "" {
		return "scenario"
	}
	if name == testName {
		return name
	}
	digest := fnv.New32a()
	_, _ = digest.Write([]byte(testName))
	return name + "-" + fmt.Sprintf("%08x", digest.Sum32())
}

// WriteFailureArtifacts writes the evidence a failed scenario needs to be
// diagnosable after the network is gone: what every node's chain actually
// did, which containers were up, and each service's logs.
//
// It is best-effort by design — a capture error must not mask the test
// failure that triggered it, and one unreadable service must not cost the
// rest of the evidence — so problems are logged rather than returned. A
// nil src means the Docker side is unavailable; the observed chains are
// recorded in-process and are still written.
func WriteFailureArtifacts(
	ctx context.Context,
	src ArtifactSource,
	plan FailureCapturePlan,
	snapshots []ChainSnapshot,
	logf func(format string, args ...any),
) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if plan.Root == "" {
		logf("devnet: no artifact directory; skipping artifact capture")
		return
	}
	dir := filepath.Join(plan.Root, plan.Name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		logf("devnet: creating %s: %v", dir, err)
		return
	}

	if data, err := json.MarshalIndent(snapshots, "", "  "); err != nil {
		logf("devnet: encoding chain events: %v", err)
	} else {
		writeArtifact(dir, "observed-chains.json", data, logf)
	}

	if src == nil {
		logf(
			"devnet: no container source; preserved observed chains only",
		)
		return
	}

	if status, err := src.ContainerStatus(ctx); err != nil {
		logf("devnet: container status: %v", err)
	} else {
		writeArtifact(dir, "container-status.txt", []byte(status), logf)
	}

	for _, svc := range plan.Services {
		logs, err := src.Logs(ctx, svc, CapturedLogTailLines)
		if err != nil {
			logf("devnet: logs for %s: %v", svc, err)
			continue
		}
		writeArtifact(dir, svc+".log", []byte(logs), logf)
	}
	logf("devnet: failure artifacts written to %s", dir)
}

func writeArtifact(
	dir, name string,
	data []byte,
	logf func(format string, args ...any),
) {
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		logf("devnet: writing %s: %v", path, err)
	}
}
