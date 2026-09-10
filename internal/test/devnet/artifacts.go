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
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// This file carries the package-wide Linux constraint but does not require
// the devnet tag. What a failed scenario preserves, and where, is therefore
// covered by an ordinary Linux `go test ./...` run; only the Docker calls
// that supply the evidence need the devnet tag and a live network.

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

// ArtifactName encodes a Go test name as one path segment. t.Name()
// renders a subtest as parent/child, which would otherwise scatter a
// scenario's evidence across nested directories.
//
// The separator is escaped rather than replaced, because replacing it is
// lossy: with the separator rewritten to an ordinary character, TestX/a-b
// and TestX/a/b both become TestX-a-b and two failing subtests write into
// one directory. Escaping keeps the mapping one-to-one, so distinct test
// names always get distinct directories -- a property of the encoding
// rather than a probability, which is what a digest of the name would
// give instead.
//
// String-level injectivity only delivers that guarantee if the
// filesystem stores the name it is given. Windows does not: it rejects
// : * ? " < > | outright, and it strips a trailing dot or space, which
// would quietly land "." and ".." -- encoded here as %2E and %2E. -- in
// one directory. Those are escaped for the same reason "." and ".." are:
// not because they escape the segment, but because the filesystem reads
// them as something other than the name asked for.
//
// Case is the one difference deliberately left unescaped, so the
// guarantee above is a guarantee on any filesystem that distinguishes
// case, and not on one that folds it: on macOS or Windows, TestX/a and
// TestX/A encode differently but land in one directory. Closing that
// would cost the readable name, which is the thing the encoding exists
// to protect -- escaping case renders TestSustainedConsensus as
// %54est%53ustained%43onsensus, and a digest suffix trades the exact
// name for a probability. DevNet captures on Linux, where case is
// significant, and two Go test names differing only in case is a naming
// problem of its own. TestArtifactNameKeepsCase pins this boundary.
//
// Only what a filesystem would reject or rewrite is escaped, so a name
// built from Go identifiers comes back exactly as written. That is every
// scenario in the canonical suite, and it is what makes the directory
// findable from the name in the failure output.
func ArtifactName(testName string) string {
	// t.Name() is never empty; this only guards a caller-supplied name.
	if testName == "" {
		return "scenario"
	}
	var encoded strings.Builder
	encoded.Grow(len(testName))
	for _, r := range testName {
		switch r {
		case '%':
			// First, so no escape below can be forged by an input that
			// spells one out literally.
			encoded.WriteString("%25")
		case '/', '\\', ':', '*', '?', '"', '<', '>', '|':
			encoded.WriteString(escapeByte(byte(r)))
		default:
			encoded.WriteRune(r)
		}
	}
	name := encoded.String()
	// A name made only of dots addresses a directory rather than naming
	// one: "." and ".." would resolve to the artifact root and its
	// parent. Escaping the leading dot keeps it a name, and stays
	// distinct from every other encoding because a literal % is already
	// escaped above.
	if strings.Trim(name, ".") == "" {
		name = "%2E" + name[1:]
	}
	// Windows strips a trailing dot or space, so ".." (encoded just
	// above as "%2E.") would be stored as "%2E" and share a directory
	// with ".". Escaping the last character alone is enough, because the
	// result then ends in a hex digit and there is nothing left to
	// strip. It stays injective: the escape can only have come from the
	// character it encodes, since a literal % is already escaped.
	if last := name[len(name)-1]; last == '.' || last == ' ' {
		name = name[:len(name)-1] + escapeByte(last)
	}
	return name
}

// escapeByte renders one byte as the percent escape used throughout
// ArtifactName. Upper-case hex, so the encoding has a single spelling
// and two names cannot differ only by the case of an escape.
func escapeByte(b byte) string {
	const hex = "0123456789ABCDEF"
	return string([]byte{'%', hex[b>>4], hex[b&0x0F]})
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
