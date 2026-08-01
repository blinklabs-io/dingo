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

package docsparity_test

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// goRelease is a major.minor Go release. Patch levels are deliberately
// ignored: go.mod states a language minimum, not a patch pin.
type goRelease struct {
	major int
	minor int
}

func (r goRelease) String() string {
	return fmt.Sprintf("%d.%d", r.major, r.minor)
}

// atLeast reports whether r is the same release as other or newer.
func (r goRelease) atLeast(other goRelease) bool {
	if r.major != other.major {
		return r.major > other.major
	}
	return r.minor >= other.minor
}

var (
	goDirectiveRe = regexp.MustCompile(`(?m)^go\s+(\d+)\.(\d+)`)
	toolchainRe   = regexp.MustCompile(`(?m)^toolchain\s+go(\d+)\.(\d+)`)
	// goVersionKeyRe captures actions/setup-go pins, either a scalar
	// (`go-version: 1.26.x`) or a matrix list (`go-version: [1.26.x]`).
	goVersionKeyRe = regexp.MustCompile(`(?m)^\s*go-version:\s*(.+?)\s*$`)
	// goImageRe captures the Blink Labs Go builder image and its full tag.
	goImageRe = regexp.MustCompile(
		"ghcr\\.io/blinklabs-io/go:([0-9][0-9A-Za-z.-]*)",
	)
	// goPrereqRe captures prose statements of the required Go release, such
	// as "Go 1.26 or later" or "Go 1.26+". The mandatory whitespace keeps it
	// from matching image tags like `ghcr.io/blinklabs-io/go:1.26.3-1`.
	goPrereqRe = regexp.MustCompile(`(?i)\bgo\s+(\d+)\.(\d+)\b`)
	releaseRe  = regexp.MustCompile(`^(\d+)\.(\d+)`)
)

// parseGoRelease reads a leading major.minor from a version string. It
// accepts "1.26", "1.26.0", "1.26.x", and "1.26.3-1".
func parseGoRelease(value string) (goRelease, bool) {
	match := releaseRe.FindStringSubmatch(strings.TrimSpace(value))
	if match == nil {
		return goRelease{}, false
	}
	major, err := strconv.Atoi(match[1])
	if err != nil {
		return goRelease{}, false
	}
	minor, err := strconv.Atoi(match[2])
	if err != nil {
		return goRelease{}, false
	}
	return goRelease{major: major, minor: minor}, true
}

// moduleGoRelease returns the minimum Go release declared by go.mod. This is
// the single source of truth every other Go version statement is checked
// against.
func moduleGoRelease(t *testing.T, root string) goRelease {
	t.Helper()

	goMod := readRepoFile(t, root, "go.mod")
	match := goDirectiveRe.FindStringSubmatch(goMod)
	if match == nil {
		t.Fatal("go.mod has no `go` directive")
	}
	release, ok := parseGoRelease(match[1] + "." + match[2])
	if !ok {
		t.Fatalf("go.mod `go` directive is not a release: %q", match[0])
	}
	return release
}

// TestGoModToolchainMatchesDirective checks the toolchain line, when present,
// is not older than the language minimum it accompanies.
func TestGoModToolchainMatchesDirective(t *testing.T) {
	root := repoRoot(t)
	want := moduleGoRelease(t, root)

	goMod := readRepoFile(t, root, "go.mod")
	match := toolchainRe.FindStringSubmatch(goMod)
	if match == nil {
		return
	}
	got, ok := parseGoRelease(match[1] + "." + match[2])
	if !ok {
		t.Fatalf("go.mod toolchain line is not a release: %q", match[0])
	}
	if !got.atLeast(want) {
		t.Errorf(
			"go.mod toolchain go%s is older than the `go %s` directive",
			got,
			want,
		)
	}
}

// TestDocumentedGoVersionMatchesGoMod checks every prose statement of the Go
// prerequisite against go.mod. Documentation states the minimum, so it has to
// be exactly the module minimum: an older value misleads contributors into a
// toolchain that cannot build the tree, and a newer one turns away a
// toolchain that can.
func TestDocumentedGoVersionMatchesGoMod(t *testing.T) {
	root := repoRoot(t)
	want := moduleGoRelease(t, root)

	for _, rel := range markdownFiles(t, root) {
		doc := readRepoFile(t, root, rel)
		for i, line := range strings.Split(doc, "\n") {
			for _, match := range goPrereqRe.FindAllStringSubmatch(line, -1) {
				got, ok := parseGoRelease(match[1] + "." + match[2])
				if !ok {
					continue
				}
				if got != want {
					t.Errorf(
						"%s states %q but go.mod requires Go %s",
						docLocation(rel, i+1),
						strings.TrimSpace(match[0]),
						want,
					)
				}
			}
		}
	}
}

// TestWorkflowGoVersionCoversGoMod checks every actions/setup-go pin can
// actually build the module. A pin may lead go.mod, but never trail it.
func TestWorkflowGoVersionCoversGoMod(t *testing.T) {
	root := repoRoot(t)
	want := moduleGoRelease(t, root)

	checked := 0
	for _, rel := range workflowFiles(t, root) {
		workflow := readRepoFile(t, root, rel)
		for i, line := range strings.Split(workflow, "\n") {
			match := goVersionKeyRe.FindStringSubmatch(line)
			if match == nil {
				continue
			}
			for _, value := range splitYAMLScalarOrList(match[1]) {
				// A matrix reference is resolved against the matrix in the
				// same file rather than skipped, so a pin cannot hide behind
				// an expression this check does not follow.
				values := []string{value}
				if strings.Contains(value, "${{") {
					resolved, ok := resolveMatrixValues(workflow, value)
					if !ok {
						t.Errorf(
							"%s: go-version %q does not resolve to a release "+
								"in this workflow",
							docLocation(rel, i+1),
							value,
						)
						continue
					}
					values = resolved
				}
				for _, value := range values {
					got, ok := parseGoRelease(value)
					if !ok {
						t.Errorf(
							"%s: go-version %q is not a release",
							docLocation(rel, i+1),
							value,
						)
						continue
					}
					checked++
					if !got.atLeast(want) {
						t.Errorf(
							"%s pins Go %s but go.mod requires at least "+
								"Go %s",
							docLocation(rel, i+1),
							got,
							want,
						)
					}
				}
			}
		}
	}
	if checked == 0 {
		t.Error("no concrete go-version pin found in .github/workflows")
	}
}

// matrixRefRe captures the matrix key a go-version expression refers to.
var matrixRefRe = regexp.MustCompile(
	`\$\{\{\s*matrix\.([A-Za-z0-9_-]+)\s*\}\}`,
)

// resolveMatrixValues expands a `${{ matrix.<key> }}` go-version expression
// into the concrete values the workflow's matrix declares for that key. It
// reports false when the expression is not a matrix reference or the key has
// no concrete values, so the caller can fail rather than skip.
func resolveMatrixValues(workflow, expr string) ([]string, bool) {
	match := matrixRefRe.FindStringSubmatch(expr)
	if match == nil {
		return nil, false
	}
	keyRe := regexp.MustCompile(
		`(?m)^\s*` + regexp.QuoteMeta(match[1]) + `:\s*(.+?)\s*$`,
	)
	var values []string
	for _, decl := range keyRe.FindAllStringSubmatch(workflow, -1) {
		for _, value := range splitYAMLScalarOrList(decl[1]) {
			if strings.Contains(value, "${{") {
				continue
			}
			values = append(values, value)
		}
	}
	if len(values) == 0 {
		return nil, false
	}
	return values, true
}

// TestGoBuilderImagesCoverGoMod checks every Dockerfile that builds Go code
// uses a builder image new enough for the module.
func TestGoBuilderImagesCoverGoMod(t *testing.T) {
	root := repoRoot(t)
	want := moduleGoRelease(t, root)
	rootDockerfile := readRepoFile(t, root, "Dockerfile")
	rootImage := goImageRe.FindStringSubmatch(rootDockerfile)
	if rootImage == nil {
		t.Fatal("root Dockerfile does not use a Blink Labs Go builder image")
	}
	wantImage := rootImage[0]

	checked := 0
	for _, rel := range dockerfiles(t, root) {
		content := readRepoFile(t, root, rel)
		for i, line := range strings.Split(content, "\n") {
			match := goImageRe.FindStringSubmatch(line)
			if match == nil {
				continue
			}
			got, ok := parseGoRelease(match[1])
			if !ok {
				t.Errorf(
					"%s: Go builder tag %q is not a release",
					docLocation(rel, i+1),
					match[1],
				)
				continue
			}
			checked++
			if match[0] != wantImage {
				t.Errorf(
					"%s uses Go builder image %q but the root Dockerfile pins %q",
					docLocation(rel, i+1),
					match[0],
					wantImage,
				)
			}
			if !got.atLeast(want) {
				t.Errorf(
					"%s builds with Go %s but go.mod requires at least Go %s",
					docLocation(rel, i+1),
					got,
					want,
				)
			}
		}
	}
	if checked == 0 {
		t.Error("no Go builder image found in any Dockerfile")
	}
}

// TestDocumentedGoBuilderImageMatchesDockerfile checks that prose naming the
// Go builder image names the tag the root Dockerfile actually uses. Docs are
// free to describe the image without pinning a tag; if they pin one, it has
// to be the real one.
func TestDocumentedGoBuilderImageMatchesDockerfile(t *testing.T) {
	root := repoRoot(t)

	dockerfile := readRepoFile(t, root, "Dockerfile")
	match := goImageRe.FindStringSubmatch(dockerfile)
	if match == nil {
		t.Fatal("root Dockerfile does not use a Blink Labs Go builder image")
	}
	want := match[0]

	for _, rel := range markdownFiles(t, root) {
		doc := readRepoFile(t, root, rel)
		for i, line := range strings.Split(doc, "\n") {
			for _, found := range goImageRe.FindAllString(line, -1) {
				if found != want {
					t.Errorf(
						"%s names %q but the root Dockerfile builds with %q",
						docLocation(rel, i+1),
						found,
						want,
					)
				}
			}
		}
	}
}

// splitYAMLScalarOrList turns a YAML value that is either a scalar or an
// inline sequence into its elements.
func splitYAMLScalarOrList(value string) []string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") {
		value = strings.TrimSuffix(strings.TrimPrefix(value, "["), "]")
	}
	var out []string
	for part := range strings.SplitSeq(value, ",") {
		part = strings.TrimSpace(part)
		part = strings.Trim(part, `"'`)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}
