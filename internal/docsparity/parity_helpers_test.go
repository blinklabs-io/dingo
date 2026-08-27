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

// Package docsparity holds checks that keep contributor-facing documentation
// in agreement with the repository configuration it describes. Every rule
// here derives its expectation from a source of truth in the tree (go.mod,
// the Makefile, docker-compose.yml, the DevNet scripts) rather than
// duplicating a value, so a change to the real thing fails the check until
// the prose is updated with it.
package docsparity_test

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// contributorDocs are the documents that describe how to build, test, and run
// this repository. They are the files the parity rules police.
var contributorDocs = []string{
	"README.md",
	"AGENTS.md",
	"CLAUDE.md",
	"ARCHITECTURE.md",
	"DATABASE.md",
	"GENESIS_SYNC.md",
	"internal/test/devnet/README.md",
}

// historicalDocs record past measurements or shipped releases. They describe
// the state of the tree at some earlier point, so present-tense parity rules
// do not apply to them.
var historicalDocs = map[string]bool{
	"benchmark_results.md":              true,
	"benchmark_results_api_backfill.md": true,
	"benchmark_results_bp_pi.md":        true,
	"benchmark_results_targeted.md":     true,
}

// repoRoot walks up from the working directory until it finds the module
// root, mirroring internal/architecture so both checks locate the tree the
// same way.
func repoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find repository root")
		}
		dir = parent
	}
}

// readRepoFile reads a file relative to the repository root and normalises
// line endings, so a Windows checkout with autocrlf enabled parses the same
// as a Linux one.
func readRepoFile(t *testing.T, root, rel string) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return strings.ReplaceAll(string(data), "\r\n", "\n")
}

// filesMatching returns tracked repository-relative paths for which match
// reports true. Using Git's index keeps local worktrees and other untracked
// files out of documentation parity checks. Source archives without Git
// metadata fall back to walking the extracted tree.
func filesMatching(
	t *testing.T,
	root string,
	match func(rel string) bool,
) []string {
	t.Helper()

	var found []string
	rootAbs, err := canonicalDiscoveryPath(root)
	if err != nil {
		return filesMatchingWalk(t, root, match)
	}
	topLevel, err := exec.Command("git", "-C", root, "rev-parse", "--show-toplevel").Output()
	if err != nil || !sameDiscoveryRoot(rootAbs, strings.TrimSpace(string(topLevel))) {
		return filesMatchingWalk(t, root, match)
	}
	cmd := exec.Command("git", "-C", root, "ls-files", "-z")
	output, err := cmd.Output()
	if err != nil {
		return filesMatchingWalk(t, root, match)
	}
	for rel := range strings.SplitSeq(string(output), "\x00") {
		if rel == "" {
			continue
		}
		rel = normalizeDiscoveryPath(rel)
		if match(rel) {
			found = append(found, rel)
		}
	}
	return found
}

// canonicalDiscoveryPath resolves aliases and symlinks before comparing
// repository roots. os.SameFile is used by sameDiscoveryRoot when possible,
// which handles aliases whose spelling differs even after filepath.Clean.
func canonicalDiscoveryPath(root string) (string, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", err
	}
	return filepath.Clean(resolved), nil
}

func sameDiscoveryRoot(canonicalRoot, gitRoot string) bool {
	canonicalGitRoot, err := canonicalDiscoveryPath(gitRoot)
	if err != nil {
		return false
	}
	if rootInfo, err := os.Stat(canonicalRoot); err == nil {
		if gitInfo, err := os.Stat(canonicalGitRoot); err == nil {
			return os.SameFile(rootInfo, gitInfo)
		}
	}
	// Windows paths are case-insensitive. EqualFold also makes the fallback
	// comparison robust when Git and the process use different path casing.
	left := filepath.ToSlash(filepath.Clean(canonicalRoot))
	right := filepath.ToSlash(filepath.Clean(canonicalGitRoot))
	if filepath.Separator == '\\' {
		return strings.EqualFold(left, right)
	}
	return left == right
}

func filesMatchingWalk(
	t *testing.T,
	root string,
	match func(rel string) bool,
) []string {
	t.Helper()

	var found []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = normalizeDiscoveryPath(rel)
		if entry.IsDir() {
			if isExcludedDiscoveryPath(rel) {
				return filepath.SkipDir
			}
			return nil
		}
		if isExcludedDiscoveryPath(rel) {
			return nil
		}
		if match(rel) {
			found = append(found, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return found
}

// excludedDiscoveryRoots are generated or dependency trees that are not part
// of a source checkout's documentation/configuration surface. They are
// expressed with slash separators because repository-relative paths use Git's
// format regardless of the host platform.
var excludedDiscoveryRoots = []string{
	".agents/worktrees",
	".claude/worktrees",
	".codex/worktrees",
	".git",
	".tools",
	".worktrees",
	"node_modules",
}

// normalizeDiscoveryPath converts a repository-relative path to the stable
// slash-separated form used by every discovery predicate. Replacing both
// separators before path.Clean keeps synthetic Windows paths testable on Unix
// and avoids filepath semantics changing the parity contract by host OS.
func normalizeDiscoveryPath(rel string) string {
	rel = strings.ReplaceAll(rel, `\`, "/")
	rel = path.Clean(rel)
	if rel == "." {
		return ""
	}
	return strings.TrimPrefix(rel, "./")
}

func isExcludedDiscoveryPath(rel string) bool {
	rel = normalizeDiscoveryPath(rel)
	for _, root := range excludedDiscoveryRoots {
		if rel == root || strings.HasPrefix(rel, root+"/") {
			return true
		}
	}
	return false
}

// markdownFiles returns every non-historical markdown document in the tree.
func markdownFiles(t *testing.T, root string) []string {
	t.Helper()

	all := filesMatching(t, root, func(rel string) bool {
		return strings.HasSuffix(rel, ".md")
	})
	kept := make([]string, 0, len(all))
	for _, rel := range all {
		if historicalDocs[path.Base(rel)] {
			continue
		}
		kept = append(kept, rel)
	}
	return kept
}

// dockerfiles returns every Dockerfile in the tree.
func dockerfiles(t *testing.T, root string) []string {
	t.Helper()

	return filesMatching(t, root, func(rel string) bool {
		name := path.Base(rel)
		return name == "Dockerfile" || strings.HasPrefix(name, "Dockerfile.")
	})
}

// workflowFiles returns every GitHub Actions workflow.
func workflowFiles(t *testing.T, root string) []string {
	t.Helper()

	return filesMatching(t, root, func(rel string) bool {
		return (strings.HasSuffix(rel, ".yml") ||
			strings.HasSuffix(rel, ".yaml")) &&
			path.Dir(rel) == ".github/workflows"
	})
}

func TestNormalizeDiscoveryPathIsPlatformIndependent(t *testing.T) {
	tests := map[string]struct {
		input    string
		excluded bool
	}{
		"windows agent worktree": {
			input:    `.codex\\worktrees\\scratch\\notes.md`,
			excluded: true,
		},
		"unix agent worktree": {
			input:    `.claude/worktrees/scratch/notes.md`,
			excluded: true,
		},
		"similar name remains": {
			input:    `.codex/worktree-notes/notes.md`,
			excluded: false,
		},
		"tracked repository file": {
			input:    `docs\\guide.md`,
			excluded: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := isExcludedDiscoveryPath(tt.input); got != tt.excluded {
				t.Fatalf("isExcludedDiscoveryPath(%q) = %v, want %v", tt.input, got, tt.excluded)
			}
		})
	}
}

func TestDocumentationDiscoveryIgnoresUntrackedFiles(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init")

	writeTestFile(t, root, "docs/tracked.md")
	writeTestFile(t, root, "Dockerfile")
	writeTestFile(t, root, ".github/workflows/tracked.yml")
	writeTestFile(t, root, ".claude/worktrees/scratch/ignored.md")
	writeTestFile(t, root, ".claude/worktrees/scratch/Dockerfile")
	writeTestFile(t, root, ".codex/worktrees/tracked.md")
	writeTestFile(t, root, ".github/workflows/ignored.yaml")
	runGit(
		t,
		root,
		"add",
		".codex/worktrees/tracked.md",
		"docs/tracked.md",
		"Dockerfile",
		".github/workflows/tracked.yml",
	)

	got, want := markdownFiles(t, root), []string{
		".codex/worktrees/tracked.md",
		"docs/tracked.md",
	}
	if !slices.Equal(got, want) {
		t.Errorf("markdownFiles() = %v, want %v", got, want)
	}
	got, want = dockerfiles(t, root), []string{"Dockerfile"}
	if !slices.Equal(got, want) {
		t.Errorf("dockerfiles() = %v, want %v", got, want)
	}
	got, want = workflowFiles(t, root), []string{
		".github/workflows/tracked.yml",
	}
	if !slices.Equal(got, want) {
		t.Errorf("workflowFiles() = %v, want %v", got, want)
	}

	parent := t.TempDir()
	runGit(t, parent, "init")
	nested := filepath.Join(parent, "nested")
	writeTestFile(t, nested, "docs/archive.md")
	writeTestFile(t, nested, "Dockerfile")
	writeTestFile(t, nested, ".github/workflows/archive.yml")
	writeTestFile(t, nested, ".claude/worktrees/scratch/ignored.md")
	writeTestFile(t, nested, ".claude/worktrees/scratch/Dockerfile")
	writeTestFile(t, nested, ".codex/worktrees/scratch/.github/workflows/ignored.yml")
	got, want = markdownFiles(t, nested), []string{"docs/archive.md"}
	if !slices.Equal(got, want) {
		t.Errorf("nested markdownFiles() = %v, want %v", got, want)
	}
	got, want = dockerfiles(t, nested), []string{"Dockerfile"}
	if !slices.Equal(got, want) {
		t.Errorf("nested dockerfiles() = %v, want %v", got, want)
	}
	got, want = workflowFiles(t, nested), []string{".github/workflows/archive.yml"}
	if !slices.Equal(got, want) {
		t.Errorf("nested workflowFiles() = %v, want %v", got, want)
	}

	archive := t.TempDir()
	writeTestFile(t, archive, "docs/tracked.md")
	writeTestFile(t, archive, ".claude/worktrees/scratch/ignored.md")
	writeTestFile(t, archive, ".codex/worktrees/scratch/ignored.md")
	writeTestFile(t, archive, ".agents/worktrees/scratch/ignored.md")
	writeTestFile(t, archive, ".worktrees/scratch/ignored.md")
	writeTestFile(t, archive, ".tools/scratch/ignored.md")
	got, want = markdownFiles(t, archive), []string{"docs/tracked.md"}
	if !slices.Equal(got, want) {
		t.Errorf("archive markdownFiles() = %v, want %v", got, want)
	}
}

func TestDocumentationDiscoveryRecognizesAliasedRepositoryRoot(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init")
	writeTestFile(t, root, "docs/tracked.md")
	writeTestFile(t, root, ".github/workflows/tracked.yml")
	writeTestFile(t, root, "Dockerfile")
	writeTestFile(t, root, ".codex/worktrees/ignored.md")
	writeTestFile(t, root, "untracked.md")
	writeTestFile(t, root, ".github/workflows/untracked.yaml")
	writeTestFile(t, root, ".codex/worktrees/scratch/.github/workflows/ignored.yml")
	runGit(t, root, "add", "docs/tracked.md", ".github/workflows/tracked.yml", "Dockerfile")

	alias := filepath.Join(t.TempDir(), "repo-alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}

	if got, want := markdownFiles(t, alias), []string{"docs/tracked.md"}; !slices.Equal(got, want) {
		t.Errorf("aliased markdownFiles() = %v, want %v", got, want)
	}
	if got, want := dockerfiles(t, alias), []string{"Dockerfile"}; !slices.Equal(got, want) {
		t.Errorf("aliased dockerfiles() = %v, want %v", got, want)
	}
	if got, want := workflowFiles(t, alias), []string{".github/workflows/tracked.yml"}; !slices.Equal(got, want) {
		t.Errorf("aliased workflowFiles() = %v, want %v", got, want)
	}
}

func writeTestFile(t *testing.T, root, rel string) {
	t.Helper()

	path := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create directory for %s: %v", rel, err)
	}
	if err := os.WriteFile(path, []byte("test\n"), 0o644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}

func runGit(t *testing.T, root string, args ...string) {
	t.Helper()

	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
	}
}

// markdownBlock is one logical chunk of a markdown document: a paragraph, a
// list, a table, or a fenced code block. Blank lines separate blocks except
// inside a fence, where they are content.
type markdownBlock struct {
	startLine int
	text      string
	fenced    bool
}

var fenceRe = regexp.MustCompile("^\\s*(`{3,}|~{3,})(.*)$")

// fenceTracker follows fenced code blocks through a document. It records the
// character and length of the opening marker so a longer fence can contain a
// shorter one, which is how a markdown document quotes a fenced example.
type fenceTracker struct {
	open   bool
	marker byte
	length int
}

// step feeds one line to the tracker and reports whether that line is a fence
// marker and whether the line sits inside a fence once it has been applied.
func (f *fenceTracker) step(line string) (isMarker, inside bool) {
	match := fenceRe.FindStringSubmatch(line)
	if match == nil {
		return false, f.open
	}
	marker := match[1]
	if !f.open {
		f.open = true
		f.marker = marker[0]
		f.length = len(marker)
		return true, true
	}
	// A closing marker uses the same character, is at least as long as the
	// opening one, and carries no info string.
	if marker[0] != f.marker || len(marker) < f.length ||
		strings.TrimSpace(match[2]) != "" {
		return false, true
	}
	f.open = false
	return true, true
}

// markdownBlocks splits a markdown document into blocks.
func markdownBlocks(doc string) []markdownBlock {
	var (
		blocks  []markdownBlock
		current []string
		start   int
		fenced  bool
		tracker fenceTracker
	)
	flush := func() {
		if len(current) == 0 {
			return
		}
		blocks = append(blocks, markdownBlock{
			startLine: start,
			text:      strings.Join(current, "\n"),
			fenced:    fenced,
		})
		current = nil
		fenced = false
	}
	for i, line := range strings.Split(doc, "\n") {
		wasOpen := tracker.open
		isMarker, _ := tracker.step(line)
		switch {
		case isMarker && !wasOpen:
			flush()
			fenced = true
			start = i + 1
			current = append(current, line)
			continue
		case isMarker && wasOpen:
			current = append(current, line)
			flush()
			continue
		case tracker.open:
			current = append(current, line)
			continue
		}
		if strings.TrimSpace(line) == "" {
			flush()
			continue
		}
		if len(current) == 0 {
			start = i + 1
		}
		current = append(current, line)
	}
	flush()
	return blocks
}

// markdownTableRow is one parsed row of a markdown table.
type markdownTableRow struct {
	line  int
	cells []string
}

// tableDividerRe matches the alignment row under a table header. The
// closing pipe is optional because GFM allows it to be omitted, and a
// table this failed to recognise would be skipped by every rule below.
// tableDividerRe matches the alignment row under a table header. Both outer
// pipes are optional because GFM allows either to be omitted, and a table this
// failed to recognise would be skipped by every rule built on top of it.
var tableDividerRe = regexp.MustCompile(`^\s*\|?[\s:|-]*-[\s:|-]*$`)

// splitTableCells splits one table line into trimmed cells, tolerating a
// missing leading or trailing pipe.
func splitTableCells(line string) []string {
	trimmed := strings.TrimSpace(line)
	trimmed = strings.TrimPrefix(trimmed, "|")
	trimmed = strings.TrimSuffix(trimmed, "|")
	parts := strings.Split(trimmed, "|")
	cells := make([]string, 0, len(parts))
	for _, part := range parts {
		cells = append(cells, strings.TrimSpace(part))
	}
	return cells
}

// markdownTableRows returns the body rows of every markdown table in doc,
// skipping header and divider lines.
func markdownTableRows(doc string) []markdownTableRow {
	var (
		rows      []markdownTableRow
		afterRule bool
		prev      string
		tracker   fenceTracker
	)
	for i, line := range strings.Split(doc, "\n") {
		if isMarker, inside := tracker.step(line); isMarker || inside {
			afterRule = false
			prev = ""
			continue
		}
		if !strings.Contains(line, "|") {
			afterRule = false
			prev = line
			continue
		}
		// A divider only counts when it sits under a header with the same
		// number of columns. Without that, a row of dashes and pipes in
		// unfenced prose would open a table that is not there.
		if !afterRule && tableDividerRe.MatchString(line) &&
			strings.Contains(prev, "|") &&
			len(splitTableCells(prev)) == len(splitTableCells(line)) {
			afterRule = true
			prev = line
			continue
		}
		prev = line
		if !afterRule {
			continue
		}
		rows = append(rows, markdownTableRow{
			line:  i + 1,
			cells: splitTableCells(line),
		})
	}
	return rows
}

// unquote strips markdown code spans from a table cell.
func unquote(cell string) string {
	return strings.Trim(strings.TrimSpace(cell), "`")
}

// docLocation renders a file and line for failure messages.
func docLocation(rel string, line int) string {
	return fmt.Sprintf("%s:%d", filepath.ToSlash(rel), line)
}
