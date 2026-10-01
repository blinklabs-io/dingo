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
	"regexp"
	"slices"
	"strings"
	"testing"
)

// designDocs describe the system as it is and must read without GitHub, so
// they state behavior instead of citing the issue or pull request behind it.
var designDocs = []string{
	"ARCHITECTURE.md",
	"DATABASE.md",
}

var (
	// hashNumber finds every "#N". Go's regexp has no lookaround, so
	// hashReference filters the candidates that are not tracker numbers.
	hashNumber = regexp.MustCompile(`#[0-9]+`)
	// trackerURL is a GitHub issue or pull-request link.
	trackerURL = regexp.MustCompile(
		`github\.com/[^\s)>\]]+/(?:issues|pulls?)/[0-9]+`,
	)
	// trackerWord is a spelled-out reference such as "issue 3377".
	trackerWord = regexp.MustCompile(
		`(?i)\b(?:issues?|prs?|pull requests?)\s+[0-9]+\b`,
	)
)

// hashReference reports whether the "#N" at line[start:end] is an issue or
// pull-request number. It is not when it is an HTML entity ("&#8212;"), an
// anchor slug that starts with a digit ("#5-storage"), or a CBOR tag in
// CDDL notation ("#6.258").
func hashReference(line string, start, end int) bool {
	if start > 0 && line[start-1] == '&' {
		return false
	}
	if end == len(line) {
		return true
	}
	next := line[end]
	if next == '-' || next == '_' || isAlphaNum(next) {
		return false
	}
	if next == '.' && end+1 < len(line) && isDigit(line[end+1]) {
		return false
	}
	return true
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func isAlphaNum(c byte) bool {
	return isDigit(c) || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// issueReferences returns every issue or pull-request reference in line.
func issueReferences(line string) []string {
	var found []string
	for _, loc := range hashNumber.FindAllStringIndex(line, -1) {
		if hashReference(line, loc[0], loc[1]) {
			found = append(found, line[loc[0]:loc[1]])
		}
	}
	found = append(found, trackerURL.FindAllString(line, -1)...)
	found = append(found, trackerWord.FindAllString(line, -1)...)
	return found
}

func TestDesignDocsCarryNoIssueReferences(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)
	for _, doc := range designDocs {
		lines := strings.Split(readRepoFile(t, root, doc), "\n")
		for i, line := range lines {
			for _, ref := range issueReferences(line) {
				t.Errorf(
					"%s:%d: %q cites an issue or pull request; "+
						"state the behavior instead",
					doc, i+1, ref,
				)
			}
		}
	}
}

func TestIssueReferencesCatchesEveryForm(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"tracked separately (issue #3377); golangci":          "#3377",
		"can reach its credential (blinklabs-io/dingo#3854).": "#3854",
		"only their indexes back no predicate (dingo#4598).":  "#4598",
		"different lane. Before #2287 the undo events":        "#2287",
		"the Limit on Eagerness (#3270) is separate":          "#3270",
		"is the #3928 wedge class.":                           "#3928",
		"ends a sentence #4143":                               "#4143",
		"falls back to the pre-issue-#3513 legacy key":        "#3513",
		"a gouroboros defect (blinklabs-io/gouroboros#1989)":  "#1989",
		"`IntersectMBO/cardano-node#4050` documents this":     "#4050",
		"the proto that landed in bark#16/PR#28":              "#28",
		"a remote `dingoctl` (see dingoctl#5) drives":         "#5",
		"see https://github.com/o/r/pull/4712":                "github.com/o/r/pull/4712",
		"(github.com/o/r/issues/12)":                          "github.com/o/r/issues/12",
		"tracked as issue 3377 upstream":                      "issue 3377",
		"reported in PR 3611":                                 "PR 3611",
		"see cardano-ledger pull request 4712 for":            "pull request 4712",
	}
	for line, want := range cases {
		got := issueReferences(line)
		if !slices.Contains(got, want) {
			t.Errorf("issueReferences(%q) = %q, want it to contain %q",
				line, got, want)
		}
	}
}

func TestIssueReferencesIgnoresOtherHashUses(t *testing.T) {
	t.Parallel()

	lines := []string{
		"## Storage Architecture",
		"### 2. Chain selection",
		"# currently is. Requires both flags together.",
		"See [DMQ](#dmq-message-authentication) for the rule.",
		"See [storage](#5-storage-layout) below.",
		"a CBOR set is tag `#6.258` and a wrapped value `#6.24`.",
		"an em dash written as &#8212; in HTML",
		"source at `ledger/queries.go#L120`",
		"LeiosVotes pull requests remain outstanding",
		"`CostModels[1]` and 0x58 0x20 prefixes",
	}
	for _, line := range lines {
		if got := issueReferences(line); len(got) != 0 {
			t.Errorf("issueReferences(%q) = %q, want none", line, got)
		}
	}
}
