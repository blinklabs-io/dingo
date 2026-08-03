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
	"sort"
	"strings"
	"testing"
)

// binariesTarget is the pattern rule that builds every command binary. It is
// the only variable-named target the parser expands, because `build` depends
// on it and its prerequisites are part of what `make build` really does.
const binariesTarget = "$(BINARIES)"

// makeRule is one parsed Makefile rule.
type makeRule struct {
	name    string
	prereqs []string
	help    string
	line    int
}

var (
	makeRuleRe = regexp.MustCompile(
		`^([A-Za-z0-9_.$()/-]+):(?:[^=]|$)(.*)$`,
	)
	phonyRe = regexp.MustCompile(`(?m)^\.PHONY:\s*(.*)$`)
	// makeCommandRe matches a `make` invocation written as a shell command
	// or as an inline code span, capturing an optional target.
	makeCommandRe = regexp.MustCompile(
		`(?m)^\s*make(?:\s+([a-z][a-z0-9_-]*))?\s*(?:#\s*(.*))?$`,
	)
	inlineMakeRe = regexp.MustCompile("`make\\s+([a-z][a-z0-9_-]*)`")
	wordRe       = regexp.MustCompile(`[a-z][a-z0-9-]*`)
)

// parseMakefile reads the Makefile into rules keyed by target name, the order
// they are declared in, and the declared .PHONY set.
func parseMakefile(t *testing.T, root string) (
	map[string]makeRule,
	[]string,
	map[string]bool,
) {
	t.Helper()

	content := readRepoFile(t, root, "Makefile")
	rules := map[string]makeRule{}
	var order []string
	for i, line := range strings.Split(content, "\n") {
		if line == "" || strings.HasPrefix(line, "\t") ||
			strings.HasPrefix(line, "#") || strings.HasPrefix(line, " ") {
			continue
		}
		match := makeRuleRe.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		name := match[1]
		if name == ".PHONY" {
			continue
		}
		rest := line[len(name)+1:]
		var help string
		if idx := strings.Index(rest, "##"); idx >= 0 {
			help = strings.TrimSpace(rest[idx+2:])
			rest = rest[:idx]
		}
		if _, seen := rules[name]; !seen {
			order = append(order, name)
		}
		rules[name] = makeRule{
			name:    name,
			prereqs: strings.Fields(rest),
			help:    help,
			line:    i + 1,
		}
	}
	if len(rules) == 0 {
		t.Fatal("no rules parsed from Makefile")
	}

	// A Makefile may split .PHONY over several declarations, so collect them
	// all rather than trusting the first.
	phony := map[string]bool{}
	for _, match := range phonyRe.FindAllStringSubmatch(content, -1) {
		for name := range strings.FieldsSeq(match[1]) {
			phony[name] = true
		}
	}
	return rules, order, phony
}

// defaultMakeTarget returns the target a bare `make` runs: the first rule in
// the file whose name is neither a special target nor a variable expansion.
// Deriving it means adding a rule above `all` cannot silently move the default
// out from under the documentation.
func defaultMakeTarget(t *testing.T, order []string) string {
	t.Helper()

	for _, name := range order {
		if strings.HasPrefix(name, ".") || strings.HasPrefix(name, "$(") {
			continue
		}
		return name
	}
	t.Fatal("Makefile declares no ordinary target")
	return ""
}

// targetPrereqs returns the prerequisites of a rule that are themselves
// targets, expanding $(BINARIES) because `build` reaches mod-tidy through it.
// Prerequisites that expand to files (source lists, downloaded tools) are not
// part of what a contributor needs to know about a target.
func targetPrereqs(rule makeRule, rules map[string]makeRule) []string {
	var out []string
	add := func(name string) {
		if _, ok := rules[name]; !ok {
			return
		}
		if !slices.Contains(out, name) {
			out = append(out, name)
		}
	}
	for _, prereq := range rule.prereqs {
		if prereq == binariesTarget {
			for _, nested := range rules[binariesTarget].prereqs {
				add(nested)
			}
			continue
		}
		if strings.HasPrefix(prereq, "$(") {
			continue
		}
		add(prereq)
	}
	return out
}

// helpNamesTarget reports whether a help string names a target as a whole
// word. Substring matching is not enough: "rebuilds" would otherwise pass for
// a dependency on `build`.
func helpNamesTarget(help, target string) bool {
	for _, word := range wordRe.FindAllString(strings.ToLower(help), -1) {
		if word == target {
			return true
		}
		if trimmed, ok := strings.CutSuffix(word, "s"); ok &&
			trimmed == target {
			return true
		}
	}
	return false
}

// TestMakefileHelpNamesDependencies checks `make help` describes what each
// target actually runs. A target that pulls in another documented target has
// to say so, otherwise the help output understates the work and contributors
// are surprised by, for example, `make test` rewriting go.mod.
func TestMakefileHelpNamesDependencies(t *testing.T) {
	root := repoRoot(t)
	rules, _, _ := parseMakefile(t, root)

	names := make([]string, 0, len(rules))
	for name := range rules {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		rule := rules[name]
		if rule.help == "" {
			continue
		}
		for _, prereq := range targetPrereqs(rule, rules) {
			if !helpNamesTarget(rule.help, prereq) {
				t.Errorf(
					"Makefile:%d: target %q runs %q first but its help "+
						"text %q does not mention it",
					rule.line,
					name,
					prereq,
					rule.help,
				)
			}
		}
	}
}

// TestMakefileDocumentedTargetsArePhony checks every target that appears in
// `make help` is declared .PHONY. All of them are commands, not files, so a
// stray file with a target's name must not silently skip the work.
func TestMakefileDocumentedTargetsArePhony(t *testing.T) {
	root := repoRoot(t)
	rules, _, phony := parseMakefile(t, root)

	names := make([]string, 0, len(rules))
	for name := range rules {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		rule := rules[name]
		if rule.help == "" || strings.HasPrefix(name, "$(") {
			continue
		}
		if !phony[name] {
			t.Errorf(
				"Makefile:%d: target %q is in `make help` but not .PHONY",
				rule.line,
				name,
			)
		}
	}
}

// TestDocumentedDefaultMakeMatchesMakefile checks every description of what a
// bare `make` does names exactly the targets the default rule depends on. It
// is the rule that catches documentation claiming the default target runs the
// test suite when it runs format and build.
func TestDocumentedDefaultMakeMatchesMakefile(t *testing.T) {
	root := repoRoot(t)
	rules, order, _ := parseMakefile(t, root)

	defaultTarget := defaultMakeTarget(t, order)
	def, ok := rules[defaultTarget]
	if !ok {
		t.Fatalf("Makefile has no %q target", defaultTarget)
	}
	want := targetPrereqs(def, rules)
	if len(want) == 0 {
		t.Fatalf("target %q has no target prerequisites", defaultTarget)
	}
	sort.Strings(want)

	checked := 0
	for _, rel := range contributorDocs {
		doc := readRepoFile(t, root, rel)
		for _, desc := range defaultMakeDescriptions(doc) {
			got := makeTargetWords(claimAboutDefault(desc.text), rules)
			if len(got) == 0 {
				continue
			}
			checked++
			if !slices.Equal(got, want) {
				t.Errorf(
					"%s describes the default `make` as %v but `make` runs "+
						"%v (from %q at Makefile:%d)",
					docLocation(rel, desc.line),
					got,
					want,
					defaultTarget+": "+strings.Join(def.prereqs, " "),
					def.line,
				)
			}
		}
	}
	if checked == 0 {
		t.Errorf(
			"no description of the default `make` target found in %v",
			contributorDocs,
		)
	}
}

// TestDocumentedMakeTargetsExist checks every `make <target>` a contributor
// is told to run is a real target.
func TestDocumentedMakeTargetsExist(t *testing.T) {
	root := repoRoot(t)
	rules, _, _ := parseMakefile(t, root)

	checked := 0
	for _, rel := range contributorDocs {
		doc := readRepoFile(t, root, rel)
		for _, ref := range makeTargetReferences(doc) {
			checked++
			if _, ok := rules[ref.text]; !ok {
				t.Errorf(
					"%s runs `make %s`, which is not a Makefile target",
					docLocation(rel, ref.line),
					ref.text,
				)
			}
		}
	}
	if checked == 0 {
		t.Error("no `make <target>` reference found in contributor docs")
	}
}

// docReference is a piece of text found at a known line of a document.
type docReference struct {
	line int
	text string
}

// defaultMakeDescriptions finds every place a document explains what a bare
// `make` does: a commented `make` line inside a code fence, a comment line
// directly above one, or prose about the default target.
func defaultMakeDescriptions(doc string) []docReference {
	var (
		found    []docReference
		prevLine string
		prevIdx  int
		tracker  fenceTracker
	)
	for i, line := range strings.Split(doc, "\n") {
		isMarker, insideCode := tracker.step(line)
		if isMarker {
			prevLine = ""
			continue
		}
		if insideCode {
			match := makeCommandRe.FindStringSubmatch(line)
			if match != nil && match[1] == "" {
				switch {
				case strings.TrimSpace(match[2]) != "":
					found = append(found, docReference{
						line: i + 1,
						text: match[2],
					})
				case strings.HasPrefix(strings.TrimSpace(prevLine), "#"):
					found = append(found, docReference{
						line: prevIdx + 1,
						text: strings.TrimSpace(prevLine)[1:],
					})
				}
			}
			prevLine = line
			prevIdx = i
			continue
		}
		lower := strings.ToLower(codeSpanRe.ReplaceAllString(line, " "))
		if strings.Contains(lower, "default target") ||
			(strings.Contains(lower, "default") &&
				strings.Contains(strings.ToLower(line), "`make`")) {
			found = append(found, docReference{line: i + 1, text: lower})
		}
	}
	return found
}

var (
	codeSpanRe = regexp.MustCompile("`[^`]*`")
	clauseRe   = regexp.MustCompile(`[.;:]\s|[.;:]$`)
)

// claimAboutDefault narrows a description to the clause that makes the claim,
// so a following sentence pointing at other targets ("run `make test` for
// those") is not read as part of what the default target does.
func claimAboutDefault(text string) string {
	lower := strings.ToLower(text)
	if !strings.Contains(lower, "default") {
		return text
	}
	for _, clause := range clauseRe.Split(text, -1) {
		if strings.Contains(strings.ToLower(clause), "default") {
			return clause
		}
	}
	return text
}

// makeTargetWords extracts the Makefile target names a description mentions,
// accepting the plural or third-person form ("formats", "builds", "tests").
func makeTargetWords(text string, rules map[string]makeRule) []string {
	var out []string
	for _, word := range wordRe.FindAllString(strings.ToLower(text), -1) {
		candidates := []string{word}
		if trimmed, ok := strings.CutSuffix(word, "s"); ok {
			candidates = append(candidates, trimmed)
		}
		for _, candidate := range candidates {
			if _, ok := rules[candidate]; !ok {
				continue
			}
			if !slices.Contains(out, candidate) {
				out = append(out, candidate)
			}
			break
		}
	}
	sort.Strings(out)
	return out
}

// makeTargetReferences finds every `make <target>` a document tells the
// reader to run, in code fences and inline code spans alike.
func makeTargetReferences(doc string) []docReference {
	var (
		found   []docReference
		tracker fenceTracker
	)
	for i, line := range strings.Split(doc, "\n") {
		isMarker, insideCode := tracker.step(line)
		if isMarker {
			continue
		}
		if insideCode {
			if match := makeCommandRe.FindStringSubmatch(line); match != nil &&
				match[1] != "" {
				found = append(found, docReference{
					line: i + 1,
					text: match[1],
				})
			}
			continue
		}
		for _, match := range inlineMakeRe.FindAllStringSubmatch(line, -1) {
			found = append(found, docReference{line: i + 1, text: match[1]})
		}
	}
	return found
}
