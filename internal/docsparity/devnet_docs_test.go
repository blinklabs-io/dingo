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
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// devnetDir is where the DevNet compose file, environment defaults, and
// wrapper scripts live.
var devnetDir = filepath.Join("internal", "test", "devnet")

// referenceNodeImage identifies the upstream Cardano implementation. A
// profile that pulls it is the conformance topology, not the all-Dingo one.
const referenceNodeImage = "cardano-node"

// conformanceFlag selects the reference topology on every DevNet script.
const conformanceFlag = "--conformance"

// devnetScripts are the entry points documentation tells contributors to run.
var devnetScripts = []string{"run-tests.sh", "start.sh", "stop.sh"}

type composeService struct {
	Image       string            `yaml:"image"`
	Profiles    []string          `yaml:"profiles"`
	Ports       []string          `yaml:"ports"`
	Environment map[string]string `yaml:"environment"`
}

type composeFile struct {
	Services map[string]composeService `yaml:"services"`
}

// portMapping is one published compose port.
type portMapping struct {
	envVar    string
	host      string
	container string
}

var (
	// portRe matches compose's [HOST_IP:]HOST_PORT:CONTAINER_PORT syntax. The
	// optional HOST_IP prefix (e.g. "127.0.0.1:") is skipped, not captured,
	// so group numbering stays the same whether or not a mapping binds to a
	// specific interface.
	portRe = regexp.MustCompile(
		`^(?:\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:)?(?:\$\{([A-Za-z0-9_]+):-(\d+)\}|(\d+)):(\d+)$`,
	)
	composeProfilesRe = regexp.MustCompile(
		`(?m)^COMPOSE_PROFILES=(\S+)`,
	)
	inlineProfilesRe = regexp.MustCompile(`COMPOSE_PROFILES=([A-Za-z0-9_-]+)`)
	portNumberRe     = regexp.MustCompile(`^3\d{3}$`)
	flagRe           = regexp.MustCompile(`--[a-z][a-z-]*`)
	scriptRefRe      = regexp.MustCompile(`(run-tests|start|stop)\.sh`)
	caseStartRe      = regexp.MustCompile(`^\s*case\b.*\bin\s*$`)
	caseEndRe        = regexp.MustCompile(`^\s*esac\b`)
	scriptFlagRe     = regexp.MustCompile(`^--[a-z][a-z-]*$`)
)

// scriptFlags returns flags handled by shell case arms. Looking only at case
// labels avoids accepting flags that occur in comments, usage text, or as a
// substring of another token.
func scriptFlags(script string) map[string]struct{} {
	flags := map[string]struct{}{}
	caseDepth := 0
	for line := range strings.SplitSeq(script, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case caseEndRe.MatchString(trimmed):
			if caseDepth > 0 {
				caseDepth--
			}
		case caseStartRe.MatchString(trimmed):
			caseDepth++
		case caseDepth > 0 && strings.HasPrefix(trimmed, "--"):
			close := strings.IndexByte(trimmed, ')')
			if close < 0 {
				continue
			}
			for pattern := range strings.SplitSeq(trimmed[:close], "|") {
				pattern = strings.TrimSpace(pattern)
				if scriptFlagRe.MatchString(pattern) {
					flags[pattern] = struct{}{}
				}
			}
		}
	}
	return flags
}

// ports parses the published ports of a service.
func (s composeService) ports() []portMapping {
	var out []portMapping
	for _, spec := range s.Ports {
		match := portRe.FindStringSubmatch(spec)
		if match == nil {
			continue
		}
		host := match[2]
		if host == "" {
			host = match[3]
		}
		out = append(out, portMapping{
			envVar:    match[1],
			host:      host,
			container: match[4],
		})
	}
	return out
}

// hostPorts returns every default host port a service publishes.
func (s composeService) hostPorts() []string {
	var out []string
	for _, mapping := range s.ports() {
		out = append(out, mapping.host)
	}
	return out
}

// isBlockProducer reports whether a node forges. Compose merge keys are
// resolved by the YAML decoder, so per-service overrides win.
func (s composeService) isBlockProducer() bool {
	return s.Environment["CARDANO_BLOCK_PRODUCER"] == "true"
}

// loadCompose parses the DevNet compose file.
func loadCompose(t *testing.T, root string) composeFile {
	t.Helper()

	raw := readRepoFile(t, root, filepath.Join(devnetDir, "docker-compose.yml"))
	var parsed composeFile
	if err := yaml.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("parse DevNet docker-compose.yml: %v", err)
	}
	if len(parsed.Services) == 0 {
		t.Fatal("DevNet docker-compose.yml declares no services")
	}
	return parsed
}

// defaultProfile returns the compose profile the checked-in .env selects.
func defaultProfile(t *testing.T, root string) string {
	t.Helper()

	env := readRepoFile(t, root, filepath.Join(devnetDir, ".env"))
	match := composeProfilesRe.FindStringSubmatch(env)
	if match == nil {
		t.Fatal("DevNet .env does not set COMPOSE_PROFILES")
	}
	return match[1]
}

// servicesInProfile returns the services belonging to a compose profile.
func servicesInProfile(
	compose composeFile,
	profile string,
) map[string]composeService {
	out := map[string]composeService{}
	for name, service := range compose.Services {
		if slices.Contains(service.Profiles, profile) {
			out[name] = service
		}
	}
	return out
}

// profileNames returns every profile declared in the compose file.
func profileNames(compose composeFile) []string {
	var out []string
	for _, service := range compose.Services {
		for _, profile := range service.Profiles {
			if !slices.Contains(out, profile) {
				out = append(out, profile)
			}
		}
	}
	sort.Strings(out)
	return out
}

// usesReferenceNode reports whether any service in the profile runs the
// upstream cardano-node image.
func usesReferenceNode(services map[string]composeService) bool {
	for _, service := range services {
		if strings.Contains(service.Image, referenceNodeImage) {
			return true
		}
	}
	return false
}

// TestDevNetDefaultProfileIsAllDingo checks the shipped default really is the
// all-Dingo network and that the reference implementation is reached only
// through the other profile.
func TestDevNetDefaultProfileIsAllDingo(t *testing.T) {
	root := repoRoot(t)
	compose := loadCompose(t, root)
	profiles := profileNames(compose)
	if len(profiles) != 2 {
		t.Fatalf("expected two DevNet profiles, found %v", profiles)
	}

	def := defaultProfile(t, root)
	if !slices.Contains(profiles, def) {
		t.Fatalf(".env selects profile %q, which compose does not define", def)
	}
	if usesReferenceNode(servicesInProfile(compose, def)) {
		t.Errorf(
			"default profile %q runs %s; docs describe the default as the "+
				"all-Dingo network",
			def,
			referenceNodeImage,
		)
	}
	for _, profile := range profiles {
		if profile == def {
			continue
		}
		if !usesReferenceNode(servicesInProfile(compose, profile)) {
			t.Errorf(
				"opt-in profile %q does not run %s; docs describe %s as the "+
					"Dingo/%s topology",
				profile,
				referenceNodeImage,
				conformanceFlag,
				referenceNodeImage,
			)
		}
	}
}

// TestReadmeDevNetTableMatchesCompose checks the README's DevNet service
// table lists the default profile's nodes with the roles and host ports
// compose actually gives them.
func TestReadmeDevNetTableMatchesCompose(t *testing.T) {
	root := repoRoot(t)
	compose := loadCompose(t, root)
	def := defaultProfile(t, root)
	services := servicesInProfile(compose, def)

	readme := readRepoFile(t, root, "README.md")
	rows := map[string]markdownTableRow{}
	for _, row := range markdownTableRows(readme) {
		if len(row.cells) == 0 {
			continue
		}
		name := unquote(row.cells[0])
		if _, ok := services[name]; ok {
			rows[name] = row
		}
	}

	names := make([]string, 0, len(services))
	for name := range services {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		service := services[name]
		hostPorts := service.hostPorts()
		if len(hostPorts) == 0 {
			continue
		}
		row, ok := rows[name]
		if !ok {
			t.Errorf(
				"README.md has no DevNet table row for %q, a node in the "+
					"default %q profile",
				name,
				def,
			)
			continue
		}
		// The service name itself is skipped: "dingo-relay" would satisfy a
		// search for "relay" no matter what the row went on to claim.
		text := strings.ToLower(strings.Join(row.cells[1:], " "))
		wantRole := "relay"
		if service.isBlockProducer() {
			wantRole = "producer"
		}
		if !strings.Contains(text, wantRole) {
			t.Errorf(
				"%s describes %q without calling it a %s; compose sets "+
					"CARDANO_BLOCK_PRODUCER=%q",
				docLocation("README.md", row.line),
				name,
				wantRole,
				service.Environment["CARDANO_BLOCK_PRODUCER"],
			)
		}
	}
}

// TestDevNetDocTablePortsMatchCompose checks every documented port next to a
// DevNet service name is a port compose really publishes.
func TestDevNetDocTablePortsMatchCompose(t *testing.T) {
	root := repoRoot(t)
	compose := loadCompose(t, root)

	envDefaults := map[string]string{}
	for _, service := range compose.Services {
		for _, mapping := range service.ports() {
			if mapping.envVar != "" {
				envDefaults[mapping.envVar] = mapping.host
			}
		}
	}

	checked := 0
	for _, rel := range contributorDocs {
		doc := readRepoFile(t, root, rel)
		for _, row := range markdownTableRows(doc) {
			if len(row.cells) == 0 {
				continue
			}
			key := unquote(row.cells[0])
			rest := strings.Join(row.cells[1:], " ")

			if service, ok := compose.Services[key]; ok {
				allowed := service.hostPorts()
				// Only a cell that is nothing but a port number is read as a
				// port, so a figure inside a description is not mistaken for
				// one.
				for _, cell := range row.cells[1:] {
					port := unquote(cell)
					if !portNumberRe.MatchString(port) {
						continue
					}
					checked++
					if !slices.Contains(allowed, port) {
						t.Errorf(
							"%s documents port %s for service %q; compose "+
								"publishes %v",
							docLocation(rel, row.line),
							port,
							key,
							allowed,
						)
					}
				}
				continue
			}

			want, ok := envDefaults[key]
			if !ok {
				continue
			}
			checked++
			if !strings.Contains(rest, want) {
				t.Errorf(
					"%s documents %q without its compose default %s",
					docLocation(rel, row.line),
					key,
					want,
				)
			}
		}
	}
	if checked == 0 {
		t.Error("no DevNet port documented in any contributor doc")
	}
}

// TestDevNetDefaultProfileNamedCorrectly checks prose that pins the default
// COMPOSE_PROFILES value against the checked-in .env.
func TestDevNetDefaultProfileNamedCorrectly(t *testing.T) {
	root := repoRoot(t)
	want := defaultProfile(t, root)

	for _, rel := range contributorDocs {
		doc := readRepoFile(t, root, rel)
		for i, line := range strings.Split(doc, "\n") {
			if !strings.Contains(strings.ToLower(line), "default") {
				continue
			}
			for _, match := range inlineProfilesRe.FindAllStringSubmatch(
				line,
				-1,
			) {
				if match[1] != want {
					t.Errorf(
						"%s calls %q the default; .env sets "+
							"COMPOSE_PROFILES=%s",
						docLocation(rel, i+1),
						match[0],
						want,
					)
				}
			}
		}
	}
}

// TestDevNetConformanceAttribution checks no passage attributes the reference
// node to the default DevNet without also naming the flag that selects it.
// This is the drift the docs had: describing the default network as Dingo
// beside cardano-node when that topology is opt-in.
func TestDevNetConformanceAttribution(t *testing.T) {
	root := repoRoot(t)

	for _, rel := range contributorDocs {
		doc := readRepoFile(t, root, rel)
		for _, block := range markdownBlocks(doc) {
			lower := strings.ToLower(strings.ReplaceAll(block.text, "`", ""))
			if !strings.Contains(lower, "default") {
				continue
			}
			if !mentionsReferenceNode(lower) {
				continue
			}
			if strings.Contains(lower, conformanceFlag) {
				continue
			}
			t.Errorf(
				"%s mentions %s while describing the default DevNet without "+
					"naming %s, which is what selects that topology",
				docLocation(rel, block.startLine),
				referenceNodeImage,
				conformanceFlag,
			)
		}
	}
}

// negations precede a reference-node mention that says the reference node is
// absent, such as "no cardano-node reference exists for this feature".
var negations = []string{"no ", "not ", "non-", "without ", "neither "}

// mentionsReferenceNode reports whether text claims the reference node is
// involved. Mentions that only state its absence do not count, so a passage
// explaining that a Dingo-only test has no cardano-node counterpart is not
// read as putting cardano-node in the default network.
func mentionsReferenceNode(lower string) bool {
	rest := lower
	for {
		idx := strings.Index(rest, referenceNodeImage)
		if idx < 0 {
			return false
		}
		prefix := rest[:idx]
		if len(prefix) > 24 {
			prefix = prefix[len(prefix)-24:]
		}
		negated := false
		for _, negation := range negations {
			if strings.Contains(prefix, negation) {
				negated = true
				break
			}
		}
		if !negated {
			return true
		}
		rest = rest[idx+len(referenceNodeImage):]
	}
}

// TestReadmeIdentifiesConformanceTopology checks the README says somewhere,
// in one breath, that the conformance flag is what brings up the reference
// node, and that its default DevNet counts match compose.
func TestReadmeIdentifiesConformanceTopology(t *testing.T) {
	root := repoRoot(t)
	compose := loadCompose(t, root)
	def := defaultProfile(t, root)
	services := servicesInProfile(compose, def)

	readme := readRepoFile(t, root, "README.md")
	section := markdownSection(readme, "## DevNet")
	if section == "" {
		t.Fatal("README.md has no `## DevNet` section")
	}

	found := false
	for _, block := range markdownBlocks(section) {
		lower := strings.ToLower(block.text)
		if strings.Contains(lower, conformanceFlag) &&
			strings.Contains(lower, referenceNodeImage) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf(
			"README.md `## DevNet` never says %s brings up %s",
			conformanceFlag,
			referenceNodeImage,
		)
	}

	var producers, relays int
	for _, service := range services {
		if len(service.hostPorts()) == 0 {
			continue
		}
		if service.isBlockProducer() {
			producers++
			continue
		}
		relays++
	}
	assertCountedRole(t, section, producers, "producer")
	assertCountedRole(t, section, relays, "relay")
}

// numberWords spell the small counts a topology description uses.
var numberWords = map[int]string{
	1: "one", 2: "two", 3: "three", 4: "four", 5: "five",
	6: "six", 7: "seven", 8: "eight", 9: "nine",
}

// assertCountedRole checks the README states how many nodes of a role the
// default DevNet has, in figures or in words.
func assertCountedRole(t *testing.T, section string, count int, role string) {
	t.Helper()

	if count == 0 {
		return
	}
	word, ok := numberWords[count]
	if !ok {
		word = fmt.Sprintf("%d", count)
	}
	pattern := fmt.Sprintf(
		`(?is)\b(%s|%d)\b[^.]{0,80}?%s`,
		regexp.QuoteMeta(word),
		count,
		regexp.QuoteMeta(role),
	)
	if !regexp.MustCompile(pattern).MatchString(section) {
		t.Errorf(
			"README.md `## DevNet` does not state that the default network "+
				"has %d %s node(s); compose defines that many",
			count,
			role,
		)
	}
}

// TestDocumentedDevNetFlagsExist checks every DevNet script flag in the docs
// is one the script handles.
func TestDocumentedDevNetFlagsExist(t *testing.T) {
	root := repoRoot(t)

	scriptFlagsByName := map[string]map[string]struct{}{}
	for _, name := range devnetScripts {
		scriptFlagsByName[name] = scriptFlags(
			readRepoFile(t, root, filepath.Join(devnetDir, name)),
		)
	}

	checked := 0
	for _, rel := range contributorDocs {
		doc := readRepoFile(t, root, rel)
		for i, line := range strings.Split(doc, "\n") {
			loc := scriptRefRe.FindStringIndex(line)
			if loc == nil {
				continue
			}
			script := scriptRefRe.FindString(line)
			if _, ok := scriptFlagsByName[script]; !ok {
				continue
			}
			for _, flag := range flagRe.FindAllString(line[loc[1]:], -1) {
				checked++
				if _, accepted := scriptFlagsByName[script][flag]; !accepted {
					t.Errorf(
						"%s documents `%s %s`, which the script does not "+
							"accept",
						docLocation(rel, i+1),
						script,
						flag,
					)
				}
			}
		}
	}
	if checked == 0 {
		t.Error("no DevNet script flag documented in contributor docs")
	}
}

// dingoModeMarkers put a code-fence example back into the default profile
// after a conformance example. They name the profile explicitly, because a
// bare "default" turns up in examples about unrelated defaults.
var dingoModeMarkers = []string{
	"dingo mode",
	"all-dingo",
	"dingo profile",
	"profiles=dingo",
}

// TestDevNetCommandExamplesUseProfileServices checks copy-paste commands name
// services that exist in the profile the surrounding example selects. A
// `docker compose logs` line for a conformance-only container does nothing
// after a default `./start.sh`, because compose never created it.
func TestDevNetCommandExamplesUseProfileServices(t *testing.T) {
	root := repoRoot(t)
	compose := loadCompose(t, root)
	def := defaultProfile(t, root)

	checked := 0
	for _, rel := range contributorDocs {
		doc := readRepoFile(t, root, rel)
		for _, block := range markdownBlocks(doc) {
			if !block.fenced {
				continue
			}
			profile := def
			for offset, line := range strings.Split(block.text, "\n") {
				lower := strings.ToLower(line)
				switch {
				case strings.Contains(lower, conformanceFlag),
					strings.Contains(lower, "profiles=conformance"),
					strings.Contains(lower, "conformance mode"),
					strings.Contains(lower, "conformance profile"):
					profile = "conformance"
				default:
					for _, marker := range dingoModeMarkers {
						if strings.Contains(lower, marker) {
							profile = def
							break
						}
					}
				}
				for token := range strings.FieldsSeq(line) {
					service, ok := compose.Services[token]
					if !ok {
						continue
					}
					checked++
					if slices.Contains(service.Profiles, profile) {
						continue
					}
					t.Errorf(
						"%s uses service %q in a %q example; compose puts "+
							"it in %v",
						docLocation(rel, block.startLine+offset),
						token,
						profile,
						service.Profiles,
					)
				}
			}
		}
	}
	if checked == 0 {
		t.Error("no DevNet service named in any documented command")
	}
}

// markdownSection returns the text of a document from a heading up to the
// next heading of the same or a higher level.
func markdownSection(doc, heading string) string {
	level := len(heading) - len(strings.TrimLeft(heading, "#"))
	lines := strings.Split(doc, "\n")
	start := -1
	for i, line := range lines {
		if strings.TrimSpace(line) == heading {
			start = i
			break
		}
	}
	if start < 0 {
		return ""
	}
	for i := start + 1; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(trimmed, "#") {
			continue
		}
		found := len(trimmed) - len(strings.TrimLeft(trimmed, "#"))
		if found <= level {
			return strings.Join(lines[start:i], "\n")
		}
	}
	return strings.Join(lines[start:], "\n")
}
