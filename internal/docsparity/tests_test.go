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
	"maps"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
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
	// portRe matches compose's [HOST_IP:]HOST_PORT:CONTAINER_PORT syntax,
	// including a bracketed IPv6 host address (e.g. "[::1]:"), the form
	// compose itself requires for IPv6 so the address's own colons don't
	// get parsed as field separators. The optional HOST_IP prefix is
	// skipped, not captured, so group numbering stays the same whether or
	// not a mapping binds to a specific interface.
	portRe = regexp.MustCompile(
		`^(?:(?:\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|\[[0-9a-fA-F:]+\]):)?(?:\$\{([A-Za-z0-9_]+):-(\d+)\}|(\d+)):(\d+)$`,
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

// TestComposeServicePorts_ParsesEveryHostIPForm covers compose's
// [HOST_IP:]HOST_PORT:CONTAINER_PORT syntax across every host-address form
// this repo's compose files use or could plausibly need: no host IP at
// all, a literal IPv4 address, a bracketed IPv6 address (which needs its
// own bracket handling since the address's own colons would otherwise be
// parsed as field separators), and the ${VAR:-default} form with an IPv4
// prefix. A mapping portRe fails to match is silently dropped from
// ports()/hostPorts(), which would make the docs-parity checks that
// consume them pass vacuously instead of catching a real mismatch -- so
// this asserts each spec produces a mapping, not just that parsing doesn't
// panic.
func TestComposeServicePorts_ParsesEveryHostIPForm(t *testing.T) {
	svc := composeService{Ports: []string{
		"3010:3001",
		"127.0.0.1:3030:3002",
		"[::1]:6001:6001",
		"${DEVNET_DINGO_NTC_PORT:-3030}:3002",
		"127.0.0.1:${DEVNET_DINGO_NTC_PORT:-3030}:3002",
	}}
	mappings := svc.ports()
	if len(mappings) != len(svc.Ports) {
		t.Fatalf(
			"every spec is a valid compose port mapping and must produce one: got %d mappings from %d specs: %+v",
			len(mappings),
			len(svc.Ports),
			mappings,
		)
	}

	want := []string{"3010", "3030", "6001", "3030", "3030"}
	got := svc.hostPorts()
	if !slices.Equal(got, want) {
		t.Errorf("hostPorts() = %v, want %v", got, want)
	}
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
// all-Dingo network and that only the conformance profile uses the reference
// implementation.
func TestDevNetDefaultProfileIsAllDingo(t *testing.T) {
	root := repoRoot(t)
	compose := loadCompose(t, root)
	profiles := profileNames(compose)
	for _, profile := range []string{"dingo", "conformance", "koios-parity"} {
		if !slices.Contains(profiles, profile) {
			t.Fatalf("expected DevNet profile %q, found %v", profile, profiles)
		}
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
	if !usesReferenceNode(servicesInProfile(compose, "conformance")) {
		t.Errorf(
			"conformance profile does not run %s; docs describe %s as the "+
				"Dingo/%s topology",
			referenceNodeImage,
			conformanceFlag,
			referenceNodeImage,
		)
	}
	if usesReferenceNode(servicesInProfile(compose, "koios-parity")) {
		t.Error("koios-parity profile must use the local Dingo image")
	}
}

// TestReadmeLinksDevNetDocs checks the README points contributors to the
// detailed DevNet instructions.
func TestReadmeLinksDevNetDocs(t *testing.T) {
	root := repoRoot(t)
	readme := readRepoFile(t, root, "README.md")
	if !regexp.MustCompile(`\[[^]]+\]\(docs/devnet\.md\)`).MatchString(readme) {
		t.Error("README.md does not link to docs/devnet.md")
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
		for _, block := range markdownBlocks(doc) {
			if block.fenced {
				continue
			}
			for i, line := range strings.Split(block.text, "\n") {
				for _, match := range goPrereqRe.FindAllStringSubmatch(line, -1) {
					got, ok := parseGoRelease(match[1] + "." + match[2])
					if !ok {
						continue
					}
					if got != want {
						t.Errorf(
							"%s states %q but go.mod requires Go %s",
							docLocation(rel, block.startLine+i),
							strings.TrimSpace(match[0]),
							want,
						)
					}
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

// koiosCoverageDoc is the document carrying the Koios coverage table, and
// koiosCoverageHeader is the header row that identifies it. Renaming either
// fails this check rather than skipping it, so the table cannot be moved out
// from under the rule.
const koiosCoverageDoc = "ARCHITECTURE.md"

var koiosCoverageHeader = []string{
	"Koios endpoint",
	"Classification",
	"Fields",
	"Dingo mapping / reason",
}

// koiosFieldKey identifies one row of the coverage contract. The classification
// of a (endpoint, field) pair is what a reader acts on: an exact-match or
// derived-match field is covered by a PASS, and the other two classes are not.
type koiosFieldKey struct {
	endpoint string
	field    string
}

// koiosDocEntry is one documented classification and where it is written.
type koiosDocEntry struct {
	class string
	line  int
}

// koiosCoverageDocEntries returns the classification the coverage table states
// for each (endpoint, field) pair, keeping wildcard entries separate.
//
// A field written with a trailing `*` stands for a group the table
// deliberately abbreviates, such as the Conway `pvt_*` voting thresholds. It
// covers every matrix field of that endpoint sharing the prefix, and a
// wildcard matching nothing is itself drift.
func koiosCoverageDocEntries(
	t *testing.T,
	root string,
) (map[koiosFieldKey]koiosDocEntry, map[koiosFieldKey]koiosDocEntry) {
	t.Helper()

	table, err := parseKoiosCoverageTable(
		readRepoFile(t, root, koiosCoverageDoc),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, problem := range table.problems {
		t.Error(problem)
	}
	return table.exact, table.wildcard
}

// koiosCoverageTable is one parsed coverage table: the classifications it
// states, and the row-level faults found while reading it.
//
// Parsing is separated from reporting so the parser's own rules can be checked
// against a table written to break them, rather than only against whatever
// ARCHITECTURE.md happens to contain today.
type koiosCoverageTable struct {
	exact    map[koiosFieldKey]koiosDocEntry
	wildcard map[koiosFieldKey]koiosDocEntry
	problems []string
}

// parseKoiosCoverageTable reads the coverage table out of doc.
//
// Every table carrying the coverage header is read, not just the first. A
// second table under the same header states the coverage contract just as the
// first does, so reading only one leaves it unchecked -- and a contradiction
// split across two tables would escape the duplicate-row rule that rejects the
// same contradiction inside one.
//
// The error covers the two conditions that leave nothing to check at all: no
// such table, or no field rows in any of them. problems holds the per-row
// faults, which are each worth reporting without abandoning the rest of the
// table.
func parseKoiosCoverageTable(doc string) (koiosCoverageTable, error) {
	lines := strings.Split(doc, "\n")
	insideFence := make([]bool, len(lines))
	var fences fenceTracker
	for i, line := range lines {
		_, insideFence[i] = fences.step(line)
	}

	var headers []int
	for i, line := range lines {
		if insideFence[i] {
			continue
		}
		if !strings.Contains(line, "|") {
			continue
		}
		cells := splitTableCells(line)
		if len(cells) != len(koiosCoverageHeader) {
			continue
		}
		matched := true
		for j, want := range koiosCoverageHeader {
			if cells[j] != want {
				matched = false
				break
			}
		}
		if matched {
			headers = append(headers, i)
		}
	}
	if len(headers) == 0 {
		return koiosCoverageTable{}, fmt.Errorf(
			"%s has no table headed %q; the Koios coverage contract is "+
				"unchecked until it is restored",
			koiosCoverageDoc,
			strings.Join(koiosCoverageHeader, " | "),
		)
	}

	table := koiosCoverageTable{
		exact:    make(map[koiosFieldKey]koiosDocEntry),
		wildcard: make(map[koiosFieldKey]koiosDocEntry),
	}
	for headerIndex, header := range headers {
		end := len(lines)
		if headerIndex+1 < len(headers) {
			end = headers[headerIndex+1]
		}
		for i := header + 2; i < end; i++ {
			if insideFence[i] {
				continue
			}
			if !strings.HasPrefix(strings.TrimSpace(lines[i]), "|") {
				break
			}
			cells := splitTableCells(lines[i])
			if len(cells) < 3 {
				table.problems = append(table.problems, fmt.Sprintf(
					"%s: coverage row has %d cells, want at least 3",
					docLocation(koiosCoverageDoc, i+1),
					len(cells),
				))
				continue
			}
			endpoint := unquote(cells[0])
			class := unquote(cells[1])
			fields := 0
			for field := range strings.SplitSeq(cells[2], ",") {
				field = unquote(field)
				if field == "" {
					continue
				}
				fields++
				key := koiosFieldKey{endpoint: endpoint, field: field}
				entry := koiosDocEntry{class: class, line: i + 1}
				target := table.exact
				if strings.HasSuffix(field, "*") {
					target = table.wildcard
				}
				// Assigning over an existing key would drop the earlier row. A
				// wrong classification followed by a correct duplicate would
				// then leave only the correct one to compare, so the table
				// would pass while still telling a reader two different things
				// about the same field.
				if previous, duplicate := target[key]; duplicate {
					table.problems = append(table.problems, fmt.Sprintf(
						"%s: duplicate coverage row for %s %s, already "+
							"documented at %s",
						docLocation(koiosCoverageDoc, entry.line),
						key.endpoint,
						key.field,
						docLocation(koiosCoverageDoc, previous.line),
					))
					continue
				}
				target[key] = entry
			}
			// A row whose Fields cell names nothing records no classification,
			// so every later check skips it: it is neither compared against
			// the matrix nor rejected for an unknown classification. Dropping
			// it silently makes an incomplete row read as a documented one.
			if fields == 0 {
				table.problems = append(table.problems, fmt.Sprintf(
					"%s: coverage row for %s names no field, so its "+
						"classification %q is never checked",
					docLocation(koiosCoverageDoc, i+1),
					endpoint,
					class,
				))
			}
		}
	}
	if len(table.exact)+len(table.wildcard) == 0 {
		return koiosCoverageTable{}, fmt.Errorf(
			"%s: the coverage table has no field rows",
			docLocation(koiosCoverageDoc, headers[0]+1),
		)
	}
	return table, nil
}

// koiosCoverageClasses returns every classification the code defines, so an
// unrecognised value in the table is reported as such rather than as a
// mismatch against every field that carries it.
func koiosCoverageClasses() map[string]bool {
	return map[string]bool{
		string(koiosparity.CoverageExactMatch):                true,
		string(koiosparity.CoverageDerivedMatch):              true,
		string(koiosparity.CoverageIntentionallyIncomparable): true,
		string(koiosparity.CoverageUnsupported):               true,
	}
}

// TestArchitectureDocumentsKoiosCoverageMatrix checks the Koios coverage table
// against koiosparity.KoiosCoverageMatrix, which is the contract the parity
// checker actually applies.
//
// The classification is the load-bearing part: a PASS covers only the
// exact-match and derived-match fields, so a table that classifies a field
// differently from the code tells an operator that a field is checked when it
// is not, or the reverse. This compares the endpoint, field and classification
// in both directions and leaves the mapping/reason prose alone, so rewording a
// reason does not fail the check.
func TestArchitectureDocumentsKoiosCoverageMatrix(t *testing.T) {
	root := repoRoot(t)
	exact, wildcard := koiosCoverageDocEntries(t, root)
	classes := koiosCoverageClasses()

	for key, entry := range exact {
		if !classes[entry.class] {
			t.Errorf(
				"%s: %s %s has unknown classification %q",
				docLocation(koiosCoverageDoc, entry.line),
				key.endpoint,
				key.field,
				entry.class,
			)
		}
	}
	for key, entry := range wildcard {
		if !classes[entry.class] {
			t.Errorf(
				"%s: %s %s has unknown classification %q",
				docLocation(koiosCoverageDoc, entry.line),
				key.endpoint,
				key.field,
				entry.class,
			)
		}
	}

	matrix := koiosparity.KoiosCoverageMatrix()
	if len(matrix) == 0 {
		t.Fatal("koiosparity.KoiosCoverageMatrix is empty")
	}

	usedWildcard := make(map[koiosFieldKey]bool)
	for _, field := range matrix {
		key := koiosFieldKey{endpoint: field.Endpoint, field: field.Field}
		class := string(field.Class)
		if entry, ok := exact[key]; ok {
			if entry.class != class {
				t.Errorf(
					"%s: %s %s is documented as %s but "+
						"koiosCoverageMatrix classifies it %s",
					docLocation(koiosCoverageDoc, entry.line),
					key.endpoint,
					key.field,
					entry.class,
					class,
				)
			}
			continue
		}
		matchKey, entry, ok := koiosWildcardFor(wildcard, key)
		if !ok {
			t.Errorf(
				"%s documents no row for %s %s (%s); every field in "+
					"koiosCoverageMatrix belongs in the coverage table",
				koiosCoverageDoc,
				key.endpoint,
				key.field,
				class,
			)
			continue
		}
		usedWildcard[matchKey] = true
		if entry.class != class {
			t.Errorf(
				"%s: %s %s covers %s, which koiosCoverageMatrix "+
					"classifies %s and not %s",
				docLocation(koiosCoverageDoc, entry.line),
				matchKey.endpoint,
				matchKey.field,
				key.field,
				class,
				entry.class,
			)
		}
	}

	documented := make(map[koiosFieldKey]bool, len(matrix))
	for _, field := range matrix {
		documented[koiosFieldKey{
			endpoint: field.Endpoint,
			field:    field.Field,
		}] = true
	}
	for _, key := range koiosSortedKeys(exact) {
		if documented[key] {
			continue
		}
		t.Errorf(
			"%s: %s %s is documented but koiosCoverageMatrix has no such "+
				"field; the table describes coverage the checker does not "+
				"apply",
			docLocation(koiosCoverageDoc, exact[key].line),
			key.endpoint,
			key.field,
		)
	}
	for _, key := range koiosSortedKeys(wildcard) {
		if usedWildcard[key] {
			continue
		}
		t.Errorf(
			"%s: %s %s matches no field in koiosCoverageMatrix",
			docLocation(koiosCoverageDoc, wildcard[key].line),
			key.endpoint,
			key.field,
		)
	}
}

// TestKoiosCoverageTableRejectsDuplicateRows pins the duplicate check in
// parseKoiosCoverageTable.
//
// The classifications are read into a map keyed by (endpoint, field), so a
// second row for a key would otherwise assign over the first. A table that
// states a wrong classification and then contradicts it with a correct
// duplicate would be read as stating only the correct one, and would pass
// while still telling a reader two different things about the same field.
//
// Both the exact and the wildcard map are checked, because they are separate
// maps and a check added to one is not a check on the other.
func TestKoiosCoverageTableRejectsDuplicateRows(t *testing.T) {
	t.Parallel()

	doc := strings.Join([]string{
		"| " + strings.Join(koiosCoverageHeader, " | ") + " |",
		"| --- | --- | --- | --- |",
		"| `/tip` | exact-match | `abs_slot` | mapped |",
		"| `/tip` | unsupported | `abs_slot` | contradicts the row above |",
		"| `/epoch_params` | exact-match | `pvt_*` | mapped |",
		"| `/epoch_params` | unsupported | `pvt_*` | contradicts it |",
	}, "\n")

	table, err := parseKoiosCoverageTable(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(table.problems) != 2 {
		t.Fatalf(
			"want both duplicate rows reported, got %d problem(s): %v",
			len(table.problems),
			table.problems,
		)
	}
	for _, want := range []string{
		"duplicate coverage row for /tip abs_slot",
		"duplicate coverage row for /epoch_params pvt_*",
	} {
		found := false
		for _, problem := range table.problems {
			if strings.Contains(problem, want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("no problem reports %q; got %v", want, table.problems)
		}
	}

	// The first row of each pair is the one kept. Reporting the duplicate is
	// the whole point, so which row survives only has to be deterministic.
	exactKey := koiosFieldKey{endpoint: "/tip", field: "abs_slot"}
	if got := table.exact[exactKey].class; got != "exact-match" {
		t.Errorf(
			"exact row for %v kept class %q, want the first row",
			exactKey,
			got,
		)
	}
	wildcardKey := koiosFieldKey{endpoint: "/epoch_params", field: "pvt_*"}
	if got := table.wildcard[wildcardKey].class; got != "exact-match" {
		t.Errorf(
			"wildcard row for %v kept class %q, want the first row",
			wildcardKey,
			got,
		)
	}
}

// TestKoiosCoverageTableRejectsRowWithNoField pins the empty-Fields check in
// parseKoiosCoverageTable.
//
// Every later check keys off a (endpoint, field) pair, so a row whose Fields
// cell names nothing contributes no pair and is skipped by all of them: it is
// neither compared against koiosCoverageMatrix nor rejected for an unknown
// classification. Without this, a row carrying an undefined classification for
// an endpoint the matrix has never heard of reads as documented coverage and
// passes.
func TestKoiosCoverageTableRejectsRowWithNoField(t *testing.T) {
	t.Parallel()

	doc := strings.Join([]string{
		"| " + strings.Join(koiosCoverageHeader, " | ") + " |",
		"| --- | --- | --- | --- |",
		"| `/tip` | exact-match | `abs_slot` | mapped |",
		"| `/account_info` | bogus-class |  | fields not filled in |",
	}, "\n")

	table, err := parseKoiosCoverageTable(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	const want = "/account_info names no field"
	found := false
	for _, problem := range table.problems {
		if strings.Contains(problem, want) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("no problem reports %q; got %v", want, table.problems)
	}
	// The rest of the table still parses, so one incomplete row does not cost
	// the checks on every other row.
	if _, ok := table.exact[koiosFieldKey{
		endpoint: "/tip",
		field:    "abs_slot",
	}]; !ok {
		t.Error("the complete row was dropped alongside the incomplete one")
	}
}

// TestKoiosCoverageTableReadsEverySuchTable pins that a second table under the
// coverage header is read too.
//
// Reading only the first leaves any later one unchecked, so a contradiction
// split across two tables would pass the duplicate-row rule that rejects the
// same contradiction inside one, and a row naming an endpoint the matrix lacks
// would never be compared.
func TestKoiosCoverageTableReadsEverySuchTable(t *testing.T) {
	t.Parallel()

	doc := strings.Join([]string{
		"| " + strings.Join(koiosCoverageHeader, " | ") + " |",
		"| --- | --- | --- | --- |",
		"| `/tip` | exact-match | `abs_slot` | mapped |",
		"",
		"Prose between the two tables.",
		"",
		"| " + strings.Join(koiosCoverageHeader, " | ") + " |",
		"| --- | --- | --- | --- |",
		"| `/tip` | unsupported | `abs_slot` | contradicts the first table |",
		"| `/nope` | exact-match | `bogus` | an endpoint the matrix lacks |",
	}, "\n")

	table, err := parseKoiosCoverageTable(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	const wantDuplicate = "duplicate coverage row for /tip abs_slot"
	found := false
	for _, problem := range table.problems {
		if strings.Contains(problem, wantDuplicate) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf(
			"no problem reports %q; got %v",
			wantDuplicate,
			table.problems,
		)
	}
	// The second table's other row has to reach the maps, or the comparison
	// against koiosCoverageMatrix never sees it either.
	if _, ok := table.exact[koiosFieldKey{
		endpoint: "/nope",
		field:    "bogus",
	}]; !ok {
		t.Error("the second table's rows were not read")
	}
}

func TestKoiosCoverageTableIgnoresFencedCopies(t *testing.T) {
	t.Parallel()

	header := "| " + strings.Join(koiosCoverageHeader, " | ") + " |"
	doc := strings.Join([]string{
		header,
		"| --- | --- | --- | --- |",
		"| `/tip` | exact-match | `abs_slot` | mapped |",
		"",
		"```markdown",
		header,
		"| --- | --- | --- | --- |",
		"| `/tip` | unsupported | `abs_slot` | example only |",
		"```",
	}, "\n")

	table, err := parseKoiosCoverageTable(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(table.problems) != 0 {
		t.Fatalf(
			"fenced copy changed the coverage contract: %v",
			table.problems,
		)
	}
	key := koiosFieldKey{endpoint: "/tip", field: "abs_slot"}
	if got := table.exact[key].class; got != "exact-match" {
		t.Errorf("parsed class %q, want the unfenced row", got)
	}
}

func TestKoiosCoverageTableBoundsAdjacentTables(t *testing.T) {
	t.Parallel()

	header := "| " + strings.Join(koiosCoverageHeader, " | ") + " |"
	doc := strings.Join([]string{
		header,
		"| --- | --- | --- | --- |",
		"| `/tip` | exact-match | `abs_slot` | mapped |",
		header,
		"| --- | --- | --- | --- |",
		"| `/tip` | unsupported | `abs_slot` | contradicts the first table |",
		"| `/nope` | exact-match | `bogus` | second table row |",
	}, "\n")

	table, err := parseKoiosCoverageTable(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(table.problems) != 1 ||
		!strings.Contains(
			table.problems[0],
			"duplicate coverage row for /tip abs_slot",
		) {
		t.Fatalf(
			"adjacent tables produced unexpected problems: %v",
			table.problems,
		)
	}
	if _, ok := table.exact[koiosFieldKey{
		endpoint: "/nope",
		field:    "bogus",
	}]; !ok {
		t.Error("the adjacent table's distinct row was not read")
	}
}

func TestKoiosCoverageTableAcceptsWildcardOnlyRows(t *testing.T) {
	t.Parallel()

	doc := strings.Join([]string{
		"| " + strings.Join(koiosCoverageHeader, " | ") + " |",
		"| --- | --- | --- | --- |",
		"| `/epoch_params` | unsupported | `pvt_*` | grouped fields |",
	}, "\n")

	table, err := parseKoiosCoverageTable(doc)
	if err != nil {
		t.Fatalf("parse wildcard-only table: %v", err)
	}
	if len(table.exact) != 0 {
		t.Fatalf("wildcard-only table produced exact rows: %v", table.exact)
	}
	key := koiosFieldKey{endpoint: "/epoch_params", field: "pvt_*"}
	if got := table.wildcard[key].class; got != "unsupported" {
		t.Errorf("wildcard class %q, want unsupported", got)
	}
}

// TestKoiosWildcardForPrefersLongestPrefix pins the wildcard selection.
//
// One endpoint may carry nested wildcards, `pvt_*` alongside `pvt_motion_*`.
// Taking the first in sort order compares a pvt_motion_ field against the
// broader row's classification and then reports the narrower row as matching
// nothing at all.
func TestKoiosWildcardForPrefersLongestPrefix(t *testing.T) {
	t.Parallel()

	wildcard := map[koiosFieldKey]koiosDocEntry{
		{endpoint: "/epoch_params", field: "pvt_*"}: {
			class: "unsupported",
			line:  1,
		},
		{endpoint: "/epoch_params", field: "pvt_motion_*"}: {
			class: "exact-match",
			line:  2,
		},
	}

	key := koiosFieldKey{
		endpoint: "/epoch_params",
		field:    "pvt_motion_no_confidence",
	}
	match, entry, ok := koiosWildcardFor(wildcard, key)
	if !ok {
		t.Fatalf("%v matched no wildcard row", key)
	}
	if match.field != "pvt_motion_*" {
		t.Errorf("matched %q, want the longest matching prefix %q",
			match.field, "pvt_motion_*")
	}
	if entry.class != "exact-match" {
		t.Errorf("matched class %q, want %q", entry.class, "exact-match")
	}

	// A field only the broader row covers still resolves to it.
	broad := koiosFieldKey{endpoint: "/epoch_params", field: "pvt_committee"}
	match, _, ok = koiosWildcardFor(wildcard, broad)
	if !ok || match.field != "pvt_*" {
		t.Errorf("%v matched %q (ok=%v), want %q",
			broad, match.field, ok, "pvt_*")
	}
}

// koiosWildcardFor returns the wildcard row covering key, if any.
func koiosWildcardFor(
	wildcard map[koiosFieldKey]koiosDocEntry,
	key koiosFieldKey,
) (koiosFieldKey, koiosDocEntry, bool) {
	var (
		best  koiosFieldKey
		found bool
	)
	for _, candidate := range koiosSortedKeys(wildcard) {
		if candidate.endpoint != key.endpoint {
			continue
		}
		prefix := strings.TrimSuffix(candidate.field, "*")
		if prefix == "" || !strings.HasPrefix(key.field, prefix) {
			continue
		}
		// One endpoint may carry nested wildcards, `pvt_*` alongside
		// `pvt_motion_*`. Taking the first in sort order compares a
		// pvt_motion_ field against the broader row's classification and then
		// reports the narrower row as matching nothing. The longest matching
		// prefix is the row a reader would take as governing the field.
		if found && len(best.field) >= len(candidate.field) {
			continue
		}
		best, found = candidate, true
	}
	if !found {
		return koiosFieldKey{}, koiosDocEntry{}, false
	}
	return best, wildcard[best], true
}

// koiosSortedKeys orders keys so failures are reported deterministically.
func koiosSortedKeys(m map[koiosFieldKey]koiosDocEntry) []koiosFieldKey {
	keys := make([]koiosFieldKey, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].endpoint != keys[j].endpoint {
			return keys[i].endpoint < keys[j].endpoint
		}
		return keys[i].field < keys[j].field
	})
	return keys
}

// lintWorkflows are the workflows that render the `lint` check. Branch
// protection matches a required context by name, so the coverage rules below
// are pinned to these files rather than searching every workflow for a
// golangci-lint invocation.
//
// There are two because `needs:` cannot cross workflow files: the lint job has
// to live inside each pipeline it gates. go-test.yml is the pull-request
// pipeline and publish.yml the main and release pipeline, and only one of them
// runs for any given event. Every rule below is checked against both, so
// covering a new module in one pipeline and forgetting the other fails here.
var lintWorkflows = []string{
	".github/workflows/go-test.yml",
	".github/workflows/publish.yml",
}

// lintActionRepo is the action whose invocations count as lint coverage. The
// version suffix is stripped before comparison so a bump does not silently
// drop a module from the check.
const lintActionRepo = "golangci/golangci-lint-action"

// workflowStep is the part of an Actions step these rules read: which action
// it runs, the directory it runs in, and the target platform it runs for.
type workflowStep struct {
	Uses string            `yaml:"uses"`
	Env  map[string]string `yaml:"env"`
	With struct {
		WorkingDirectory string `yaml:"working-directory"`
	} `yaml:"with"`
}

// workflowJob is one job's steps.
type workflowJob struct {
	Steps []workflowStep `yaml:"steps"`
}

// actionsWorkflow is the minimal shape of a workflow file.
type actionsWorkflow struct {
	Jobs map[string]workflowJob `yaml:"jobs"`
}

// lintRun is one golangci-lint invocation: the module directory it covers and
// the GOOS it covers it for.
type lintRun struct {
	dir  string
	goos string
}

// defaultLintGOOS is the platform a step with no GOOS override runs as. The
// lint job runs on ubuntu-latest, so an unset GOOS means linux.
const defaultLintGOOS = "linux"

// goModuleDirs returns the repository-relative directory of every Go module
// in the tree, with "." for the root module. This is the source of truth the
// lint workflow is checked against: a module that exists in the tree but not
// in CI is the gap these rules exist to catch, so adding a nested module
// fails this check until the workflow covers it.
func goModuleDirs(t *testing.T, root string) []string {
	t.Helper()

	// filesMatching passes a repository-relative path, so match on the base
	// name: comparing the whole path would find only the root module and
	// leave this check passing vacuously.
	mods := filesMatching(t, root, func(rel string) bool {
		return filepath.Base(rel) == "go.mod"
	})
	dirs := make([]string, 0, len(mods))
	for _, rel := range mods {
		dirs = append(dirs, filepath.ToSlash(filepath.Dir(rel)))
	}
	sort.Strings(dirs)
	return dirs
}

// lintRuns returns every golangci-lint invocation the named workflow makes.
func lintRuns(t *testing.T, root, workflow string) []lintRun {
	t.Helper()

	raw := readRepoFile(t, root, workflow)
	var parsed actionsWorkflow
	if err := yaml.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("parse %s: %v", workflow, err)
	}

	var runs []lintRun
	for _, job := range parsed.Jobs {
		for _, step := range job.Steps {
			action, _, _ := strings.Cut(step.Uses, "@")
			if action != lintActionRepo {
				continue
			}
			dir := filepath.ToSlash(
				strings.TrimSpace(step.With.WorkingDirectory),
			)
			if dir == "" {
				dir = "."
			}
			goos := strings.TrimSpace(step.Env["GOOS"])
			if goos == "" {
				goos = defaultLintGOOS
			}
			runs = append(runs, lintRun{dir: dir, goos: goos})
		}
	}
	if len(runs) == 0 {
		t.Fatalf(
			"%s runs no %s step",
			workflow,
			lintActionRepo,
		)
	}
	return runs
}

// TestLintCoversEveryGoModule checks that the lint job runs golangci-lint
// against every Go module in the tree on the default platform. A nested
// module has its own go.mod, so the root module's `./...` never reaches it:
// without a run of its own, a green `lint` check says nothing about that
// module's code.
//
// Only default-GOOS runs count. A GOOS=windows run builds a different set of
// files, so letting it satisfy a module would allow the linux run for that
// module to be dropped while this check stayed green.
func TestLintCoversEveryGoModule(t *testing.T) {
	root := repoRoot(t)
	modules := goModuleDirs(t, root)

	for _, workflow := range lintWorkflows {
		covered := make(map[string]bool)
		for _, run := range lintRuns(t, root, workflow) {
			if run.goos == defaultLintGOOS {
				covered[run.dir] = true
			}
		}

		for _, dir := range modules {
			if !covered[dir] {
				t.Errorf(
					"module %s has a go.mod but %s never lints it on "+
						"%s; add a golangci-lint step with "+
						"working-directory: %s",
					dir,
					workflow,
					defaultLintGOOS,
					dir,
				)
			}
		}
	}
}

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
	topLevel, err := exec.Command("git", "-C", root, "rev-parse", "--show-toplevel").
		Output()
	if err != nil ||
		!sameDiscoveryRoot(rootAbs, strings.TrimSpace(string(topLevel))) {
		return filesMatchingWalk(t, rootAbs, match)
	}
	cmd := exec.Command("git", "-C", root, "ls-files", "-z")
	output, err := cmd.Output()
	if err != nil {
		return filesMatchingWalk(t, rootAbs, match)
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
	err := filepath.WalkDir(
		root,
		func(path string, entry os.DirEntry, err error) error {
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
		},
	)
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
	".tools",
	".worktrees",
}

// excludedDiscoveryDirectories are generic dependency and VCS directories
// that may occur below any fallback-discovery root. Unlike generated worktree
// roots, these must be excluded recursively at every path depth.
var excludedDiscoveryDirectories = []string{
	".git",
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
	for component := range strings.SplitSeq(rel, "/") {
		if slices.Contains(excludedDiscoveryDirectories, component) {
			return true
		}
	}
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
				t.Fatalf(
					"isExcludedDiscoveryPath(%q) = %v, want %v",
					tt.input,
					got,
					tt.excluded,
				)
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
	writeTestFile(
		t,
		nested,
		".codex/worktrees/scratch/.github/workflows/ignored.yml",
	)
	got, want = markdownFiles(t, nested), []string{"docs/archive.md"}
	if !slices.Equal(got, want) {
		t.Errorf("nested markdownFiles() = %v, want %v", got, want)
	}
	got, want = dockerfiles(t, nested), []string{"Dockerfile"}
	if !slices.Equal(got, want) {
		t.Errorf("nested dockerfiles() = %v, want %v", got, want)
	}
	got, want = workflowFiles(
		t,
		nested,
	), []string{
		".github/workflows/archive.yml",
	}
	if !slices.Equal(got, want) {
		t.Errorf("nested workflowFiles() = %v, want %v", got, want)
	}

	archive := t.TempDir()
	writeTestFile(t, archive, "docs/tracked.md")
	writeTestFile(t, archive, ".claude/worktrees/scratch/ignored.md")
	writeTestFile(t, archive, ".codex/worktrees/scratch/ignored.md")
	writeTestFile(t, archive, ".codex/worktrees/scratch/Dockerfile")
	writeTestFile(
		t,
		archive,
		".codex/worktrees/scratch/.github/workflows/ignored.yml",
	)
	writeTestFile(t, archive, ".agents/worktrees/scratch/ignored.md")
	writeTestFile(t, archive, ".worktrees/scratch/ignored.md")
	writeTestFile(t, archive, ".tools/scratch/ignored.md")
	writeTestFile(t, archive, "vendor/project/.git/config")
	writeTestFile(t, archive, "vendor/project/.git/README.md")
	writeTestFile(t, archive, "vendor/project/node_modules/pkg/README.md")
	writeTestFile(t, archive, "vendor/project/node_modules/pkg/Dockerfile")
	got, want = markdownFiles(t, archive), []string{"docs/tracked.md"}
	if !slices.Equal(got, want) {
		t.Errorf("archive markdownFiles() = %v, want %v", got, want)
	}
	got, want = dockerfiles(t, archive), []string{}
	if !slices.Equal(got, want) {
		t.Errorf("archive dockerfiles() = %v, want %v", got, want)
	}

	// Use a matcher broader than workflowFiles so the fallback walk reaches a
	// workflow nested inside a generated worktree. The parent implementation
	// walked this path and returned it because it did not exclude .codex.
	got, want = filesMatching(t, archive, func(rel string) bool {
		return strings.HasSuffix(rel, "/.github/workflows/ignored.yml")
	}), []string{}
	if !slices.Equal(got, want) {
		t.Errorf("archive nested workflow files = %v, want %v", got, want)
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
	writeTestFile(
		t,
		root,
		".codex/worktrees/scratch/.github/workflows/ignored.yml",
	)
	runGit(
		t,
		root,
		"add",
		"docs/tracked.md",
		".github/workflows/tracked.yml",
		"Dockerfile",
	)

	alias := filepath.Join(t.TempDir(), "repo-alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}

	if got, want := markdownFiles(t, alias), []string{"docs/tracked.md"}; !slices.Equal(
		got,
		want,
	) {
		t.Errorf("aliased markdownFiles() = %v, want %v", got, want)
	}
	if got, want := dockerfiles(t, alias), []string{"Dockerfile"}; !slices.Equal(
		got,
		want,
	) {
		t.Errorf("aliased dockerfiles() = %v, want %v", got, want)
	}
	if got, want := workflowFiles(t, alias), []string{".github/workflows/tracked.yml"}; !slices.Equal(
		got,
		want,
	) {
		t.Errorf("aliased workflowFiles() = %v, want %v", got, want)
	}
}

func TestDocumentationDiscoveryFollowsAliasedSourceArchive(t *testing.T) {
	archive := t.TempDir()
	writeTestFile(t, archive, "docs/source.md")
	writeTestFile(t, archive, "Dockerfile")
	writeTestFile(t, archive, ".github/workflows/source.yml")
	writeTestFile(t, archive, ".codex/worktrees/scratch/ignored.md")
	writeTestFile(t, archive, ".codex/worktrees/scratch/Dockerfile")
	writeTestFile(
		t,
		archive,
		".codex/worktrees/scratch/.github/workflows/ignored.yml",
	)

	alias := filepath.Join(t.TempDir(), "archive-alias")
	if err := os.Symlink(archive, alias); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}

	if got, want := markdownFiles(t, alias), []string{"docs/source.md"}; !slices.Equal(
		got,
		want,
	) {
		t.Errorf("aliased archive markdownFiles() = %v, want %v", got, want)
	}
	if got, want := dockerfiles(t, alias), []string{"Dockerfile"}; !slices.Equal(
		got,
		want,
	) {
		t.Errorf("aliased archive dockerfiles() = %v, want %v", got, want)
	}
	if got, want := workflowFiles(t, alias), []string{".github/workflows/source.yml"}; !slices.Equal(
		got,
		want,
	) {
		t.Errorf("aliased archive workflowFiles() = %v, want %v", got, want)
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

// The two CI pipelines. go-test.yml runs on a pull request; publish.yml runs
// on a push to main and on a release tag. Exactly one of them runs for any
// given event, which is what keeps a merge from starting both.
const (
	prPipeline      = ".github/workflows/go-test.yml"
	publishPipeline = ".github/workflows/publish.yml"
)

// pipelineStages are the jobs both pipelines must define identically. They are
// duplicated between the files rather than factored into a reusable workflow
// because `needs:` cannot cross workflow files, and a called workflow would
// rename every check context that branch protection matches on. The price of
// that duplication is drift, which is what this file exists to prevent.
var pipelineStages = []string{
	"lint",
	"govulncheck",
	"go-test-linux-quick",
	"go-test-linux",
	"go-test-linux-race",
	"go-test-macos",
}

// pipelineJobs decodes a workflow's jobs as plain YAML values, so a comparison
// covers everything a job declares -- runner, services, env, every step and its
// command -- rather than the handful of fields a typed struct would name.
// Comments do not survive the decode, so the two files may explain themselves
// differently.
func pipelineJobs(t *testing.T, root, workflow string) map[string]any {
	t.Helper()

	var parsed struct {
		Jobs map[string]any `yaml:"jobs"`
	}
	raw := readRepoFile(t, root, workflow)
	if err := yaml.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("parse %s: %v", workflow, err)
	}
	if len(parsed.Jobs) == 0 {
		t.Fatalf("%s declares no jobs", workflow)
	}
	return parsed.Jobs
}

// jobNeeds returns a job's `needs` list. Actions accepts either a scalar or a
// sequence, so both shapes are normalized here.
func jobNeeds(t *testing.T, workflow, name string, job any) []string {
	t.Helper()

	fields, ok := job.(map[string]any)
	if !ok {
		t.Fatalf("%s job %s is not a mapping", workflow, name)
	}
	switch needs := fields["needs"].(type) {
	case nil:
		return nil
	case string:
		return []string{needs}
	case []any:
		out := make([]string, 0, len(needs))
		for _, entry := range needs {
			text, ok := entry.(string)
			if !ok {
				t.Fatalf(
					"%s job %s has a non-string needs entry %v",
					workflow,
					name,
					entry,
				)
			}
			out = append(out, text)
		}
		return out
	default:
		t.Fatalf(
			"%s job %s has an unsupported needs shape %T",
			workflow,
			name,
			fields["needs"],
		)
		return nil
	}
}

// TestPipelineStagesMatch checks that every shared stage is defined the same
// way in both pipelines. A fix applied to the pull-request pipeline and not to
// the publish one means main is tested differently from the change that was
// reviewed, which is the failure mode the old split between go-test.yml and
// publish.yml's `ci` job actually produced: the release gate drifted into
// running commands no pull request had run.
func TestPipelineStagesMatch(t *testing.T) {
	root := repoRoot(t)
	prJobs := pipelineJobs(t, root, prPipeline)
	publishJobs := pipelineJobs(t, root, publishPipeline)

	for _, stage := range pipelineStages {
		pr, ok := prJobs[stage]
		if !ok {
			t.Errorf("%s has no %s job", prPipeline, stage)
			continue
		}
		published, ok := publishJobs[stage]
		if !ok {
			t.Errorf("%s has no %s job", publishPipeline, stage)
			continue
		}
		if !reflect.DeepEqual(pr, published) {
			t.Errorf(
				"job %s differs between %s and %s; the stages are duplicated "+
					"because needs: cannot cross workflow files, so a change "+
					"to one has to be made in both",
				stage,
				prPipeline,
				publishPipeline,
			)
		}
	}
}

// TestPipelineStagesAreOrdered checks the dependency chain that makes the
// pipeline cheap before it is expensive: lint gates the quick Linux suite,
// which gates the platform suites. Without it a stage could be detached
// from its gate and start fanning out three runners again on a change that does
// not compile.
func TestPipelineStagesAreOrdered(t *testing.T) {
	root := repoRoot(t)

	// The platform suites that must not start until the cheap gate is green.
	fanOut := []string{
		"go-test-linux",
		"go-test-linux-race",
		"go-test-macos",
	}

	for _, workflow := range []string{prPipeline, publishPipeline} {
		jobs := pipelineJobs(t, root, workflow)

		quick, ok := jobs["go-test-linux-quick"]
		if !ok {
			t.Errorf("%s has no go-test-linux-quick job", workflow)
			continue
		}
		if !contains(jobNeeds(t, workflow, "go-test-linux-quick", quick), "lint") {
			t.Errorf(
				"%s: go-test-linux-quick does not need lint; the cheapest "+
					"check has to gate the suite below it",
				workflow,
			)
		}

		for _, name := range fanOut {
			job, ok := jobs[name]
			if !ok {
				t.Errorf("%s has no %s job", workflow, name)
				continue
			}
			needs := jobNeeds(t, workflow, name, job)
			if !contains(needs, "go-test-linux-quick") {
				t.Errorf(
					"%s: %s does not need go-test-linux-quick; it would fan "+
						"out before the cheap Linux suite has run",
					workflow,
					name,
				)
			}
		}
	}
}

// TestReleaseGatesOnGovulncheck checks that a tag cannot publish while
// govulncheck is failing.
//
// This was lost once already. `make govulncheck` was the last step of
// publish.yml's `ci` job and create-draft-release needed `[ci]`, so the gate
// was implicit in the job boundary. Splitting those steps into separate jobs
// dropped it, and nothing failed, because a missing edge in a dependency graph
// looks exactly like a graph that never had one.
//
// go-test.yml is deliberately not checked here. Its govulncheck job gates
// nothing, so a new upstream advisory fails the run without blocking every
// merge in the repository while it is triaged.
func TestReleaseGatesOnGovulncheck(t *testing.T) {
	root := repoRoot(t)
	jobs := pipelineJobs(t, root, publishPipeline)

	job, ok := jobs["create-draft-release"]
	if !ok {
		t.Fatalf("%s has no create-draft-release job", publishPipeline)
	}
	needs := jobNeeds(t, publishPipeline, "create-draft-release", job)
	if !contains(needs, "govulncheck") {
		t.Errorf(
			"%s: create-draft-release does not need govulncheck; a tag could "+
				"upload binaries and publish a release with a reachable "+
				"vulnerability",
			publishPipeline,
		)
	}
}

// buildGates names, for each build job, every test job for its own runner OS.
// Gating a build on its own platform and no other is what lets the Linux
// binaries start while macOS tests are still running.
//
// Linux has two suites and a build must wait for both. Naming only one would
// leave the other free to be dropped from the build's dependencies with this
// check still green, which is exactly the hole that would let build-linux run
// without the race suite.
var buildGates = map[string][]string{
	"build-linux": {"go-test-linux", "go-test-linux-race"},
	"build-macos": {"go-test-macos"},
}

// otherPlatformTests are the test jobs a given build job must NOT depend on.
var otherPlatformTests = map[string][]string{
	"build-linux": {"go-test-macos"},
	"build-macos": {"go-test-linux", "go-test-linux-race"},
}

// TestBuildsGateOnTheirOwnPlatform checks that each build job waits for its own
// runner OS's tests and for nothing else's. Depending on another platform is
// what put the whole build stage behind the slowest and least predictable test
// job in the pipeline; depending on none of them is what let images and
// binaries publish from a commit no suite had covered.
func TestBuildsGateOnTheirOwnPlatform(t *testing.T) {
	root := repoRoot(t)

	for _, workflow := range []string{prPipeline, publishPipeline} {
		jobs := pipelineJobs(t, root, workflow)

		for _, build := range sortedKeys(buildGates) {
			job, ok := jobs[build]
			if !ok {
				t.Errorf("%s has no %s job", workflow, build)
				continue
			}
			needs := jobNeeds(t, workflow, build, job)

			for _, gate := range buildGates[build] {
				if !contains(needs, gate) {
					t.Errorf(
						"%s: %s does not need %s; a build must not start "+
							"until every suite for its own platform passes",
						workflow,
						build,
						gate,
					)
				}
			}
			for _, foreign := range otherPlatformTests[build] {
				if contains(needs, foreign) {
					t.Errorf(
						"%s: %s needs %s, a test job for another platform; "+
							"that puts this build behind an unrelated "+
							"platform's runtime",
						workflow,
						build,
						foreign,
					)
				}
			}
		}
	}
}

// TestPipelineTriggersDoNotOverlap checks that a single event never starts both
// pipelines. They define the same job names, so an event that triggered both
// would render two check runs called `lint`, two called `go-test (Linux)`, and
// leave branch protection matching an ambiguous context -- besides doubling the
// runner cost of every merge, which is what this split was made to avoid.
func TestPipelineTriggersDoNotOverlap(t *testing.T) {
	root := repoRoot(t)

	// The complete allowlist per pipeline, not a list of events to reject.
	// Rejecting named events only would pass an addition nobody thought of:
	// pull_request_target on publish.yml would start both pipelines for one
	// pull request, and would do it with a writable token against unreviewed
	// code.
	allowed := map[string][]string{
		prPipeline:      {"pull_request", "workflow_dispatch"},
		publishPipeline: {"push"},
	}

	for _, workflow := range []string{prPipeline, publishPipeline} {
		want := make(map[string]struct{}, len(allowed[workflow]))
		for _, event := range allowed[workflow] {
			want[event] = struct{}{}
		}

		got := workflowTriggers(t, root, workflow)
		for event := range got {
			if _, ok := want[event]; !ok {
				t.Errorf(
					"%s triggers on %s, which is not in its allowlist %v; "+
						"exactly one pipeline may run for any given event",
					workflow,
					event,
					allowed[workflow],
				)
			}
		}
		for event := range want {
			if _, ok := got[event]; !ok {
				t.Errorf(
					"%s no longer triggers on %s",
					workflow,
					event,
				)
			}
		}
	}
}

// workflowTriggers returns the event names in a workflow's `on:` block.
func workflowTriggers(t *testing.T, root, workflow string) map[string]struct{} {
	t.Helper()

	// `on` is a YAML 1.1 boolean, so gopkg.in/yaml.v3 decodes the unquoted key
	// as `true` rather than the string "on". Both spellings are accepted here
	// so this does not depend on how the workflow happens to quote it.
	var parsed map[string]any
	raw := readRepoFile(t, root, workflow)
	if err := yaml.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("parse %s: %v", workflow, err)
	}

	var block any
	for key, value := range parsed {
		if key == "on" || key == "true" {
			block = value
			break
		}
	}
	if block == nil {
		t.Fatalf("%s has no on: block", workflow)
	}

	events := make(map[string]struct{})
	switch on := block.(type) {
	case string:
		events[on] = struct{}{}
	case []any:
		for _, entry := range on {
			events[fmt.Sprint(entry)] = struct{}{}
		}
	case map[string]any:
		for key := range on {
			events[key] = struct{}{}
		}
	default:
		t.Fatalf("%s has an unsupported on: shape %T", workflow, block)
	}
	return events
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}

func sortedKeys(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// goTestTimeout matches the -timeout a `go test` step passes, which Go
// applies per test binary rather than per invocation.
var goTestTimeout = regexp.MustCompile(`-timeout[= ]([0-9]+)m`)

// timeoutJob is a job reduced to the two numbers this file's timeout
// invariant relates: the runner-level backstop and the per-package
// diagnostics its steps pass to `go test`.
type timeoutJob struct {
	TimeoutMinutes int `yaml:"timeout-minutes"`
	Steps          []struct {
		Name string `yaml:"name"`
		Run  string `yaml:"run"`
	} `yaml:"steps"`
}

// timeoutJobs decodes a workflow's jobs into that reduced shape.
func timeoutJobs(t *testing.T, root, workflow string) map[string]timeoutJob {
	t.Helper()

	var parsed struct {
		Jobs map[string]timeoutJob `yaml:"jobs"`
	}
	raw := readRepoFile(t, root, workflow)
	if err := yaml.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("parse %s: %v", workflow, err)
	}
	if len(parsed.Jobs) == 0 {
		t.Fatalf("%s declares no jobs", workflow)
	}
	return parsed.Jobs
}

// TestPackageTimeoutsStayUnderJobBackstops keeps the two timeouts in their
// intended roles. `go test -timeout` panics with a goroutine dump naming the
// stuck test; `timeout-minutes` kills the runner and names nothing. The
// diagnostic is therefore only useful while every step's timeout can elapse
// inside the job's cap -- steps run in sequence, so the sum is what has to
// fit. Raising a -timeout past that point silently demotes the job to a
// nameless kill, which is how a hang gets investigated from a blank log.
func TestPackageTimeoutsStayUnderJobBackstops(t *testing.T) {
	root := repoRoot(t)

	for _, workflow := range []string{prPipeline, publishPipeline} {
		jobs := timeoutJobs(t, root, workflow)
		names := make([]string, 0, len(jobs))
		for name := range jobs {
			names = append(names, name)
		}
		sort.Strings(names)

		for _, name := range names {
			job := jobs[name]
			budget := 0
			for _, step := range job.Steps {
				if !strings.Contains(step.Run, "go test") {
					continue
				}
				for _, match := range goTestTimeout.FindAllStringSubmatch(
					step.Run,
					-1,
				) {
					minutes, err := strconv.Atoi(match[1])
					if err != nil {
						t.Errorf(
							"%s job %s step %q has an unparsable -timeout %q",
							workflow,
							name,
							step.Name,
							match[1],
						)
						continue
					}
					budget += minutes
				}
			}
			if budget == 0 {
				continue
			}
			if job.TimeoutMinutes == 0 {
				t.Errorf(
					"%s job %s passes %dm of go test -timeout but declares "+
						"no timeout-minutes, leaving GitHub's 360-minute "+
						"default as the backstop",
					workflow,
					name,
					budget,
				)
				continue
			}
			if budget >= job.TimeoutMinutes {
				t.Errorf(
					"%s job %s passes %dm of go test -timeout under a %dm "+
						"timeout-minutes backstop; the runner would be "+
						"killed before the per-package timeout could name "+
						"the stuck test",
					workflow,
					name,
					budget,
					job.TimeoutMinutes,
				)
			}
		}
	}
}

const publishWorkflow = ".github/workflows/publish.yml"

var releaseServiceTestTriggers = []string{
	"POSTGRES_PASSWORD",
	"POSTGRES_DSN",
	"MYSQL_ROOT_PASSWORD",
	"MYSQL_DSN",
	"DINGO_TEST_S3_BUCKET",
}

type releaseWorkflowStep struct {
	Name string            `yaml:"name"`
	Run  string            `yaml:"run"`
	Env  map[string]string `yaml:"env"`
}

type releaseWorkflowJob struct {
	Env   map[string]string     `yaml:"env"`
	Steps []releaseWorkflowStep `yaml:"steps"`
}

type releaseWorkflow struct {
	Env  map[string]string             `yaml:"env"`
	Jobs map[string]releaseWorkflowJob `yaml:"jobs"`
}

func releaseStepEnv(
	workflow releaseWorkflow,
	job releaseWorkflowJob,
	step releaseWorkflowStep,
) map[string]string {
	env := make(map[string]string)
	maps.Copy(env, workflow.Env)
	maps.Copy(env, job.Env)
	maps.Copy(env, step.Env)
	return env
}

// TestReleaseValidationSeparatesServicesFromRace keeps the tagged release
// gate aligned with the two Linux jobs in go-test.yml: the service-backed
// suite covers PostgreSQL and MySQL without race instrumentation, while the
// race suite covers the same packages with those optional backends disabled.
// Running all three conformance backends under the race detector exceeds the
// package timeout without identifying a stuck test.
//
// The two runs used to be consecutive steps of a single `ci` job. They are
// sibling jobs now -- go-test (Linux) and go-test (Linux, race) -- so that the
// 22-minute service suite and the 35-minute race suite run side by side
// instead of adding up to a 57-minute job on the critical path of every merge.
// This walks every job for that reason: what matters is that the release path
// contains both runs and that the race one is not service-backed, not which
// job each lives in.
func TestReleaseValidationSeparatesServicesFromRace(t *testing.T) {
	root := repoRoot(t)
	raw := readRepoFile(t, root, publishWorkflow)
	var workflow releaseWorkflow
	if err := yaml.Unmarshal([]byte(raw), &workflow); err != nil {
		t.Fatalf("parse %s: %v", publishWorkflow, err)
	}

	serviceRun := false
	raceRun := false
	for jobName, job := range workflow.Jobs {
		for _, step := range job.Steps {
			if !strings.Contains(step.Run, "go test") ||
				!strings.Contains(step.Run, "./...") {
				continue
			}
			env := releaseStepEnv(workflow, job, step)
			if strings.Contains(step.Run, "-race") {
				raceRun = true
				for _, key := range releaseServiceTestTriggers {
					if env[key] != "" {
						t.Errorf(
							"release race step %q in job %q exposes %s; run service-backed conformance without -race",
							step.Name,
							jobName,
							key,
						)
					}
				}
				continue
			}

			if env["POSTGRES_PASSWORD"] != "" &&
				env["MYSQL_ROOT_PASSWORD"] != "" &&
				env["DINGO_TEST_S3_BUCKET"] != "" {
				serviceRun = true
			}
		}
	}

	if !serviceRun {
		t.Errorf(
			"%s has no service-backed uninstrumented full test run",
			publishWorkflow,
		)
	}
	if !raceRun {
		t.Errorf("%s has no full race test run", publishWorkflow)
	}
}

// serviceDurability lists, per database image, the server options the CI
// service containers must be started with. The conformance replays reset the
// schema between every vector, so a run issues tens of thousands of
// committing statements and is bound by fsync latency on a slow runner disk.
// The containers are discarded with the job, so nothing here needs to survive
// a crash.
//
// Each option is matched as whole whitespace-separated tokens, so a value
// that merely contains one (`--innodb-file-per-table=OFFLINE`) does not count.
var serviceDurability = []struct {
	imagePrefix string
	options     []string
}{
	{
		imagePrefix: "postgres:",
		options: []string{
			"-c fsync=off",
			"-c synchronous_commit=off",
			"-c full_page_writes=off",
		},
	},
	{
		imagePrefix: "mysql:",
		options: []string{
			"--innodb-file-per-table=OFF",
			"--innodb-flush-log-at-trx-commit=0",
			"--sync-binlog=0",
			"--skip-log-bin",
		},
	},
}

// serviceWorkflowJob includes the runnable startup and cleanup configuration.
type serviceWorkflowJob struct {
	Services map[string]struct {
		Image   string `yaml:"image"`
		Command string `yaml:"command"`
	} `yaml:"services"`
	Steps []struct {
		Name string `yaml:"name"`
		Run  string `yaml:"run"`
		If   string `yaml:"if"`
	} `yaml:"steps"`
}

// TestServiceContainersRelaxDurability checks the actual Docker launch
// arguments, health admission and cleanup in both pipelines.
func TestServiceContainersRelaxDurability(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	for _, workflow := range []string{prPipeline, publishPipeline} {
		var parsed struct {
			Jobs map[string]serviceWorkflowJob `yaml:"jobs"`
		}
		if err := yaml.Unmarshal([]byte(readRepoFile(t, root, workflow)), &parsed); err != nil {
			t.Fatalf("parse %s: %v", workflow, err)
		}
		seen := make(map[string]int)
		for jobName, job := range parsed.Jobs {
			started := false
			cleaned := false
			for serviceName, service := range job.Services {
				for _, want := range serviceDurability {
					if !strings.HasPrefix(service.Image, want.imagePrefix) {
						continue
					}
					seen[want.imagePrefix]++
					arguments := " " + strings.Join(
						strings.Fields(service.Command), " ",
					) + " "
					for _, option := range want.options {
						if !strings.Contains(arguments, " "+option+" ") {
							t.Errorf(
								"%s: job %s service %s (%s) lacks %q",
								workflow, jobName, serviceName,
								service.Image, option,
							)
						}
					}
				}
			}
			for _, step := range job.Steps {
				if step.Name == "stop-test-databases" {
					cleaned = step.If == "always()" &&
						strings.Contains(
							step.Run,
							`if [ "$name" = "$container" ]`,
						) &&
						strings.Contains(
							step.Run,
							`docker rm --force "$container"`,
						)
				}
				command := strings.ReplaceAll(step.Run, "\\\n", " ")
				for line := range strings.SplitSeq(command, "\n") {
					fields := strings.Fields(line)
					if len(fields) < 3 || fields[0] != "docker" ||
						fields[1] != "run" {
						continue
					}
					for _, want := range serviceDurability {
						for i, field := range fields {
							if !strings.HasPrefix(field, want.imagePrefix) {
								continue
							}
							started = true
							seen[want.imagePrefix]++
							arguments := " " + strings.Join(
								fields[i+1:],
								" ",
							) + " "
							for _, option := range want.options {
								if !strings.Contains(
									arguments,
									" "+option+" ",
								) {
									t.Errorf(
										"%s: job %s Docker %s lacks %q",
										workflow,
										jobName,
										field,
										option,
									)
								}
							}
							for _, admission := range []string{
								"docker inspect --format", "'running healthy') break",
								`docker logs "$container"`, "exit 1",
							} {
								if !strings.Contains(step.Run, admission) {
									t.Errorf(
										"%s: job %s lacks health admission %q",
										workflow,
										jobName,
										admission,
									)
								}
							}
						}
					}
				}
			}
			if started && !cleaned {
				t.Errorf(
					"%s: job %s does not always clean its owned database containers",
					workflow,
					jobName,
				)
			}
		}
		for _, want := range serviceDurability {
			if seen[want.imagePrefix] == 0 {
				t.Errorf(
					"%s starts no %s database container",
					workflow,
					want.imagePrefix,
				)
			}
		}
	}
}

// TestGitignoreCoversLocalSecretsAndKeepsTrackedFixtures pins both halves of
// the secret-file patterns: names that carry local credentials are ignored,
// and every tracked fixture that matches one of them is un-ignored explicitly,
// so an intentional fixture is never one `git add -f` away from looking like
// a leak.
func TestGitignoreCoversLocalSecretsAndKeepsTrackedFixtures(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)
	if err := exec.Command(
		"git", "-C", root, "rev-parse", "--git-dir",
	).Run(); err != nil {
		t.Skip("not a git checkout")
	}

	for _, name := range []string{
		".env",
		"payment.skey",
		"payment.vkey",
		"node.key",
		"tls/server.pem",
		"credentials.json",
		"dingo.yaml",
	} {
		// --no-index judges the pattern alone, whether or not the path exists
		// or is tracked.
		err := exec.Command(
			"git", "-C", root, "check-ignore", "--no-index", "-q", name,
		).Run()
		if err != nil {
			t.Errorf("%s is not ignored: %v", name, err)
		}
	}

	tracked, err := exec.Command(
		"git", "-C", root, "ls-files", "-ci", "--exclude-standard",
	).Output()
	if err != nil {
		t.Fatalf("listing ignored tracked files: %v", err)
	}
	if got := strings.TrimSpace(string(tracked)); got != "" {
		t.Errorf(
			"tracked files match an ignore pattern without a negation:\n%s",
			got,
		)
	}
}

// TestDockerfilesExposingMetricsBindThem keeps an image that publishes the
// metrics port reachable on it. The binary binds metrics to loopback unless
// told otherwise, so an image that EXPOSEs 12798 without setting the bind
// address advertises a port that refuses every connection from outside the
// container, including an orchestrator's probes and scrapers.
func TestDockerfilesExposingMetricsBindThem(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)
	for _, rel := range dockerfiles(t, root) {
		var exposes, binds bool
		for line := range strings.SplitSeq(readRepoFile(t, root, rel), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			switch strings.ToUpper(fields[0]) {
			case "EXPOSE":
				exposes = exposes || slices.Contains(fields[1:], "12798")
			case "ENV":
				binds = binds ||
					strings.HasPrefix(fields[1], "DINGO_METRICS_BIND_ADDR=")
			}
		}
		if exposes && !binds {
			t.Errorf(
				"%s exposes the metrics port without setting DINGO_METRICS_BIND_ADDR",
				rel,
			)
		}
	}
}
