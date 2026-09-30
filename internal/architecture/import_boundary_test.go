package architecture_test

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
)

const modulePath = "github.com/blinklabs-io/dingo"

type importBoundaryRule struct {
	from      string
	forbidden []string
	reason    string
	// testImportExemptions maps a repository-relative _test.go file to the
	// forbidden packages it may import. Keep each entry to a test that must
	// compose both sides of the boundary, and name the reason beside it.
	testImportExemptions map[string][]string
}

// importBoundaryRules encode reviewed package directions for critical domains.
// When an architecture review approves a new dependency, update this list in
// the same change as ARCHITECTURE.md and keep the reason field explicit.
var importBoundaryRules = []importBoundaryRule{
	{
		from: "ledger",
		forbidden: []string{
			"connmanager",
			"peergov",
			"mempool",
		},
		reason: "ledger owns validation and state, while node wiring translates " +
			"neutral events/callbacks into networking or mempool actions",
		testImportExemptions: map[string][]string{
			// Drives a Dijkstra collateral-return transaction through both
			// mempool admission and ledger block application.
			"ledger/dijkstra_collateral_return_production_test.go": {
				"mempool",
			},
		},
	},
	{
		from:      "chainselection",
		forbidden: []string{"peergov"},
		reason: "chain selection should emit neutral chain-switch decisions " +
			"without depending on peer governance policy",
	},
	{
		from:      "chainsync",
		forbidden: []string{"ledger"},
		reason: "chainsync should track protocol state without importing " +
			"concrete ledger state",
	},
	{
		from: "database",
		forbidden: []string{
			".",
			"ledger",
			"mempool",
			"dmq",
			"connmanager",
			"peergov",
			"ouroboros",
			"chainsync",
			"chainselection",
			"internal/node",
			"api",
		},
		reason: "database and storage plugins sit below ledger, mempool, " +
			"dmq, networking, node composition, and API packages",
	},
}

// TestImportBoundaries checks reviewed package dependency directions against
// every local Go import in the guarded package trees.
func TestImportBoundaries(t *testing.T) {
	repoRoot := findRepoRoot(t)
	var violations []string
	var errs []error
	violationsCh := make(chan string)
	errsCh := make(chan error)

	var wg sync.WaitGroup
	for _, rule := range importBoundaryRules {
		wg.Go(func() {

			ruleViolations, err := importBoundaryViolations(repoRoot, rule)
			if err != nil {
				errsCh <- err
				return
			}
			for _, violation := range ruleViolations {
				violationsCh <- violation
			}
		})
	}

	go func() {
		wg.Wait()
		close(violationsCh)
		close(errsCh)
	}()

	for violationsCh != nil || errsCh != nil {
		select {
		case violation, ok := <-violationsCh:
			if !ok {
				violationsCh = nil
				continue
			}
			violations = append(violations, violation)
		case err, ok := <-errsCh:
			if !ok {
				errsCh = nil
				continue
			}
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		errorMessages := make([]string, 0, len(errs))
		for _, err := range errs {
			errorMessages = append(errorMessages, err.Error())
		}
		slices.Sort(errorMessages)
		t.Fatalf(
			"check import boundaries:\n%s",
			strings.Join(errorMessages, "\n"),
		)
	}

	if len(violations) > 0 {
		slices.Sort(violations)
		t.Fatalf(
			"forbidden local imports found:\n%s",
			strings.Join(violations, "\n"),
		)
	}
}

// importBoundaryViolations checks one boundary rule, parsing files in parallel
// while bounding concurrent parser work to the configured Go CPU parallelism.
func importBoundaryViolations(
	repoRoot string,
	rule importBoundaryRule,
) ([]string, error) {
	files, err := goFilesBelow(repoRoot, rule.from)
	if err != nil {
		return nil, err
	}

	var violations []string
	var errs []error
	var mu sync.Mutex
	var wg sync.WaitGroup
	jobs := make(chan string)

	for range runtime.GOMAXPROCS(0) {
		wg.Go(func() {

			for file := range jobs {
				fileViolations, err := importBoundaryViolationsForFile(
					repoRoot,
					rule,
					file,
				)
				mu.Lock()
				if err != nil {
					errs = append(errs, err)
				} else {
					violations = append(violations, fileViolations...)
				}
				mu.Unlock()
			}
		})
	}
	for _, file := range files {
		jobs <- file
	}
	close(jobs)
	wg.Wait()

	if len(errs) > 0 {
		errorMessages := make([]string, 0, len(errs))
		for _, err := range errs {
			errorMessages = append(errorMessages, err.Error())
		}
		slices.Sort(errorMessages)
		return nil, fmt.Errorf("%s", strings.Join(errorMessages, "\n"))
	}
	return violations, nil
}

// importBoundaryViolationsForFile reports every forbidden import from one Go
// source file for the given boundary rule.
func importBoundaryViolationsForFile(
	repoRoot string,
	rule importBoundaryRule,
	file string,
) ([]string, error) {
	imports, err := importsForFile(file)
	if err != nil {
		return nil, err
	}

	relFile, err := relativePath(repoRoot, file)
	if err != nil {
		return nil, err
	}
	var exempt []string
	if strings.HasSuffix(relFile, "_test.go") {
		exempt = rule.testImportExemptions[relFile]
	}
	var violations []string
	for _, importPath := range imports {
		for _, forbidden := range rule.forbidden {
			if !isLocalPackageImport(importPath, forbidden) ||
				slices.Contains(exempt, forbidden) {
				continue
			}
			violations = append(
				violations,
				fmt.Sprintf(
					"%s imports %s: %s",
					relFile,
					importPath,
					rule.reason,
				),
			)
		}
	}
	return violations, nil
}

// findRepoRoot walks upward from the test working directory until it finds the
// module root.
func findRepoRoot(t *testing.T) string {
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

// goFilesBelow returns all Go source files below a repository-relative
// directory, excluding fixture and hidden directories.
func goFilesBelow(repoRoot, dir string) ([]string, error) {
	root := filepath.Join(repoRoot, dir)
	var files []string
	err := filepath.WalkDir(
		root,
		func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if shouldSkipDir(entry.Name()) {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(entry.Name(), ".go") {
				files = append(files, path)
			}
			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("walk %s: %w", root, err)
	}
	return files, nil
}

// shouldSkipDir reports whether a directory should be ignored by the boundary
// scan.
func shouldSkipDir(name string) bool {
	return name == "testdata" || strings.HasPrefix(name, ".")
}

// importsForFile parses a Go file in imports-only mode and returns its import
// paths without quote characters.
func importsForFile(file string) ([]string, error) {
	parsed, err := parser.ParseFile(
		token.NewFileSet(),
		file,
		nil,
		parser.ImportsOnly,
	)
	if err != nil {
		return nil, fmt.Errorf("parse imports for %s: %w", file, err)
	}

	imports := make([]string, 0, len(parsed.Imports))
	for _, importSpec := range parsed.Imports {
		imports = append(imports, strings.Trim(importSpec.Path.Value, `"`))
	}
	return imports, nil
}

// isLocalPackageImport reports whether an import path matches the forbidden
// local package itself or any of its subpackages.
func isLocalPackageImport(importPath, forbidden string) bool {
	if forbidden == "." {
		return importPath == modulePath
	}
	forbiddenPath := modulePath + "/" + forbidden
	return importPath == forbiddenPath ||
		strings.HasPrefix(importPath, forbiddenPath+"/")
}

// relativePath formats a source path relative to the repository root for stable
// test failure messages.
func relativePath(repoRoot, file string) (string, error) {
	rel, err := filepath.Rel(repoRoot, file)
	if err != nil {
		return "", fmt.Errorf("make %s relative to %s: %w", file, repoRoot, err)
	}
	return filepath.ToSlash(rel), nil
}

// TestGoFilesBelowIncludesTestFiles pins that the shared walker returns
// _test.go files: TestSQLiteTestOpensSetSynchronous scans only test code, so a
// walker that dropped them would leave that guard with nothing to check.
func TestGoFilesBelowIncludesTestFiles(t *testing.T) {
	t.Parallel()

	repoRoot := findRepoRoot(t)
	files, err := goFilesBelow(repoRoot, "internal/architecture")
	if err != nil {
		t.Fatalf("list go files: %v", err)
	}
	want := filepath.Join(
		repoRoot,
		"internal",
		"architecture",
		"sqlite_test_durability_test.go",
	)
	if !slices.Contains(files, want) {
		t.Fatalf("goFilesBelow omitted %s; got %v", want, files)
	}
}

// TestImportBoundaryTestExemptionIsScoped pins that a test-import exemption
// admits only the named package from the named file: another test file, and
// another forbidden package from the exempt file, are still reported.
func TestImportBoundaryTestExemptionIsScoped(t *testing.T) {
	t.Parallel()

	repoRoot := t.TempDir()
	writeGoFile := func(rel string, imports ...string) {
		t.Helper()
		var src strings.Builder
		src.WriteString("package ledger\n\nimport (\n")
		for _, imp := range imports {
			fmt.Fprintf(&src, "\t_ %q\n", modulePath+"/"+imp)
		}
		src.WriteString(")\n")
		path := filepath.Join(repoRoot, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(src.String()), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeGoFile("ledger/exempt_test.go", "mempool", "peergov")
	writeGoFile("ledger/other_test.go", "mempool")
	writeGoFile("ledger/state.go", "mempool")

	rule := importBoundaryRule{
		from:      "ledger",
		forbidden: []string{"peergov", "mempool"},
		reason:    "test",
		testImportExemptions: map[string][]string{
			"ledger/exempt_test.go": {"mempool"},
		},
	}
	violations, err := importBoundaryViolations(repoRoot, rule)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(violations)
	want := []string{
		"ledger/exempt_test.go imports " + modulePath + "/peergov: test",
		"ledger/other_test.go imports " + modulePath + "/mempool: test",
		"ledger/state.go imports " + modulePath + "/mempool: test",
	}
	if !slices.Equal(violations, want) {
		t.Fatalf("violations:\n%v\nwant:\n%v", violations, want)
	}
}
