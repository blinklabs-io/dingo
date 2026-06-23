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
			"connmanager",
			"peergov",
			"ouroboros",
			"chainsync",
			"chainselection",
			"internal/node",
			"api",
		},
		reason: "database and storage plugins sit below ledger, mempool, " +
			"networking, node composition, and API packages",
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
		rule := rule
		wg.Add(1)
		go func() {
			defer wg.Done()

			ruleViolations, err := importBoundaryViolations(repoRoot, rule)
			if err != nil {
				errsCh <- err
				return
			}
			for _, violation := range ruleViolations {
				violationsCh <- violation
			}
		}()
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
		t.Fatalf("check import boundaries:\n%s", strings.Join(errorMessages, "\n"))
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
	sem := make(chan struct{}, runtime.GOMAXPROCS(0))

	for _, file := range files {
		file := file
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() {
				<-sem
			}()

			fileViolations, err := importBoundaryViolationsForFile(
				repoRoot,
				rule,
				file,
			)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			violations = append(violations, fileViolations...)
		}()
	}
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

	var violations []string
	for _, importPath := range imports {
		for _, forbidden := range rule.forbidden {
			if !isLocalPackageImport(importPath, forbidden) {
				continue
			}
			relFile, err := relativePath(repoRoot, file)
			if err != nil {
				return nil, err
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
