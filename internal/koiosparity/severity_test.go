package koiosparity

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCountSignificantExcludesInformational pins that the number reported with
// a parity failure counts the mismatches that caused it.
//
// DetermineStatus deliberately treats the lifecycle and pool-departure
// categories as no-ops, so an epoch can hold many of them and still pass.
// Counting them in the failure message points the reader at rows that are by
// definition never the reason. Preview epoch 198 failed on 3 mismatches and
// reported 12.
func TestCountSignificantExcludesInformational(t *testing.T) {
	mismatches := []CheckMismatch{
		{Category: CategoryAcctOnlyDingo},
		{Category: CategoryAcctOnlyDingo},
		{Category: CategoryAcctOnlyDingo},
		{Category: CategoryAcctZeroReward},
	}
	for range 8 {
		mismatches = append(
			mismatches,
			CheckMismatch{Category: CategoryPoolDeparted},
		)
	}
	require.Len(t, mismatches, 12)
	assert.Equal(t, 3, CountSignificant(mismatches))
	assert.Equal(t, StatusFail, DetermineStatus(mismatches))
}

// TestCountSignificantCountsErrors keeps the error categories significant:
// they drive StatusError, so they are a reason too.
func TestCountSignificantCountsErrors(t *testing.T) {
	mismatches := []CheckMismatch{
		{Category: CategoryDBError},
		{Category: CategoryReferenceLag},
		{Category: CategoryPoolDeparted},
	}
	assert.Equal(t, 2, CountSignificant(mismatches))
	assert.Equal(t, StatusError, DetermineStatus(mismatches))
}

// TestCountSignificantAgreesWithDetermineStatus is the invariant that matters
// more than either number: a status of PASS and a non-zero significant count
// cannot coexist, in either direction. The two must read the same
// classification, or a future category added to one will silently disagree
// with the other.
//
// The loop is AllCategories itself, not a copy of it: a hand-maintained list
// here would omit exactly the category a contributor also forgot to add to
// severityOf, and the guard would pass on the case it exists to catch.
func TestCountSignificantAgreesWithDetermineStatus(t *testing.T) {
	for _, cat := range AllCategories {
		t.Run(cat, func(t *testing.T) {
			ms := []CheckMismatch{{Category: cat}}
			passed := DetermineStatus(ms) == StatusPass
			assert.Equal(t, passed, CountSignificant(ms) == 0,
				"status and significant count must classify %q the same way",
				cat)
		})
	}
}

// TestAllCategoriesCoversEveryConstant keeps AllCategories honest.
//
// AllCategories is what makes the classification guard above load-bearing, so
// it must not itself be a list that can fall behind. This reads the Category*
// constants straight out of the package source and requires the two to be the
// same set: a new category constant that never reaches AllCategories fails
// here rather than sliding past a guard that simply never sees it.
func TestAllCategoriesCoversEveryConstant(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	require.NoError(t, err)
	pkg, ok := pkgs["koiosparity"]
	require.True(t, ok, "package koiosparity must be parseable from .")

	declared := map[string]string{} // constant name -> literal value
	for _, file := range pkg.Files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, name := range vs.Names {
					if !strings.HasPrefix(name.Name, "Category") ||
						i >= len(vs.Values) {
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					value, err := strconv.Unquote(lit.Value)
					require.NoError(t, err)
					declared[name.Name] = value
				}
			}
		}
	}
	require.NotEmpty(t, declared, "no Category* constants found to check")

	listed := map[string]bool{}
	for _, cat := range AllCategories {
		require.False(t, listed[cat],
			"AllCategories lists %q more than once", cat)
		listed[cat] = true
	}
	for name, value := range declared {
		assert.True(t, listed[value],
			"constant %s (%q) is missing from AllCategories, so severityOf's "+
				"classification of it is unguarded", name, value)
		delete(listed, value)
	}
	for cat := range listed {
		assert.Fail(t, "unknown category listed",
			"AllCategories contains %q, which is not a Category* constant",
			cat)
	}
}
