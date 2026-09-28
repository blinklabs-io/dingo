package architecture_test

import (
	"bytes"
	"errors"
	"go/build/constraint"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDevnetFilesStayLinuxOnly keeps the platform boundary directory-wide.
// Go build constraints are file-scoped, so a newly added untagged file would
// otherwise silently put part of the Docker-backed harness into macOS and
// Windows `go test ./...` runs again.
func TestDevnetFilesStayLinuxOnly(t *testing.T) {
	root := filepath.Join(findRepoRoot(t), "internal", "test", "devnet")
	err := filepath.WalkDir(
		root,
		func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			expr, err := parseGoBuildConstraint(content)
			if err != nil || !requiresBuildTag(expr, "linux") {
				rel, relErr := filepath.Rel(root, path)
				if relErr != nil {
					return relErr
				}
				t.Errorf(
					"%s must have a build constraint that requires linux",
					filepath.ToSlash(rel),
				)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("check DevNet platform constraints: %v", err)
	}
}

var (
	errNoGoBuildConstraint = errors.New("no //go:build constraint")
	errMultipleGoBuild     = errors.New("multiple //go:build constraints")
)

// parseGoBuildConstraint finds the //go:build directive in the portion of a
// Go source file where the go command recognizes it. Leading line and block
// comments may contain a license header; directives inside a block comment or
// after the first non-comment token do not apply to the file.
func parseGoBuildConstraint(content []byte) (constraint.Expr, error) {
	var goBuild string
	p := content
	inBlockComment := false

lines:
	for len(p) > 0 {
		line := p
		if i := bytes.IndexByte(line, '\n'); i >= 0 {
			line, p = line[:i], p[i+1:]
		} else {
			p = nil
		}
		line = bytes.TrimSpace(line)

		if !inBlockComment && constraint.IsGoBuild(string(line)) {
			if goBuild != "" {
				return nil, errMultipleGoBuild
			}
			goBuild = string(line)
		}

	comments:
		for len(line) > 0 {
			if inBlockComment {
				if i := bytes.Index(line, []byte("*/")); i >= 0 {
					inBlockComment = false
					line = bytes.TrimSpace(line[i+2:])
					continue comments
				}
				continue lines
			}
			if bytes.HasPrefix(line, []byte("//")) {
				continue lines
			}
			if bytes.HasPrefix(line, []byte("/*")) {
				inBlockComment = true
				line = bytes.TrimSpace(line[2:])
				continue comments
			}
			break lines
		}
	}

	if goBuild == "" {
		return nil, errNoGoBuildConstraint
	}
	return constraint.Parse(goBuild)
}

// requiresBuildTag returns true only when the expression structurally proves
// that tag must be true. It deliberately rejects expressions it cannot prove,
// keeping additions such as "linux || windows" out of the DevNet tree.
func requiresBuildTag(expr constraint.Expr, tag string) bool {
	switch expr := expr.(type) {
	case *constraint.TagExpr:
		return expr.Tag == tag
	case *constraint.NotExpr:
		return false
	case *constraint.AndExpr:
		return requiresBuildTag(expr.X, tag) ||
			requiresBuildTag(expr.Y, tag)
	case *constraint.OrExpr:
		return requiresBuildTag(expr.X, tag) &&
			requiresBuildTag(expr.Y, tag)
	default:
		return false
	}
}

func TestRequiresBuildTag(t *testing.T) {
	tests := map[string]bool{
		"//go:build linux":                                       true,
		"//go:build linux && devnet":                             true,
		"//go:build devnet && linux":                             true,
		"//go:build (linux && devnet) || (linux && conformance)": true,
		"//go:build windows":                                     false,
		"//go:build devnet":                                      false,
		"//go:build !windows":                                    false,
		"//go:build linux || windows":                            false,
	}
	for line, want := range tests {
		t.Run(line, func(t *testing.T) {
			expr, err := constraint.Parse(line)
			if err != nil {
				t.Fatalf("parse constraint: %v", err)
			}
			if got := requiresBuildTag(expr, "linux"); got != want {
				t.Fatalf("requiresBuildTag() = %v, want %v", got, want)
			}
		})
	}
}

func TestParseGoBuildConstraint(t *testing.T) {
	tests := map[string]struct {
		content string
		want    string
		wantErr error
	}{
		"first line": {
			content: "//go:build linux\n\npackage devnet\n",
			want:    "linux",
		},
		"after line comment license": {
			content: "// Copyright 2026 Blink Labs Software\n" +
				"// Licensed under the Apache License, Version 2.0\n\n" +
				"//go:build linux && devnet\n\npackage devnet\n",
			want: "linux && devnet",
		},
		"after block comment license": {
			content: "/* Copyright 2026 Blink Labs Software */\n\n" +
				"//go:build linux && devnet\n\npackage devnet\n",
			want: "linux && devnet",
		},
		"inside block comment": {
			content: "/*\n//go:build linux\n*/\npackage devnet\n",
			wantErr: errNoGoBuildConstraint,
		},
		"after package clause": {
			content: "package devnet\n\n//go:build linux\n",
			wantErr: errNoGoBuildConstraint,
		},
		"multiple constraints": {
			content: "//go:build linux\n//go:build devnet\npackage devnet\n",
			wantErr: errMultipleGoBuild,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			expr, err := parseGoBuildConstraint([]byte(test.content))
			if !errors.Is(err, test.wantErr) {
				t.Fatalf(
					"parseGoBuildConstraint() error = %v, want %v",
					err,
					test.wantErr,
				)
			}
			if test.wantErr != nil {
				return
			}
			if got := expr.String(); got != test.want {
				t.Fatalf(
					"parseGoBuildConstraint() = %q, want %q",
					got,
					test.want,
				)
			}
		})
	}
}
