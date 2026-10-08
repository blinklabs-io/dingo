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

package secretfile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReadStripsTrailingLineEndings(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		content string
		want    string
	}{
		{name: "lf", content: "s3cret\n", want: "s3cret"},
		{name: "crlf", content: "s3cret\r\n", want: "s3cret"},
		{name: "none", content: "s3cret", want: "s3cret"},
		{name: "repeated", content: "s3cret\n\n", want: "s3cret"},
		{
			name:    "inner whitespace kept",
			content: " a b\tc \n",
			want:    " a b\tc ",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := Read(writeFile(t, tc.content))
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("Read = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReadRejectsEmptyValue(t *testing.T) {
	t.Parallel()
	for _, content := range []string{"", "\n", "\r\n"} {
		if _, err := Read(writeFile(t, content)); err == nil {
			t.Fatalf("Read(%q): expected error for empty value", content)
		}
	}
}

func TestReadRejectsOversizedFile(t *testing.T) {
	t.Parallel()
	_, err := Read(writeFile(t, strings.Repeat("x", MaxBytes+1)))
	if err == nil {
		t.Fatal("expected error for oversized file")
	}
	if _, err := Read(writeFile(t, strings.Repeat("x", MaxBytes))); err != nil {
		t.Fatalf("file of exactly MaxBytes: %v", err)
	}
}

func TestReadErrorOmitsContent(t *testing.T) {
	t.Parallel()
	const secret = "do-not-print-me"
	_, err := Read(writeFile(t, secret+strings.Repeat("x", MaxBytes)))
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaks file content: %v", err)
	}
}

func TestReadRejectsEmptyPathAndMissingFile(t *testing.T) {
	t.Parallel()
	if _, err := Read(""); err == nil {
		t.Fatal("expected error for empty path")
	}
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := Read(missing); err == nil {
		t.Fatal("expected error for missing file")
	}
}
