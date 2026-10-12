package docsparity_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestExecutableReleaseInputsPinned(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	digest := regexp.MustCompile(`@sha256:[a-f0-9]{64}$`)
	stages := map[string]bool{}
	for _, line := range strings.Split(readRepoFile(t, root, "Dockerfile"), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "FROM" {
			continue
		}
		image := fields[1]
		if !stages[image] && !digest.MatchString(image) {
			t.Errorf("external image is not digest pinned: %s", image)
		}
		if len(fields) >= 4 && strings.EqualFold(fields[2], "AS") {
			stages[fields[3]] = true
		}
	}
	paths, err := filepath.Glob(filepath.Join(root, ".github/workflows/*.yml"))
	if err != nil {
		t.Fatal(err)
	}
	sha := regexp.MustCompile(`@[a-f0-9]{40}$`)
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(content), "\n") {
			line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "-"))
			if !strings.HasPrefix(line, "uses:") {
				continue
			}
			value := strings.Fields(strings.TrimSpace(strings.TrimPrefix(line, "uses:")))[0]
			if !strings.HasPrefix(value, "./") && !sha.MatchString(value) {
				t.Errorf("%s: action is not SHA pinned: %s", filepath.Base(path), value)
			}
		}
	}
}
