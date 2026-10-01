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

package load

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var blocksMarkerRe = regexp.MustCompile(`BLOCKS_MARKER="([^"]+)"`)

// TestTimedLoadMarkerMatchesLoader keeps the log line timed-load.sh parses
// the block count from in step with the loader. The script takes blocks/s
// from that line's blocks_copied attribute, so a reworded message would
// otherwise make every timing run fail or report no blocks.
func TestTimedLoadMarkerMatchesLoader(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	script := readFile(t, root, "internal", "test", "load", "timed-load.sh")
	marker := captureOne(t, blocksMarkerRe, script, "BLOCKS_MARKER")
	loader := readFile(t, root, "internal", "node", "load.go")
	pattern := regexp.MustCompile(
		`logger\.Info\(\s*"` + regexp.QuoteMeta(marker) +
			`",\s*"blocks_copied"`,
	)
	require.Regexp(
		t,
		pattern,
		loader,
		"internal/node/load.go no longer logs %q with a blocks_copied attribute",
		marker,
	)
}

// TestTimedLoadMakeTargetRunsScript keeps the make target pointed at the
// script and the script's entry points documented.
func TestTimedLoadMakeTargetRunsScript(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	makefile := readFile(t, root, "Makefile")
	require.Regexp(
		t,
		regexp.MustCompile(
			`(?m)^test-load-timing:.*\n\t\./internal/test/load/timed-load\.sh$`,
		),
		makefile,
	)
	docs := readFile(t, root, "docs", "benchmarks.md")
	for _, name := range []string{
		"make test-load-timing",
		"DINGO_LOAD_IMMUTABLE_DIR",
		"DINGO_LOAD_PROFILE",
		"DINGO_LOAD_MAX_SECONDS",
	} {
		require.True(
			t,
			strings.Contains(docs, name),
			"docs/benchmarks.md does not document %s",
			name,
		)
	}
}
