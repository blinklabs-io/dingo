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

package dbtest

import (
	"errors"
	"testing"
)

// TestTemplateCleanupError pins the rule that keeps a failed removal of the
// scratch template directory from poisoning the process. buildMetadataTemplate
// runs once behind a sync.Once and its error is cached, so a removal failure
// folded into a successful build would fail every later test in the binary.
func TestTemplateCleanupError(t *testing.T) {
	t.Parallel()
	buildErr := errors.New("build failed")
	removeErr := errors.New("remove failed")
	for _, test := range []struct {
		name      string
		err       error
		removeErr error
		wantBuild bool
		wantRemov bool
	}{
		{
			name: "success with clean removal returns nil",
		},
		{
			// The regression: a cleanup failure must not turn a usable
			// template into a cached, permanent failure.
			name:      "success with failed removal stays successful",
			removeErr: removeErr,
		},
		{
			name:      "failure with clean removal keeps build error",
			err:       buildErr,
			wantBuild: true,
		},
		{
			name:      "failure with failed removal reports both",
			err:       buildErr,
			removeErr: removeErr,
			wantBuild: true,
			wantRemov: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := templateCleanupError(
				test.err,
				"/tmp/example",
				test.removeErr,
			)
			if !test.wantBuild && !test.wantRemov && got != nil {
				t.Fatalf("expected nil error, got %v", got)
			}
			if test.wantBuild && !errors.Is(got, buildErr) {
				t.Errorf("expected build error in %v", got)
			}
			if test.wantRemov != errors.Is(got, removeErr) {
				t.Errorf(
					"removal error present = %t, want %t (%v)",
					errors.Is(got, removeErr),
					test.wantRemov,
					got,
				)
			}
		})
	}
}
