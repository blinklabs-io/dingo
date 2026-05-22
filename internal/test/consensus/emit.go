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

package consensus

import (
	"fmt"
	"os"

	"github.com/blinklabs-io/dingo/internal/test/consensus/format"
)

// WriteVector encodes v via format.EncodeTestVector and writes the
// result to path with mode 0o644. Errors wrap with the destination
// path for easier triage.
func WriteVector(path string, v format.TestVector) error {
	raw, err := format.EncodeTestVector(v)
	if err != nil {
		return fmt.Errorf("write vector %s: %w", path, err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return fmt.Errorf("write vector %s: %w", path, err)
	}
	return nil
}
