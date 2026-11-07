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

package koiosparity

import (
	"os"
	"testing"
	"time"
)

// TestMain shortens the 5xx retry backoff for every test in the package. It is
// set before any test starts, so no test races a write; tests assert on how
// many attempts a failing server sees, never on the delay between them.
func TestMain(m *testing.M) {
	koiosRetryBackoff5xx = time.Millisecond
	os.Exit(m.Run())
}
