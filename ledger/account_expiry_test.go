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

package ledger

import "testing"

func TestAccountExpiredAtEpoch(t *testing.T) {
	cases := []struct {
		exp, cur uint64
		expired  bool
	}{
		{0, 0, false},   // unset => active
		{0, 100, false}, // unset => active regardless of epoch
		{10, 9, false},  // exp >= cur => active
		{10, 10, false}, // boundary: exp == cur => active (CIP: expired is < cur)
		{10, 11, true},  // exp < cur => expired
	}
	for _, c := range cases {
		if got := accountExpiredAtEpoch(c.exp, c.cur); got != c.expired {
			t.Fatalf("accountExpiredAtEpoch(%d,%d)=%v want %v", c.exp, c.cur, got, c.expired)
		}
	}
}
