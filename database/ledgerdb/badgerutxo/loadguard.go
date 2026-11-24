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

package badgerutxo

// reloadableTags are the row families loaded from certificate and
// transaction rows, which a v0.94.2 metadata store keeps whatever else it
// holds, so a load of them is never refused.
var reloadableTags = map[byte]bool{
	DelegationTag:  true,
	ClosureUtxoTag: true,
}

// loadRefused reports whether a load of the rows under prefix is refused once
// RefuseLoads was called.
func loadRefused(prefix []byte) bool {
	return len(prefix) == 0 || !reloadableTags[prefix[0]]
}
