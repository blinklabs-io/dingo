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

package leiosheader

import (
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// ReferencedEndorserBlock returns the Leios endorser block named by a Dijkstra
// ranking-block header through its leios_announcement field. The decoder
// admits only the twelve-field header body, so the announcement is the only
// form a decoded header can carry.
func ReferencedEndorserBlock(
	header lcommon.BlockHeader,
) (lcommon.Blake2b256, uint64, bool) {
	dijkstraHeader, ok := header.(*dijkstra.DijkstraBlockHeader)
	if !ok || dijkstraHeader == nil {
		return lcommon.Blake2b256{}, 0, false
	}
	return dijkstraHeader.LeiosAnnouncement()
}
