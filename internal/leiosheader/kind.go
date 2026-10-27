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

const (
	// KindAnnouncesEB is set when a ranking block announces an endorser
	// block (leios_announcement present).
	KindAnnouncesEB uint8 = 1
	// KindCertifiesEB is set when a ranking block carries a certificate for
	// the endorser block its parent announced (certified flag set).
	KindCertifiesEB uint8 = 2
)

// RankingBlockKind classifies a ranking-block header as a bitmask of
// KindAnnouncesEB and KindCertifiesEB. A plain ranking block, and every
// header that is not a Dijkstra header, yields 0.
func RankingBlockKind(header lcommon.BlockHeader) uint8 {
	dijkstraHeader, ok := header.(*dijkstra.DijkstraBlockHeader)
	if !ok || dijkstraHeader == nil {
		return 0
	}
	var kind uint8
	if _, _, announces := dijkstraHeader.LeiosAnnouncement(); announces {
		kind |= KindAnnouncesEB
	}
	if certified, present := dijkstraHeader.LeiosCertified(); present &&
		certified {
		kind |= KindCertifiesEB
	}
	return kind
}
