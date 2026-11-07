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

package ouroboros

import (
	"errors"

	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// errDijkstraHeaderWithoutLeios rejects a Dijkstra header whose body has
// only the ten Praos fields where the twelve-field Leios header is required.
var errDijkstraHeaderWithoutLeios = errors.New(
	"dijkstra header body has 10 fields; the 12-field Leios header is required",
)

// gouroboros decodes a Dijkstra header body with either the ten Praos fields
// (cardano-node 11.2.0 on DijkstraNet) or those plus leios_certified and
// leios_announcement. Musashi runs the Leios prototype, whose Dijkstra headers
// always carry the two Leios fields, so on Musashi dingo rejects the
// ten-field form.
func (o *Ouroboros) requiresLeiosDijkstraHeader() bool {
	return o.config.NetworkMagic == ouroboros.NetworkCardanoMusashi.NetworkMagic
}

// requireLeiosHeaderExtension returns errDijkstraHeaderWithoutLeios when
// header is a Dijkstra header decoded from a ten-field body.
func requireLeiosHeaderExtension(header gledger.BlockHeader) error {
	h, ok := header.(*gdijkstra.DijkstraBlockHeader)
	if ok && h != nil && len(h.LeiosHeaderExtension) == 0 {
		return errDijkstraHeaderWithoutLeios
	}
	return nil
}
