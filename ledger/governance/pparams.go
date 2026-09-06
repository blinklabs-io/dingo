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

package governance

import (
	"fmt"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// conwayGovernanceProtocolParameters returns the Conway fields consumed by
// ratification while retaining the caller's original concrete parameter value
// for enactment. A nil result identifies the intentional pre-Conway no-op.
// Every pre-Conway parameter type implements the shared protocol-parameter
// accessors, so capability detection must remain an explicit concrete-type
// check rather than a broad interface assertion.
func conwayGovernanceProtocolParameters(
	pparams lcommon.ProtocolParameters,
) (*conway.ConwayProtocolParameters, error) {
	switch p := pparams.(type) {
	case *conway.ConwayProtocolParameters:
		if p == nil {
			return nil, fmt.Errorf(
				"nil governance protocol parameters of type %T",
				pparams,
			)
		}
		return p, nil
	case *dijkstra.DijkstraProtocolParameters:
		if p == nil {
			return nil, fmt.Errorf(
				"nil governance protocol parameters of type %T",
				pparams,
			)
		}
		return &p.ConwayProtocolParameters, nil
	default:
		return nil, nil
	}
}
