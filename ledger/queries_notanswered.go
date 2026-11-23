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

import (
	"errors"
	"fmt"
)

// ErrQueryNotAnswered identifies a query Dingo decodes but does not answer
// because a faithful reply needs state it does not hold. A wrong answer would
// be taken for the node's, so the query fails instead. LocalStateQuery has no
// failure reply to a query, so the failure ends the client's session; the
// error names the query and what is missing so the connection log explains it.
var ErrQueryNotAnswered = errors.New("query not answered")

// queryNotAnswered builds the ErrQueryNotAnswered error for a query, naming
// it as the protocol does and the state it needs.
func queryNotAnswered(name, missing string) error {
	return fmt.Errorf("%w: %s needs %s", ErrQueryNotAnswered, name, missing)
}

const (
	// missingRatificationOutcome is what GetGovState reports for the ratify
	// state while a governance action is pending.
	missingRatificationOutcome = "the completed ratification outcome of the " +
		"epoch boundary, which is not stored, while a governance action is " +
		"pending"
	// missingDRepDistributionSnapshot is what GetGovState reports when the
	// boundary that opened the epoch recorded no DRep distribution.
	missingDRepDistributionSnapshot = "the DRep stake distribution of the " +
		"boundary that opened the epoch, which only a boundary this node " +
		"processed records"
	// missingBoundaryPoolSnapshot is what GetGovState reports while the
	// boundary's mark pool snapshot is still being written.
	missingBoundaryPoolSnapshot = "the mark pool stake snapshot of the " +
		"boundary that opened the epoch, which is still being written"
	// missingEpochState is the whole ledger state the debug queries return.
	missingEpochState = "the whole epoch state in the ledger's encoding, " +
		"which is not held in that form"
)
