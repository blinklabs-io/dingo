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

package epochengine

import (
	"fmt"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
)

// DelegatorInactivityActivationEpoch reads the durable CIP-0163 activation
// marker and reports the activation epoch A, whether activation has occurred,
// and any read/parse error. An empty marker means activation has not run yet
// (activated == false, epoch 0). It is the read-back counterpart of the marker
// write in activateDelegatorInactivityIfNeeded and is consumed by the reward
// round's activation-floor clamp.
func (e *Engine) DelegatorInactivityActivationEpoch(
	txn ledgerdb.Txn,
) (epoch uint64, activated bool, err error) {
	marker, err := e.ledgerDB().GetSyncState(
		database.DelegatorInactivityActivatedSyncKey,
		txn,
	)
	if err != nil {
		return 0, false, err
	}
	if marker == "" {
		return 0, false, nil
	}
	epoch, err = strconv.ParseUint(marker, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf(
			"parse delegator-inactivity activation marker %q: %w",
			marker,
			err,
		)
	}
	return epoch, true, nil
}
