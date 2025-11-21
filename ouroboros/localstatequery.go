// Copyright 2025 Blink Labs Software
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
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

func (o *Ouroboros) localstatequeryServerConnOpts() []olocalstatequery.LocalStateQueryOptionFunc {
	return []olocalstatequery.LocalStateQueryOptionFunc{
		olocalstatequery.WithAcquireFunc(o.localstatequeryServerAcquire),
		olocalstatequery.WithQueryFunc(o.localstatequeryServerQuery),
		olocalstatequery.WithReleaseFunc(o.localstatequeryServerRelease),
	}
}

func (o *Ouroboros) localstatequeryServerAcquire(
	ctx olocalstatequery.CallbackContext,
	acquireTarget olocalstatequery.AcquireTarget,
	reAcquire bool,
) error {
	// TODO: create "view" from ledger state (#382)
	return nil
}

func (o *Ouroboros) localstatequeryServerQuery(
	ctx olocalstatequery.CallbackContext,
	query olocalstatequery.QueryWrapper,
) (any, error) {
	return o.LedgerState.Query(query.Query)
}

func (o *Ouroboros) localstatequeryServerRelease(
	ctx olocalstatequery.CallbackContext,
) error {
	// TODO: release "view" from ledger state (#382)
	return nil
}
