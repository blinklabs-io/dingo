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
	"bytes"
	"encoding/json"
	"fmt"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gshelley "github.com/blinklabs-io/gouroboros/ledger/shelley"
)

func initialPools(
	genesis *gshelley.ShelleyGenesis,
) (map[string]lcommon.PoolRegistrationCertificate, map[string][]lcommon.Address, error) {
	pools, delegations, err := genesis.InitialPools()
	if err != nil {
		return nil, nil, err
	}
	if genesis.ExtraConfig == nil {
		return pools, delegations, nil
	}
	for poolID, extraPool := range genesis.ExtraConfig.StakePools.Data {
		rawKey, ok := extraPool.Unknown["blsKey"]
		if !ok || len(bytes.TrimSpace(rawKey)) == 0 {
			continue
		}
		var alias *lcommon.LeiosKey
		if err := json.Unmarshal(rawKey, &alias); err != nil {
			return nil, nil, fmt.Errorf(
				"decode genesis pool %s blsKey: %w",
				poolID,
				err,
			)
		}
		if alias == nil {
			continue
		}
		certificate, ok := pools[poolID]
		if !ok {
			return nil, nil, fmt.Errorf(
				"genesis pool %s has blsKey but no registration",
				poolID,
			)
		}
		if certificate.LeiosKey != nil &&
			(!bytes.Equal(certificate.LeiosKey.PublicKey, alias.PublicKey) ||
				!bytes.Equal(
					certificate.LeiosKey.PossessionProof,
					alias.PossessionProof,
				)) {
			return nil, nil, fmt.Errorf(
				"genesis pool %s declares different blsKey and leiosKey values",
				poolID,
			)
		}
		certificate.LeiosKey = alias
		pools[poolID] = certificate
	}
	return pools, delegations, nil
}
