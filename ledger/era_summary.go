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

package ledger

import (
	"errors"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// It returns the system start timestamp from the Shelley genesis.
func (ls *LedgerState) SystemStart() (time.Time, error) {
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return time.Time{}, errors.New("unable to get shelley genesis")
	}
	return shelleyGenesis.SystemStart, nil
}

// It returns all epochs stored in the database.
func (ls *LedgerState) GetEpochs() ([]models.Epoch, error) {
	ls.RLock()
	defer ls.RUnlock()
	if len(ls.epochCache) == 0 {
		return nil, nil
	}
	epochs := make([]models.Epoch, len(ls.epochCache))
	copy(epochs, ls.epochCache)
	return epochs, nil
}

// It returns protocol parameters for the specific epoch.
func (ls *LedgerState) GetPParamsForEpoch(
	epoch uint64,
	era eras.EraDesc,
) (lcommon.ProtocolParameters, error) {
	if era.DecodePParamsFunc == nil {
		return nil, nil
	}
	return ls.db.GetPParams(epoch, era.DecodePParamsFunc, nil)
}
