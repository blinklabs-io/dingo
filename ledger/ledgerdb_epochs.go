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
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// setEpochSource serves a rollback's epoch and era switch reads, and the
// database's own epoch reads, from the set. It reads the committed set with no transaction: a
// rollback runs before the set applies its own.
type setEpochSource struct {
	ls *LedgerState
}

func (d setEpochSource) Epochs() ([]models.Epoch, error) {
	r, _ := d.ls.epochStateReader(nil)
	if r == nil {
		return nil, ledgerStateUnserved(nodesettings.FamilyEpochs)
	}
	view, err := d.ls.epochStateView(r, nil)
	if err != nil {
		return nil, err
	}
	return sortedEpochs(view, nil), nil
}

func (d setEpochSource) EraSwitch() (*database.MidEpochEraSwitch, error) {
	r, _ := d.ls.epochStateReader(nil)
	if r == nil {
		return nil, ledgerStateUnserved(nodesettings.FamilyEraSwitch)
	}
	_, sw, err := d.ls.epochStateViewWithSwitch(r, nil)
	return sw, err
}

// installEpochSource gives the database the set's epochs and era switch for
// the reads and rollbacks it runs.
func (ls *LedgerState) installEpochSource() {
	if ls.db != nil {
		ls.db.SetEpochSource(setEpochSource{ls})
	}
}
