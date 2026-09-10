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

package database

import (
	"errors"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ErrScriptNotFound signals that no indexed script has the requested hash.
var ErrScriptNotFound = errors.New("script not found")

// GetScript retrieves an indexed script by its hash.
func (d *Database) GetScript(
	hash []byte,
	txn *Txn,
) (*models.Script, error) {
	if len(hash) == 0 {
		return nil, ErrScriptNotFound
	}
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	scriptHash := lcommon.NewBlake2b224(hash)
	ret, err := d.metadata.GetScript(scriptHash, txn.Metadata())
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, ErrScriptNotFound
	}
	return ret, nil
}
