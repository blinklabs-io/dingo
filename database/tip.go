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

package database

import ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"

func GetTip(db *Database) (ochainsync.Tip, error) {
	return db.GetTip(nil)
}

func (d *Database) GetTip(txn *Txn) (ochainsync.Tip, error) {
	tmpTip := ochainsync.Tip{}
	if txn == nil {
		tip, err := d.metadata.GetTip(nil)
		if err != nil {
			return tmpTip, err
		}
		tmpTip = tip
	} else {
		tip, err := d.metadata.GetTip(txn.Metadata())
		if err != nil {
			return tmpTip, err
		}
		tmpTip = tip
	}
	return tmpTip, nil
}

// SetTip saves the current tip
func SetTip(db *Database, tip ochainsync.Tip) error {
	return db.SetTip(tip, nil)
}

func (d *Database) SetTip(tip ochainsync.Tip, txn *Txn) error {
	if txn == nil {
		return d.metadata.SetTip(tip, nil)
	} else {
		return d.metadata.SetTip(tip, txn.Metadata())
	}
}
