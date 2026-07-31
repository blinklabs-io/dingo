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

import (
	"github.com/blinklabs-io/dingo/database/recovery"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// GetTip returns the current tip as represented by the protocol
func (d *Database) GetTip(txn *Txn) (ochainsync.Tip, error) {
	if txn == nil {
		return d.metadata.GetTip(nil)
	}
	return d.metadata.GetTip(txn.Metadata())
}

// SetTip saves the current tip
//
// Moving the tip is what makes a combined transaction interesting to crash
// recovery, so the target point is recorded as the transaction's intent here.
// A transaction that already described itself — a rollback, say — keeps its own
// description; see Txn.SetRecoveryIntent.
func (d *Database) SetTip(tip ochainsync.Tip, txn *Txn) error {
	if txn == nil {
		return d.metadata.SetTip(tip, nil)
	}
	txn.SetRecoveryIntent(recovery.Intent{
		Kind:        recovery.IntentBlockAdd,
		Slot:        tip.Point.Slot,
		Hash:        tip.Point.Hash,
		BlockNumber: tip.BlockNumber,
	})
	return d.metadata.SetTip(tip, txn.Metadata())
}
