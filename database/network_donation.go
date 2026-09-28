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

import "fmt"

// DeleteNetworkDonationsAfterSlot removes donation records added after the
// given slot. This is used during chain rollbacks, alongside
// DeleteNetworkStateAfterSlot.
func (d *Database) DeleteNetworkDonationsAfterSlot(
	slot uint64,
	txn *Txn,
) error {
	return d.withMetadataWriteTxn(txn, func(txn *Txn) error {
		if err := d.metadata.DeleteNetworkDonationsAfterSlot(
			slot,
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf(
				"failed to delete network donations after slot %d: %w",
				slot,
				err,
			)
		}
		return nil
	})
}
