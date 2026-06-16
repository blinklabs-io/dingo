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

package models

// NetworkDonation records the total Conway treasury donation contributed by a
// block, tagged with the epoch the block belongs to. Donations accumulate
// during an epoch and are moved into the treasury at the next epoch boundary
// (see NetworkState). Rows are keyed by slot so a chain rollback can drop them
// with DeleteNetworkDonationsAfterSlot, mirroring NetworkState.
//
// Amount is a plain uint64 (stored as an integer, like Slot/Epoch) rather than
// types.Uint64: donation totals are bounded well below 2^63, so an integer
// column lets SUM aggregate directly across SQLite/Postgres/MySQL without the
// per-backend CAST that the text-encoded types.Uint64 would require.
type NetworkDonation struct {
	ID     uint   `gorm:"primarykey"`
	Slot   uint64 `gorm:"uniqueIndex;not null"`
	Epoch  uint64 `gorm:"index;not null"`
	Amount uint64 `gorm:"not null"`
}

// TableName returns the table name for NetworkDonation.
func (NetworkDonation) TableName() string {
	return "network_donation"
}
