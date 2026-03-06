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

// TransactionMetadataLabel stores per-label transaction metadata values
// for efficient label-based querying.
type TransactionMetadataLabel struct {
	ID            uint   `gorm:"primaryKey"`
	TransactionID uint   `gorm:"index;uniqueIndex:idx_tx_metadata_label_tx_label"`
	Label         uint64 `gorm:"index;uniqueIndex:idx_tx_metadata_label_tx_label"`
	Slot          uint64 `gorm:"index"`
	CborValue     []byte
	JsonValue     string
}

func (TransactionMetadataLabel) TableName() string {
	return "transaction_metadata_label"
}
