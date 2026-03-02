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

import "time"

// BackfillCheckpoint tracks progress of automatic historical
// metadata backfill. After a Mithril bootstrap in API storage mode,
// stored blocks are replayed to populate transaction metadata. The
// checkpoint enables resumable progress if the process is interrupted.
type BackfillCheckpoint struct {
	ID         uint   `gorm:"primarykey"`
	Phase      string `gorm:"uniqueIndex;size:64;not null"` // "metadata"
	LastSlot   uint64 // Last successfully processed slot
	TotalSlots uint64 // Total slots to process (for progress)
	StartedAt  time.Time
	UpdatedAt  time.Time
	Completed  bool
}

func (BackfillCheckpoint) TableName() string {
	return "backfill_checkpoint"
}
