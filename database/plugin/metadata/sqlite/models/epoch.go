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

package models

type Epoch struct {
	ID uint `gorm:"primarykey"`
	// NOTE: we would normally use this as the primary key, but GORM doesn't
	// like a primary key value of 0
	EpochId       uint64 `gorm:"uniqueIndex"`
	StartSlot     uint64
	Nonce         []byte
	EraId         uint
	SlotLength    uint
	LengthInSlots uint
}

func (Epoch) TableName() string {
	return "epoch"
}
