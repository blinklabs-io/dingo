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

// Script represents the content of a script, indexed by its hash
// This avoids storing duplicate script data when the same script appears
// in multiple transactions
type Script struct {
	Hash        []byte `gorm:"index;unique;size:64"`
	Content     []byte
	ID          uint `gorm:"primaryKey"`
	CreatedSlot uint64
	Type        uint8 `gorm:"index"`
}

func (Script) TableName() string {
	return "script"
}
