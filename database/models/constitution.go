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

// Constitution represents the on-chain constitution document reference.
// The constitution is established via governance action and contains a URL
// and hash pointing to the full document, plus an optional guardrails script.
type Constitution struct {
	ID          uint    `gorm:"primarykey"`
	AnchorUrl   string  `gorm:"size:128;not null"`
	AnchorHash  []byte  `gorm:"size:32;not null"`
	PolicyHash  []byte  `gorm:"size:28"`
	AddedSlot   uint64  `gorm:"uniqueIndex;not null"`
	DeletedSlot *uint64 `gorm:"index"`
}

// TableName returns the table name
func (Constitution) TableName() string {
	return "constitution"
}
