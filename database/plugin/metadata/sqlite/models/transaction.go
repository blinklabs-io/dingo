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

type Transaction struct {
	ID         uint   `gorm:"primaryKey"`
	Hash       []byte `gorm:"uniqueIndex"`
	Type       string
	BlockHash  []byte `gorm:"index"`
	BlockIndex uint32
	Inputs     []byte
	Outputs    []byte
}

func (Transaction) TableName() string {
	return "transaction"
}
