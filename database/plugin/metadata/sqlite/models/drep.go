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

type Drep struct {
	ID         uint   `gorm:"primarykey"`
	Credential []byte `gorm:"index"`
	AnchorUrl  string
	AnchorHash []byte
	AddedSlot  uint64
}

func (d *Drep) TableName() string {
	return "drep"
}

type RegistrationDrep struct {
	ID             uint   `gorm:"primarykey"`
	DrepCredential []byte `gorm:"index"`
	AddedSlot      uint64
	DepositAmount  uint64
	AnchorUrl      string
	AnchorHash     []byte
}

func (RegistrationDrep) TableName() string {
	return "registration_drep"
}
