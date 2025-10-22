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
	AnchorUrl     string
	Credential    []byte `gorm:"uniqueIndex"`
	AnchorHash    []byte
	CertificateID uint `gorm:"index"`
	ID            uint `gorm:"primarykey"`
	AddedSlot     uint64
	Active        bool `gorm:"default:true"`
}

func (d *Drep) TableName() string {
	return "drep"
}

type DeregistrationDrep struct {
	DrepCredential []byte `gorm:"index"`
	CertificateID  uint   `gorm:"index"`
	ID             uint   `gorm:"primarykey"`
	AddedSlot      uint64
	DepositAmount  uint64
}

func (DeregistrationDrep) TableName() string {
	return "deregistration_drep"
}

type RegistrationDrep struct {
	AnchorUrl      string
	DrepCredential []byte `gorm:"index"`
	AnchorHash     []byte
	CertificateID  uint `gorm:"index"`
	ID             uint `gorm:"primarykey"`
	AddedSlot      uint64
	DepositAmount  uint64
}

func (RegistrationDrep) TableName() string {
	return "registration_drep"
}

type UpdateDrep struct {
	AnchorUrl     string
	Credential    []byte `gorm:"index"`
	AnchorHash    []byte
	CertificateID uint `gorm:"index"`
	ID            uint `gorm:"primarykey"`
	AddedSlot     uint64
}

func (UpdateDrep) TableName() string {
	return "update_drep"
}
