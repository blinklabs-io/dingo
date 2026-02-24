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

import (
	"errors"

	"github.com/blinklabs-io/dingo/database/types"
)

var (
	ErrDrepNotFound = errors.New("drep not found")
	// ErrDrepActivityNotUpdated is returned when an
	// UpdateDRepActivity call matches no DRep record.
	ErrDrepActivityNotUpdated = errors.New(
		"drep activity not updated: no matching record",
	)
)

type Drep struct {
	AnchorURL  string `gorm:"column:anchor_url;size:128"`
	Credential []byte `gorm:"uniqueIndex;size:28"`
	AnchorHash []byte
	ID         uint   `gorm:"primarykey"`
	AddedSlot  uint64 `gorm:"index"`
	// Last activity epoch (vote, register, update).
	LastActivityEpoch uint64 `gorm:"index;default:0"`
	// Epoch when DRep expires (activity + inactivity).
	ExpiryEpoch uint64 `gorm:"index;default:0"`
	Active      bool   `gorm:"default:true"`
}

func (d *Drep) TableName() string {
	return "drep"
}

type DeregistrationDrep struct {
	DrepCredential []byte `gorm:"index;size:28"`
	CertificateID  uint   `gorm:"index"`
	ID             uint   `gorm:"primarykey"`
	AddedSlot      uint64 `gorm:"index"`
	DepositAmount  types.Uint64
}

func (DeregistrationDrep) TableName() string {
	return "deregistration_drep"
}

type RegistrationDrep struct {
	AnchorURL      string `gorm:"column:anchor_url;size:128"`
	DrepCredential []byte `gorm:"uniqueIndex:idx_drep_reg_cred_slot;size:28"`
	AnchorHash     []byte
	CertificateID  uint   `gorm:"index"`
	ID             uint   `gorm:"primarykey"`
	AddedSlot      uint64 `gorm:"uniqueIndex:idx_drep_reg_cred_slot"`
	DepositAmount  types.Uint64
}

func (RegistrationDrep) TableName() string {
	return "registration_drep"
}

type UpdateDrep struct {
	AnchorURL     string `gorm:"column:anchor_url;size:128"`
	Credential    []byte `gorm:"index;size:28"`
	AnchorHash    []byte
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
}

func (UpdateDrep) TableName() string {
	return "update_drep"
}
