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
	"fmt"
	"log/slog"

	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
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
	AnchorURL     string `gorm:"column:anchor_url;size:128"`
	Credential    []byte `gorm:"uniqueIndex:idx_drep_credential,priority:2;size:28"`
	AnchorHash    []byte
	ID            uint   `gorm:"primarykey"`
	AddedSlot     uint64 `gorm:"index"`
	CredentialTag uint8  `gorm:"uniqueIndex:idx_drep_credential,priority:1;not null;default:0"`
	// Last activity epoch (vote, register, update).
	LastActivityEpoch uint64 `gorm:"index;default:0"`
	// Epoch when DRep expires (activity + inactivity).
	ExpiryEpoch uint64 `gorm:"index;default:0"`
	Active      bool   `gorm:"default:true;index:idx_drep_active"`
}

// MigrateDrepCredentialTagIndex drops the legacy hash-only DRep uniqueness
// constraint before AutoMigrate installs the tag-aware composite unique index.
func MigrateDrepCredentialTagIndex(db *gorm.DB, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if !db.Migrator().HasTable(&Drep{}) {
		return nil
	}
	if !db.Migrator().HasIndex(&Drep{}, "idx_drep_credential") {
		return nil
	}
	if db.Migrator().HasColumn(&Drep{}, "credential_tag") {
		return nil
	}
	logger.Info(
		"dropping legacy drep credential unique index before tag-aware migration",
	)
	if err := db.Migrator().DropIndex(&Drep{}, "idx_drep_credential"); err != nil {
		return fmt.Errorf("drop drep credential index: %w", err)
	}
	return nil
}

func (d *Drep) TableName() string {
	return "drep"
}

type DeregistrationDrep struct {
	DrepCredential []byte `gorm:"index:idx_dereg_drep_credential,priority:2;size:28"`
	CertificateID  uint   `gorm:"index"`
	ID             uint   `gorm:"primarykey"`
	CredentialTag  uint8  `gorm:"index:idx_dereg_drep_credential,priority:1;not null;default:0"`
	AddedSlot      uint64 `gorm:"index"`
	DepositAmount  types.Uint64
}

func (DeregistrationDrep) TableName() string {
	return "deregistration_drep"
}

type RegistrationDrep struct {
	AnchorURL      string `gorm:"column:anchor_url;size:128"`
	DrepCredential []byte `gorm:"uniqueIndex:idx_drep_reg_cred_slot,priority:2;size:28"`
	AnchorHash     []byte
	CertificateID  uint   `gorm:"index"`
	ID             uint   `gorm:"primarykey"`
	CredentialTag  uint8  `gorm:"uniqueIndex:idx_drep_reg_cred_slot,priority:1;not null;default:0"`
	AddedSlot      uint64 `gorm:"uniqueIndex:idx_drep_reg_cred_slot,priority:3"`
	DepositAmount  types.Uint64
}

func (RegistrationDrep) TableName() string {
	return "registration_drep"
}

type UpdateDrep struct {
	AnchorURL     string `gorm:"column:anchor_url;size:128"`
	Credential    []byte `gorm:"index:idx_update_drep_credential,priority:2;size:28"`
	AnchorHash    []byte
	CertificateID uint   `gorm:"index"`
	ID            uint   `gorm:"primarykey"`
	CredentialTag uint8  `gorm:"index:idx_update_drep_credential,priority:1;not null;default:0"`
	AddedSlot     uint64 `gorm:"index"`
}

func (UpdateDrep) TableName() string {
	return "update_drep"
}
