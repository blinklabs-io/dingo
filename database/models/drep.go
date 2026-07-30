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
	AnchorURL     string
	Credential    []byte
	AnchorHash    []byte
	ID            uint
	AddedSlot     uint64
	CredentialTag uint8
	// Last activity epoch (vote, register, update).
	LastActivityEpoch uint64
	// Epoch when DRep expires (activity + inactivity).
	ExpiryEpoch uint64
	Active      bool
}

// DrepListRow is a Drep row extended with the credential's first
// on-chain appearance slot, as returned by GetDreps for
// registration-order listings.
type DrepListRow struct {
	AnchorURL         string
	Credential        []byte
	AnchorHash        []byte
	ID                uint
	AddedSlot         uint64
	CredentialTag     uint8
	LastActivityEpoch uint64
	ExpiryEpoch       uint64
	Active            bool
	FirstSeenSlot     uint64
	// LastRegistrationSlot is the added_slot of the most recent
	// registration certificate, 0 when no cert history exists.
	LastRegistrationSlot uint64
}

func (d *Drep) TableName() string {
	return "drep"
}

type DeregistrationDrep struct {
	DrepCredential []byte
	CertificateID  uint
	ID             uint
	CredentialTag  uint8
	AddedSlot      uint64
	DepositAmount  types.Uint64
}

func (DeregistrationDrep) TableName() string {
	return "deregistration_drep"
}

type RegistrationDrep struct {
	AnchorURL      string
	DrepCredential []byte
	AnchorHash     []byte
	CertificateID  uint
	ID             uint
	CredentialTag  uint8
	AddedSlot      uint64
	DepositAmount  types.Uint64
}

func (RegistrationDrep) TableName() string {
	return "registration_drep"
}

type UpdateDrep struct {
	AnchorURL     string
	Credential    []byte
	AnchorHash    []byte
	CertificateID uint
	ID            uint
	CredentialTag uint8
	AddedSlot     uint64
}

func (UpdateDrep) TableName() string {
	return "update_drep"
}
