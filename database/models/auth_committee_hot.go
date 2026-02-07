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
)

var ErrCommitteeMemberNotFound = errors.New("committee member not found")

type AuthCommitteeHot struct {
	ColdCredential []byte `gorm:"index;size:28"`
	HostCredential []byte `gorm:"index;size:28"`
	ID             uint   `gorm:"primarykey"`
	CertificateID  uint   `gorm:"index"`
	AddedSlot      uint64 `gorm:"index"`
}

func (AuthCommitteeHot) TableName() string {
	return "auth_committee_hot"
}
