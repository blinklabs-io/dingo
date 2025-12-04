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

// Certificate maps transaction certificates to their specialized table records.
// Provides unified indexing across all certificate types without requiring joins.
//
// All certificate types now have dedicated specialized models. The CertificateID field
// references the ID of the specific certificate record based on CertType.
type Certificate struct {
	BlockHash     []byte `gorm:"index"`
	ID            uint   `gorm:"primaryKey"`
	TransactionID uint   `gorm:"index;uniqueIndex:uniq_tx_cert;constraint:OnDelete:CASCADE"`
	CertificateID uint   `gorm:"index"` // Polymorphic FK to certificate table based on CertType. Not DB-enforced.
	Slot          uint64 `gorm:"index"`
	CertIndex     uint   `gorm:"column:cert_index;uniqueIndex:uniq_tx_cert"`
	CertType      uint   `gorm:"index"`
}

func (Certificate) TableName() string {
	return "certs"
}

func (c Certificate) Type() uint {
	return c.CertType
}
