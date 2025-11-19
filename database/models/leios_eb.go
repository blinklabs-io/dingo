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
	"github.com/blinklabs-io/dingo/database/types"
)

// LeiosEb represents a Leios endorser block certificate
type LeiosEb struct {
	ElectionID              []byte       `gorm:"uniqueIndex:uniq_leios_eb"`
	EndorserBlockHash       []byte       `gorm:"uniqueIndex:uniq_leios_eb"`
	PersistentVotersData    []byte       // JSON-encoded persistent voters
	NonpersistentVotersData []byte       // JSON-encoded non-persistent voters
	AggregateEligSigData    []byte       // JSON-encoded aggregate eligibility signature
	AggregateVoteSigData    []byte       // JSON-encoded aggregate vote signature
	ID                      uint         `gorm:"primaryKey"`
	CertificateID           uint         `gorm:"uniqueIndex:uniq_leios_eb_cert"`
	AddedSlot               types.Uint64 `gorm:"index"`
}

// TableName returns the database table name for the LeiosEb model.
func (LeiosEb) TableName() string {
	return "leios_eb"
}
