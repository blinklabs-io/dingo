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

// ScriptType represents the type of script
type ScriptType uint8

const (
	ScriptTypeNative   ScriptType = 0
	ScriptTypePlutusV1 ScriptType = 1
	ScriptTypePlutusV2 ScriptType = 2
	ScriptTypePlutusV3 ScriptType = 3
)

// Script represents a script entry in the witness set
type Script struct {
	ID            uint   `gorm:"primaryKey"`
	TransactionID uint   `gorm:"index"`
	Type          uint8  `gorm:"index"` // ScriptType (0=Native, 1=PlutusV1, 2=PlutusV2, 3=PlutusV3)
	ScriptData    []byte `gorm:"type:bytea"`
	Transaction   *Transaction
}

func (Script) TableName() string {
	return "script"
}
