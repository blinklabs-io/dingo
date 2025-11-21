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

// Script represents a script entry in the witness set
// Type corresponds to ScriptRefType constants from gouroboros/ledger/common:
// 0=NativeScript (ScriptRefTypeNativeScript)
// 1=PlutusV1 (ScriptRefTypePlutusV1)
// 2=PlutusV2 (ScriptRefTypePlutusV2)
// 3=PlutusV3 (ScriptRefTypePlutusV3)
type Script struct {
	ID            uint   `gorm:"primaryKey"`
	TransactionID uint   `gorm:"index"`
	Type          uint8  `gorm:"index"` // Script type
	ScriptData    []byte `gorm:"type:bytea"`
	Transaction   *Transaction
}

func (Script) TableName() string {
	return "script"
}
