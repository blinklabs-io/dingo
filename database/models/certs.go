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

import "github.com/blinklabs-io/dingo/database/types"

type Certificate struct {
	Cbor       []byte `gorm:"-"`
	Pool       []byte
	Credential []byte
	Drep       []byte
	CertType   uint `gorm:"index"`
	Epoch      uint64
	Amount     types.Uint64
	ID         uint `gorm:"primaryKey"`
}

func (Certificate) TableName() string {
	return "certs"
}

func (c Certificate) Type() uint {
	return c.CertType
}
