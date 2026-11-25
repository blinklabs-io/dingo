// Copyright 2026 Blink Labs Software
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

// AppliedTransaction locates a transaction on the chain: the block it was
// applied in, its position there, its era and its phase-2 validity. A Leios
// closure transaction is located by the ranking block that certified its
// endorser block.
type AppliedTransaction struct {
	Hash       []byte
	BlockHash  []byte
	Slot       uint64
	BlockIndex uint32
	Type       uint8
	Valid      bool
}
