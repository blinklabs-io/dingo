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

import "github.com/blinklabs-io/dingo/database/types"

// NetworkState stores treasury and reserves balances at a given slot.
type NetworkState struct {
	ID       uint
	Treasury types.Uint64
	Reserves types.Uint64
	Slot     uint64
}

// SyncState stores ephemeral key-value pairs used during
// one-time sync/load operations. Cleaned up after completion.
type SyncState struct {
	Key   string
	Value string
}
