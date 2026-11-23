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

package database

// ConsumedUtxoPruneFloorSyncKey names the sync_state row in which a v0.94.2
// database recorded the slot up to which it had deleted spent utxo rows. The
// revert that precedes the move of those rows into the live UTxO set, and a
// truncate of a database that still holds the row, do not go below it. The
// row is deleted once the move has finished.
const ConsumedUtxoPruneFloorSyncKey = "consumed_utxo_prune_floor"
