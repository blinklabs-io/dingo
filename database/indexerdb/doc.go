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

// Package indexerdb defines the IndexerDB: chain history and the address and
// asset projections that serve the APIs. It is not consulted by validation.
//
// The node opens it as its own *database.Database, which the APIs read
// directly. It follows the ledger on its own schedule, and Store records its
// tip, committed in the same transaction as the rows that tip covers, and its
// readiness. The ledger holds no handle to it.
package indexerdb
