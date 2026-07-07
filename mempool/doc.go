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

// Package mempool implements Dingo's transaction pool. It accepts
// transactions from local clients (N2C) and relayed txsubmission
// traffic (N2N), validates them against the current ledger state,
// and holds them until they are included in a block, evicted, or expired.
//
// Mempool is the top-level type. It validates every submitted
// transaction through the ledger package — UTxO resolution, fees,
// ExUnit budgets, validity interval, size, and the full UTxO validation
// rules enforced by the ledger package — before admitting it. Transactions
// outside their validity interval relative to the current tip are rejected
// at submission time rather than held until expiry.
//
// # Eviction and watermarks
//
// The pool uses a two-level watermark scheme:
//
//   - EvictionWatermark  — above this fill level, lowest-priority txs
//     are evicted to make room for higher-priority ones; a value of 0
//     disables eviction entirely
//   - RejectionWatermark — above this fill level, new submissions are
//     rejected outright
//
// When eviction is enabled, it is driven by transaction priority (fee density),
// not arrival order. With the default configuration, Dingo instead applies
// backpressure at full mempool capacity.
//
// # Events
//
//   - MempoolAddTxEventType    — a tx was admitted to the pool
//   - MempoolRemoveTxEventType — a tx was removed (included, evicted, or expired)
package mempool
