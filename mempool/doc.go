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
// and holds them until they are included in a block or evicted.
//
// Pool is the backend-neutral node contract. FIFO is the default backend and
// orders transactions by successful admission: independent submissions retain
// arrival order, and a duplicate refresh does not move a transaction. Mempool
// remains the concrete queue embedded by FIFO for source compatibility.
//
// The FIFO backend validates every submitted
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
//   - EvictionWatermark  — above this fill level, the oldest transactions
//     are evicted in successful-admission order to make room for new ones
//   - RejectionWatermark — above this fill level, new submissions are
//     rejected outright
//
// Eviction is oldest-first in successful-admission order. It is not driven by
// fee density or another priority score.
//
// # Events
//
//   - MempoolAddTxEventType    — a tx was admitted to the pool
//   - MempoolRemoveTxEventType — a tx was removed (included, evicted, or expired)
package mempool
