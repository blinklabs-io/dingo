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
// Service is the backend-neutral node contract. FIFO is the default backend
// and orders transactions by successful admission: independent submissions
// retain arrival order, and a duplicate refresh does not move a transaction.
// DAG is the alternative backend. It indexes pending producers and spenders
// plus parent/child edges, and exposes parents before descendants with FIFO
// tie-breaking between ready transactions. DAG never watermark-evicts; network
// intake waits for admission headroom instead. Mempool remains the shared
// engine embedded by both backends for source compatibility.
//
// Both backends validate every submitted
// transaction through the ledger package — UTxO resolution, fees,
// ExUnit budgets, validity interval, size, and the full UTxO validation
// rules enforced by the ledger package — before admitting it. Transactions
// outside their validity interval relative to the current tip are rejected
// at submission time rather than held until expiry.
//
// # Eviction and watermarks
//
// FIFO uses a two-level watermark scheme:
//
//   - EvictionWatermark  — above this fill level, the oldest transactions
//     are evicted in successful-admission order to make room for new ones
//   - RejectionWatermark — above this fill level, new submissions are
//     rejected outright
//
// FIFO eviction is oldest-first in successful-admission order. It is not driven
// by fee density or another priority score. DAG ignores EvictionWatermark,
// preserves admitted transactions, and exposes admission headroom so network
// intake pauses before the rejection watermark. Direct submissions above that
// watermark receive MempoolFullError.
//
// # Events
//
//   - MempoolAddTxEventType    — a tx was admitted to the pool
//   - MempoolRemoveTxEventType — a tx was removed (included, evicted, or expired)
package mempool
