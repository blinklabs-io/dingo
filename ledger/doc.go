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

// Package ledger owns Dingo's consensus-critical state: the UTxO set,
// protocol parameters, stake distribution, certificates, governance
// actions, epoch/nonce bookkeeping, and Plutus script execution. It
// is the authority on whether a block or transaction is valid under
// the active era's rules.
//
// LedgerState is the top-level type. It is created by the node at
// startup, wired to the database and event bus, and consulted by the
// mempool, the ouroboros handlers, and the block forger.
//
// # Rollback and state restoration
//
// When the primary chain rolls back to an earlier point, LedgerState
// replays state restoration against the metadata store. State
// restoration logic is implemented by the selected metadata backend
// (RestoreAccountStateAtSlot, RestorePoolStateAtSlot,
// RestoreDrepStateAtSlot, DeleteCertificatesAfterSlot). This package
// orchestrates the calls in the correct order and emits
// TransactionEvent with Rollback=true for each undone transaction so
// downstream consumers can undo their own derived state.
//
// # Epoch nonces (Ouroboros Praos)
//
// Epoch nonce computation differs between TPraos (Shelley–Alonzo) and
// Praos (Babbage–Conway). The per-era formulas live in ledger/eras/,
// and CalculateEtaVConway / CalculateEtaVBabbage apply the Praos
// "N"-prefixed VRF domain separation before accumulating into the
// rolling nonce. The stability window for nonce freezing is 4k/f
// (Praos), not 3k/f (TPraos) — this matters on devnets where 4k/f can
// exceed the epoch length.
//
// # Sub-packages
//
//   - ledger/forging — block production (BlockForger, BlockBuilder)
//   - ledger/leader  — VRF leader election for block production
//   - ledger/snapshot — stake snapshot capture at epoch boundaries
//   - ledger/eras     — per-era validation rule implementations
package ledger
