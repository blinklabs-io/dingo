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

// Package chain defines the chain-neutral contracts between the parts of a
// node: the ledger, consensus, network and (optionally) mempool roles, and the
// values they exchange.
//
// Nothing here knows a particular chain. A block, header or transaction
// crosses a role boundary as a Payload: its encoded bytes, a small decoded
// view, and, when both ends are built into the same binary, the chain's own
// decoded value. A built-in implementation reads that value directly; an
// external one reads the bytes, and only then is the encoding paid for.
//
// The role interfaces follow seams the Cardano implementation already has
// (the ledger handed to the chain manager, the forger's leader and block
// validation interfaces, the mempool transaction validator, chain selection).
// The Cardano implementation of every role is in the sdk/cardano package.
// Every contract is experimental, like the rest of the sdk module.
package chain
