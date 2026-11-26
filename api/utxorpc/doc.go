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

// Package utxorpc implements Dingo's UTxO RPC server, serving the
// utxorpc.v1alpha and utxorpc.v1beta gRPC APIs defined by the UTxO RPC spec.
//
// The Utxorpc type is a gRPC server that translates incoming requests
// into queries against the ledger and mempool packages and streams
// results back to clients. It is started when
// DINGO_PLUGINS_API_UTXORPC_CONFIG_PORT is non-zero, in either storage mode.
// An "api" node answers query requests from its IndexerDB; a "core" node
// answers UTxO and inline datum reads from the ledger's live UTxO set and
// refuses transaction reads (ErrQueryNeedsAPIStorage).
//
// # Predicate evaluation
//
// SearchUtxos uses UtxoPredicate filters over live UTxOs. Exact addresses are
// compared by complete output address bytes, while payment/delegation parts
// are credential-scoped; a nil SearchUtxos predicate scans all addresses.
// TxPredicate evaluation for
// transaction streams uses composite operators (not / all_of / any_of)
// around leaf predicates (address, policy, certificate, consumes,
// produces, …). That path is stricter: evalTxPredicateOutcome returns
// predNoMatch for a nil converted TxPredicate node.
//
// # Authentication
//
// This server does not authenticate clients. Authentication, rate
// limiting, and TLS termination are expected to be handled by a
// reverse proxy or API gateway in front of the node. Do not add auth
// middleware here — keep the node itself transport-neutral.
package utxorpc
