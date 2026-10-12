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

// Package peras is the home for Dingo's Ouroboros Peras (CIP-0140) consensus
// support. Peras layers stake-weighted voting on top of Praos: block
// production is unchanged, voting committees elected per round cast votes that
// aggregate into certificates, and a certificate boosts the block it names in
// chain selection. The package currently holds no logic; see README.md.
package peras
