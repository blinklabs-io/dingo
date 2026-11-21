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

// Package sdk is the module Dingo plugins build against.
//
// It is a separate Go module, github.com/blinklabs-io/dingo/sdk, so a plugin
// can be built without depending on the node. Two import rules keep it that
// way, and the node repository's import-boundary check enforces both:
//
//   - no package in this module imports github.com/blinklabs-io/dingo or any
//     of its packages, internal ones included;
//   - only packages under sdk/cardano import gouroboros, so every other
//     contract stays chain-neutral.
//
// The module is released with sdk/vX.Y.Z tags. Every contract it defines is
// versioned v0 and experimental, and the module stays at major version 0,
// until a 1.0 is declared; until then a minor release may change a contract.
package sdk
