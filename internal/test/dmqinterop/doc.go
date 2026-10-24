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

// Package dmqinterop holds the tests that run Dingo's DMQ stack against a real
// Intersect dmq-node. They need Docker and a network, so they are built only
// with the dingo_dmq_interop tag; run.sh sets everything up and runs them. See
// README.md.
package dmqinterop
