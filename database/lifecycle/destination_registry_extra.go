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

//go:build dingo_extra_plugins

package lifecycle

// RegisterBuiltinDestinations registers every cloud destination scheme
// compiled into this build (S3, GCS) on registry. Composition code
// (node/CLI startup) calls this once, right after NewDestinationRegistry,
// so the set of available schemes is explicit at the call site instead of
// depending on which files happened to be linked in.
func RegisterBuiltinDestinations(registry *DestinationRegistry) {
	RegisterS3(registry)
	RegisterGCS(registry)
}
