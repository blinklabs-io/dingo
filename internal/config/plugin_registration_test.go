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

package config

// Blank-imported so the default blob/metadata plugins are registered in
// this package's test binary, matching cmd/dingo's real import graph
// (via the database package). Without these, database/plugin's global
// registry is empty here and LoadConfig's plugin config validation
// can't exercise real plugin option/name checks.
import (
	_ "github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	_ "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
)
