// Copyright 2025 Blink Labs Software
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

package cardano

import "embed"

// EmbeddedConfigPreviewNetworkFS contains the embedded Cardano configuration files
// for the preview network. This includes config.json and all genesis files required
// for preview network operation when no external config files are available.
//
//go:embed preview
var EmbeddedConfigPreviewNetworkFS embed.FS
