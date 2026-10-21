// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lifecycle

import "errors"

// ErrSnapshotTrustRequired marks an unkeyed local snapshot whose filesystem
// permissions allow another principal to replace or alter it.
var ErrSnapshotTrustRequired = errors.New(
	"snapshot trust key is required for this local snapshot location",
)

var ErrSnapshotDirectoryReplaced = errors.New(
	"snapshot directory was replaced while it was being written",
)
