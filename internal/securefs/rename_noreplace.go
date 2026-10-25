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

// Package securefs provides filesystem operations whose security properties
// are not available from the portable os API.
package securefs

import (
	"errors"
	"os"
)

// ErrRenameNoReplaceUnsupported reports that the running kernel cannot
// provide an atomic rename that refuses an existing destination.
var ErrRenameNoReplaceUnsupported = errors.New(
	"atomic no-replace rename is unsupported",
)

// RenameNoReplace atomically renames one direct child of root to another and
// fails if the destination already exists.
func RenameNoReplace(root *os.Root, oldName, newName string) error {
	return renameNoReplace(root, oldName, newName)
}
