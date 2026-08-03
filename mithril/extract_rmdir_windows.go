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

//go:build windows

package mithril

import (
	"fmt"
	"os"
)

// removeEmptyExtractDir removes name from root only if it is an empty
// directory, and reports an error for anything else.
//
// Windows has no handle-relative removal to address the entry through, so the
// type is established first and Root.Remove is asked to remove it. That leaves
// the window Unix closes here: a writer who replaces the directory with a file
// between the two has their file unlinked. Reaching it means winning a race
// against a publication that is already refusing to proceed, and Windows is
// not a deployment target for the node, so the check is where the guarantee
// rests rather than the removal.
func removeEmptyExtractDir(root *os.Root, name string) error {
	info, err := root.Lstat(name)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return &os.PathError{Op: "rmdir", Path: name, Err: os.ErrInvalid}
	}
	if err := root.Remove(name); err != nil {
		return fmt.Errorf("removing extraction destination: %w", err)
	}
	return nil
}
